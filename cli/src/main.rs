//! `congresskit-cli` — build, refresh, and query the bundled congressional-trade
//! parquet data.
//!
//! # Commands
//!
//! ```text
//! congresskit-cli backfill [--from 2014] [--to 2024] [--chamber house|senate|both]
//! congresskit-cli nightly-append
//! congresskit-cli manifest
//! congresskit-cli query --ticker NVDA
//! congresskit-cli query --member "Pelosi"
//! ```
//!
//! `backfill` and `nightly-append` ingest STOCK Act periodic transaction reports
//! from the US House Clerk (fully automated) and, best-effort, the US Senate EFD
//! (degrades cleanly when the host blocks the datacenter IP). Each year is
//! written to `data/year=YYYY/congress-YYYY.parquet`, enriched with member party
//! and a stable bioguide id from the public unitedstates/congress-legislators
//! roster.

mod house;
mod roster;
mod senate;

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use congresskit::{read_trades, write_trades, Trade};
use sha2::{Digest, Sha256};

use house::{HouseYear, PtrFiling};
use roster::Roster;

/// Default first backfill year. House e-filed PTRs are reliably text-extractable
/// from 2014 onward.
const DEFAULT_FROM_YEAR: i32 = 2014;

/// Concurrent PTR-PDF downloads. The House Clerk site is tolerant; keep this
/// modest to stay polite.
const PTR_FETCH_CONCURRENCY: usize = 8;

fn user_agent() -> String {
    std::env::var("CONGRESSKIT_USER_AGENT")
        .unwrap_or_else(|_| "congresskit contact@example.com".to_string())
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum ChamberArg {
    House,
    Senate,
    Both,
}

impl ChamberArg {
    fn house(self) -> bool {
        matches!(self, ChamberArg::House | ChamberArg::Both)
    }
    fn senate(self) -> bool {
        matches!(self, ChamberArg::Senate | ChamberArg::Both)
    }
}

#[derive(Parser)]
#[command(name = "congresskit-cli", about = "US congressional stock trades")]
struct Cli {
    /// Data directory (default: `<cwd>/data`).
    #[arg(long, env = "CONGRESSKIT_DATA_DIR", global = true)]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download and rebuild per-year parquet from House Clerk + Senate EFD.
    Backfill {
        /// First year to include (default 2014).
        #[arg(long)]
        from: Option<i32>,
        /// Last year to include (default: current year).
        #[arg(long)]
        to: Option<i32>,
        /// Chamber to ingest.
        #[arg(long, value_enum, default_value_t = ChamberArg::Both)]
        chamber: ChamberArg,
    },
    /// Refresh the current year (nightly update).
    NightlyAppend,
    /// Generate `data/manifest.json` with a SHA-256 per parquet file.
    Manifest,
    /// Read bundled parquet and print matching trades.
    Query {
        /// Ticker (case-insensitive).
        #[arg(long)]
        ticker: Option<String>,
        /// Member name substring or bioguide id.
        #[arg(long)]
        member: Option<String>,
        /// Maximum rows to print.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let data_dir = cli.data_dir.unwrap_or_else(|| PathBuf::from("data"));

    match cli.cmd {
        Command::Backfill { from, to, chamber } => {
            let from = from.unwrap_or(DEFAULT_FROM_YEAR);
            let to = to.unwrap_or_else(current_year);
            backfill(&data_dir, from, to, chamber).await
        }
        Command::NightlyAppend => {
            let year = current_year();
            backfill(&data_dir, year, year, ChamberArg::Both).await
        }
        Command::Manifest => write_manifest(&data_dir),
        Command::Query {
            ticker,
            member,
            limit,
        } => query(&data_dir, ticker, member, limit),
    }
}

// ---------------------------------------------------------------------------
// backfill
// ---------------------------------------------------------------------------

async fn backfill(data_dir: &Path, from: i32, to: i32, chamber: ChamberArg) -> Result<()> {
    let client = http_client()?;
    let cookie_client = cookie_client()?;
    let roster = match Roster::fetch(&client).await {
        Ok(r) => Some(r),
        Err(e) => {
            eprintln!("roster fetch failed ({e}); trades will carry empty party/bioguide_id");
            None
        }
    };

    let mut totals = Totals::default();
    for year in from..=to {
        let mut rows: Vec<Trade> = Vec::new();
        // doc_id -> (last, first) so the roster join has the House filing names.
        let mut filing_names: HashMap<String, (String, String)> = HashMap::new();

        if chamber.house() {
            match ingest_house_year(&client, year).await {
                Ok((house, names)) => {
                    eprintln!(
                        "{year} house: {} trades from {} PTRs ({} scanned-skip, {} fetch-fail)",
                        house.trades.len(),
                        house.ptr_filings,
                        house.skipped_scanned,
                        house.fetch_failed
                    );
                    totals.house_rows += house.trades.len();
                    totals.scanned_skipped += house.skipped_scanned;
                    *totals.house_by_year.entry(year).or_default() += house.trades.len();
                    rows.extend(house.trades);
                    filing_names.extend(names);
                }
                Err(e) => eprintln!("{year} house: ingest failed ({e}), skipping"),
            }
        }

        if chamber.senate() {
            match senate::ingest_year(&cookie_client, year).await {
                Ok(s) if s.blocked => {
                    totals.senate_blocked = true;
                    eprintln!("{year} senate: blocked/unreachable, skipping (see README)");
                }
                Ok(s) => {
                    eprintln!(
                        "{year} senate: {} trades ({} paper-skip)",
                        s.trades.len(),
                        s.skipped_paper
                    );
                    totals.senate_rows += s.trades.len();
                    totals.senate_paper_skipped += s.skipped_paper;
                    *totals.senate_by_year.entry(year).or_default() += s.trades.len();
                    rows.extend(s.trades);
                }
                Err(e) => eprintln!("{year} senate: ingest failed ({e}), skipping"),
            }
        }

        if let Some(r) = &roster {
            let unmatched = r.enrich(&mut rows, &filing_names);
            totals.matched += rows.len() - unmatched;
            totals.unmatched += unmatched;
        }

        write_year(data_dir, year, &rows)?;
    }

    totals.report();
    write_manifest(data_dir)
}

#[derive(Default)]
struct Totals {
    house_rows: usize,
    senate_rows: usize,
    scanned_skipped: usize,
    senate_paper_skipped: usize,
    matched: usize,
    unmatched: usize,
    senate_blocked: bool,
    house_by_year: BTreeMap<i32, usize>,
    senate_by_year: BTreeMap<i32, usize>,
}

impl Totals {
    fn report(&self) {
        eprintln!("---");
        eprintln!("house rows: {}", self.house_rows);
        eprintln!("senate rows: {}", self.senate_rows);
        eprintln!(
            "scanned/unparseable House PDFs skipped: {}",
            self.scanned_skipped
        );
        eprintln!(
            "senate paper/scanned filings skipped: {}",
            self.senate_paper_skipped
        );
        let total = self.matched + self.unmatched;
        let rate = if total > 0 {
            100.0 * self.unmatched as f64 / total as f64
        } else {
            0.0
        };
        eprintln!(
            "roster join: {} matched, {} unmatched ({rate:.1}% unmatched)",
            self.matched, self.unmatched
        );
        if self.senate_blocked {
            eprintln!(
                "senate: blocked from this network (datacenter/CI IP); House-only data committed"
            );
        }
        for (y, n) in &self.house_by_year {
            eprintln!("  house {y}: {n}");
        }
        for (y, n) in &self.senate_by_year {
            eprintln!("  senate {y}: {n}");
        }
    }
}

// ---------------------------------------------------------------------------
// House ingest
// ---------------------------------------------------------------------------

async fn ingest_house_year(
    client: &reqwest::Client,
    year: i32,
) -> Result<(HouseYear, HashMap<String, (String, String)>)> {
    let zip_url =
        format!("https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip");
    let zip_bytes = client
        .get(&zip_url)
        .send()
        .await
        .with_context(|| format!("fetch {zip_url}"))?
        .error_for_status()
        .with_context(|| format!("{zip_url} status"))?
        .bytes()
        .await
        .context("read FD zip body")?;

    let filings = house::parse_index_zip(&zip_bytes, year)?;
    let mut names = HashMap::new();
    for f in &filings {
        names.insert(f.doc_id.clone(), (f.last.clone(), f.first.clone()));
    }

    let mut year_out = HouseYear {
        ptr_filings: filings.len(),
        ..Default::default()
    };

    // Fetch + parse PTR PDFs with bounded concurrency.
    use futures::stream::{self, StreamExt};
    let results: Vec<PtrResult> = stream::iter(filings)
        .map(|filing| fetch_and_parse_ptr(client, year, filing))
        .buffer_unordered(PTR_FETCH_CONCURRENCY)
        .collect()
        .await;

    for r in results {
        match r {
            PtrResult::Trades(mut t) if !t.is_empty() => year_out.trades.append(&mut t),
            PtrResult::Trades(_) | PtrResult::Scanned => year_out.skipped_scanned += 1,
            PtrResult::FetchFailed => year_out.fetch_failed += 1,
        }
    }

    Ok((year_out, names))
}

enum PtrResult {
    Trades(Vec<Trade>),
    /// PDF had no extractable transaction rows (scanned image).
    Scanned,
    FetchFailed,
}

async fn fetch_and_parse_ptr(client: &reqwest::Client, year: i32, filing: PtrFiling) -> PtrResult {
    let url = house::ptr_pdf_url(year, &filing.doc_id);
    let bytes = match client.get(&url).send().await {
        Ok(r) if r.status().is_success() => match r.bytes().await {
            Ok(b) => b,
            Err(_) => return PtrResult::FetchFailed,
        },
        _ => return PtrResult::FetchFailed,
    };
    // pdf-extract can panic on a malformed PDF; isolate it so one bad file does
    // not abort the whole year.
    let text = match std::panic::catch_unwind(|| pdf_extract::extract_text_from_mem(&bytes)) {
        Ok(Ok(t)) => t,
        _ => return PtrResult::Scanned,
    };
    let trades = house::parse_ptr_text(&text, &filing);
    if trades.is_empty() {
        PtrResult::Scanned
    } else {
        PtrResult::Trades(trades)
    }
}

// ---------------------------------------------------------------------------
// HTTP clients
// ---------------------------------------------------------------------------

fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(user_agent())
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .context("build http client")
}

/// A cookie-jar client for the Senate EFD flow, routed through a residential
/// proxy when `SENATE_PROXY` (preferred) or `HTTPS_PROXY` is set. Akamai blocks
/// bare datacenter IPs, so CI needs a proxy; without one this client attempts
/// direct and the caller degrades cleanly on the resulting 403.
fn cookie_client() -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .user_agent(user_agent())
        .cookie_store(true)
        .timeout(std::time::Duration::from_secs(60));
    if let Some(proxy_url) = senate_proxy() {
        builder = builder.proxy(
            reqwest::Proxy::all(&proxy_url)
                .with_context(|| format!("invalid SENATE_PROXY url: {proxy_url}"))?,
        );
        eprintln!("senate: routing through proxy");
    }
    builder.build().context("build cookie http client")
}

/// The configured Senate proxy: `SENATE_PROXY` wins over `HTTPS_PROXY`.
fn senate_proxy() -> Option<String> {
    std::env::var("SENATE_PROXY")
        .or_else(|_| std::env::var("HTTPS_PROXY"))
        .ok()
        .filter(|s| !s.trim().is_empty())
}

// ---------------------------------------------------------------------------
// write per-year parquet
// ---------------------------------------------------------------------------

fn write_year(data_dir: &Path, year: i32, rows: &[Trade]) -> Result<()> {
    if rows.is_empty() {
        eprintln!("{year}: no rows, leaving year file unchanged");
        return Ok(());
    }
    let dir = data_dir.join(format!("year={year}"));
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    let path = dir.join(format!("congress-{year}.parquet"));
    write_trades(&path, rows).with_context(|| format!("write {}", path.display()))?;
    eprintln!("wrote {} ({} rows)", path.display(), rows.len());
    Ok(())
}

// ---------------------------------------------------------------------------
// manifest
// ---------------------------------------------------------------------------

fn write_manifest(data_dir: &Path) -> Result<()> {
    let mut entries: BTreeMap<String, String> = BTreeMap::new();
    for path in find_parquet(data_dir)? {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("parquet filename")?
            .to_string();
        let bytes = std::fs::read(&path)?;
        let mut h = Sha256::new();
        h.update(&bytes);
        let hex: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
        entries.insert(name, format!("sha256:{hex}"));
    }
    let json = serde_json::to_string_pretty(&entries)?;
    let path = data_dir.join("manifest.json");
    std::fs::create_dir_all(data_dir)?;
    std::fs::write(&path, json)?;
    eprintln!("wrote {} ({} files)", path.display(), entries.len());
    Ok(())
}

fn find_parquet(data_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !data_dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(data_dir)? {
        let path = entry?.path();
        if path.is_dir() {
            for sub in std::fs::read_dir(&path)? {
                let p = sub?.path();
                if p.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    out.push(p);
                }
            }
        } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

// ---------------------------------------------------------------------------
// query (reads local parquet)
// ---------------------------------------------------------------------------

fn query(
    data_dir: &Path,
    ticker: Option<String>,
    member: Option<String>,
    limit: usize,
) -> Result<()> {
    let mut rows = Vec::new();
    for path in find_parquet(data_dir)? {
        rows.extend(read_trades(&std::fs::read(&path)?)?);
    }
    if rows.is_empty() {
        bail!(
            "no parquet found under {}; run backfill first",
            data_dir.display()
        );
    }

    if let Some(t) = &ticker {
        rows.retain(|r| r.ticker.eq_ignore_ascii_case(t));
    }
    if let Some(m) = &member {
        let needle = m.to_lowercase();
        rows.retain(|r| {
            r.member_name.to_lowercase().contains(&needle) || r.bioguide_id.eq_ignore_ascii_case(m)
        });
    }
    rows.sort_by_key(|r| std::cmp::Reverse(r.txn_date));

    println!(
        "{:<10} {:<6} {:<22} {:<4} {:<8} {:<8} {:>20}",
        "txn_date", "tick", "member", "cham", "party", "type", "amount"
    );
    for r in rows.iter().take(limit) {
        println!(
            "{:<10} {:<6} {:<22} {:<4} {:<8} {:<8} {:>9}-{:<10}",
            r.txn_date,
            truncate(&r.ticker, 6),
            truncate(&r.member_name, 22),
            r.chamber.as_str().chars().take(4).collect::<String>(),
            truncate(&r.party, 8),
            r.txn_type.as_str(),
            r.amount_low,
            r.amount_high,
        );
    }
    Ok(())
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        s.chars().take(n.saturating_sub(1)).collect::<String>() + "…"
    }
}

// ---------------------------------------------------------------------------
// calendar helper (system clock; current year only)
// ---------------------------------------------------------------------------

fn current_year() -> i32 {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let z = secs / 86_400 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }) as i32
}
