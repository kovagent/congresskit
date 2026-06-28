//! Senate EFD ingest (efdsearch.senate.gov), proxy-aware.
//!
//! The EFD host is bot-protected and returns 403 from datacenter / CI IP
//! ranges. A residential proxy (set via `SENATE_PROXY`, see `cookie_client`)
//! lets the flow run from CI. Without a working proxy this path attempts direct
//! and degrades gracefully: on a block it logs a warning and returns an empty,
//! `blocked = true` result so the backfill continues with House data.
//!
//! Flow:
//! 1. GET `/search/home/` to obtain the `csrftoken` cookie.
//! 2. POST `/search/home/` with `prohibition_agreement=1` to accept terms.
//! 3. POST `/search/report/data/` (DataTables JSON, `X-Requested-With:
//!    XMLHttpRequest`) paging periodic transaction reports for the year.
//! 4. Follow each report to its electronic PTR page (structured HTML table) and
//!    parse the transactions, or count+skip a scanned paper filing.

use anyhow::Result;
use congresskit::{Chamber, Owner, Trade, TxnType};
use serde::Deserialize;

const BASE: &str = "https://efdsearch.senate.gov";
const HOME_URL: &str = "https://efdsearch.senate.gov/search/home/";
const DATA_URL: &str = "https://efdsearch.senate.gov/search/report/data/";
/// EFD report-type code for a periodic transaction report.
const REPORT_TYPE_PTR: &str = "11";
/// DataTables page size.
const PAGE_LEN: usize = 100;

/// Outcome of a Senate ingest attempt for a year.
pub struct SenateYear {
    pub trades: Vec<Trade>,
    /// PTRs that were paper/scanned filings with no electronic table.
    pub skipped_paper: usize,
    /// `true` when the host blocked the request (403) or was unreachable.
    pub blocked: bool,
}

impl SenateYear {
    fn blocked() -> Self {
        SenateYear {
            trades: Vec::new(),
            skipped_paper: 0,
            blocked: true,
        }
    }
}

/// Attempt Senate EFD ingest for `year`. Never errors on a block; returns an
/// empty, `blocked = true` result so the caller can report it honestly.
///
/// `client` must have a cookie store enabled and, in CI, a proxy configured.
pub async fn ingest_year(client: &reqwest::Client, year: i32) -> Result<SenateYear> {
    let Some(csrf) = accept_agreement(client).await else {
        return Ok(SenateYear::blocked());
    };

    let mut out = SenateYear {
        trades: Vec::new(),
        skipped_paper: 0,
        blocked: false,
    };

    // Step 3: page the report list for the year.
    let mut start = 0;
    loop {
        let body = match fetch_report_page(client, &csrf, year, start).await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "senate report-data page failed; stopping paging");
                break;
            }
        };
        let page = match parse_report_list_json(&body) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "senate report-data JSON parse failed; stopping");
                break;
            }
        };
        if page.rows.is_empty() {
            break;
        }

        // Step 4: follow each PTR report to its electronic table.
        for row in &page.rows {
            match fetch_report(client, row).await {
                ReportOutcome::Trades(mut t) => out.trades.append(&mut t),
                ReportOutcome::Paper => out.skipped_paper += 1,
                ReportOutcome::Failed => {}
            }
        }

        start += page.rows.len();
        if start >= page.records_filtered || page.rows.len() < PAGE_LEN {
            break;
        }
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// Agreement flow (steps 1-2)
// ---------------------------------------------------------------------------

/// Run the home-page + prohibition-agreement handshake. Returns the CSRF token
/// for subsequent POSTs, or `None` if blocked/unreachable.
async fn accept_agreement(client: &reqwest::Client) -> Option<String> {
    let home = match client.get(HOME_URL).send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "senate EFD unreachable; skipping senate ingest");
            return None;
        }
    };
    if !home.status().is_success() {
        tracing::warn!(
            status = home.status().as_u16(),
            "senate EFD returned a block at the accept-terms page (expected from datacenter/CI IPs without a proxy); skipping senate ingest"
        );
        return None;
    }
    let body = home.text().await.unwrap_or_default();
    let csrf = extract_csrf(&body)?;

    let accept = client
        .post(HOME_URL)
        .header("Referer", HOME_URL)
        .form(&[
            ("csrfmiddlewaretoken", csrf.as_str()),
            ("prohibition_agreement", "1"),
        ])
        .send()
        .await;
    match accept {
        Ok(r) if r.status().is_success() || r.status().is_redirection() => Some(csrf),
        Ok(r) => {
            tracing::warn!(
                status = r.status().as_u16(),
                "senate EFD rejected the agreement post; skipping"
            );
            None
        }
        Err(e) => {
            tracing::warn!(error = %e, "senate EFD agreement post failed; skipping");
            None
        }
    }
}

async fn fetch_report_page(
    client: &reqwest::Client,
    csrf: &str,
    year: i32,
    start: usize,
) -> Result<String> {
    // DataTables server-side form. The date filter bounds the year; report_types
    // restricts to periodic transaction reports.
    let start_s = start.to_string();
    let len_s = PAGE_LEN.to_string();
    let from = format!("01/01/{year} 00:00:00");
    let to = format!("12/31/{year} 23:59:59");
    let form = [
        ("start", start_s.as_str()),
        ("length", len_s.as_str()),
        ("report_types", REPORT_TYPE_PTR),
        ("filer_types", ""),
        ("submitted_start_date", from.as_str()),
        ("submitted_end_date", to.as_str()),
        ("candidate_state", ""),
        ("senator_state", ""),
        ("office_id", ""),
        ("first_name", ""),
        ("last_name", ""),
        ("csrfmiddlewaretoken", csrf),
    ];
    let resp = client
        .post(DATA_URL)
        .header("X-Requested-With", "XMLHttpRequest")
        .header("Referer", "https://efdsearch.senate.gov/search/")
        .form(&form)
        .send()
        .await?
        .error_for_status()?;
    Ok(resp.text().await?)
}

// ---------------------------------------------------------------------------
// Report-list JSON (step 3) — pure parser, fixture-tested
// ---------------------------------------------------------------------------

/// One report-list entry: filer name, state, link to the report, and its date.
#[derive(Debug, PartialEq)]
pub struct ReportRef {
    pub first: String,
    pub last: String,
    pub state: String,
    /// Absolute URL to the report view page.
    pub url: String,
    pub filing_date: i32,
}

/// A parsed page of the DataTables response.
pub struct ReportPage {
    pub rows: Vec<ReportRef>,
    pub records_filtered: usize,
}

#[derive(Deserialize)]
struct RawDataTables {
    data: Vec<Vec<String>>,
    #[serde(rename = "recordsFiltered", default)]
    records_filtered: usize,
}

/// Parse the EFD `/search/report/data/` DataTables JSON.
///
/// Each `data` row is `[first, last, state_or_office, report_link_html, date]`.
/// The report link HTML holds `<a href="/search/view/ptr/<id>/">…</a>`.
pub fn parse_report_list_json(body: &str) -> Result<ReportPage> {
    let raw: RawDataTables = serde_json::from_str(body)?;
    let mut rows = Vec::new();
    for cells in &raw.data {
        if cells.len() < 5 {
            continue;
        }
        let Some(href) = extract_href(&cells[3]) else {
            continue;
        };
        let url = if href.starts_with("http") {
            href
        } else {
            format!("{BASE}{href}")
        };
        rows.push(ReportRef {
            first: cells[0].trim().to_string(),
            last: cells[1].trim().to_string(),
            state: cells[2].trim().to_string(),
            url,
            filing_date: parse_efd_date(&cells[4]),
        });
    }
    Ok(ReportPage {
        rows,
        records_filtered: raw.records_filtered,
    })
}

// ---------------------------------------------------------------------------
// Per-report fetch + electronic-PTR HTML (step 4)
// ---------------------------------------------------------------------------

enum ReportOutcome {
    Trades(Vec<Trade>),
    /// Paper/scanned filing with no electronic table.
    Paper,
    Failed,
}

async fn fetch_report(client: &reqwest::Client, report: &ReportRef) -> ReportOutcome {
    // Only electronic PTRs have a parseable table; paper filings live under
    // /search/view/paper/ and are scanned images we count and skip.
    if report.url.contains("/paper/") {
        return ReportOutcome::Paper;
    }
    let html = match client.get(&report.url).send().await {
        Ok(r) if r.status().is_success() => match r.text().await {
            Ok(t) => t,
            Err(_) => return ReportOutcome::Failed,
        },
        _ => return ReportOutcome::Failed,
    };
    let trades = parse_efd_ptr_html(&html, report);
    if trades.is_empty() {
        ReportOutcome::Paper
    } else {
        ReportOutcome::Trades(trades)
    }
}

/// Parse an electronic-PTR HTML page's transaction table into [`Trade`] rows.
///
/// The table body has one `<tr>` per transaction with cells, in order:
/// `# | Owner | Ticker | Asset Name | Asset Type | Type | Comment |
/// Transaction Date | Amount`. Type is `Purchase` / `Sale` / `Sale (Partial)` /
/// `Exchange`; Owner is the filer / spouse / child / joint text.
pub fn parse_efd_ptr_html(html: &str, report: &ReportRef) -> Vec<Trade> {
    let member_name = format!("{} {}", report.first, report.last)
        .trim()
        .to_string();
    let mut out = Vec::new();
    for row in table_rows(html) {
        let cells: Vec<String> = row_cells(&row);
        if cells.len() < 9 {
            continue;
        }
        let Some(txn_type) = TxnType::from_code(&normalize_type(&cells[5])) else {
            continue;
        };
        let txn_date = parse_efd_date(&cells[7]);
        if txn_date == 0 {
            continue; // header / non-data row
        }
        let ticker = clean(&cells[2]);
        let asset_description = clean(&cells[3]);
        let (amount_low, amount_high) = parse_amount(&cells[8]);
        out.push(Trade {
            filing_date: report.filing_date,
            doc_id: report_id(&report.url),
            chamber: Chamber::Senate,
            member_name: member_name.clone(),
            party: String::new(),
            bioguide_id: String::new(),
            state: report.state.clone(),
            district: String::new(),
            txn_date,
            notification_date: 0,
            ticker: if ticker == "--" {
                String::new()
            } else {
                ticker
            },
            asset_description: asset_description.clone(),
            asset_type: classify_asset(&cells[4], &asset_description),
            txn_type,
            amount_low,
            amount_high,
            owner: parse_owner(&cells[1]),
            source: "senate_efd".to_string(),
        });
    }
    out
}

/// Map the EFD owner text to an [`Owner`].
fn parse_owner(s: &str) -> Owner {
    let t = clean(s).to_ascii_lowercase();
    if t.contains("spouse") {
        Owner::Spouse
    } else if t.contains("child") || t.contains("dependent") {
        Owner::Child
    } else if t.contains("joint") {
        Owner::Joint
    } else {
        Owner::SelfFiler
    }
}

/// Normalize an EFD type label to the codes [`TxnType::from_code`] accepts.
fn normalize_type(s: &str) -> String {
    let t = clean(s).to_ascii_lowercase();
    if t.contains("partial") {
        "S (partial)".to_string()
    } else if t.contains("purchase") {
        "P".to_string()
    } else if t.contains("exchange") {
        "E".to_string()
    } else if t.contains("sale") || t.contains("sold") {
        "S".to_string()
    } else {
        clean(s)
    }
}

/// Classify the asset by EFD asset-type text, falling back to the description.
fn classify_asset(asset_type_cell: &str, desc: &str) -> String {
    let t = clean(asset_type_cell).to_ascii_lowercase();
    let d = desc.to_ascii_lowercase();
    if t.contains("stock") || d.contains("[st]") {
        "stock"
    } else if t.contains("option") || d.contains("option") {
        "option"
    } else {
        "other"
    }
    .to_string()
}

/// Parse an EFD amount band `"$1,001 - $15,000"` to `(low, high)` integer
/// dollars. A single value yields equal low/high; `(0, 0)` if none.
fn parse_amount(cell: &str) -> (i64, i64) {
    let dollars: Vec<i64> = clean(cell)
        .split('$')
        .skip(1)
        .filter_map(|chunk| {
            let cleaned: String = chunk
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == ',')
                .filter(|c| *c != ',')
                .collect();
            cleaned.parse::<i64>().ok()
        })
        .collect();
    match dollars.as_slice() {
        [] => (0, 0),
        [v] => (*v, *v),
        [lo, hi, ..] => (*lo, *hi),
    }
}

/// The report id (UUID) from a `/search/view/ptr/<id>/` URL, else the URL tail.
fn report_id(url: &str) -> String {
    url.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string()
}

// ---------------------------------------------------------------------------
// Tiny HTML helpers (tag-scan; the EFD table is flat enough to avoid a DOM dep)
// ---------------------------------------------------------------------------

/// Each `<tr>…</tr>` body inside the document (case-insensitive).
fn table_rows(html: &str) -> Vec<String> {
    let lower = html.to_ascii_lowercase();
    let mut rows = Vec::new();
    let mut search = 0;
    while let Some(open_rel) = lower[search..].find("<tr") {
        let open = search + open_rel;
        let Some(gt) = lower[open..].find('>') else {
            break;
        };
        let content_start = open + gt + 1;
        let Some(close_rel) = lower[content_start..].find("</tr>") else {
            break;
        };
        let content_end = content_start + close_rel;
        rows.push(html[content_start..content_end].to_string());
        search = content_end + 5;
    }
    rows
}

/// Each `<td>…</td>` (or `<th>`) inner text in a row, tags stripped.
fn row_cells(row: &str) -> Vec<String> {
    let lower = row.to_ascii_lowercase();
    let mut cells = Vec::new();
    let mut search = 0;
    while let Some(open_rel) = lower[search..].find("<td") {
        let open = search + open_rel;
        let Some(gt) = lower[open..].find('>') else {
            break;
        };
        let content_start = open + gt + 1;
        let Some(close_rel) = lower[content_start..].find("</td>") else {
            break;
        };
        let content_end = content_start + close_rel;
        cells.push(strip_tags(&row[content_start..content_end]));
        search = content_end + 5;
    }
    cells
}

/// Strip HTML tags and collapse whitespace.
fn strip_tags(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    clean(&out)
}

fn clean(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&nbsp;", " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// First `href="…"` value in an HTML fragment.
fn extract_href(fragment: &str) -> Option<String> {
    let key = "href=\"";
    let pos = fragment.find(key)? + key.len();
    let rest = &fragment[pos..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Parse the Django `csrfmiddlewaretoken` value out of the home-page HTML.
fn extract_csrf(html: &str) -> Option<String> {
    let needle = "name=\"csrfmiddlewaretoken\"";
    let pos = html.find(needle)?;
    let after = &html[pos..];
    let val_key = "value=\"";
    let vpos = after.find(val_key)? + val_key.len();
    let rest = &after[vpos..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Parse an EFD date `MM/DD/YYYY` (optionally with a trailing time) to `i32`
/// `YYYYMMDD`; 0 on failure.
fn parse_efd_date(s: &str) -> i32 {
    let date = s.split_whitespace().next().unwrap_or("");
    let parts: Vec<&str> = date.split('/').collect();
    if parts.len() != 3 {
        return 0;
    }
    match (
        parts[0].parse::<i32>(),
        parts[1].parse::<i32>(),
        parts[2].parse::<i32>(),
    ) {
        (Ok(m), Ok(d), Ok(y)) if (1..=12).contains(&m) && (1..=31).contains(&d) && y >= 1900 => {
            y * 10000 + m * 100 + d
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_csrf_token() {
        let html = r#"<input type="hidden" name="csrfmiddlewaretoken" value="abc123XYZ">"#;
        assert_eq!(extract_csrf(html).as_deref(), Some("abc123XYZ"));
        assert_eq!(extract_csrf("<form></form>"), None);
    }

    #[test]
    fn parses_report_list_fixture() {
        let body =
            std::fs::read_to_string("../crates/congresskit/tests/fixtures/senate_report_list.json")
                .unwrap();
        let page = parse_report_list_json(&body).unwrap();
        assert_eq!(page.records_filtered, 2);
        assert_eq!(page.rows.len(), 2);
        let r = &page.rows[0];
        assert_eq!(r.first, "Thomas");
        assert_eq!(r.last, "Carper");
        assert_eq!(r.state, "DE");
        assert_eq!(r.filing_date, 20240115);
        assert!(
            r.url
                .starts_with("https://efdsearch.senate.gov/search/view/ptr/"),
            "got {}",
            r.url
        );
        // The paper filing keeps its /paper/ url so the fetcher counts it skipped.
        assert!(page.rows[1].url.contains("/paper/"));
    }

    #[test]
    fn parses_electronic_ptr_html_fixture() {
        let html = std::fs::read_to_string("../crates/congresskit/tests/fixtures/senate_ptr.html")
            .unwrap();
        let report = ReportRef {
            first: "Thomas".into(),
            last: "Carper".into(),
            state: "DE".into(),
            url: "https://efdsearch.senate.gov/search/view/ptr/abc-123/".into(),
            filing_date: 20240115,
        };
        let trades = parse_efd_ptr_html(&html, &report);
        assert_eq!(trades.len(), 3, "expected 3 transactions");

        let aapl = trades.iter().find(|t| t.ticker == "AAPL").expect("AAPL");
        assert_eq!(aapl.txn_type, TxnType::Purchase);
        assert_eq!(aapl.amount_low, 1001);
        assert_eq!(aapl.amount_high, 15000);
        assert_eq!(aapl.txn_date, 20240103);
        assert_eq!(aapl.owner, Owner::Spouse);
        assert_eq!(aapl.chamber, Chamber::Senate);
        assert_eq!(aapl.source, "senate_efd");
        assert_eq!(aapl.asset_type, "stock");
        assert_eq!(aapl.doc_id, "abc-123");

        let msft = trades.iter().find(|t| t.ticker == "MSFT").expect("MSFT");
        assert_eq!(msft.txn_type, TxnType::Sale);
        assert_eq!(msft.amount_low, 15001);
        assert_eq!(msft.amount_high, 50000);

        let nvda = trades.iter().find(|t| t.ticker == "NVDA").expect("NVDA");
        assert_eq!(nvda.txn_type, TxnType::PartialSale);
    }

    #[test]
    fn amount_band_parses() {
        assert_eq!(parse_amount("$1,001 - $15,000"), (1001, 15000));
        assert_eq!(parse_amount("$50,001 - $100,000"), (50001, 100000));
        assert_eq!(parse_amount("--"), (0, 0));
    }

    #[test]
    fn type_and_owner_normalize() {
        assert_eq!(normalize_type("Purchase"), "P");
        assert_eq!(normalize_type("Sale (Full)"), "S");
        assert_eq!(normalize_type("Sale (Partial)"), "S (partial)");
        assert_eq!(parse_owner("Spouse"), Owner::Spouse);
        assert_eq!(parse_owner("Self"), Owner::SelfFiler);
        assert_eq!(parse_owner("Dependent Child"), Owner::Child);
    }
}
