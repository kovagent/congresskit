# congresskit

US congressional stock trades (STOCK Act periodic transaction reports) for Rust. Served from bundled parquet with on-demand fetch and a local cache. No API keys. Offline after the first query.

## Install

```toml
[dependencies]
congresskit = "0.1.0"
```

To track unreleased changes, depend on the repository directly:

```toml
congresskit = { git = "https://github.com/userFRM/congresskit" }
```

## Quick start

```rust,no_run
#[tokio::main]
async fn main() -> congresskit::Result<()> {
    for t in congresskit::trades_for("NVDA").await?.iter().take(5) {
        println!(
            "{} {} ({}) {} {} ${}-{}",
            t.txn_date, t.member_name, t.party, t.txn_type, t.ticker, t.amount_low, t.amount_high
        );
    }
    Ok(())
}
```

## Client pattern

```rust,no_run
use congresskit::Congresskit;

#[tokio::main]
async fn main() -> congresskit::Result<()> {
    let client = Congresskit::new();
    let buys = client.buys("AAPL").await?;
    println!("{} disclosed purchases", buys.len());
    Ok(())
}
```

## Coverage

| Chamber | Source | Status |
|---|---|---|
| House | US House Clerk financial disclosures | Full, fully automated. |
| Senate | US Senate Electronic Financial Disclosure (EFD) | Implemented. The EFD host is bot-protected and returns 403 from datacenter and CI IP ranges, so Senate ingest in CI requires a residential proxy set via the `SENATE_PROXY` secret (provider-agnostic: any `http://`, `https://`, or `socks5://` residential endpoint). With a working proxy the nightly job accepts the EFD terms, pages the periodic-transaction-report list, parses each electronic PTR table, and normalizes to the same schema (`chamber=senate`, `source=senate_efd`). Without a proxy it attempts direct and degrades cleanly (logs a warning and exits the source) when blocked. House needs no proxy. |

Each trade is enriched with `party` and a stable `bioguide_id` joined from the public-domain unitedstates/congress-legislators roster. Members whose filing name and state do not match a roster entry keep their filing name with an empty `party` and `bioguide_id`; the join rate is reported by the backfill, never guessed.

Some older House periodic transaction reports are scanned images rather than e-filed PDFs and carry no extractable text. Those filings are counted and skipped, not invented; the backfill reports the skipped count.

## CLI

```bash
congresskit-cli backfill --from 2014 --to 2024 --chamber both
congresskit-cli nightly-append
congresskit-cli manifest
congresskit-cli query --ticker NVDA
congresskit-cli query --member "Pelosi"
```

## Data

One row per disclosed transaction, partitioned by year as `data/year=YYYY/congress-YYYY.parquet` (zstd), with a SHA-256 per file in `data/manifest.json`. All data is US public record: House trades from the US House Clerk, Senate trades from the US Senate EFD, member party and identity from the public-domain unitedstates/congress-legislators dataset.

Trade performance and returns require joining an external price source and are intentionally out of congresskit's scope. congresskit ships the raw disclosed trades plus member identity, not prices.

## Cache

Year files are fetched on demand from GitHub raw, cached locally under the platform cache directory (override with `CONGRESSKIT_CACHE_DIR`), revalidated with ETags, and verified against the SHA-256 manifest. On a transient network failure the last good cached file is served. The origin and CDN mirror can be overridden with `CONGRESSKIT_BASE_URL` and `CONGRESSKIT_MIRROR_URL`.

## API

Full API reference is on [docs.rs](https://docs.rs/congresskit).

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
