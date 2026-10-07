//! Offline cache prewarmer for the range cache proxy.
//!
//! Reads a manifest of `<proxy-path> <bytes=start-end>` entries and warms the
//! local cache through the same allow-list, strong-ETag and temp-file commit
//! rules the online proxy uses. The manifest never names an upstream URL:
//! only the configured `UPSTREAM_BASE` is ever contacted.
//!
//! Usage:
//!   UPSTREAM_BASE=http://127.0.0.1:9000/ \
//!   CACHE_DIR=./cache-data \
//!   prewarm ./prewarm.txt
//!
//! Manifest example (blank lines and `#` comments allowed):
//!
//! ```text
//! # path                 range
//! /obj/alpha              bytes=0-999
//! /obj/alpha              bytes=200000-200999
//! ```
//!
//! Exit status is non-zero if any entry was rejected or failed.

use std::process::ExitCode;

use range_cache_proxy::prewarm;
use range_cache_proxy::ProxyConfig;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "warn,range_cache_proxy=info".parse().unwrap()),
        )
        .init();

    let manifest_path = match std::env::args().nth(1) {
        Some(p) => p,
        None => {
            eprintln!("usage: prewarm <manifest-file>");
            return ExitCode::from(2);
        }
    };

    let text = match tokio::fs::read_to_string(&manifest_path).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("cannot read manifest {manifest_path}: {e}");
            return ExitCode::from(2);
        }
    };

    let upstream_base = match std::env::var("UPSTREAM_BASE") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("UPSTREAM_BASE is required (loopback only, must end with '/')");
            return ExitCode::from(2);
        }
    };
    let cache_dir = std::env::var("CACHE_DIR").unwrap_or_else(|_| "./cache-data".into());

    let mut config = ProxyConfig::new(upstream_base, cache_dir);
    if std::env::var("ALLOW_NON_LOOPBACK").as_deref() == Ok("1") {
        config.require_loopback_upstream = false;
    }

    let state = match range_cache_proxy::build_state(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("cache/upstream setup failed: {e:#}");
            return ExitCode::from(2);
        }
    };

    let entries = prewarm::parse_manifest(&text);
    let reports = prewarm::run_manifest(&state, entries).await;
    print!("{}", prewarm::render_report(&reports));

    if prewarm::all_ok(&reports) {
        ExitCode::SUCCESS
    } else {
        // One bad item must never look like a successful batch.
        ExitCode::from(1)
    }
}
