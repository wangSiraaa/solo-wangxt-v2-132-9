//! Local, manifest-driven cache warmer.
//!
//! Usage:
//!   warmup --manifest /path/to/warmup.txt
//!
//! Manifest lines are `PATH bytes=START-END`. The upstream URL is taken only
//! from `UPSTREAM_BASE`; manifest entries cannot select an upstream.

use std::process::ExitCode;
use std::sync::Arc;

use range_cache_proxy::{
    build_app, parse_warmup_manifest, summarize_with_rejections, warm_entries, ProxyConfig,
};

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,range_cache_proxy=debug".parse().unwrap()),
        )
        .init();

    let manifest_path = match parse_args() {
        Ok(path) => path,
        Err(message) => {
            eprintln!("{message}");
            eprintln!("usage: warmup --manifest <local-manifest-file>");
            return ExitCode::from(2);
        }
    };

    let manifest = match std::fs::read_to_string(&manifest_path) {
        Ok(text) => text,
        Err(e) => {
            eprintln!("unable to read manifest {manifest_path}: {e}");
            return ExitCode::from(2);
        }
    };

    let upstream_base = match std::env::var("UPSTREAM_BASE") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("UPSTREAM_BASE is required");
            return ExitCode::from(2);
        }
    };
    let cache_dir = std::env::var("CACHE_DIR").unwrap_or_else(|_| "./cache-data".into());
    let mut config = ProxyConfig::new(upstream_base, cache_dir);
    if std::env::var("ALLOW_NON_LOOPBACK").as_deref() == Ok("1") {
        config.require_loopback_upstream = false;
    }

    let (_app, state) = match build_app(config).await {
        Ok(value) => value,
        Err(e) => {
            eprintln!("failed to initialize cache: {e}");
            return ExitCode::from(2);
        }
    };

    let (entries, rejected) = parse_warmup_manifest(&manifest);
    let executed = warm_entries(Arc::clone(&state), entries).await.items;
    let summary = summarize_with_rejections(executed, rejected);
    print!("{}", summary.to_report());

    if summary.success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn parse_args() -> Result<String, String> {
    let mut args = std::env::args().skip(1);
    let mut manifest = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--manifest" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--manifest requires a file path".to_string())?;
                manifest = Some(value);
            }
            "--help" | "-h" => {
                return Err("local cache warmup".into());
            }
            other => return Err(format!("unexpected argument: {other}")),
        }
    }
    manifest.ok_or_else(|| "--manifest is required".to_string())
}
