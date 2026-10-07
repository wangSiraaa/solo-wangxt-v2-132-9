#![cfg(feature = "test-support")]

mod common;

use common::*;
use range_cache_proxy::{
    parse_warmup_manifest, summarize_with_rejections, warm_entries, WarmStatus,
};
use std::sync::Arc;

async fn warm_manifest(env: &Env, manifest: &str) -> range_cache_proxy::WarmSummary {
    let (entries, rejected) = parse_warmup_manifest(manifest);
    let executed = warm_entries(Arc::clone(&env.state), entries).await.items;
    summarize_with_rejections(executed, rejected)
}

#[tokio::test]
async fn warms_discontinuous_ranges_and_later_gets_serve_same_bytes() {
    let env = spawn_env().await;
    let total = 300_000usize;
    let expected = support::object_bytes("alpha-v1", total);
    reset_stats(&env).await;

    let summary = warm_manifest(
        &env,
        "/obj/alpha bytes=1000-1999\n/obj/alpha bytes=10000-10999\n",
    )
    .await;
    assert!(summary.success(), "{}", summary.to_report());
    assert_eq!(summary.downloaded, 2);
    assert_eq!(summary.items[0].status, WarmStatus::Downloaded);
    assert_eq!(summary.items[0].requested_bytes, Some(1000));
    assert_eq!(summary.items[0].downloaded_bytes, 1000);
    assert_eq!(summary.items[0].verified_bytes, 1000);
    assert_eq!(summary.items[1].downloaded_bytes, 1000);

    let st = stats_for(&env, "alpha").await;
    assert_eq!(st["partial_206"], 2);
    assert_eq!(st["bytes_sent"], 2000);

    for rh in ["bytes=1000-1999", "bytes=10000-10999"] {
        let resp = env
            .client
            .get(format!("{}/obj/alpha", env.proxy))
            .header("Range", rh)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 206);
        let start: usize = rh
            .strip_prefix("bytes=")
            .unwrap()
            .split('-')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        let body = resp.bytes().await.unwrap();
        assert_eq!(body.len(), 1000);
        assert_eq!(
            sha256_hex(&body),
            sha256_hex(&expected[start..start + 1000])
        );
    }

    // GET validation only sends 304s; no object bytes should have moved.
    let st = stats_for(&env, "alpha").await;
    assert_eq!(st["bytes_sent"], 2000);

    // Idempotent warming: already-covered intervals are not rewritten or
    // downloaded again.
    let second = warm_manifest(
        &env,
        "/obj/alpha bytes=1000-1999\n/obj/alpha bytes=10000-10999\n",
    )
    .await;
    assert!(second.success(), "{}", second.to_report());
    assert_eq!(second.hits, 2);
    assert_eq!(second.downloaded, 0);
    assert_eq!(second.downloaded_bytes, 0);
    let st = stats_for(&env, "alpha").await;
    assert_eq!(st["bytes_sent"], 2000);
}

#[tokio::test]
async fn rejects_path_escape_or_upstream_url_but_continues_valid_items() {
    let env = spawn_env().await;
    reset_stats(&env).await;

    let manifest = "\
/obj/alpha bytes=0-99
/obj/%2e%2e/%2e%2e/etc/passwd bytes=0-99
/obj/tiny bytes=0-9
http://127.0.0.1:1/obj/alpha bytes=0-9
//127.0.0.1:1/obj/alpha bytes=0-9
";
    let summary = warm_manifest(&env, manifest).await;
    assert!(!summary.success());
    assert_eq!(summary.downloaded, 2, "valid items on both sides continue");
    assert_eq!(summary.failed, 3);
    assert_eq!(summary.items.len(), 5);

    assert_eq!(summary.items[0].status, WarmStatus::Downloaded);
    assert_eq!(summary.items[1].status, WarmStatus::Failed);
    assert!(summary.items[1]
        .reason
        .as_deref()
        .unwrap_or_default()
        .contains("escapes"));
    assert_eq!(summary.items[2].status, WarmStatus::Downloaded);
    assert_eq!(summary.items[3].status, WarmStatus::Failed);
    assert!(summary.items[3]
        .reason
        .as_deref()
        .unwrap_or_default()
        .contains("upstream URL"));
    assert_eq!(summary.items[4].status, WarmStatus::Failed);

    let alpha = stats_for(&env, "alpha").await;
    assert_eq!(alpha["partial_206"], 1);
    let tiny = stats_for(&env, "tiny").await;
    assert_eq!(tiny["partial_206"], 1);
}

#[tokio::test]
async fn upstream_truncation_fails_only_its_item_without_commit() {
    let env = spawn_env().await;
    reset_stats(&env).await;

    let summary = warm_manifest(
        &env,
        "/obj/alpha bytes=0-99\n/obj/truncated bytes=0-999\n",
    )
    .await;
    assert!(!summary.success(), "{}", summary.to_report());
    assert_eq!(summary.downloaded, 1);
    assert_eq!(summary.failed, 1);

    let good = &summary.items[0];
    assert_eq!(good.status, WarmStatus::Downloaded);
    assert_eq!(good.verified_bytes, 100);

    let bad = &summary.items[1];
    assert_eq!(bad.status, WarmStatus::Failed);
    assert_eq!(bad.requested_bytes, Some(1000));
    assert_eq!(bad.downloaded_bytes, 500);
    assert_eq!(bad.verified_bytes, 0);
    let reason = bad.reason.as_deref().unwrap_or_default();
    assert!(reason.contains("expected 50000 bytes"));
    assert!(reason.contains("received 500"));

    assert_eq!(count_rows(&env, "/obj/truncated"), 0);
    for entry in std::fs::read_dir(env.cache_dir.path().join("tmp")).unwrap() {
        let name = entry.unwrap().file_name();
        assert!(!name.to_string_lossy().ends_with(".tmp"));
    }

    // The good item remains a normal cache hit with the exact bytes.
    let expected = support::object_bytes("alpha-v1", 300_000);
    let resp = env
        .client
        .get(format!("{}/obj/alpha", env.proxy))
        .header("Range", "bytes=0-99")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&expected[0..100]));

    let resp = env
        .client
        .get(format!("{}/obj/truncated", env.proxy))
        .header("Range", "bytes=0-99")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502);
}

fn count_rows(env: &Env, object_path: &str) -> usize {
    let conn =
        rusqlite::Connection::open(env.cache_dir.path().join("range-cache.sqlite3")).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM versions v
         JOIN objects o ON o.id = v.object_id
         WHERE o.path = ?1",
        [object_path],
        |r| r.get::<_, i64>(0),
    )
    .unwrap() as usize
}
