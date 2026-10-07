#![cfg(feature = "test-support")]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use common::*;
use range_cache_proxy::prewarm::{self, Outcome};
use range_cache_proxy::support;
use range_cache_proxy::{build_state, ProxyConfig};
use tempfile::TempDir;

/// An upstream + shared cache directory. The prewarm driver runs directly in
/// process against this state (as the CLI would), and the HTTP proxy is then
/// started on the same on-disk cache to prove normal GETs hit warmed bytes.
struct PrewarmEnv {
    client: reqwest::Client,
    proxy: String,
    state: Arc<range_cache_proxy::ProxyState>,
    upstream: String,
    _upstream_base: String,
    cache_dir: TempDir,
}

impl TestEnv for PrewarmEnv {
    fn http_client(&self) -> &reqwest::Client {
        &self.client
    }
    fn upstream_base_no_slash(&self) -> &str {
        &self.upstream
    }
}

async fn spawn_env() -> PrewarmEnv {
    spawn_env_with_upstream(support::spawn().await).await
}

async fn spawn_env_with_upstream(upstream_base: String) -> PrewarmEnv {
    let upstream = upstream_base.trim_end_matches('/').to_string();
    let cache_dir = TempDir::new().unwrap();
    let config = ProxyConfig::new(upstream_base.clone(), cache_dir.path().to_path_buf());
    let state = build_state(config.clone()).await.unwrap();
    let app = range_cache_proxy::build_app(config).await.unwrap().0;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service()).await.unwrap();
    });
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    PrewarmEnv {
        client,
        proxy: format!("http://{addr}"),
        state,
        upstream,
        _upstream_base: upstream_base,
        cache_dir,
    }
}

/// Deterministic bytes for a raw test object (same generator shape as the
/// in-process test upstream).
fn raw_object_bytes(seed: &str, len: usize) -> Vec<u8> {
    support::object_bytes(seed, len)
}

/// A deliberately dishonest HTTP/1.1 upstream speaking raw TCP (hyper on the
/// server side refuses to send a Content-Length that does not match the
/// body, so this behavior can only be exercised below HTTP).
///
/// Serves:
///   /obj/alpha             strong ETag, honors Range, honors If-None-Match
///   /obj/tiny              strong ETag, honors Range
///   /obj/truncated-range   every Range response announces the full interval
///                          but closes after half the bytes
/// One request per connection.
async fn spawn_raw_upstream() -> String {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => continue,
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                let head = String::from_utf8_lossy(&buf[..n]);
                let mut lines = head.lines();
                let request_line = lines.next().unwrap_or("");
                let header = |name: &str| -> Option<String> {
                    head.lines().find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        (k.eq_ignore_ascii_case(name)).then(|| v.trim().to_string())
                    })
                };
                let (_, rest) = match request_line.split_once(' ') {
                    Some(v) => v,
                    None => return,
                };
                let target = rest.split(' ').next().unwrap_or("/");
                let path = target.split('?').next().unwrap_or("/");

                match path {
                    "/obj/alpha" | "/obj/tiny" => {
                        let total = if path == "/obj/alpha" { 300_000 } else { 300 };
                        let seed = if path == "/obj/alpha" {
                            "alpha-v1"
                        } else {
                            "tiny-v1"
                        };
                        let data = raw_object_bytes(seed, total);
                        let etag = format!("\"{seed}\"");
                        let inm = header("if-none-match");
                        if inm.as_deref() == Some(etag.as_str()) {
                            let resp = format!(
                                "HTTP/1.1 304 Not Modified\r\n\
                                 ETag: {etag}\r\nContent-Length: 0\r\n\r\n"
                            );
                            sock_write(&mut sock, resp.as_bytes()).await;
                            return;
                        }
                        if let Some(r) = header("range") {
                            if let Some(iv) = range_header_bounds(&r, total as u64) {
                                let (s, e_incl) = iv;
                                let body = data[s as usize..=e_incl as usize].to_vec();
                                let n = body.len();
                                let resp_head = format!(
                                    "HTTP/1.1 206 Partial Content\r\n\
                                     Content-Type: application/octet-stream\r\n\
                                     ETag: {etag}\r\n\
                                     Content-Range: bytes {s}-{e_incl}/{total}\r\n\
                                     Content-Length: {n}\r\nConnection: close\r\n\r\n"
                                );
                                let mut out = resp_head.into_bytes();
                                out.extend_from_slice(&body);
                                sock_write(&mut sock, &out).await;
                                return;
                            }
                        }
                        let resp_head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\
                             ETag: {etag}\r\nContent-Length: {total}\r\nConnection: close\r\n\r\n"
                        );
                        let mut out = resp_head.into_bytes();
                        out.extend_from_slice(&data);
                        sock_write(&mut sock, &out).await;
                    }
                    "/obj/truncated-range" => {
                        const TOTAL: usize = 20_000;
                        let data = raw_object_bytes("truncated-range-v1", TOTAL);
                        let r = header("range").unwrap_or_else(|| "bytes=0-".into());
                        let (s, e_incl) = range_header_bounds(&r, TOTAL as u64)
                            .unwrap_or((0, TOTAL as u64 - 1));
                        let advertised = e_incl + 1 - s;
                        let cut = s + advertised / 2; // half, then the socket closes
                        let body = data[s as usize..cut as usize].to_vec();
                        let resp_head = format!(
                            "HTTP/1.1 206 Partial Content\r\n\
                             Content-Type: application/octet-stream\r\n\
                             ETag: \"truncated-range-v1\"\r\n\
                             Content-Range: bytes {s}-{e_incl}/{TOTAL}\r\n\
                             Content-Length: {advertised}\r\nConnection: close\r\n\r\n"
                        );
                        let mut out = resp_head.into_bytes();
                        out.extend_from_slice(&body);
                        sock_write(&mut sock, &out).await;
                    }
                    _ => {
                        sock_write(
                            &mut sock,
                            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
                        )
                        .await;
                    }
                }
            });
        }
    });
    format!("http://{addr}/")
}

async fn sock_write(sock: &mut tokio::net::TcpStream, bytes: &[u8]) {
    use tokio::io::AsyncWriteExt;
    let _ = sock.write_all(bytes).await;
    let _ = sock.flush().await;
}

/// Parse a single `bytes=start-end` (or open/suffix) against a known length,
/// returning inclusive resolved bounds.
fn range_header_bounds(header: &str, total: u64) -> Option<(u64, u64)> {
    use range_cache_proxy::range::{parse_range, resolve, RangeSpec};
    match parse_range(header)? {
        RangeSpec::Single(iv) => resolve(iv, total).ok(),
        RangeSpec::Multiple => None,
    }
}

async fn prewarm_manifest(env: &PrewarmEnv, text: &str) -> Vec<prewarm::ItemReport> {
    let entries = prewarm::parse_manifest(text);
    prewarm::run_manifest(&env.state, entries).await
}

async fn get(
    env: &PrewarmEnv,
    path: &str,
    headers: &[(&str, &str)],
) -> reqwest::Response {
    let mut rb = env.client.get(format!("{}{}", env.proxy, path));
    for (k, v) in headers {
        rb = rb.header(*k, *v);
    }
    rb.send().await.unwrap()
}

fn sqlite_conn(env: &PrewarmEnv) -> rusqlite::Connection {
    rusqlite::Connection::open(env.cache_dir.path().join("range-cache.sqlite3")).unwrap()
}

fn segments_for(env: &PrewarmEnv, obj_path: &str) -> Vec<(i64, i64)> {
    let conn = sqlite_conn(env);
    let mut stmt = conn
        .prepare(
            "SELECT s.start, s.end FROM segments s
             JOIN versions v ON v.id = s.version_id
             JOIN objects o ON o.id = v.object_id
             WHERE o.path = ?1 ORDER BY s.start",
        )
        .unwrap();
    stmt.query_map([obj_path], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn version_count_for(env: &PrewarmEnv, obj_path: &str) -> usize {
    let conn = sqlite_conn(env);
    conn.query_row(
        "SELECT COUNT(*) FROM versions v JOIN objects o ON o.id = v.object_id
         WHERE o.path = ?1",
        [obj_path],
        |r| r.get::<_, i64>(0),
    )
    .unwrap() as usize
}

// ---------------------------------------------------------------------------
// 1. Warm two discontinuous intervals; normal GETs afterwards serve the same
//    bytes from cache (digest-verified), with no additional upstream bytes.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_discontinuous_intervals_warmed_and_gets_hit() {
    let env = spawn_env().await;
    let total = 300_000usize;
    let expected = support::object_bytes("alpha-v1", total);
    reset_stats(&env).await;

    let manifest = "\
# comment line is ignored
/obj/alpha bytes=1000-1999
/obj/alpha bytes=200000-200999
";
    let reports = prewarm_manifest(&env, manifest).await;
    assert_eq!(reports.len(), 2);
    for r in &reports {
        assert_eq!(r.outcome, Outcome::Downloaded, "{r:?}");
        assert_eq!(r.covered_bytes, 1000);
        assert_eq!(r.requested_bytes, Some(1000));
        assert_eq!(r.fetched_bytes, 1000, "exact gap fetch transfers exactly the gap");
    }

    let st = stats_for(&env, "alpha").await;
    assert_eq!(st["partial_206"], 2);
    assert_eq!(st["bytes_sent"], 2000);
    // Segments: exactly the two warmed intervals, not merged into one
    // (there is a real gap between them).
    let segs = segments_for(&env, "/obj/alpha");
    assert_eq!(segs, vec![(1000, 2000), (200000, 201000)]);

    // Normal proxy GETs over both warmed intervals: 206 with identical bytes,
    // and the upstream sees only the cheap revalidations (zero body bytes).
    for (rh, lo, hi) in [
        ("bytes=1000-1999", 1000usize, 2000usize),
        ("bytes=200000-200999", 200_000, 201_000),
    ] {
        let resp = get(&env, "/obj/alpha", &[("Range", rh)]).await;
        assert_eq!(resp.status(), 206, "{rh}");
        let body = resp.bytes().await.unwrap();
        assert_eq!(body.len(), hi - lo);
        assert_eq!(sha256_hex(&body), sha256_hex(&expected[lo..hi]));
    }
    let st = stats_for(&env, "alpha").await;
    assert_eq!(st["bytes_sent"], 2000, "GETs must not pull upstream body bytes");
}

// ---------------------------------------------------------------------------
// 2. Re-running the manifest only hits: no re-download, no rewrite of
//    already covered intervals.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rerun_is_idempotent_zero_new_bytes() {
    let env = spawn_env().await;
    reset_stats(&env).await;
    let manifest = "/obj/alpha bytes=0-4999\n/obj/alpha bytes=200000-200999\n";

    let first = prewarm_manifest(&env, manifest).await;
    assert!(first.iter().all(|r| r.outcome == Outcome::Downloaded));
    assert_eq!(first.iter().map(|r| r.fetched_bytes).sum::<u64>(), 6000);

    let before = stats_for(&env, "alpha").await;
    let second = prewarm_manifest(&env, manifest).await;
    for r in &second {
        assert_eq!(r.outcome, Outcome::Hit, "{r:?}");
        assert_eq!(r.fetched_bytes, 0, "hits transfer zero body bytes");
        assert_eq!(r.covered_bytes, r.requested_bytes.unwrap());
    }
    let after = stats_for(&env, "alpha").await;
    // Each hit still performs the strong revalidation request ...
    assert_eq!(after["requests"] - before["requests"], 2);
    // ... but the upstream sends no body bytes and no 206s.
    assert_eq!(after["bytes_sent"], before["bytes_sent"]);
    assert_eq!(after["partial_206"], before["partial_206"]);

    // Segments unchanged (same merged set, no duplicates).
    assert_eq!(
        segments_for(&env, "/obj/alpha"),
        vec![(0, 5000), (200000, 201000)]
    );
}

// ---------------------------------------------------------------------------
// 3. Path traversal / absolute-URL entries are rejected, legal entries in the
//    same manifest still run; one failure never makes the batch succeed.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn traversal_entries_rejected_legal_entries_continue() {
    let env = spawn_env().await;
    reset_stats(&env).await;

    let manifest = "\
/obj/../../etc/passwd bytes=0-9
/obj/alpha bytes=0-99
/obj/%2e%2e/%2e%2e/secret bytes=0-9
http://example.invalid/x bytes=0-9
/obj/tiny bytes=200-249
";
    let reports = prewarm_manifest(&env, manifest).await;
    assert_eq!(reports.len(), 5);

    assert_eq!(reports[0].outcome, Outcome::Rejected);
    assert_eq!(reports[1].outcome, Outcome::Downloaded);
    assert_eq!(reports[1].fetched_bytes, 100);
    assert_eq!(reports[2].outcome, Outcome::Rejected);
    assert_eq!(reports[3].outcome, Outcome::Rejected);
    assert_eq!(reports[4].outcome, Outcome::Downloaded);
    assert_eq!(reports[4].fetched_bytes, 50);

    assert!(
        !prewarm::all_ok(&reports),
        "a batch containing rejected entries must not be reported as all-ok"
    );
    // Every report line carries a human-readable failure reason.
    for r in reports.iter().filter(|r| r.outcome == Outcome::Rejected) {
        assert!(r.reason.is_some(), "{r:?}");
    }

    // The legal entries really are cached and serve exact bytes.
    let expected = support::object_bytes("alpha-v1", 300_000);
    let resp = get(&env, "/obj/alpha", &[("Range", "bytes=0-99")]).await;
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&expected[0..100]));
    let tiny = support::object_bytes("tiny-v1", 300);
    let resp = get(&env, "/obj/tiny", &[("Range", "bytes=200-249")]).await;
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&tiny[200..250]));

    // Nothing was ever fetched for the escaped paths.
    assert_eq!(segments_for(&env, "/etc/passwd"), vec![] as Vec<_>);
    let st = stats_for(&env, "alpha").await;
    assert_eq!(st["partial_206"], 1);
}

// ---------------------------------------------------------------------------
// 4. Upstream truncation on a range response: only that item fails, no
//    pseudo-complete segment is committed, and the item before it still
//    succeeds.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn truncated_upstream_fails_one_item_without_fake_segment() {
    // hyper refuses to serialize a lying Content-Length server-side, so this
    // scenario needs the raw-HTTP dishonest upstream.
    let env = spawn_env_with_upstream(spawn_raw_upstream().await).await;

    let manifest = "\
/obj/alpha bytes=0-999
/obj/truncated-range bytes=0-9999
/obj/tiny bytes=0-49
";
    let reports = prewarm_manifest(&env, manifest).await;
    assert_eq!(reports.len(), 3);

    assert_eq!(reports[0].outcome, Outcome::Downloaded);
    assert_eq!(reports[0].fetched_bytes, 1000);

    let bad = &reports[1];
    assert_eq!(bad.outcome, Outcome::Failed);
    assert!(bad.reason.is_some());
    // Half the advertised bytes really crossed the wire, but they must not
    // count as a covered interval.
    assert_eq!(bad.fetched_bytes, 5000);
    assert_eq!(bad.covered_bytes, 0, "truncated bytes are never committed");

    // The following legal item still runs.
    assert_eq!(reports[2].outcome, Outcome::Downloaded);
    assert_eq!(reports[2].fetched_bytes, 50);

    // No version/segment exists for the truncated object and no spool file
    // is left behind.
    assert_eq!(version_count_for(&env, "/obj/truncated-range"), 0);
    assert_eq!(segments_for(&env, "/obj/truncated-range"), vec![] as Vec<_>);
    assert!(spool_leftovers(&env).is_empty(), "prewarm must clean up spools");

    assert!(!prewarm::all_ok(&reports));

    // Re-running gives the same one failure deterministically; alpha/tiny
    // entries are now hits, the truncated item still cannot fake success.
    let again = prewarm_manifest(&env, manifest).await;
    assert_eq!(again[0].outcome, Outcome::Hit);
    assert_eq!(again[1].outcome, Outcome::Failed);
    assert_eq!(again[1].covered_bytes, 0);
    assert_eq!(again[2].outcome, Outcome::Hit);
    assert!(spool_leftovers(&env).is_empty());

    // A normal GET to the truncated object's range is itself a 502, never a
    // short 206.
    let resp = get(&env, "/obj/truncated-range", &[("Range", "bytes=0-9999")]).await;
    assert_eq!(resp.status(), 502);
    // Drain the error body and let the request future finish dropping its
    // spool before checking on-disk state.
    let _ = resp.bytes().await;
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        if spool_leftovers(&env).is_empty() {
            break;
        }
    }
    assert!(
        spool_leftovers(&env).is_empty(),
        "temp spool files must be cleaned up: {:?}",
        spool_leftovers(&env)
    );
}

fn spool_leftovers(env: &PrewarmEnv) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(env.cache_dir.path().join("tmp"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("spool-"))
                .unwrap_or(false)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// 5. Weak-ETag objects cannot be prewarmed: the item fails and no segment is
//    recorded, because weak validators prove nothing about exact bytes.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn weak_etag_item_fails_and_caches_nothing() {
    let env = spawn_env().await;
    reset_stats(&env).await;
    let manifest = "/obj/weak bytes=0-999\n";
    let reports = prewarm_manifest(&env, manifest).await;
    assert_eq!(reports[0].outcome, Outcome::Failed);
    assert!(reports[0].reason.is_some());
    assert_eq!(reports[0].covered_bytes, 0);
    assert_eq!(segments_for(&env, "/obj/weak"), vec![] as Vec<_>);
    assert!(!prewarm::all_ok(&reports));
}

// ---------------------------------------------------------------------------
// 6. Partial coverage + idempotence across runs: an entry overlapping an
//    already covered interval fetches only the gap; a later rerun is a hit.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn overlapping_entry_fetches_only_gap_then_hits() {
    let env = spawn_env().await;
    reset_stats(&env).await;

    let r1 = prewarm_manifest(&env, "/obj/alpha bytes=0-999\n").await;
    assert_eq!(r1[0].outcome, Outcome::Downloaded);
    assert_eq!(r1[0].fetched_bytes, 1000);

    // Overlaps 500 cached bytes and extends 1000 new ones: only [1000,2000)
    // must be fetched.
    let r2 = prewarm_manifest(&env, "/obj/alpha bytes=500-1999\n").await;
    assert_eq!(r2[0].outcome, Outcome::Downloaded);
    assert_eq!(r2[0].fetched_bytes, 1000);
    assert_eq!(r2[0].covered_bytes, 1500);

    let r3 = prewarm_manifest(&env, "/obj/alpha bytes=500-1999\n").await;
    assert_eq!(r3[0].outcome, Outcome::Hit);
    assert_eq!(r3[0].fetched_bytes, 0);

    assert_eq!(segments_for(&env, "/obj/alpha"), vec![(0, 2000)]);
}

// ---------------------------------------------------------------------------
// 7. Report rendering is readable and includes per-item byte columns and a
//    summary line.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn report_is_readable_and_honest_about_failures() {
    let env = spawn_env().await;
    let text = "/obj/alpha bytes=0-99\n/obj/%2e%2e/x bytes=0-9\n";
    let reports = prewarm_manifest(&env, text).await;
    let rendered = prewarm::render_report(&reports);
    let mut lines = rendered.lines();
    assert!(lines.next().unwrap().contains("outcome"));
    let body: Vec<&str> = rendered.lines().collect();
    assert!(body.iter().any(|l| l.contains("downloaded") && l.contains("/obj/alpha")));
    assert!(body.iter().any(|l| l.contains("rejected")));
    assert!(body.iter().any(|l| l.contains("reason:")));
    assert!(
        body.last()
            .unwrap()
            .starts_with("summary entries=2 hit=0 downloaded=1 rejected=1 failed=0")
    );
}
