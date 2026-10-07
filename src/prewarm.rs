//! Restricted, offline cache prewarmer.
//!
//! Maintenance runs this before the test starts so the first real client does
//! not pay for the upstream download. It reuses the *exact* same machinery as
//! the online request path:
//!
//! * targets are reconstructed through the proxy's path allow-list
//!   ([`crate::proxy::resolve_target`]) — a manifest entry names a proxy
//!   path plus a `Range`, never an upstream URL; traversal segments, NUL,
//!   backslashes and absolute-form URLs are rejected per entry;
//! * bytes can only enter the cache through the strong-ETag 206 proof
//!   ([`crate::proxy::store_proven_206`]) or a strong 200 commit
//!   ([`crate::proxy::commit_200`]); weak/missing validators fail the item
//!   and never touch a blob;
//! * every upstream response is spooled to a temp file and length-checked
//!   before commit ([`crate::proxy::capture_upstream`]); an early close is a
//!   failed item, never a shorter "complete" segment;
//! * segments merge only inside one proven version and only the missing
//!   sub-intervals are fetched, so re-running the manifest neither
//!   re-downloads nor re-writes the bytes already covered.
//!
//! Each manifest entry gets its own report line (hit / downloaded / failed /
//! rejected) with actual byte counts; one bad entry never makes the batch
//! look successful.

use std::sync::Arc;

use axum::http::Uri;

use crate::error::{ProxyError, Result};
use crate::etag::ETag;
use crate::proxy::{
    capture_upstream, cold_range, commit_200, lock_for, revalidate_cached, resolve_target,
    store_proven_206, target_key, with_db, ColdOutcome, CommitOutcome, ProveOutcome, ProxyState,
    Revalidation, MAX_PLAN_ITERATIONS,
};
use crate::range::{self, RangeSpec};

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

/// One parsed manifest entry. Only origin-form proxy paths (plus an optional
/// query) and exactly one `bytes=` interval are accepted — an upstream URL in
/// the manifest can never name the host to fetch.
#[derive(Debug, Clone)]
pub struct ManifestEntry {
    /// 1-based line number in the manifest file.
    pub line_no: usize,
    pub path: String,
    pub range: String,
}

/// Parse a manifest. Blank lines and whole-line `#` comments are skipped;
/// every other line must be `<proxy-path> <bytes=start-end>`. A rejected line
/// is returned as an `Err(reason)` in place, so the runner can report it
/// instead of aborting the remaining entries.
pub fn parse_manifest(text: &str) -> Vec<std::result::Result<ManifestEntry, (usize, String)>> {
    let mut out = Vec::new();
    for (idx, raw_line) in text.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (path, rest) = match line.split_once(char::is_whitespace) {
            Some(v) => v,
            None => {
                out.push(Err((
                    line_no,
                    "expected '<path> bytes=start-end' on one line".into(),
                )));
                continue;
            }
        };
        let path = path.trim();
        // Only the first whitespace-delimited token after the path is read.
        let range = rest.split_whitespace().next().unwrap_or("").trim();

        // The manifest names a proxy-side path only. Absolute-form requests
        // (scheme/host) and scheme-relative forms are refused outright; the
        // raw/decoded traversal checks inside resolve_target still run again
        // afterwards.
        if !path.starts_with('/')
            || path.contains("://")
            || path.starts_with("//")
            || path.contains('\\')
            || path.contains('\0')
        {
            out.push(Err((
                line_no,
                "only origin-form proxy paths starting with '/' are allowed (no upstream URLs)"
                    .into(),
            )));
            continue;
        }
        match range::parse_range(range) {
            Some(RangeSpec::Single(_)) => {}
            Some(RangeSpec::Multiple) => {
                out.push(Err((
                    line_no,
                    "multiple byte intervals per entry are not supported".into(),
                )));
                continue;
            }
            None => {
                out.push(Err((line_no, format!("unparsable Range header: {range:?}"))));
                continue;
            }
        }
        out.push(Ok(ManifestEntry {
            line_no,
            path: path.to_string(),
            range: range.to_string(),
        }));
    }
    out
}

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

/// Outcome of one manifest entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The requested interval was already fully covered; revalidation proved
    /// the cached strong version current (304). Zero upstream body bytes.
    Hit,
    /// Some bytes were missing and were fetched and committed under a strong
    /// validator this run.
    Downloaded,
    /// The entry was refused before any upstream contact (bad path,
    /// traversal, absolute URL, malformed range).
    Rejected,
    /// Upstream contact failed, truncated, lacked a strong validator or
    /// contradicted its own Content-Range. No complete interval was faked.
    Failed,
}

/// Readable, per-entry result.
#[derive(Debug, Clone)]
pub struct ItemReport {
    pub line_no: usize,
    pub path: String,
    pub range: String,
    pub outcome: Outcome,
    /// Bytes actually transferred from the upstream this run (304 = 0).
    pub fetched_bytes: u64,
    /// Bytes of the requested interval present in the cache afterwards.
    /// Equals the interval length on hit/download; on failure it reports
    /// only what genuinely got committed, never the requested length.
    pub covered_bytes: u64,
    /// Length of the requested interval once it could be resolved.
    pub requested_bytes: Option<u64>,
    pub reason: Option<String>,
}

impl ItemReport {
    pub fn ok(&self) -> bool {
        matches!(self.outcome, Outcome::Hit | Outcome::Downloaded)
    }
}

fn rejected(line_no: usize, path: &str, range: &str, reason: impl Into<String>) -> ItemReport {
    ItemReport {
        line_no,
        path: path.to_string(),
        range: range.to_string(),
        outcome: Outcome::Rejected,
        fetched_bytes: 0,
        covered_bytes: 0,
        requested_bytes: None,
        reason: Some(reason.into()),
    }
}

/// Running counters for one item; carried to the failure path so a failed
/// item still reports the bytes really transferred and covered rather than a
/// guessed zero or a faked full coverage.
struct Progress {
    fetched: u64,
    requested: Option<u64>,
    version_id: Option<i64>,
    start: u64,
    end_excl: u64,
}

impl Progress {
    fn new() -> Self {
        Progress {
            fetched: 0,
            requested: None,
            version_id: None,
            start: 0,
            end_excl: 0,
        }
    }

    /// How much of `[start, end_excl)` the committed segments really cover.
    async fn covered_now(&self, state: &ProxyState) -> u64 {
        let (vid, start, end_excl) = match self.version_id {
            Some(vid) => (vid, self.start, self.end_excl),
            None => return 0,
        };
        let segments = with_db(state, move |c| crate::db::covered_segments(c, vid))
            .await
            .unwrap_or_default();
        let mut covered = 0u64;
        for (s, e) in segments {
            let os = s.max(start);
            let oe = e.min(end_excl);
            covered += oe.saturating_sub(os);
        }
        covered
    }
}

/// Bytes that actually crossed the wire for errors that may carry such a
/// count (a lying Content-Length). 304s/stream I/O errors report what we
/// know; a truncated body reports the bytes already spooled.
fn fetched_from_error(err: &ProxyError) -> Option<u64> {
    match err {
        ProxyError::TruncatedBody { received, .. } => Some(*received),
        _ => None,
    }
}

/// Run every entry in order and return one report per entry. Entries are
/// processed sequentially so the output order matches the manifest and a
/// failure in one item never aborts the rest.
pub async fn run_manifest(
    state: &Arc<ProxyState>,
    entries: Vec<std::result::Result<ManifestEntry, (usize, String)>>,
) -> Vec<ItemReport> {
    let mut reports = Vec::with_capacity(entries.len());
    for entry in entries {
        match entry {
            Err((line_no, reason)) => reports.push(rejected(line_no, "", "", reason)),
            Ok(e) => reports.push(prewarm_one(state, &e).await),
        }
    }
    reports
}

// ---------------------------------------------------------------------------
// Per-item driver
// ---------------------------------------------------------------------------

async fn prewarm_one(state: &Arc<ProxyState>, entry: &ManifestEntry) -> ItemReport {
    match drive(state, entry).await {
        Ok(report) => report,
        Err((err, mut progress)) => {
            // Allow-list refusals happen before any upstream contact; report
            // them as rejected rather than as an upstream failure.
            let rejected_by_allow_list = matches!(
                err,
                ProxyError::PathEscape | ProxyError::UpstreamNotAllowed
            );
            // A length-lying response may have transferred bytes we spooled
            // but never committed; record the real number.
            if progress.fetched == 0 {
                if let Some(n) = fetched_from_error(&err) {
                    progress.fetched = n;
                }
            }
            ItemReport {
                line_no: entry.line_no,
                path: entry.path.clone(),
                range: entry.range.clone(),
                outcome: if rejected_by_allow_list {
                    Outcome::Rejected
                } else {
                    Outcome::Failed
                },
                fetched_bytes: progress.fetched,
                covered_bytes: if rejected_by_allow_list {
                    0
                } else {
                    progress.covered_now(state).await
                },
                requested_bytes: progress.requested,
                reason: Some(err.to_string()),
            }
        }
    }
}

async fn drive(
    state: &Arc<ProxyState>,
    entry: &ManifestEntry,
) -> std::result::Result<ItemReport, (ProxyError, Progress)> {
    let mut prog = Progress::new();
    let result = drive_inner(state, entry, &mut prog).await;
    result.map_err(|e| (e, prog))
}

async fn drive_inner(
    state: &Arc<ProxyState>,
    entry: &ManifestEntry,
    prog: &mut Progress,
) -> Result<ItemReport> {
    let path = entry.path.clone();
    let range_hdr = entry.range.clone();

    // Same entry point as an online request: rebuild the target from the
    // configured base URL and run every traversal/allow-list check. The
    // manifest can supply no URL of its own here.
    let uri: Uri = path.parse().map_err(|_| ProxyError::PathEscape)?;
    let target = resolve_target(state, &uri)?;
    let key = target_key(&target);

    // Same per-object serialization as serving traffic.
    let obj_lock = lock_for(state, &key).await;
    let _guard = obj_lock.lock().await;

    let single = match range::parse_range(&range_hdr) {
        Some(RangeSpec::Single(iv)) => iv,
        _ => return Err(ProxyError::State("manifest range rejected late".into())),
    };

    for _ in 0..MAX_PLAN_ITERATIONS {
        let key_for_db = key.clone();
        let latest = with_db(state, move |c| crate::db::latest_strong_version(c, &key_for_db))
            .await?;
        let total = match latest.as_ref().and_then(|v| v.total_length) {
            Some(t) => t,
            // Cold object / unknown length: forward the range exactly as the
            // online cold path does.
            None => match cold_range(state, &target, &key, Some(&range_hdr), None).await? {
                ColdOutcome::Replan { version, fetched } => {
                    prog.fetched += fetched;
                    prog.version_id = Some(version.id);
                    continue;
                }
                ColdOutcome::Reply(resp) => {
                    use axum::http::StatusCode;
                    if resp.status() == StatusCode::RANGE_NOT_SATISFIABLE {
                        return Err(ProxyError::UnsatisfiableRange);
                    }
                    // A strong 200 (upstream ignores Range) commits the whole
                    // representation. Confirm via the DB that a covering
                    // strong version really exists instead of assuming it;
                    // weak/no-validator 200s and weak 206s commit nothing.
                    let key_find = key.clone();
                    let committed =
                        with_db(state, move |c| crate::db::latest_strong_version(c, &key_find))
                            .await?;
                    if let Some(v) = committed {
                        if let Some(total) = v.total_length {
                            if let Ok((s, e_incl)) = range::resolve(single, total) {
                                let vid = v.id;
                                let covered = with_db(state, move |c| {
                                    crate::db::covered_segments(c, vid)
                                })
                                .await?;
                                let end_excl = e_incl + 1;
                                if range::missing_within(s, end_excl, &covered).is_empty() {
                                    prog.fetched += total;
                                    prog.requested = Some(end_excl - s);
                                    prog.version_id = Some(v.id);
                                    prog.start = s;
                                    prog.end_excl = end_excl;
                                    return Ok(ItemReport {
                                        line_no: entry.line_no,
                                        path: entry.path.clone(),
                                        range: entry.range.clone(),
                                        outcome: Outcome::Downloaded,
                                        fetched_bytes: prog.fetched,
                                        covered_bytes: end_excl - s,
                                        requested_bytes: Some(end_excl - s),
                                        reason: None,
                                    });
                                }
                            }
                        }
                    }
                    return Err(ProxyError::Unprovable);
                }
            },
        };
        let ver = latest.expect("total length implies a version row");

        let (start, end_inclusive) =
            range::resolve(single, total).map_err(|_| ProxyError::UnsatisfiableRange)?;
        let end_excl = end_inclusive + 1;
        let requested = end_excl - start;
        prog.requested = Some(requested);
        prog.version_id = Some(ver.id);
        prog.start = start;
        prog.end_excl = end_excl;

        let vid = ver.id;
        let covered =
            with_db(state, move |c| crate::db::covered_segments(c, vid)).await?;
        let gaps = range::missing_within(start, end_excl, &covered);

        if gaps.is_empty() {
            // Whole interval already on disk. Like the online path, disk is
            // not proof: revalidate the strong validator upstream. A 304
            // moves zero bytes; a 200 commits a new version and re-plans.
            return match revalidate_cached(state, &target, &key, &ver).await? {
                Revalidation::Fresh => Ok(ItemReport {
                    line_no: entry.line_no,
                    path: entry.path.clone(),
                    range: entry.range.clone(),
                    // "Hit" means zero upstream body bytes this run.
                    outcome: if prog.fetched == 0 {
                        Outcome::Hit
                    } else {
                        Outcome::Downloaded
                    },
                    fetched_bytes: prog.fetched,
                    covered_bytes: requested,
                    requested_bytes: Some(requested),
                    reason: None,
                }),
                Revalidation::Changed(newver) => {
                    prog.fetched += newver.total_length.unwrap_or(0);
                    prog.version_id = Some(newver.id);
                    continue;
                }
            };
        }

        // Fill each gap with its own exact Range under the current STRONG
        // etag, mirroring the serving strategy gap for gap.
        let mut changed = false;
        for &(gs, ge) in &gaps {
            let range_header = format!("bytes={gs}-{}", ge - 1);
            let ir = ETag {
                weak: false,
                raw_tag: ver.etag_tag.clone(),
            }
            .to_wire();
            let (status, cap) =
                capture_upstream(state, &target, Some(&range_header), Some(&ir)).await?;
            match status {
                reqwest::StatusCode::PARTIAL_CONTENT => {
                    prog.fetched += cap.len;
                    // Truncated bodies never reach this point: capture
                    // enforces Content-Length and store_proven_206 enforces
                    // the exact Content-Range, strong ETag and total length.
                    match store_proven_206(
                        state,
                        &key,
                        Some(&ver),
                        cap,
                        Some((gs, ge)),
                        Some(total),
                    )
                    .await?
                    {
                        ProveOutcome::Stored(_) => {}
                        ProveOutcome::Passthrough(_) => {
                            return Err(ProxyError::Unprovable);
                        }
                    }
                }
                reqwest::StatusCode::OK => {
                    // If-Range failed: the object changed. The 200 is the
                    // new full representation; commit it and re-plan. Bytes
                    // of the old gaps are never spliced into anything.
                    prog.fetched += cap.len;
                    match commit_200(state, &key, cap).await? {
                        CommitOutcome::Strong(newver) => {
                            prog.version_id = Some(newver.id);
                            changed = true;
                            break;
                        }
                        CommitOutcome::Uncached(_) => return Err(ProxyError::Unprovable),
                    }
                }
                reqwest::StatusCode::RANGE_NOT_SATISFIABLE => {
                    return Err(ProxyError::UnsatisfiableRange);
                }
                _ => return Err(ProxyError::TruncatedUpstream),
            }
        }
        if changed {
            continue;
        }

        // Every gap fetch either committed or returned an error. Verify from
        // the committed segments that the interval is now complete; never
        // report success just because the loop finished.
        let vid = ver.id;
        let covered_now =
            with_db(state, move |c| crate::db::covered_segments(c, vid)).await?;
        let still_missing = range::missing_within(start, end_excl, &covered_now);
        if still_missing.is_empty() {
            return Ok(ItemReport {
                line_no: entry.line_no,
                path: entry.path.clone(),
                range: entry.range.clone(),
                outcome: Outcome::Downloaded,
                fetched_bytes: prog.fetched,
                covered_bytes: requested,
                requested_bytes: Some(requested),
                reason: None,
            });
        } else {
            return Err(ProxyError::State(format!(
                "{} byte(s) of the requested interval remain uncovered after commit",
                still_missing.iter().map(|(s, e)| e - s).sum::<u64>()
            )));
        }
    }
    Err(ProxyError::State(
        "upstream version churn; prewarm did not converge".into(),
    ))
}

// ---------------------------------------------------------------------------
// Readable report
// ---------------------------------------------------------------------------

/// Render the per-entry table plus a summary. A trailing summary line gives
/// machine-checkable totals so operators can see exactly what happened.
pub fn render_report(reports: &[ItemReport]) -> String {
    let label = |o: Outcome| match o {
        Outcome::Hit => "hit",
        Outcome::Downloaded => "downloaded",
        Outcome::Rejected => "rejected",
        Outcome::Failed => "failed",
    };
    let mut out = String::new();
    out.push_str(
        "line  outcome      requested  fetched  covered  range              path\n",
    );
    for r in reports {
        let path = if r.path.is_empty() { "-" } else { &r.path };
        let range = if r.range.is_empty() { "-" } else { &r.range };
        out.push_str(&format!(
            "{:>4}  {:<11}  {:>9}  {:>7}  {:>7}  {:<17}  {}\n",
            r.line_no,
            label(r.outcome),
            r.requested_bytes
                .map(|n| n.to_string())
                .unwrap_or_else(|| "-".into()),
            r.fetched_bytes,
            r.covered_bytes,
            range,
            path,
        ));
        if let Some(reason) = &r.reason {
            out.push_str(&format!("        reason: {reason}\n"));
        }
    }
    let hits = reports.iter().filter(|r| r.outcome == Outcome::Hit).count();
    let downloaded = reports
        .iter()
        .filter(|r| r.outcome == Outcome::Downloaded)
        .count();
    let rejected = reports
        .iter()
        .filter(|r| r.outcome == Outcome::Rejected)
        .count();
    let failed = reports
        .iter()
        .filter(|r| r.outcome == Outcome::Failed)
        .count();
    let fetched: u64 = reports.iter().map(|r| r.fetched_bytes).sum();
    let covered: u64 = reports.iter().map(|r| r.covered_bytes).sum();
    out.push_str(&format!(
        "summary entries={} hit={hits} downloaded={downloaded} rejected={rejected} failed={failed} \
         fetched_bytes={fetched} covered_bytes={covered}\n",
        reports.len()
    ));
    out
}

/// True only when every entry hit or downloaded successfully.
pub fn all_ok(reports: &[ItemReport]) -> bool {
    reports.iter().all(ItemReport::ok)
}
