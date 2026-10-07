//! Restricted local cache warming.
//!
//! A manifest may name only an origin-form path and one byte range. Every
//! item is resolved through the same pinned-upstream construction and path
//! traversal checks as a normal GET. It can never supply an alternate
//! upstream URL, host or redirect target.

use std::sync::Arc;

use axum::http::Uri;

use crate::db::VersionRow;
use crate::error::{ProxyError, Result};
use crate::etag::ETag;
use crate::range::{self, RangeSpec, RawInterval};

use super::{
    capture_upstream, cold_range, commit_200, lock_for, repair_version, resolve_target,
    store_proven_206, target_key, with_db, ColdOutcome, CommitOutcome, ProveOutcome, ProxyState,
};

/// One local manifest entry: a request path and exactly one byte range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmEntry {
    /// Origin-form path with optional query, e.g. `/obj/alpha?x=1`.
    pub path: String,
    /// Raw HTTP range, e.g. `bytes=100-199`.
    pub range: String,
    /// One-based source line number.
    pub line: usize,
}

/// Outcome shown for one manifest item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmStatus {
    /// Every requested byte was already present and verified current.
    Hit,
    /// At least one missing byte was downloaded and committed.
    Downloaded,
    /// The item was rejected or upstream could not provide valid bytes.
    Failed,
}

impl WarmStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            WarmStatus::Hit => "hit",
            WarmStatus::Downloaded => "downloaded",
            WarmStatus::Failed => "failed",
        }
    }
}

/// The independently recorded result of one manifest item.
#[derive(Debug, Clone)]
pub struct WarmItemResult {
    pub line: usize,
    pub path: String,
    pub range: String,
    pub status: WarmStatus,
    /// Bytes the item requested. `None` when the object length was not known
    /// and the requested interval could not be resolved.
    pub requested_bytes: Option<u64>,
    /// Bytes that were already covered before this item downloaded anything.
    pub hit_bytes: u64,
    /// New object bytes read from upstream for this item.
    pub downloaded_bytes: u64,
    /// Bytes proven readable from the final cache blob after processing.
    pub verified_bytes: u64,
    pub reason: Option<String>,
}

/// Per-batch counters plus every item's independent result.
#[derive(Debug, Clone)]
pub struct WarmSummary {
    pub items: Vec<WarmItemResult>,
    pub hits: usize,
    pub downloaded: usize,
    pub failed: usize,
    pub hit_bytes: u64,
    pub downloaded_bytes: u64,
    pub verified_bytes: u64,
}

impl WarmSummary {
    pub fn success(&self) -> bool {
        self.failed == 0
    }

    /// Readable, line-oriented report suitable for CLI output and logs.
    pub fn to_report(&self) -> String {
        let mut out = String::new();
        out.push_str("cache warmup report\n");
        out.push_str(&format!(
            "summary: {} total, {} hit, {} downloaded, {} failed; {} hit bytes, {} downloaded bytes, {} verified bytes\n",
            self.items.len(),
            self.hits,
            self.downloaded,
            self.failed,
            self.hit_bytes,
            self.downloaded_bytes,
            self.verified_bytes,
        ));
        for item in &self.items {
            let reason = item.reason.as_deref().unwrap_or("");
            out.push_str(&format!(
                "line {:>4}: {:<10} requested={:<8} hit={:<8} downloaded={:<8} verified={:<8} {} {} {}\n",
                item.line,
                item.status.as_str(),
                format_option(item.requested_bytes),
                item.hit_bytes,
                item.downloaded_bytes,
                item.verified_bytes,
                item.path,
                item.range,
                reason,
            ));
        }
        out
    }
}

fn format_option(v: Option<u64>) -> String {
    v.map(|n| n.to_string()).unwrap_or_else(|| "-".into())
}

/// Parse the restricted text manifest:
///
/// ```text
/// # blank lines and comments are ignored
/// /obj/alpha bytes=0-999
/// /obj/alpha?x=1 bytes=-100
/// ```
///
/// Absolute URLs are rejected because the upstream origin is startup
/// configuration, never manifest input.
pub fn parse_warmup_manifest(input: &str) -> (Vec<WarmEntry>, Vec<WarmItemResult>) {
    let mut entries = Vec::new();
    let mut rejected = Vec::new();

    for (index, raw_line) in input.lines().enumerate() {
        let line_no = index + 1;
        let line = raw_line.trim_end();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((path, range)) = line.split_once(char::is_whitespace) else {
            rejected.push(rejected_result(
                line_no,
                line,
                "-",
                None,
                "manifest line must contain PATH and bytes=RANGE".into(),
            ));
            continue;
        };
        let range = range.split_whitespace().next().unwrap_or_default();
        let path_display = truncate_for_report(path);

        if !path.starts_with('/')
            || path.contains("://")
            || path.starts_with("//")
            || path.contains('#')
        {
            rejected.push(rejected_result(
                line_no,
                &path_display,
                range,
                None,
                "only an origin-form path without a URL fragment is allowed; upstream URL is not accepted".into(),
            ));
            continue;
        }
        if range.is_empty() {
            rejected.push(rejected_result(
                line_no,
                &path_display,
                range,
                None,
                "missing bytes=RANGE".into(),
            ));
            continue;
        }
        let parsed = match range::parse_range(range) {
            Some(RangeSpec::Single(iv)) => iv,
            Some(RangeSpec::Multiple) => {
                rejected.push(rejected_result(
                    line_no,
                    &path_display,
                    range,
                    None,
                    "multiple ranges are not allowed in warmup manifest".into(),
                ));
                continue;
            }
            None => {
                rejected.push(rejected_result(
                    line_no,
                    &path_display,
                    range,
                    None,
                    "malformed or unsupported Range".into(),
                ));
                continue;
            }
        };
        if let Some(bytes) = literal_requested_bytes(parsed) {
            if bytes == 0 {
                rejected.push(rejected_result(
                    line_no,
                    &path_display,
                    range,
                    Some(0),
                    "zero-length range".into(),
                ));
                continue;
            }
        }
        entries.push(WarmEntry {
            path: path.to_string(),
            range: range.to_string(),
            line: line_no,
        });
    }

    (entries, rejected)
}

fn literal_requested_bytes(iv: RawInterval) -> Option<u64> {
    match (iv.first, iv.end, iv.suffix) {
        (start, Some(end), None) if end >= start => Some(end - start + 1),
        _ => None,
    }
}

fn rejected_result(
    line: usize,
    path: &str,
    range: &str,
    requested_bytes: Option<u64>,
    reason: String,
) -> WarmItemResult {
    WarmItemResult {
        line,
        path: path.to_string(),
        range: range.to_string(),
        status: WarmStatus::Failed,
        requested_bytes,
        hit_bytes: 0,
        downloaded_bytes: 0,
        verified_bytes: 0,
        reason: Some(reason),
    }
}

fn truncate_for_report(s: &str) -> String {
    const MAX: usize = 180;
    if s.len() <= MAX {
        s.to_string()
    } else {
        let mut out = s.chars().take(MAX - 3).collect::<String>();
        out.push_str("...");
        out
    }
}

/// Warm all entries sequentially. A failure is recorded for that item and
/// later entries still run; the returned summary is never a blanket success.
pub async fn warm_entries(
    state: Arc<ProxyState>,
    entries: Vec<WarmEntry>,
) -> WarmSummary {
    let mut items = Vec::with_capacity(entries.len());
    for entry in entries {
        items.push(warm_one(&state, entry).await);
    }
    summarize(items)
}

/// Combine parse rejections with executed items, preserving source order.
pub fn summarize_with_rejections(
    executed: Vec<WarmItemResult>,
    rejected: Vec<WarmItemResult>,
) -> WarmSummary {
    let mut items = executed;
    items.extend(rejected);
    items.sort_by_key(|item| item.line);
    summarize(items)
}

fn summarize(items: Vec<WarmItemResult>) -> WarmSummary {
    let mut summary = WarmSummary {
        items,
        hits: 0,
        downloaded: 0,
        failed: 0,
        hit_bytes: 0,
        downloaded_bytes: 0,
        verified_bytes: 0,
    };
    for item in &summary.items {
        match item.status {
            WarmStatus::Hit => summary.hits += 1,
            WarmStatus::Downloaded => summary.downloaded += 1,
            WarmStatus::Failed => summary.failed += 1,
        }
        summary.hit_bytes += item.hit_bytes;
        summary.downloaded_bytes += item.downloaded_bytes;
        summary.verified_bytes += item.verified_bytes;
    }
    summary
}

async fn warm_one(state: &Arc<ProxyState>, entry: WarmEntry) -> WarmItemResult {
    let mut result = WarmItemResult {
        line: entry.line,
        path: entry.path.clone(),
        range: entry.range.clone(),
        status: WarmStatus::Failed,
        requested_bytes: None,
        hit_bytes: 0,
        downloaded_bytes: 0,
        verified_bytes: 0,
        reason: None,
    };

    let uri: Uri = match entry.path.parse() {
        Ok(uri) => uri,
        Err(_) => {
            result.reason = Some("invalid request path".into());
            return result;
        }
    };
    let target = match resolve_target(state, &uri) {
        Ok(target) => target,
        Err(e) => {
            result.reason = Some(e.to_string());
            return result;
        }
    };
    if uri.scheme().is_some() || uri.authority().is_some() || uri.fragment().is_some() {
        result.reason = Some("absolute URLs and URL fragments are not accepted".into());
        return result;
    }
    let key = target_key(&target);
    let obj_lock = lock_for(state, &key).await;
    let _guard = obj_lock.lock().await;

    let single = match range::parse_range(&entry.range) {
        Some(RangeSpec::Single(iv)) => iv,
        Some(RangeSpec::Multiple) => {
            result.reason = Some("multiple ranges are not allowed".into());
            return result;
        }
        None => {
            result.reason = Some("malformed or unsupported Range".into());
            return result;
        }
    };
    result.requested_bytes = literal_requested_bytes(single);

    match warm_locked(state, &target, &key, single, &entry.range, &mut result).await {
        Ok(()) => {}
        Err(e) => {
            result.status = WarmStatus::Failed;
            if result.reason.is_none() {
                result.reason = Some(e.to_string());
            }
            if let ProxyError::TruncatedUpstreamCapture { expected, received } = e {
                result.downloaded_bytes = received;
                if result.requested_bytes.is_none() {
                    result.requested_bytes = Some(expected);
                }
                result.reason = Some(format!(
                    "upstream truncation: expected {expected} bytes, received {received}"
                ));
            } else if let ProxyError::UpstreamRead { received, .. } = e {
                result.downloaded_bytes = received;
                result.reason = Some(format!("upstream read failed after {received} bytes"));
            }
        }
    }

    result
}

async fn warm_locked(
    state: &Arc<ProxyState>,
    target: &url::Url,
    key: &str,
    single: RawInterval,
    range_hdr: &str,
    result: &mut WarmItemResult,
) -> Result<()> {
    let mut fetched_during_item = false;
    let mut repairs = 0;
    for _ in 0..(super::MAX_PLAN_ITERATIONS + super::MAX_CACHE_REPAIRS) {
        let key_lookup = key.to_string();
        let latest = with_db(state, move |c| {
            crate::db::latest_strong_version(c, &key_lookup)
        })
        .await?;

        let total = match latest.as_ref().and_then(|v| v.total_length) {
            Some(total) => total,
            None => {
                match cold_range(state, target, key, Some(range_hdr), None).await? {
                    ColdOutcome::Replan(downloaded) => {
                        result.downloaded_bytes += downloaded;
                        fetched_during_item = true;
                        continue;
                    }
                    ColdOutcome::Reply(_, captured, cached) if cached => {
                        result.downloaded_bytes += captured;
                        fetched_during_item = true;
                        continue;
                    }
                    ColdOutcome::Reply(_, captured, _) => {
                        result.downloaded_bytes = captured;
                        return Err(ProxyError::State(
                            "upstream did not provide a cacheable strong 206".into(),
                        ));
                    }
                }
            }
        };
        let ver = latest.expect("total length implies a version row");
        let (start, end_inclusive) = range::resolve(single, total)
            .map_err(|_| ProxyError::State("range became unsatisfiable".into()))?;
        let end_excl = end_inclusive + 1;
        result.requested_bytes = Some(end_excl - start);

        let vid = ver.id;
        let covered = with_db(state, move |c| crate::db::covered_segments(c, vid)).await?;
        let gaps = range::missing_within(start, end_excl, &covered);
        let requested = end_excl - start;
        let already_covered =
            requested - gaps.iter().map(|&(gs, ge)| ge - gs).sum::<u64>();
        let existing_hit = if fetched_during_item {
            0
        } else {
            already_covered
        };
        result.hit_bytes = existing_hit;

        if !gaps.is_empty() {
            for &(gs, ge) in &gaps {
                fill_gap(
                    state,
                    target,
                    key,
                    &ver,
                    gs,
                    ge,
                    total,
                    &mut fetched_during_item,
                    result,
                )
                .await?;
            }
        } else if !fetched_during_item {
            // Reuse the exact strong-validator proof as normal GETs. This
            // makes warming idempotent without blindly trusting old bytes.
            match super::revalidate_cached(state, target, key, &ver).await? {
                super::Revalidation::Fresh => {
                    result.hit_bytes = requested;
                }
                super::Revalidation::Changed(_, downloaded) => {
                    result.hit_bytes = 0;
                    result.downloaded_bytes += downloaded;
                    fetched_during_item = true;
                    continue;
                }
            }
        }

        let checked = match state.store.verify_range(ver.id, start, end_excl).await {
            Ok(checked) => checked,
            Err(ProxyError::BlobTruncated) => {
                if repairs >= super::MAX_CACHE_REPAIRS {
                    return Err(ProxyError::BlobTruncated);
                }
                repairs += 1;
                repair_version(state, ver.id).await?;
                fetched_during_item = false;
                result.hit_bytes = 0;
                continue;
            }
            Err(ProxyError::Io(io)) if io.kind() == std::io::ErrorKind::UnexpectedEof => {
                if repairs >= super::MAX_CACHE_REPAIRS {
                    return Err(ProxyError::BlobTruncated);
                }
                repairs += 1;
                repair_version(state, ver.id).await?;
                fetched_during_item = false;
                result.hit_bytes = 0;
                continue;
            }
            Err(e) => return Err(e),
        };
        result.verified_bytes = checked;
        result.status = if fetched_during_item {
            WarmStatus::Downloaded
        } else {
            WarmStatus::Hit
        };
        result.reason = None;
        return Ok(());
    }
    Err(ProxyError::State("upstream version churn; giving up".into()))
}

#[allow(clippy::too_many_arguments)]
async fn fill_gap(
    state: &Arc<ProxyState>,
    target: &url::Url,
    key: &str,
    ver: &VersionRow,
    gs: u64,
    ge: u64,
    total: u64,
    fetched_during_item: &mut bool,
    result: &mut WarmItemResult,
) -> Result<()> {
    let exact_hdr = format!("bytes={gs}-{}", ge - 1);
    let ir = ETag {
        weak: false,
        raw_tag: ver.etag_tag.clone(),
    }
    .to_wire();
    let (status, cap) = capture_upstream(state, target, Some(&exact_hdr), Some(&ir)).await?;
    let captured = cap.len;
    match status {
        reqwest::StatusCode::PARTIAL_CONTENT => {
            let n = captured;
            match store_proven_206(
                state,
                key,
                Some(ver),
                cap,
                Some((gs, ge)),
                Some(total),
            )
            .await?
            {
                ProveOutcome::Stored(_) => {
                    result.downloaded_bytes += n;
                    *fetched_during_item = true;
                    Ok(())
                }
                ProveOutcome::Passthrough(_) => Err(ProxyError::State(format!(
                    "206 failed strong ETag/Content-Range validation for {exact_hdr}"
                ))),
            }
        }
        reqwest::StatusCode::OK => {
            result.downloaded_bytes += captured;
            // The configured object changed while proving the old ETag.
            // Commit the complete representation through the same temp-file
            // path, but report this item as failed: the operator must see
            // that the requested interval was not warmed under the manifest's
            // intended current version without assuming how ranges map now.
            match commit_200(state, key, cap).await? {
                CommitOutcome::Strong(_) | CommitOutcome::Uncached(_) => {
                    Err(ProxyError::State("object changed during warming".into()))
                }
            }
        }
        reqwest::StatusCode::RANGE_NOT_SATISFIABLE => {
            result.downloaded_bytes += captured;
            Err(ProxyError::State("upstream reported 416".into()))
        }
        other => {
            result.downloaded_bytes += captured;
            Err(ProxyError::State(format!("upstream status {other}")))
        }
    }
}
