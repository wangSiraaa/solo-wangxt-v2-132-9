use axum::http::StatusCode;

/// Internal error type. Everything here maps to a 502/500 response; protocol
/// level outcomes (416, upstream 200/206) are handled explicitly instead.
#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("upstream I/O failure: {0}")]
    Upstream(#[from] reqwest::Error),
    #[error("upstream refused to serve bytes (truncated or malformed)")]
    TruncatedUpstream,
    /// Upstream announced `expected` bytes but the body closed after
    /// `received`. The captured bytes are never committed.
    #[error("upstream body truncated: received {received} of {expected} bytes")]
    TruncatedBody { received: u64, expected: u64 },
    /// Upstream evidence could not prove a cache version: a strong validator
    /// was missing/weak, or a 206 contradicted its claimed interval.
    #[error("upstream bytes cannot be proven by a strong validator")]
    Unprovable,
    /// Upstream reported the requested interval unsatisfiable (416).
    #[error("upstream reports the range is unsatisfiable")]
    UnsatisfiableRange,
    /// A cached blob file was shorter than the SQLite segments claimed.
    #[error("cached blob was truncated on disk")]
    BlobTruncated,
    #[error("metadata store failure: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("blob file failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("upstream is not the configured local test service")]
    UpstreamNotAllowed,
    #[error("requested path escapes the upstream prefix")]
    PathEscape,
    #[error("internal state error: {0}")]
    State(String),
}

impl ProxyError {
    pub fn status(&self) -> StatusCode {
        match self {
            // The configured upstream behaved wrongly (truncated body, sent
            // Content-Range that contradicts its length, ...). Never turn
            // that into a successful complete response.
            ProxyError::TruncatedUpstream
            | ProxyError::TruncatedBody { .. }
            | ProxyError::Unprovable
            | ProxyError::UnsatisfiableRange
            | ProxyError::BlobTruncated
            | ProxyError::Upstream(_) => StatusCode::BAD_GATEWAY,
            ProxyError::UpstreamNotAllowed | ProxyError::PathEscape => StatusCode::FORBIDDEN,
            ProxyError::Db(_) | ProxyError::Io(_) | ProxyError::State(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, ProxyError>;
