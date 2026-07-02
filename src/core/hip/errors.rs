use thiserror::Error;

#[derive(Debug, Error)]
pub enum HipError {
    #[error("hip io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("hip frame too large: {size} > {max}")]
    FrameTooLarge { size: u64, max: u64 },
    #[error("hip protocol violation: {0}")]
    Protocol(String),
    #[error("hip msgpack encode error: {0}")]
    Encode(String),
    #[error("hip msgpack decode error: {0}")]
    Decode(String),
    #[error("hip handshake failed: {0}")]
    Handshake(String),
    #[error("hip server returned error: code={code} message={message} fatal={fatal}")]
    Server {
        code: String,
        message: String,
        fatal: bool,
    },
    #[error("hip session closed: {0}")]
    SessionClosed(String),
    #[error("hip request timed out after {0} ms")]
    Timeout(u64),
    #[error("hip upload sha256 mismatch for path `{path}`")]
    Sha256Mismatch { path: String },
    #[error("hip tls error: {0}")]
    Tls(String),
    #[error("hip config error: {0}")]
    Config(String),
}

impl From<rmp_serde::encode::Error> for HipError {
    fn from(err: rmp_serde::encode::Error) -> Self {
        Self::Encode(err.to_string())
    }
}

impl From<rmp_serde::decode::Error> for HipError {
    fn from(err: rmp_serde::decode::Error) -> Self {
        Self::Decode(err.to_string())
    }
}
