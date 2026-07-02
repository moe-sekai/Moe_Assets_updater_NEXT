//! HIP/1 (Haruki Ingest Protocol v1) client implementation.
//!
//! HIP is a length-prefixed binary protocol used to talk to the pjsk.moe
//! dedup gateway. See docs/hip.md (upcoming) for the full spec. This module
//! only implements the client side (framing, codec, session, errors).

pub mod client;
pub mod codec;
pub mod errors;
pub mod frame;

pub use client::{HelloParams, HipClient, HipClientConfig, HipSession, PlacementHint};
pub use codec::{
    CheckAckItem, CheckAction, CheckBatch, CheckBatchItem, CheckResult, Commit, CommitAck,
    CommitStats, Hello, HelloAck, UploadAck, UploadBegin, UploadEnd,
};
pub use errors::HipError;
pub use frame::{Frame, FrameType, MAX_DEFAULT_FRAME_BYTES};
