pub(crate) mod ingest;
pub(crate) mod layer;
pub(crate) mod options;

#[cfg(doc)]
use crate::tracing::Options;

use crate::api::TimelineId;
use ingest::ConnectError;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum InitError {
    /// No auth was provided, set with
    /// [`Options::set_auth`][crate::tracing::Options::set_auth]/[`Options::with_auth`][crate::tracing::Options::with_auth]
    /// or set the `MODALITY_AUTH_TOKEN` environment variable.
    #[error("Authentication required, set init option or env var MODALITY_AUTH_TOKEN")]
    AuthRequired,

    /// Auth was provided, but was not accepted by modality.
    #[error(transparent)]
    AuthFailed(ConnectError),

    /// Errors that it is assumed there is no way to handle without human intervention, meant for
    /// consumers to just print and carry on or panic.
    #[error(transparent)]
    UnexpectedFailure(#[from] anyhow::Error),
}

/// Retrieve the current local timeline ID. Useful for for sending alongside data and a custom nonce
/// for recording timeline interactions on remote timelines.
pub fn timeline_id() -> TimelineId {
    ingest::current_timeline()
}
