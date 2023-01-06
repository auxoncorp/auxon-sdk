#![deny(warnings, clippy::all)]
use std::collections::BTreeMap;
#[cfg(feature = "server")]
pub mod server;

use modality_mutator_protocol::attrs::{AttrKey, AttrVal};
use uuid::Uuid;

/// Mutator representation for HTTP
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[cfg_attr(feature = "server", derive(utoipa::ToSchema))]
pub struct Mutator {
    /// HTTP-server local identifier for the mutator.
    #[cfg_attr(feature = "server", schema(example = "abc123"))]
    pub mutator_correlation_id: String,

    /// Mutator's attributes
    #[cfg_attr(feature = "server", schema())]
    pub attributes: BTreeMap<AttrKey, AttrVal>,
}

/// Mutation request representation for HTTP
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[cfg_attr(feature = "server", derive(utoipa::ToSchema))]
pub struct Mutation {
    #[cfg_attr(feature = "server", schema())]
    pub mutation: Uuid,

    /// Mutation parameters
    #[cfg_attr(feature = "server", schema())]
    pub params: BTreeMap<AttrKey, AttrVal>,
}

pub type GetAllMutatorsResponse = Vec<Mutator>;

pub const MUTATOR_API_KEY_HEADER: &str = "mutator_apikey";
pub const MUTATOR_API_CONTROL_HEADER: &str = "x-auxon-mutation-control";
