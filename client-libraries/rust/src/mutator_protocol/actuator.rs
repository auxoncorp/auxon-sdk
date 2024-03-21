use async_trait::async_trait;

use crate::api::{AttrKey, AttrVal};
use std::collections::BTreeMap;

/// "Infallible" operational view on a mutator actuator.
#[async_trait]
pub trait MutatorActuator {
    /// Input params attribute iterator should not contain duplicate keys.
    /// It is effectively a map of key-value pairs.
    ///
    /// The keys are expected to be of either format:
    ///   * `<param-key>`
    ///   * OR `mutator.params.<param-key>`
    async fn inject(
        &mut self,
        mutation_id: uuid::Uuid,
        params: BTreeMap<AttrKey, AttrVal>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn reset(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// A non-async version of MutatorActuator. This isn't supported by the provided http server, but
/// may be useful if the abstraction is needed in other host contexts.
pub trait SyncMutatorActuator {
    /// Input params attribute iterator should not contain duplicate keys.
    /// It is effectively a map of key-value pairs.
    ///
    /// The keys are expected to be of either format:
    ///   * `<param-key>`
    ///   * OR `mutator.params.<param-key>`
    fn inject(
        &mut self,
        mutation_id: uuid::Uuid,
        params: BTreeMap<AttrKey, AttrVal>,
    ) -> Result<(), Box<dyn std::error::Error>>;

    fn reset(&mut self) -> Result<(), Box<dyn std::error::Error>>;
}
