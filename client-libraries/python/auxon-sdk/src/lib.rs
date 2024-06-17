mod config;
mod ingest;
mod mutator;
mod types;

use auxon_sdk::{ingest_client::dynamic::DynamicIngestError, mutation_plane_client::parent_connection::CommsError};
use pyo3::prelude::*;

#[pymodule]
fn _auxon_sdk(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<auxon_sdk::api::TimelineId>()?;
    m.add_class::<auxon_sdk::api::EventCoordinate>()?;
    m.add_class::<auxon_sdk::mutation_plane::types::MutationId>()?;
    m.add_class::<auxon_sdk::mutation_plane::types::MutatorId>()?;
    m.add_class::<config::PluginConfig>()?;
    m.add_class::<ingest::IngestClient>()?;
    m.add_class::<mutator::MutatorParam>()?;
    m.add_class::<mutator::MutatorHost>()?;
    m.add_class::<mutator::PyMutatorDescriptor>()?;
    Ok(())
}

pub struct SdkError(Box<dyn std::error::Error + Send + Sync>);

impl From<Box<dyn std::error::Error + Send + Sync>> for SdkError {
    fn from(value: Box<dyn std::error::Error + Send + Sync>) -> Self {
        SdkError(value)
    }
}

impl From<std::io::Error> for SdkError {
    fn from(value: std::io::Error) -> Self {
        SdkError(Box::new(value))
    }
}

impl From<&str> for SdkError {
    fn from(value: &str) -> Self {
        SdkError(value.into())
    }
}

impl From<CommsError> for SdkError {
    fn from(value: CommsError) -> Self {
        SdkError(value.into())
    }
}

impl From<DynamicIngestError> for SdkError {
    fn from(value: DynamicIngestError) -> Self {
        SdkError(Box::new(value))
    }
}

impl From<SdkError> for PyErr {
    fn from(value: SdkError) -> Self {
        pyo3::exceptions::PyValueError::new_err(value.0.to_string())
    }
}
