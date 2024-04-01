mod layer;

use crate::tracing::InitError;
pub use crate::tracing::{
    ingest::ModalityIngestTaskHandle, options::Options, r#async::layer::ModalityLayer,
};

use anyhow::Context as _;
use tracing_core::Dispatch;

/// A global tracer instance for [tracing.rs](https://tracing.rs/) that sends traces via a network
/// socket to [Modality](https://auxon.io/).
///
/// This is the asynchronous version of `TracingModality`, it must be initialized and `finish`ed
/// from within a tokio runtime. See [`crate::tracing::blocking::TracingModality`] for a version that can be
/// initialized outside a tokio runtime. Both versions support tracing from within and outside of a
/// tokio runtime.
pub struct TracingModality {
    ingest_handle: ModalityIngestTaskHandle,
}

impl TracingModality {
    /// Initialize with default options and set as the global default tracer.
    pub async fn init() -> Result<Self, InitError> {
        Self::init_with_options(Default::default()).await
    }

    /// Initialize with the provided options and set as the global default tracer.
    pub async fn init_with_options(opts: Options) -> Result<Self, InitError> {
        let (layer, ingest_handle) = ModalityLayer::init_with_options(opts)
            .await
            .context("initialize ModalityLayer")?;

        let disp = Dispatch::new(layer.into_subscriber());
        tracing::dispatcher::set_global_default(disp).unwrap();

        Ok(Self { ingest_handle })
    }

    /// Stop accepting new trace events, flush all existing events, and stop ingest thread.
    pub async fn finish(self) {
        self.ingest_handle.finish().await;
    }
}
