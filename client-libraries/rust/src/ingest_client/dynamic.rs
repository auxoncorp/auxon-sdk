use crate::api::{AttrVal, TimelineId};
use crate::ingest_protocol::{IngestMessage, IngestResponse, InternedAttrKey};
use thiserror::Error;

use crate::ingest_client::{
    BoundTimelineState, IngestClient, IngestClientCommon, IngestError, IngestStatus, ReadyState,
};

/// A more dynamic ingest client, for places where the session types are difficult to use.
pub struct DynamicIngestClient {
    common: IngestClientCommon,
    bound_timeline: Option<TimelineId>,
}

impl From<IngestClient<ReadyState>> for DynamicIngestClient {
    fn from(c: IngestClient<ReadyState>) -> Self {
        Self {
            common: c.common,
            bound_timeline: None,
        }
    }
}

impl From<IngestClient<BoundTimelineState>> for DynamicIngestClient {
    fn from(c: IngestClient<BoundTimelineState>) -> Self {
        Self {
            common: c.common,
            bound_timeline: Some(c.state.timeline_id),
        }
    }
}

impl DynamicIngestClient {
    pub async fn declare_attr_key(
        &mut self,
        key_name: String,
    ) -> Result<InternedAttrKey, IngestError> {
        self.common.declare_attr_key(key_name).await
    }

    pub async fn open_timeline(&mut self, id: TimelineId) -> Result<(), IngestError> {
        self.common
            .send(&IngestMessage::OpenTimeline { id })
            .await?;

        self.bound_timeline = Some(id);
        Ok(())
    }

    pub fn close_timeline(&mut self) {
        self.bound_timeline = None;
    }

    pub async fn timeline_metadata(
        &mut self,
        attrs: impl IntoIterator<Item = (InternedAttrKey, AttrVal)>,
    ) -> Result<(), DynamicIngestError> {
        if self.bound_timeline.is_none() {
            return Err(DynamicIngestError::NoBoundTimeline);
        }

        self.common.timeline_metadata(attrs).await?;
        Ok(())
    }

    pub async fn event(
        &mut self,
        ordering: u128,
        attrs: impl IntoIterator<Item = (InternedAttrKey, AttrVal)>,
    ) -> Result<(), DynamicIngestError> {
        if self.bound_timeline.is_none() {
            return Err(DynamicIngestError::NoBoundTimeline);
        }

        self.common.event(ordering, attrs).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), IngestError> {
        self.common.flush().await
    }

    pub async fn status(&mut self) -> Result<IngestStatus, IngestError> {
        let resp = self
            .common
            .send_recv(&IngestMessage::IngestStatusRequest {})
            .await?;

        match resp {
            IngestResponse::IngestStatusResponse {
                current_timeline,
                events_received,
                events_written,
                events_pending,
                error_count,
            } => Ok(IngestStatus {
                current_timeline,
                events_received,
                events_written,
                events_pending,
                error_count: error_count.unwrap_or(0),
            }),
            _ => Err(IngestError::ProtocolError(
                "Invalid status response recieved",
            )),
        }
    }
}

#[derive(Error, Debug)]
pub enum DynamicIngestError {
    #[error(transparent)]
    IngestError(#[from] IngestError),

    #[error("Invalid state: a timeline must be bound before submitting events")]
    NoBoundTimeline,
}

#[cfg(feature = "pyo3")]
impl From<DynamicIngestError> for pyo3::PyErr {
    fn from(value: DynamicIngestError) -> Self {
        pyo3::exceptions::PyValueError::new_err(value.to_string())
    }
}
