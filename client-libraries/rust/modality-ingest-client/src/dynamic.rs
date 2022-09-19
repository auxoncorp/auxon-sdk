use modality_api::{AttrVal, TimelineId};
use modality_ingest_protocol::{IngestMessage, InternedAttrKey};
use thiserror::Error;

use crate::{IngestClientCommon, IngestError, IngestClient, ReadyState, BoundTimelineState};

/// A more dynamic ingest client, for places where the session types are difficult to use.
pub struct DynamicIngestClient {
    common: IngestClientCommon,
    bound_timeline: Option<TimelineId>,
}

impl From<IngestClient<ReadyState>> for DynamicIngestClient {
    fn from(c: IngestClient<ReadyState>) -> Self {
        Self {
            common: c.common,
            bound_timeline: None
        }
    }
}

impl From<IngestClient<BoundTimelineState>> for DynamicIngestClient {
    fn from(c: IngestClient<BoundTimelineState>) -> Self {
        Self {
            common: c.common,
            bound_timeline: Some(c.state.timeline_id)
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
}

#[derive(Error, Debug)]
pub enum DynamicIngestError {
    #[error(transparent)]
    IngestError(#[from] IngestError),

    #[error("Invalid state: a timeline must be bound before submitting events")]
    NoBoundTimeline
}
