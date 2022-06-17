use crate::types::{AttrKey, AttrVal, TimelineId};
use minicbor::{data::Tag, encode, Decode, Encode, Encoder};

#[derive(Encode, Decode, Debug)]
pub enum IngestMessage {
    #[n(0)]
    AuthRequest {
        #[n(0)]
        token: Vec<u8>,
    },

    #[n(1)]
    AuthResponse {
        #[n(0)]
        ok: bool,

        #[n(1)]
        message: Option<String>,
    },

    #[n(2)]
    UnauthenticatedResponse {},

    #[n(100)]
    IngestStatusRequest {},

    #[n(101)]
    IngestStatusResponse {
        #[n(0)]
        current_timeline: Option<TimelineId>,

        #[n(1)]
        events_received: u64,

        #[n(2)]
        events_written: u64,

        #[n(3)]
        events_pending: u64,
    },

    #[n(102)]
    /// An advisory message, asking the server to immediately write any pending events to disk.
    Flush {},

    #[n(110)]
    DeclareAttrKey {
        #[n(0)]
        name: String,

        #[n(1)]
        wire_id: u32,
    },

    #[n(112)]
    OpenTimeline {
        #[n(0)]
        id: TimelineId,
    },

    #[n(113)]
    TimelineMetadata {
        #[n(0)]
        attrs: PackedAttrKvs,
    },

    #[n(114)]
    Event {
        #[n(0)]
        be_ordering: Vec<u8>,

        #[n(1)]
        attrs: PackedAttrKvs,
    },
}
