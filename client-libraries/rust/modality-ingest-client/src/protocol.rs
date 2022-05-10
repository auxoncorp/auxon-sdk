use crate::types::{AttrVal, EventAttrKey, TimelineAttrKey, TimelineId};
use minicbor::{data::Tag, encode, Decode, Encode, Encoder};

pub const TAG_NS: Tag = Tag::Unassigned(40000);
pub const TAG_LOGICAL_TIME: Tag = Tag::Unassigned(40001);
pub const TAG_TIMELINE_ID: Tag = Tag::Unassigned(40002);

#[derive(Decode, Debug)]
pub enum IngestResponse {
    #[n(1)]
    AuthResponse {
        #[n(0)]
        ok: bool,

        #[n(1)]
        message: Option<String>,
    },

    #[n(2)]
    UnauthenticatedResponse {},
}

#[derive(Encode, Debug)]
pub enum IngestMessage {
    #[n(0)]
    AuthRequest {
        #[n(0)]
        token: Vec<u8>,
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
        attrs: PackedAttrKvs<TimelineAttrKey>,
    },

    #[n(114)]
    Event {
        #[n(0)]
        /// Up to 16 bytes
        be_ordering: Vec<u8>,

        #[n(1)]
        attrs: PackedAttrKvs<EventAttrKey>,
    },
}

/// A way to bundle together attr kvs for transport purposes.  The 'u32' is meant to represent an
/// attr key, where the name->number mapping is defined elsewhere in the protocol.
///
/// These are encoded in cbor in a 'reasonably compact' way: an array of alternating u32 key and
/// AttrVals.
#[derive(Debug)]
pub struct PackedAttrKvs<K: Into<u32> + Copy + std::fmt::Debug>(pub Vec<(K, AttrVal)>);

impl<K: Into<u32> + Copy + std::fmt::Debug> Encode for PackedAttrKvs<K> {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.array((self.0.len() * 2) as u64)?;
        for (k, v) in self.0.iter() {
            e.u32((*k).into())?;
            v.encode(e)?;
        }

        Ok(())
    }
}
