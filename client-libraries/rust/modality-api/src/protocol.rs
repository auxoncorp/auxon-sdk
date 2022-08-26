use crate::types::{AttrVal, LogicalTime, Nanoseconds, TimelineId};
use minicbor::{data::Tag, decode, encode, Decode, Decoder, Encode, Encoder};
use uuid::Uuid;

pub const TAG_NS: Tag = Tag::Unassigned(40000);
pub const TAG_LOGICAL_TIME: Tag = Tag::Unassigned(40001);
pub const TAG_TIMELINE_ID: Tag = Tag::Unassigned(40002);

#[cfg_attr(feature = "client", derive(Decode))]
#[derive(Debug)]
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
}

#[cfg_attr(feature = "client", derive(Encode))]
#[derive(Debug)]
pub enum IngestMessage {
    #[n(0)]
    AuthRequest {
        #[n(0)]
        token: Vec<u8>,
    },

    #[n(100)]
    IngestStatusRequest {},

    #[n(102)]
    /// An advisory message, asking the server to immediately write any pending events to disk.
    Flush {},

    #[n(110)]
    DeclareAttrKey {
        #[n(0)]
        name: String,

        #[n(1)]
        wire_id: InternedAttrKey,
    },

    #[n(112)]
    OpenTimeline {
        #[n(0)]
        id: TimelineId,
    },

    #[n(113)]
    TimelineMetadata {
        #[n(0)]
        attrs: PackedAttrKvs<InternedAttrKey>,
    },

    #[n(114)]
    Event {
        #[n(0)]
        be_ordering: Vec<u8>,

        #[n(1)]
        attrs: PackedAttrKvs<InternedAttrKey>,
    },
}

/// The numeric representation of an `AttrKey` after it has been declared on a connection.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct InternedAttrKey(pub(crate) u32);

impl From<u32> for InternedAttrKey {
    fn from(i: u32) -> Self {
        InternedAttrKey(i)
    }
}

impl Into<u32> for InternedAttrKey {
    fn into(self) -> u32 {
        self.0
    }
}

impl Encode for InternedAttrKey {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.u32(self.0)?;
        Ok(())
    }
}

impl<'b> Decode<'b> for InternedAttrKey {
    fn decode(d: &mut Decoder<'b>) -> Result<Self, decode::Error> {
        let i = d.u32()?;
        Ok(i.into())
    }
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

impl Encode for Nanoseconds {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_NS)?.u64(self.get_raw())?;
        Ok(())
    }
}

impl Encode for LogicalTime {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_LOGICAL_TIME)?.encode(self.get_raw())?;
        Ok(())
    }
}

impl Encode for TimelineId {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_TIMELINE_ID)?.bytes(self.get_raw().as_bytes())?;
        Ok(())
    }
}

impl Encode for AttrVal {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        match self {
            AttrVal::String(s) => {
                e.str(s.as_str())?;
            }
            AttrVal::Integer(i) => {
                e.i64(*i)?;
            }
            AttrVal::BigInt(bi) => {
                if **bi >= 0i128 {
                    e.tag(Tag::PosBignum)?.bytes(&bi.to_be_bytes())?;
                } else {
                    // this is what the spec says to do. don't ask me.
                    e.tag(Tag::NegBignum)?.bytes(&((-1 - **bi).to_be_bytes()))?;
                }
            }
            AttrVal::Float(f) => {
                e.f64(**f)?;
            }
            AttrVal::Bool(b) => {
                e.bool(*b)?;
            }
            AttrVal::Timestamp(ns) => {
                ns.encode(e)?;
            }
            AttrVal::LogicalTime(lt) => {
                lt.encode(e)?;
            }
            AttrVal::TimelineId(tid) => {
                tid.encode(e)?;
            }
        }

        Ok(())
    }
}

impl<'b> Decode<'b> for TimelineId {
    fn decode(d: &mut Decoder<'b>) -> Result<Self, decode::Error> {
        let t = d.tag()?;
        if t != TAG_TIMELINE_ID {
            return Err(decode::Error::Message("Expected TAG_TIMELINE_ID"));
        }

        Uuid::from_slice(d.bytes()?)
            .map(Into::into)
            .map_err(|_uuid_err| decode::Error::Message("Error decoding uuid for TimelineId"))
    }
}
