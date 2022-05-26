use std::{cmp::Ordering, ops::Deref};

use minicbor::{data::Tag, decode, encode, Decode, Decoder, Encode, Encoder};
pub use uuid::Uuid;

use crate::protocol::{TAG_LOGICAL_TIME, TAG_NS, TAG_TIMELINE_ID};

#[derive(Debug, Copy, Clone)]
#[repr(transparent)]
pub struct TimelineAttrKey(pub(crate) u32);

impl From<TimelineAttrKey> for u32 {
    fn from(k: TimelineAttrKey) -> Self {
        k.0
    }
}

#[derive(Debug, Copy, Clone)]
#[repr(transparent)]
pub struct EventAttrKey(pub(crate) u32);

impl From<EventAttrKey> for u32 {
    fn from(k: EventAttrKey) -> Self {
        k.0
    }
}

////////////
// BigInt //
////////////

/// Newtype wrapper to get correct-by-construction promises
/// about minimal AttrVal variant selection.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BigInt(Box<i128>);

impl BigInt {
    pub fn new_attr_val(big_i: i128) -> AttrVal {
        // Store it as an Integer if it's small enough
        if big_i < (i64::MIN as i128) || big_i > (i64::MAX as i128) {
            AttrVal::BigInt(BigInt(Box::new(big_i)))
        } else {
            AttrVal::Integer(big_i as i64)
        }
    }
}
impl std::fmt::Display for BigInt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}

impl AsRef<i128> for BigInt {
    fn as_ref(&self) -> &i128 {
        self.0.as_ref()
    }
}

impl Deref for BigInt {
    type Target = i128;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

/////////////////
// Nanoseconds //
/////////////////

/// A timestamp in nanoseconds
#[derive(Copy, Clone, Eq, PartialEq, Debug, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Nanoseconds(u64);

impl Nanoseconds {
    pub fn get_raw(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Nanoseconds {
    fn from(n: u64) -> Self {
        Nanoseconds(n)
    }
}

impl std::fmt::Display for Nanoseconds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}ns", self.0)
    }
}

impl Encode for Nanoseconds {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_NS)?.u64(self.get_raw())?;
        Ok(())
    }
}

/////////////////
// LogicalTime //
/////////////////

/// A segmented logical clock
#[derive(Eq, PartialEq, Clone, Debug, Hash)]
pub struct LogicalTime(Box<[u64; 4]>);

impl LogicalTime {
    pub fn unary<A: Into<u64>>(a: A) -> Self {
        LogicalTime(Box::new([0, 0, 0, a.into()]))
    }

    pub fn binary<A: Into<u64>, B: Into<u64>>(a: A, b: B) -> Self {
        LogicalTime(Box::new([0, 0, a.into(), b.into()]))
    }

    pub fn trinary<A: Into<u64>, B: Into<u64>, C: Into<u64>>(a: A, b: B, c: C) -> Self {
        LogicalTime(Box::new([0, a.into(), b.into(), c.into()]))
    }

    pub fn quaternary<A: Into<u64>, B: Into<u64>, C: Into<u64>, D: Into<u64>>(
        a: A,
        b: B,
        c: C,
        d: D,
    ) -> Self {
        LogicalTime(Box::new([a.into(), b.into(), c.into(), d.into()]))
    }

    pub fn get_raw(&self) -> &[u64; 4] {
        &self.0
    }
}

impl Ord for LogicalTime {
    fn cmp(&self, other: &Self) -> Ordering {
        for (a, b) in self.0.iter().zip(other.0.iter()) {
            match a.cmp(b) {
                Ordering::Equal => (), // continue to later segments
                Ordering::Less => return Ordering::Less,
                Ordering::Greater => return Ordering::Greater,
            }
        }

        Ordering::Equal
    }
}

impl PartialOrd for LogicalTime {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for LogicalTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}:{}", self.0[0], self.0[1], self.0[2], self.0[3])
    }
}

impl Encode for LogicalTime {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_LOGICAL_TIME)?.encode(self.get_raw())?;
        Ok(())
    }
}

////////////////
// TimelineId //
////////////////

/// Timelines are identified by a UUID. These are timeline *instances*; a given location (identified
/// by its name) is associated with many timelines.
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug)]
pub struct TimelineId(Uuid);

impl TimelineId {
    pub fn zero() -> Self {
        TimelineId(Uuid::nil())
    }

    pub fn allocate() -> Self {
        TimelineId(Uuid::new_v4())
    }

    pub fn get_raw(&self) -> &Uuid {
        &self.0
    }
}

impl From<Uuid> for TimelineId {
    fn from(uuid: Uuid) -> Self {
        TimelineId(uuid)
    }
}

impl std::fmt::Display for TimelineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Encode for TimelineId {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_TIMELINE_ID)?.bytes(self.get_raw().as_bytes())?;
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

/////////////
// AttrVal //
/////////////

#[derive(Clone, Debug)]
pub enum AttrVal {
    TimelineId(Box<TimelineId>),
    String(String),
    Integer(i64),
    BigInt(BigInt),
    Float(f64),
    Bool(bool),
    Timestamp(Nanoseconds),
    LogicalTime(LogicalTime),
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
                e.f64(*f)?;
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
