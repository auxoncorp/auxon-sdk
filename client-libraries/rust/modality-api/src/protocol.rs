use crate::{AttrVal, LogicalTime, Nanoseconds, TimelineId};
use minicbor::{data::Tag, decode, encode, Decode, Decoder, Encode, Encoder};
use uuid::Uuid;

pub const TAG_NS: Tag = Tag::Unassigned(40000);
pub const TAG_LOGICAL_TIME: Tag = Tag::Unassigned(40001);
pub const TAG_TIMELINE_ID: Tag = Tag::Unassigned(40002);

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
