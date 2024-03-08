use crate::api::types::{AttrVal, EventCoordinate, LogicalTime, Nanoseconds, TimelineId};
use minicbor::{data::Tag, decode, encode, Decode, Decoder, Encode, Encoder};
use uuid::Uuid;

pub const TAG_NS: Tag = Tag::Unassigned(40000);
pub const TAG_LOGICAL_TIME: Tag = Tag::Unassigned(40001);
pub const TAG_TIMELINE_ID: Tag = Tag::Unassigned(40002);
pub const TAG_EVENT_COORDINATE: Tag = Tag::Unassigned(40003);

impl Encode for Nanoseconds {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_NS)?.u64(self.get_raw())?;
        Ok(())
    }
}

impl<'b> Decode<'b> for Nanoseconds {
    fn decode(d: &mut Decoder<'b>) -> Result<Self, decode::Error> {
        let t = d.tag()?;
        if t != TAG_NS {
            return Err(decode::Error::Message("Expected TAG_NS"));
        }

        Ok(d.u64()?.into())
    }
}

impl Encode for LogicalTime {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_LOGICAL_TIME)?.encode(self.get_raw())?;
        Ok(())
    }
}

impl<'b> Decode<'b> for LogicalTime {
    fn decode(d: &mut Decoder<'b>) -> Result<Self, decode::Error> {
        let t = d.tag()?;
        if t != TAG_LOGICAL_TIME {
            return Err(decode::Error::Message("Expected TAG_LOGICAL_TIME"));
        }

        let els: Result<Vec<u64>, decode::Error> = d.array_iter()?.collect();
        let els = els?;
        if els.len() != 4 {
            return Err(decode::Error::Message("LogicalTime array length must be 4"));
        }

        Ok(LogicalTime::quaternary(els[0], els[1], els[2], els[3]))
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

impl Encode for EventCoordinate {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_EVENT_COORDINATE)?.bytes(&self.as_bytes())?;
        Ok(())
    }
}

impl<'b> Decode<'b> for EventCoordinate {
    fn decode(d: &mut Decoder<'b>) -> Result<Self, decode::Error> {
        let t = d.tag()?;
        if t != TAG_EVENT_COORDINATE {
            return Err(decode::Error::Message("Expected TAG_EVENT_COORDINATE"));
        }

        EventCoordinate::from_byte_slice(d.bytes()?)
            .ok_or(decode::Error::Message("Error decoding event coordinate"))
    }
}

impl Encode for AttrVal {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        match self {
            AttrVal::String(s) => {
                e.str(s.as_ref())?;
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
            AttrVal::EventCoordinate(ec) => {
                ec.encode(e)?;
            }
        }

        Ok(())
    }
}

impl<'b> Decode<'b> for AttrVal {
    fn decode(d: &mut Decoder<'b>) -> Result<Self, decode::Error> {
        use minicbor::data::Type;
        let t = d.datatype()?;
        match t {
            Type::Bool => Ok((d.bool()?).into()),

            Type::U8 => Ok((d.u8()?).into()),
            Type::U16 => Ok((d.u16()?).into()),
            Type::U32 => Ok((d.u32()?).into()),
            Type::I8 => Ok((d.i8()?).into()),
            Type::I16 => Ok((d.i16()?).into()),
            Type::I32 => Ok((d.i32()?).into()),
            Type::I64 => Ok((d.i64()?).into()),

            Type::U64 => Ok((d.u64()? as i128).into()),
            Type::F32 => Ok((d.f32()?).into()),
            Type::F64 => Ok((d.f64()?).into()),

            Type::String => Ok(d.str()?.into()),
            Type::StringIndef => {
                let mut s = String::new();
                for s_res in d.str_iter()? {
                    s += s_res?;
                }
                Ok(s.into())
            }

            Type::Tag => {
                // probe == lookahead
                match d.probe().tag()? {
                    TAG_NS => Ok(Nanoseconds::decode(d)?.into()),
                    TAG_LOGICAL_TIME => Ok(LogicalTime::decode(d)?.into()),
                    TAG_TIMELINE_ID => Ok(TimelineId::decode(d)?.into()),

                    Tag::PosBignum | Tag::NegBignum => {
                        let tag = d.tag()?;
                        let bytes = d.bytes()?;
                        if bytes.len() != 16 {
                            // Lame
                            return Err(decode::Error::Message(
                                "Bignums must be encoded as exactly 16 bytes",
                            ));
                        }
                        // LAAAAAAAAAAAAAAAAAAAME
                        let mut encoded_num = i128::from_be_bytes([
                            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                            bytes[7], bytes[8], bytes[9], bytes[10], bytes[11], bytes[12],
                            bytes[13], bytes[14], bytes[15],
                        ]);
                        if tag == Tag::NegBignum {
                            encoded_num = -1 - encoded_num;
                        }

                        Ok(encoded_num.into())
                    }

                    _ => Err(decode::Error::Message("Unexpected Tag for Attrval")),
                }
            }
            _ => Err(decode::Error::TypeMismatch(
                t,
                "Unexpected datatype for AttrVal",
            )),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn round_trip_attr_val() {
        proptest!(|(attr_val in crate::api::proptest_strategies::attr_val())| {
            let mut buf = vec![];
            minicbor::encode(&attr_val, &mut buf)?;

            let attr_val_prime: AttrVal = minicbor::decode(&buf)?;
            prop_assert_eq!(attr_val, attr_val_prime);
        });
    }

    #[test]
    fn round_trip_attr_val_with_codec_specific_negative_number_edge_cases() {
        let edges = [
            std::i8::MIN as i64,
            std::i16::MIN as i64,
            std::i32::MIN as i64,
        ];
        for edge in edges {
            for offset in -3..=3 {
                let val = edge + offset;

                let attr_val = AttrVal::from(val);
                let mut buf = vec![];
                minicbor::encode(&attr_val, &mut buf).unwrap();

                let attr_val_prime: AttrVal = minicbor::decode(&buf).unwrap();
                assert_eq!(attr_val, attr_val_prime);
            }
        }
    }
}
