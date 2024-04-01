//! Semi-custom serde support for AttrVals.
//!
//! This is set up to overlay the dynamic type semantics of AttrVals onto Serde value semantics as
//! much as possible. Where the variant cannot be directly distinguished from the Serde type (or
//! value, in the case of bigint), we delegate to an enum to distinguish them. The actual
//! representation used will depend on the serializer, but for json it's a map:
//! `{"TimelineId", "..."}`.

use crate::api::types::{AttrVal, EventCoordinate, LogicalTime, Nanoseconds, TimelineId};
use ordered_float::OrderedFloat;
use serde::{
    de::{value::MapAccessDeserializer, Visitor},
    Deserialize, Serialize,
};

#[derive(serde::Serialize)]
enum TaggedAttrValSer<'a> {
    TimelineId(&'a TimelineId),
    Timestamp(&'a Nanoseconds),
    LogicalTime(&'a LogicalTime),
    EventCoordinate(&'a EventCoordinate),
    /// thanks json, you're a real pal
    BigInt(&'a str),
    NonFiniteFloat(NonFiniteFloat),
}

#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[derive(Copy, Clone, serde::Serialize, serde::Deserialize)]
enum NonFiniteFloat {
    #[serde(rename = "NaN")]
    NaN,
    #[serde(rename = "-NaN")]
    NegNaN,
    #[serde(rename = "Infinity")]
    Infinity,
    #[serde(rename = "-Infinity")]
    NegInfinity,
}

#[derive(serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
enum TaggedAttrVal {
    TimelineId(TimelineId),
    Timestamp(Nanoseconds),
    LogicalTime(LogicalTime),
    EventCoordinate(EventCoordinate),
    BigInt(String),
    NonFiniteFloat(NonFiniteFloat),
}

impl Serialize for AttrVal {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            AttrVal::TimelineId(tl_id) => {
                TaggedAttrValSer::TimelineId(tl_id.as_ref()).serialize(ser)
            }
            AttrVal::String(s) => s.serialize(ser),
            AttrVal::Integer(i) => i.serialize(ser),
            AttrVal::BigInt(bi) => TaggedAttrValSer::BigInt(&format!("{bi}")).serialize(ser),
            AttrVal::Float(f) => {
                if f.is_finite() {
                    f.serialize(ser)
                } else {
                    TaggedAttrValSer::NonFiniteFloat(match (f.is_nan(), f.is_sign_negative()) {
                        (true, true) => NonFiniteFloat::NegNaN,
                        (true, false) => NonFiniteFloat::NaN,
                        (false, true) => NonFiniteFloat::NegInfinity,
                        (false, false) => NonFiniteFloat::Infinity,
                    })
                    .serialize(ser)
                }
            }
            AttrVal::Bool(b) => b.serialize(ser),
            AttrVal::Timestamp(ns) => TaggedAttrValSer::Timestamp(ns).serialize(ser),
            AttrVal::LogicalTime(lt) => TaggedAttrValSer::LogicalTime(lt).serialize(ser),
            AttrVal::EventCoordinate(ec) => TaggedAttrValSer::EventCoordinate(ec).serialize(ser),
        }
    }
}

impl<'de> Deserialize<'de> for AttrVal {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        de.deserialize_any(AttrValVisitor)
    }
}

struct AttrValVisitor;
impl<'de> Visitor<'de> for AttrValVisitor {
    type Value = AttrVal;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an encoded AttrVal")
    }

    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_i8<E: serde::de::Error>(self, v: i8) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_i16<E: serde::de::Error>(self, v: i16) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_i32<E: serde::de::Error>(self, v: i32) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_u8<E: serde::de::Error>(self, v: u8) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_u16<E: serde::de::Error>(self, v: u16) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_u32<E: serde::de::Error>(self, v: u32) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_f32<E: serde::de::Error>(self, v: f32) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        let val: TaggedAttrVal = Deserialize::deserialize(MapAccessDeserializer::new(map))?;
        match val {
            TaggedAttrVal::TimelineId(tl_id) => Ok(tl_id.into()),
            TaggedAttrVal::Timestamp(ts) => Ok(ts.into()),
            TaggedAttrVal::LogicalTime(lt) => Ok(lt.into()),
            TaggedAttrVal::EventCoordinate(ec) => Ok(ec.into()),
            TaggedAttrVal::BigInt(s) => {
                let num: i128 = s.parse().map_err(|_| {
                    <A::Error as serde::de::Error>::invalid_value(
                        serde::de::Unexpected::Str(&s),
                        &"a string containing a signed integer",
                    )
                })?;
                Ok(num.into())
            }
            TaggedAttrVal::NonFiniteFloat(nff) => match nff {
                NonFiniteFloat::NaN => Ok(Self::Value::Float(OrderedFloat(f64::NAN))),
                NonFiniteFloat::NegNaN => Ok(Self::Value::Float(OrderedFloat(-f64::NAN))),
                NonFiniteFloat::Infinity => Ok(Self::Value::Float(OrderedFloat(f64::INFINITY))),
                NonFiniteFloat::NegInfinity => {
                    Ok(Self::Value::Float(OrderedFloat(f64::NEG_INFINITY)))
                }
            },
        }
    }
}

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for AttrVal {
    fn schema_name() -> String {
        "AttrVal".to_owned()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        use schemars::schema::*;

        let tagged_attr_val_schema = schemars::schema_for!(TaggedAttrVal);
        gen.definitions_mut()
            .extend(tagged_attr_val_schema.definitions.clone());

        Schema::Object(SchemaObject {
            subschemas: Some(Box::new(SubschemaValidation {
                any_of: Some(vec![
                    schemars::schema_for!(String).schema.into(),
                    schemars::schema_for!(i64).schema.into(),
                    schemars::schema_for!(f64).schema.into(),
                    schemars::schema_for!(bool).schema.into(),
                    tagged_attr_val_schema.schema.into(),
                ]),
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}
