use std::{borrow::Cow, ops::Deref};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AttrKey(Cow<'static, str>);

impl From<&str> for AttrKey {
    fn from(s: &str) -> Self {
        AttrKey(Cow::from(s.to_owned()))
    }
}
impl From<String> for AttrKey {
    fn from(s: String) -> Self {
        AttrKey(Cow::from(s))
    }
}
impl AsRef<str> for AttrKey {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}
impl From<AttrKey> for String {
    fn from(k: AttrKey) -> Self {
        match k.0 {
            Cow::Borrowed(b) => b.to_owned(),
            Cow::Owned(o) => o,
        }
    }
}

pub mod mutator {
    use super::AttrKey;
    use std::borrow::Cow;

    pub const ID: AttrKey = AttrKey(Cow::Borrowed("mutator.id"));
    pub const NAME: AttrKey = AttrKey(Cow::Borrowed("mutator.name"));
    pub const DESCRIPTION: AttrKey = AttrKey(Cow::Borrowed("mutator.description"));
    pub const LAYER: AttrKey = AttrKey(Cow::Borrowed("mutator.layer"));
    pub const GROUP: AttrKey = AttrKey(Cow::Borrowed("mutator.group"));
    pub const STATEFULNESS: AttrKey = AttrKey(Cow::Borrowed("mutator.statefulness"));
    pub const OPERATION: AttrKey = AttrKey(Cow::Borrowed("mutator.operation"));
    pub const SAFETY: AttrKey = AttrKey(Cow::Borrowed("mutator.safety"));
    pub const SOURCE_FILE: AttrKey = AttrKey(Cow::Borrowed("mutator.source.file"));
    pub const SOURCE_LINE: AttrKey = AttrKey(Cow::Borrowed("mutator.source.line"));

    pub const MUTATION_EDGE_ID: AttrKey = AttrKey(Cow::Borrowed("mutator.mutation_edge_id"));
    pub const RECEIVE_TIME: AttrKey = AttrKey(Cow::Borrowed("mutator.receive_time"));
}

#[derive(Clone, Debug, PartialEq)]
pub enum AttrVal {
    TimelineId(Box<TimelineId>),
    EventCoordinate(Box<EventCoordinate>),
    String(Cow<'static, str>),
    Integer(i64),
    BigInt(BigInt),
    Float(f64),
    Bool(bool),
    Timestamp(Nanoseconds),
    LogicalTime(LogicalTime),
}

impl AttrVal {
    pub fn attr_type(&self) -> AttrType {
        match self {
            AttrVal::TimelineId(_) => AttrType::TimelineId,
            AttrVal::EventCoordinate(_) => AttrType::EventCoordinate,
            AttrVal::String(_) => AttrType::String,
            AttrVal::Integer(_) => AttrType::Integer,
            AttrVal::BigInt(_) => AttrType::BigInt,
            AttrVal::Float(_) => AttrType::Float,
            AttrVal::Bool(_) => AttrType::Bool,
            AttrVal::Timestamp(_) => AttrType::Nanoseconds,
            AttrVal::LogicalTime(_) => AttrType::LogicalTime,
        }
    }

    pub fn as_timeline_id(self) -> std::result::Result<TimelineId, WrongAttrTypeError> {
        self.try_into()
    }

    pub fn as_event_coordinate(self) -> std::result::Result<EventCoordinate, WrongAttrTypeError> {
        self.try_into()
    }

    pub fn as_string(self) -> std::result::Result<Cow<'static, str>, WrongAttrTypeError> {
        self.try_into()
    }

    pub fn as_int(self) -> std::result::Result<i64, WrongAttrTypeError> {
        self.try_into()
    }

    pub fn as_bigint(self) -> std::result::Result<i128, WrongAttrTypeError> {
        self.try_into()
    }

    pub fn as_float(self) -> std::result::Result<f64, WrongAttrTypeError> {
        self.try_into()
    }

    pub fn as_bool(self) -> std::result::Result<bool, WrongAttrTypeError> {
        self.try_into()
    }

    pub fn as_timestamp(self) -> std::result::Result<Nanoseconds, WrongAttrTypeError> {
        self.try_into()
    }

    pub fn as_logical_time(self) -> std::result::Result<LogicalTime, WrongAttrTypeError> {
        self.try_into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct TimelineId(pub Uuid);

impl From<Uuid> for TimelineId {
    fn from(u: Uuid) -> Self {
        TimelineId(u)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct EventCoordinate {
    pub timeline_id: TimelineId,
    pub id: OpaqueEventId,
}
impl EventCoordinate {
    pub fn as_bytes(&self) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[0..16].copy_from_slice(self.timeline_id.0.as_bytes());
        bytes[16..32].copy_from_slice(&self.id);
        bytes
    }

    pub fn from_byte_slice(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 32 {
            return None;
        }

        Some(EventCoordinate {
            timeline_id: Uuid::from_slice(&bytes[0..16]).ok()?.into(),
            id: bytes[16..32].try_into().ok()?,
        })
    }
}

pub type OpaqueEventId = [u8; 16];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BigInt(pub Box<i128>);

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
        self.0.fmt(f)
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

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Nanoseconds(pub u64);

impl From<u64> for Nanoseconds {
    fn from(v: u64) -> Self {
        Nanoseconds(v)
    }
}

#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, PartialOrd, Ord)]
pub enum AttrType {
    TimelineId,
    EventCoordinate,
    String,
    Integer,
    BigInt,
    Float,
    Bool,
    Nanoseconds,
    LogicalTime,
    Any,
}

impl std::fmt::Display for AttrType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttrType::TimelineId => "TimelineId",
            AttrType::String => "String",
            AttrType::Integer => "Integer",
            AttrType::BigInt => "BigInteger",
            AttrType::Float => "Float",
            AttrType::Bool => "Bool",
            AttrType::Nanoseconds => "Nanoseconds",
            AttrType::LogicalTime => "LogicalTime",
            AttrType::Any => "Any",
            AttrType::EventCoordinate => "Coordinate",
        }
        .fmt(f)
    }
}

pub mod conversion {
    use std::convert::TryFrom;

    use super::*;

    impl From<TimelineId> for AttrVal {
        fn from(timeline_id: TimelineId) -> Self {
            AttrVal::TimelineId(Box::new(timeline_id))
        }
    }

    impl From<EventCoordinate> for AttrVal {
        fn from(coord: EventCoordinate) -> Self {
            AttrVal::EventCoordinate(Box::new(coord))
        }
    }

    impl From<&str> for AttrVal {
        fn from(s: &str) -> Self {
            AttrVal::String(Cow::from(s.to_owned()))
        }
    }
    impl From<Cow<'static, str>> for AttrVal {
        fn from(s: Cow<'static, str>) -> Self {
            AttrVal::String(s)
        }
    }

    impl From<String> for AttrVal {
        fn from(s: String) -> Self {
            AttrVal::String(s.into())
        }
    }

    impl From<&String> for AttrVal {
        fn from(s: &String) -> Self {
            AttrVal::String(s.clone().into())
        }
    }

    impl From<Nanoseconds> for AttrVal {
        fn from(ns: Nanoseconds) -> Self {
            AttrVal::Timestamp(ns)
        }
    }

    macro_rules! impl_from_integer {
        ($ty:ty) => {
            impl From<$ty> for AttrVal {
                fn from(i: $ty) -> Self {
                    AttrVal::Integer(i as i64)
                }
            }
        };
    }

    impl_from_integer!(i8);
    impl_from_integer!(i16);
    impl_from_integer!(i32);
    impl_from_integer!(i64);
    impl_from_integer!(u8);
    impl_from_integer!(u16);
    impl_from_integer!(u32);

    macro_rules! impl_from_bigint {
        ($ty:ty) => {
            impl From<$ty> for AttrVal {
                fn from(i: $ty) -> Self {
                    BigInt::new_attr_val(i as i128)
                }
            }
        };
    }

    impl_from_bigint!(u64);
    impl_from_bigint!(i128);

    macro_rules! impl_from_float {
        ($ty:ty) => {
            impl From<$ty> for AttrVal {
                fn from(f: $ty) -> Self {
                    AttrVal::Float((f as f64).into())
                }
            }
        };
    }

    impl_from_float!(f32);
    impl_from_float!(f64);

    impl From<bool> for AttrVal {
        fn from(b: bool) -> Self {
            AttrVal::Bool(b)
        }
    }

    impl From<LogicalTime> for AttrVal {
        fn from(t: LogicalTime) -> Self {
            AttrVal::LogicalTime(t)
        }
    }

    macro_rules! impl_try_from_attr_val {
        ($variant:path, $ty:ty, $expected:path) => {
            impl TryFrom<AttrVal> for $ty {
                type Error = WrongAttrTypeError;

                fn try_from(value: AttrVal) -> std::result::Result<Self, Self::Error> {
                    if let $variant(x) = value {
                        Ok(x.into())
                    } else {
                        Err(WrongAttrTypeError {
                            actual: value.attr_type(),
                            expected: $expected,
                        })
                    }
                }
            }
        };
    }

    macro_rules! impl_try_from_attr_val_deref {
        ($variant:path, $ty:ty, $expected:path) => {
            impl TryFrom<AttrVal> for $ty {
                type Error = WrongAttrTypeError;

                fn try_from(value: AttrVal) -> std::result::Result<Self, Self::Error> {
                    if let $variant(x) = value {
                        Ok((*x).clone())
                    } else {
                        Err(WrongAttrTypeError {
                            actual: value.attr_type(),
                            expected: $expected,
                        })
                    }
                }
            }
        };
    }

    impl_try_from_attr_val_deref!(AttrVal::TimelineId, TimelineId, AttrType::TimelineId);
    impl_try_from_attr_val_deref!(
        AttrVal::EventCoordinate,
        EventCoordinate,
        AttrType::EventCoordinate
    );

    impl_try_from_attr_val!(AttrVal::Integer, i64, AttrType::Integer);
    impl_try_from_attr_val!(AttrVal::String, Cow<'static, str>, AttrType::String);
    impl_try_from_attr_val_deref!(AttrVal::BigInt, i128, AttrType::BigInt);
    impl_try_from_attr_val!(AttrVal::Float, f64, AttrType::Float);
    impl_try_from_attr_val!(AttrVal::Bool, bool, AttrType::Bool);
    impl_try_from_attr_val!(AttrVal::LogicalTime, LogicalTime, AttrType::LogicalTime);
    impl_try_from_attr_val!(AttrVal::Timestamp, Nanoseconds, AttrType::Nanoseconds);
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[error("Wrong attribute type: expected {expected:?}, found {actual:?}")]
pub struct WrongAttrTypeError {
    actual: AttrType,
    expected: AttrType,
}

/// A segmented logical clock
#[derive(Eq, PartialEq, Clone, Debug, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (a, b) in self.0.iter().zip(other.0.iter()) {
            match a.cmp(b) {
                std::cmp::Ordering::Equal => (), // continue to later segments
                std::cmp::Ordering::Less => return std::cmp::Ordering::Less,
                std::cmp::Ordering::Greater => return std::cmp::Ordering::Greater,
            }
        }

        std::cmp::Ordering::Equal
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
