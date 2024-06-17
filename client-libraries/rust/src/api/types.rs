use std::{borrow::Cow, cmp::Ordering, ops::Deref, str::FromStr};

use ordered_float::OrderedFloat;
pub use uuid::Uuid;

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct AttrKey(Cow<'static, str>);

impl AttrKey {
    pub const fn new(k: String) -> Self {
        Self(Cow::Owned(k))
    }

    pub const fn new_static(k: &'static str) -> Self {
        Self(Cow::Borrowed(k))
    }
}

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

impl std::fmt::Display for AttrKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

////////////
// BigInt //
////////////

/// Newtype wrapper to get correct-by-construction promises
/// about minimal AttrVal variant selection.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
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

/////////////////
// Nanoseconds //
/////////////////

/// A timestamp in nanoseconds
#[derive(
    Copy, Clone, Eq, PartialEq, Debug, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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

impl FromStr for Nanoseconds {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Nanoseconds(s.parse::<u64>()?))
    }
}

/////////////////
// LogicalTime //
/////////////////

/// A segmented logical clock
#[derive(Eq, PartialEq, Clone, Debug, Hash, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
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

#[cfg(feature = "pyo3")]
#[pyo3::pymethods]
impl LogicalTime {
    #[staticmethod]
    #[pyo3(name = "unary")]
    pub fn unary_py(a: u64) -> Self {
        LogicalTime(Box::new([0, 0, 0, a]))
    }

    #[staticmethod]
    #[pyo3(name = "binary")]
    pub fn binary_py(a: u64, b: u64) -> Self {
        LogicalTime(Box::new([0, 0, a, b]))
    }

    #[staticmethod]
    #[pyo3(name = "trinary")]
    pub fn trinary_py(a: u64, b: u64, c: u64) -> Self {
        LogicalTime(Box::new([0, a, b, c]))
    }

    #[staticmethod]
    #[pyo3(name = "quaternary")]
    pub fn quaternary_py(a: u64, b: u64, c: u64, d: u64) -> Self {
        LogicalTime(Box::new([a, b, c, d]))
    }

    pub fn as_tuple(&self) -> (u64, u64, u64, u64) {
        (self.0[0], self.0[1], self.0[2], self.0[3])
    }

    pub fn as_array(&self) -> [u64; 4] {
        *(self.0)
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

impl FromStr for LogicalTime {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut segments = s.rsplit(':');

        if let Ok(mut time) = segments.try_fold(Vec::new(), |mut acc, segment| {
            segment.parse::<u64>().map(|t| {
                acc.insert(0, t);
                acc
            })
        }) {
            while time.len() < 4 {
                time.insert(0, 0)
            }

            let time_array = time.into_boxed_slice().try_into().map_err(|_| ())?;

            Ok(LogicalTime(time_array))
        } else {
            Err(())
        }
    }
}

////////////////
// TimelineId //
////////////////

pub const TIMELINE_ID_SIGIL: char = '%';

/// Timelines are identified by a UUID. These are timeline *instances*; a given location (identified
/// by its name) is associated with many timelines.
#[derive(
    Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Debug, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct TimelineId(Uuid);

#[cfg(feature = "pyo3")]
#[pyo3::pymethods]
impl TimelineId {
    #[staticmethod]
    pub fn zero_py() -> Self {
        TimelineId(Uuid::nil())
    }

    #[staticmethod]
    #[pyo3(name = "allocate")]
    pub fn allocate_py() -> Self {
        TimelineId(Uuid::new_v4())
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    fn __hash__(&self) -> u64 {
        use std::hash::Hash as _;
        use std::hash::Hasher as _;
        let mut hasher = std::hash::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

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

/////////////////////
// EventCoordinate //
/////////////////////

pub type OpaqueEventId = [u8; 16];

#[derive(
    Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
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

impl std::fmt::Display for EventCoordinate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{TIMELINE_ID_SIGIL}")?;

        // print the uuid as straight hex, for compactness
        for byte in self.timeline_id.0.as_bytes() {
            write!(f, "{byte:02x}")?;
        }

        write!(f, ":{}", EncodeHexWithoutLeadingZeroes(&self.id))
    }
}

pub struct EncodeHexWithoutLeadingZeroes<'a>(pub &'a [u8]);

impl<'a> std::fmt::Display for EncodeHexWithoutLeadingZeroes<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut cursor = 0;
        let bytes = self.0;
        while bytes[cursor] == 0 && cursor < bytes.len() - 1 {
            cursor += 1;
        }

        if cursor == bytes.len() {
            write!(f, "0")?;
        } else {
            for byte in bytes.iter().skip(cursor) {
                write!(f, "{byte:02x}")?;
            }
        }

        Ok(())
    }
}

/////////////
// AttrVal //
/////////////

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AttrVal {
    TimelineId(Box<TimelineId>),
    EventCoordinate(Box<EventCoordinate>),
    String(Cow<'static, str>),
    Integer(i64),
    BigInt(BigInt),
    Float(OrderedFloat<f64>),
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

impl std::fmt::Display for AttrVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttrVal::String(s) => s.fmt(f),
            AttrVal::Integer(i) => i.fmt(f),
            AttrVal::BigInt(bi) => bi.fmt(f),
            AttrVal::Float(fp) => fp.fmt(f),
            AttrVal::Bool(b) => b.fmt(f),
            AttrVal::Timestamp(ns) => ns.fmt(f),
            AttrVal::LogicalTime(lt) => lt.fmt(f),
            AttrVal::EventCoordinate(ec) => ec.fmt(f),
            AttrVal::TimelineId(tid) => tid.fmt(f),
        }
    }
}

impl FromStr for AttrVal {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // N.B. Eventually we will want  parsing that is informed by the AttrKey, that will allow
        // us to parse things like `AttrVal::Timestamp` or a uniary `AttrVal::LogicalTime` which
        // are both currently parsed as (Big)Int
        Ok(if let Ok(v) = s.to_lowercase().parse::<bool>() {
            v.into()
        } else if let Ok(v) = s.parse::<i128>() {
            // this will decide if the number should be `Integer` or `BigInt` based on value
            v.into()
        } else if let Ok(v) = s.parse::<f64>() {
            v.into()
        } else if let Ok(v) = s.parse::<LogicalTime>() {
            v.into()
        } else if let Ok(v) = s.parse::<Uuid>() {
            v.into()
        } else {
            // N.B. This will trim any number of leading and trailing single or double quotes, It
            // does not have any ability to escape quote marks.
            AttrVal::String(s.trim_matches(|c| c == '"' || c == '\'').to_owned().into())
        })
    }
}

impl From<String> for AttrVal {
    fn from(s: String) -> AttrVal {
        AttrVal::String(Cow::Owned(s))
    }
}

impl From<&str> for AttrVal {
    fn from(s: &str) -> AttrVal {
        AttrVal::String(Cow::Owned(s.to_owned()))
    }
}

impl From<Cow<'static, str>> for AttrVal {
    fn from(s: Cow<'static, str>) -> Self {
        AttrVal::String(s)
    }
}

impl From<&String> for AttrVal {
    fn from(s: &String) -> Self {
        AttrVal::String(Cow::Owned(s.clone()))
    }
}

impl From<bool> for AttrVal {
    fn from(b: bool) -> AttrVal {
        AttrVal::Bool(b)
    }
}

impl From<Nanoseconds> for AttrVal {
    fn from(ns: Nanoseconds) -> AttrVal {
        AttrVal::Timestamp(ns)
    }
}

impl From<LogicalTime> for AttrVal {
    fn from(lt: LogicalTime) -> AttrVal {
        AttrVal::LogicalTime(lt)
    }
}

impl From<Uuid> for AttrVal {
    fn from(u: Uuid) -> AttrVal {
        AttrVal::TimelineId(Box::new(u.into()))
    }
}

impl From<EventCoordinate> for AttrVal {
    fn from(coord: EventCoordinate) -> Self {
        AttrVal::EventCoordinate(Box::new(coord))
    }
}

impl From<TimelineId> for AttrVal {
    fn from(timeline_id: TimelineId) -> Self {
        AttrVal::TimelineId(Box::new(timeline_id))
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

#[cfg(feature = "pyo3")]
impl<'py> pyo3::FromPyObject<'py> for AttrVal {
    fn extract_bound(
        ob: &pyo3::prelude::Bound<'py, pyo3::prelude::PyAny>,
    ) -> pyo3::prelude::PyResult<Self> {
        use pyo3::prelude::*;

        // Check the most common types first
        if let Ok(i) = ob.extract::<i64>() {
            return Ok(i.into());
        }

        if let Ok(f) = ob.extract::<f64>() {
            return Ok(f.into());
        }

        if let Ok(s) = ob.extract::<String>() {
            return Ok(s.into());
        }

        if let Ok(b) = ob.extract::<bool>() {
            return Ok(b.into());
        }

        if let Ok(ts) = ob.extract::<std::time::SystemTime>() {
            match ts.duration_since(std::time::UNIX_EPOCH) {
                Ok(dur) => {
                    let ns = dur.as_nanos();
                    if ns > u64::MAX as u128 {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "Timestamp value too large",
                        ));
                    } else {
                        return Ok(Nanoseconds::from(ns as u64).into());
                    }
                }
                Err(_) => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "Timestamp before UNIX epoch",
                    ));
                }
            }
        }

        // Less common types are checked later
        if let Ok(tl_id) = ob.extract::<TimelineId>() {
            return Ok(tl_id.0.into());
        }

        if let Ok(lt) = ob.extract::<LogicalTime>() {
            return Ok(lt.into());
        }

        if let Ok(ec) = ob.extract::<EventCoordinate>() {
            return Ok(ec.into());
        }

        if let Ok(i) = ob.extract::<i128>() {
            return Ok(i.into());
        }

        if let Ok(id) = ob.extract::<crate::mutation_plane::types::MutationId>() {
            return Ok(i128::from_le_bytes(Uuid::from(id).into_bytes()).into());
        }

        if let Ok(id) = ob.extract::<crate::mutation_plane::types::MutatorId>() {
            return Ok(i128::from_le_bytes(Uuid::from(id).into_bytes()).into());
        }

        Err(pyo3::exceptions::PyValueError::new_err(
            "Cannot represent value as AttrVal",
        ))
    }
}

#[cfg(feature = "pyo3")]
impl pyo3::IntoPy<pyo3::PyObject> for AttrVal {
    fn into_py(self, py: pyo3::prelude::Python<'_>) -> pyo3::PyObject {
        match self {
            AttrVal::TimelineId(tid) => tid.into_py(py),
            AttrVal::EventCoordinate(ec) => ec.into_py(py),
            AttrVal::String(s) => s.into_py(py),
            AttrVal::Integer(i) => i.into_py(py),
            AttrVal::BigInt(bi) => bi.into_py(py),
            AttrVal::Float(f) => f.into_py(py),
            AttrVal::Bool(b) => b.into_py(py),
            AttrVal::Timestamp(ns) => {
                (std::time::UNIX_EPOCH + std::time::Duration::from_nanos(ns.get_raw())).into_py(py)
            }
            AttrVal::LogicalTime(lt) => lt.into_py(py),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_logical_time() {
        let reference = Ok(LogicalTime::quaternary(0u64, 0u64, 0u64, 42u64));

        // should parse
        assert_eq!(reference, "42".parse());
        assert_eq!(reference, "0:42".parse());
        assert_eq!(reference, "0:0:42".parse());
        assert_eq!(reference, "0:0:0:42".parse());

        // should not parse
        assert_eq!(Err(()), ":".parse::<LogicalTime>());
        assert_eq!(Err(()), "::".parse::<LogicalTime>());
        assert_eq!(Err(()), ":0".parse::<LogicalTime>());
        assert_eq!(Err(()), "0:".parse::<LogicalTime>());
        assert_eq!(Err(()), "127.0.0.1:8080".parse::<LogicalTime>());
        assert_eq!(Err(()), "localhost:8080".parse::<LogicalTime>());
        assert_eq!(Err(()), "example.com:8080".parse::<LogicalTime>());
    }

    #[test]
    fn parse_attr_vals() {
        // Bool
        assert_eq!(Ok(AttrVal::Bool(false)), "false".parse());
        assert_eq!(Ok(AttrVal::Bool(true)), "true".parse());

        // Integer
        assert_eq!(Ok(AttrVal::Integer(37)), "37".parse());

        // BigInt
        assert_eq!(
            Ok(BigInt::new_attr_val(36893488147419103232i128)),
            "36893488147419103232".parse()
        );

        // Float
        assert_eq!(Ok(AttrVal::Float(76.37f64.into())), "76.37".parse());

        // TimelineId
        assert_eq!(
            Ok(AttrVal::TimelineId(Box::new(
                Uuid::parse_str("bec14bc0-1dea-4b68-b138-62f7b6827e35")
                    .unwrap()
                    .into()
            ))),
            "bec14bc0-1dea-4b68-b138-62f7b6827e35".parse()
        );

        // Timestamp
        // N.B. This is impossible to parse as an `AttrVal` since it's just a number which will
        // have already been parsed as a (Big)Int. Could try parsing more complex date strings?

        // LogicalTime
        // N.B. There is no way to specify a single segment logical time, again it will have
        // already been parsed as a (Big)Int, try 2, 3, and 4 segment
        let lt_ref = Ok(AttrVal::LogicalTime(LogicalTime::quaternary(
            0u64, 0u64, 0u64, 42u64,
        )));
        assert_eq!(lt_ref, "0:42".parse());
        assert_eq!(lt_ref, "0:0:42".parse());
        assert_eq!(lt_ref, "0:0:0:42".parse());

        // String
        assert_eq!(
            Ok(AttrVal::String("Hello, World!".into())),
            "\"Hello, World!\"".parse()
        );
        assert_eq!(
            Ok(AttrVal::String("Hello, World!".into())),
            "'Hello, World!'".parse()
        );
        assert_eq!(
            Ok(AttrVal::String("Hello, World!".into())),
            "Hello, World!".parse()
        );

        assert_eq!(Ok(AttrVal::String("".into())), "\"\"".parse());
        assert_eq!(Ok(AttrVal::String("".into())), "\"".parse());

        assert_eq!(Ok(AttrVal::String("".into())), "''".parse());
        assert_eq!(Ok(AttrVal::String("".into())), "'".parse());

        assert_eq!(Ok(AttrVal::String("".into())), "".parse());
    }
}
