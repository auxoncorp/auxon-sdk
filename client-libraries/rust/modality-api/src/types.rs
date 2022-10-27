use std::{cmp::Ordering, ops::Deref, str::FromStr};

use ordered_float::OrderedFloat;
pub use uuid::Uuid;

/// They key naming an attribute.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct AttrKey(pub(crate) String);

impl AttrKey {
    pub fn new(k: String) -> Self {
        Self(k)
    }
}

impl From<String> for AttrKey {
    fn from(s: String) -> AttrKey {
        AttrKey(s)
    }
}

impl From<AttrKey> for String {
    fn from(k: AttrKey) -> String {
        k.0
    }
}

impl AsRef<str> for AttrKey {
    fn as_ref(&self) -> &str {
        &self.0
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

/////////////
// AttrVal //
/////////////

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum AttrVal {
    TimelineId(Box<TimelineId>),
    String(String),
    Integer(i64),
    BigInt(BigInt),
    Float(OrderedFloat<f64>),
    Bool(bool),
    Timestamp(Nanoseconds),
    LogicalTime(LogicalTime),
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
            AttrVal::String(s.trim_matches(|c| c == '"' || c == '\'').into())
        })
    }
}

impl From<String> for AttrVal {
    fn from(s: String) -> AttrVal {
        AttrVal::String(s)
    }
}

impl From<&str> for AttrVal {
    fn from(s: &str) -> AttrVal {
        AttrVal::String(s.to_string())
    }
}

impl From<i64> for AttrVal {
    fn from(i: i64) -> AttrVal {
        AttrVal::Integer(i)
    }
}

impl From<i128> for AttrVal {
    fn from(i: i128) -> AttrVal {
        BigInt::new_attr_val(i)
    }
}

impl From<f64> for AttrVal {
    fn from(f: f64) -> AttrVal {
        AttrVal::Float(f.into())
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
