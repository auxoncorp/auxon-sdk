use crate::api::AttrVal;
use std::hash::Hash;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, Copy, Clone, PartialOrd, Ord, Hash)]
pub struct ParticipantId(Uuid);

#[derive(Debug, PartialEq, Eq, Copy, Clone, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct MutatorId(Uuid);

#[derive(Debug, PartialEq, Eq, Copy, Clone, Hash)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct MutationId(Uuid);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TriggerCRDT(Vec<u8>);

impl TriggerCRDT {
    pub fn new(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl AsRef<[u8]> for TriggerCRDT {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<T> From<T> for TriggerCRDT
where
    T: Iterator<Item = u8>,
{
    fn from(t: T) -> Self {
        Self(t.collect())
    }
}

impl From<TriggerCRDT> for Vec<u8> {
    fn from(v: TriggerCRDT) -> Self {
        v.0
    }
}

impl ParticipantId {
    pub fn allocate() -> Self {
        Self(Uuid::new_v4())
    }
}

impl From<Uuid> for ParticipantId {
    fn from(v: Uuid) -> Self {
        Self(v)
    }
}

impl From<ParticipantId> for Uuid {
    fn from(v: ParticipantId) -> Self {
        v.0
    }
}

impl MutatorId {
    pub fn allocate() -> Self {
        Self(Uuid::new_v4())
    }
}

impl From<Uuid> for MutatorId {
    fn from(v: Uuid) -> Self {
        Self(v)
    }
}

impl From<MutatorId> for Uuid {
    fn from(v: MutatorId) -> Self {
        v.0
    }
}

impl From<Uuid> for MutationId {
    fn from(v: Uuid) -> Self {
        Self(v)
    }
}

impl From<MutationId> for Uuid {
    fn from(v: MutationId) -> Self {
        v.0
    }
}

impl AsRef<Uuid> for ParticipantId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

impl AsRef<Uuid> for MutatorId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

impl AsRef<Uuid> for MutationId {
    fn as_ref(&self) -> &Uuid {
        &self.0
    }
}

impl std::fmt::Display for ParticipantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for MutatorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for MutationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, PartialEq, Clone, minicbor::Encode, minicbor::Decode)]
#[cbor(transparent)]
pub struct AttrKvs(#[n(0)] pub Vec<AttrKv>);

#[derive(Debug, PartialEq, Clone, minicbor::Encode, minicbor::Decode)]
pub struct AttrKv {
    #[n(0)]
    pub key: String,

    #[n(1)]
    pub value: AttrVal,
}

#[cfg(feature = "pyo3")]
#[pyo3::pymethods]
impl MutatorId {
    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    fn __hash__(&self) -> u64 {
        use std::hash::Hasher as _;
        let mut hasher = std::hash::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(feature = "pyo3")]
#[pyo3::pymethods]
impl MutationId {
    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    fn __hash__(&self) -> u64 {
        use std::hash::Hasher as _;
        let mut hasher = std::hash::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}
