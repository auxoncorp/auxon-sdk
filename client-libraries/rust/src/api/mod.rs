//! The base types used throughout Auxon's SDK

pub mod types;
pub use types::*;

#[cfg(feature = "modality")]
pub mod protocol;

mod serde;

#[cfg(any(test, feature = "test_support"))]
pub mod proptest_strategies;
