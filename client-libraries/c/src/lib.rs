#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc, clippy::not_unsafe_ptr_arg_deref)]

pub(crate) mod error;
pub(crate) mod ingest;
pub(crate) mod mutation;
pub(crate) mod rt;
pub(crate) mod tracing;
pub(crate) mod types;
pub(crate) mod util;

pub(crate) use error::{capi_result, Error, NullPtrExt};
pub(crate) use rt::runtime;
pub(crate) use types::{attr_val, timeline_id};
