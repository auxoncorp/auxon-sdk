use crate::api::types::{
    AttrVal, EventCoordinate, LogicalTime, Nanoseconds, OpaqueEventId, TimelineId,
};
use proptest::prelude::*;
use std::borrow::Cow;
use uuid::Uuid;

pub fn attr_val() -> impl Strategy<Value = AttrVal> {
    prop_oneof![
        timeline_id().prop_map_into(),
        cow_string().prop_map_into(),
        any::<i64>().prop_map_into(),
        any::<i128>().prop_map_into(),
        any::<f64>().prop_map_into(),
        any::<bool>().prop_map_into(),
        nanoseconds().prop_map_into(),
        logical_time().prop_map_into(),
    ]
}

pub fn timeline_id() -> impl Strategy<Value = TimelineId> {
    any::<[u8; 16]>().prop_map(|arr| Uuid::from_bytes(arr).into())
}

pub fn cow_string() -> impl Strategy<Value = Cow<'static, str>> {
    any::<String>().prop_map(|s| s.into())
}

pub fn nanoseconds() -> impl Strategy<Value = Nanoseconds> {
    any::<u64>().prop_map_into()
}

pub fn logical_time() -> impl Strategy<Value = LogicalTime> {
    any::<(u64, u64, u64, u64)>().prop_map(|(a, b, c, d)| LogicalTime::quaternary(a, b, c, d))
}

pub fn opaque_event_id() -> impl Strategy<Value = OpaqueEventId> {
    any::<[u8; 16]>()
}

pub fn event_coordinate() -> impl Strategy<Value = EventCoordinate> {
    (timeline_id(), opaque_event_id()).prop_map(|(timeline_id, opaque_event_id)| EventCoordinate {
        timeline_id,
        id: opaque_event_id,
    })
}
