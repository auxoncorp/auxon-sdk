pub mod options;

use crate::{
    api::{AttrVal, BigInt, LogicalTime, Nanoseconds, TimelineId, Uuid},
    ingest_client::{BoundTimelineState, IngestClient, IngestError as SdkIngestError},
    ingest_protocol::InternedAttrKey,
};
use anyhow::Context;
use once_cell::sync::Lazy;
use std::{
    borrow::Borrow,
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::RwLock,
};
use thiserror::Error;
use tracing_serde_structured::{
    DebugRecord, RecordMap, SerializeId, SerializeMetadata, SerializeRecord, SerializeRecordFields,
    SerializeValue,
};
use tracing_serde_wire::{Packet, TWOther, TracingWire};

pub use options::Options;

// spans can be defined on any thread and then sent to another and entered/etc, track globally
static SPAN_NAMES: Lazy<RwLock<HashMap<u64, String>>> = Lazy::new(|| RwLock::new(HashMap::new()));

#[derive(Debug, Error)]
pub enum ConnectError {
    /// No auth was provided
    #[error("Authentication required")]
    AuthRequired,
    /// Auth was provided, but was not accepted by modality
    #[error("Authenticating with the provided auth failed")]
    AuthFailed(SdkIngestError),
    /// Errors that it is assumed there is no way to handle without human intervention, meant for
    /// consumers to just print and carry on or panic.
    #[error(transparent)]
    UnexpectedFailure(#[from] anyhow::Error),
}

#[derive(Debug, Error)]
pub enum IngestError {
    /// Errors that it is assumed there is no way to handle without human intervention, meant for
    /// consumers to just print and carry on or panic.
    #[error(transparent)]
    UnexpectedFailure(#[from] anyhow::Error),
}

pub struct TracingModality {
    client: IngestClient<BoundTimelineState>,
    event_keys: HashMap<String, InternedAttrKey>,
    timeline_keys: HashMap<String, InternedAttrKey>,
    timeline_id: TimelineId,
}

impl TracingModality {
    pub async fn connect() -> Result<Self, ConnectError> {
        let opt = Options::default();

        Self::connect_with_options(opt).await
    }

    pub async fn connect_with_options(options: Options) -> Result<Self, ConnectError> {
        let url = url::Url::parse(&format!("modality-ingest://{}/", options.server_addr)).unwrap();
        let unauth_client = IngestClient::connect(&url, false)
            .await
            .context("init ingest client")?;

        let auth_key = options.auth.ok_or(ConnectError::AuthRequired)?;
        let client = unauth_client
            .authenticate(auth_key)
            .await
            .map_err(ConnectError::AuthFailed)?;

        let timeline_id = TimelineId::allocate();

        let client = client
            .open_timeline(timeline_id)
            .await
            .context("open new timeline")?;

        let mut tracer = Self {
            client,
            event_keys: HashMap::new(),
            timeline_keys: HashMap::new(),
            timeline_id,
        };

        for (key, value) in options.metadata {
            let timeline_key_name = tracer
                .get_or_create_timeline_attr_key(key)
                .await
                .context("get or define timeline attr key")?;

            tracer
                .client
                .timeline_metadata([(timeline_key_name, value)])
                .await
                .context("apply timeline metadata")?;
        }

        Ok(tracer)
    }

    pub fn timeline_id(&self) -> TimelineId {
        self.timeline_id
    }

    pub async fn handle_packet<'a>(&mut self, pkt: Packet<'_>) -> Result<(), IngestError> {
        match pkt.message {
            TracingWire::NewSpan { id, attrs, values } => {
                let mut records = match values {
                    SerializeRecord::Ser(_event) => {
                        unreachable!("this variant can't be sent")
                    }
                    SerializeRecord::De(record_map) => record_map,
                };

                let name = {
                    // store name for future use
                    let name = records
                        .get(&"name".into())
                        .or_else(|| records.get(&"message".into()))
                        .map(|n| format!("{:?}", n))
                        .unwrap_or_else(|| attrs.metadata.name.to_string());

                    SPAN_NAMES
                        .write()
                        .expect("span name lock poisoned, this is a bug")
                        .deref_mut()
                        .insert(id.id.get(), name.clone());

                    name
                };

                let mut packed_attrs = Vec::new();

                packed_attrs.push((
                    self.get_or_create_event_attr_key("event.name".to_string())
                        .await?,
                    AttrVal::String(name.into()),
                ));

                let kind = records
                    .remove(&"modality.kind".into())
                    .and_then(tracing_value_to_attr_val)
                    .unwrap_or_else(|| "span:defined".into());
                packed_attrs.push((
                    self.get_or_create_event_attr_key("event.internal.rs.kind".to_string())
                        .await?,
                    kind,
                ));

                let span_id = records
                    .remove(&"modality.span_id".into())
                    .and_then(tracing_value_to_attr_val)
                    .unwrap_or_else(|| BigInt::new_attr_val(id.id.get() as i128));
                packed_attrs.push((
                    self.get_or_create_event_attr_key("event.internal.rs.span_id".to_string())
                        .await?,
                    span_id,
                ));

                self.pack_common_attrs(&mut packed_attrs, attrs.metadata, records, pkt.tick)
                    .await?;

                self.client
                    .event(pkt.tick.into(), packed_attrs)
                    .await
                    .context("send packed event")?;
            }
            TracingWire::Record { .. } => {
                // TODO: span events can't be added to after being sent, impl this once we can use
                // timelines to represent spans
            }
            TracingWire::RecordFollowsFrom { .. } => {
                // TODO: span events can't be added to after being sent, impl this once we can use
                // timelines to represent spans
            }
            TracingWire::Event(ev) => {
                let mut packed_attrs = Vec::new();

                let mut records = match ev.fields {
                    SerializeRecordFields::Ser(_event) => {
                        unreachable!("this variant can't be sent")
                    }
                    SerializeRecordFields::De(record_map) => record_map,
                };

                let kind = records
                    .remove(&"modality.kind".into())
                    .and_then(tracing_value_to_attr_val)
                    .unwrap_or_else(|| "event".into());
                packed_attrs.push((
                    self.get_or_create_event_attr_key("event.internal.rs.kind".to_string())
                        .await?,
                    kind,
                ));

                self.pack_common_attrs(&mut packed_attrs, ev.metadata, records, pkt.tick)
                    .await?;

                self.client
                    .event(pkt.tick.into(), packed_attrs)
                    .await
                    .context("send packed event")?;
            }
            TracingWire::Enter(SerializeId { id }) => {
                let mut packed_attrs = Vec::new();

                {
                    // get stored span name
                    let name = SPAN_NAMES
                        .read()
                        .expect("span name lock poisoned, this is a bug")
                        .deref()
                        .get(&id.get())
                        .map(|n| format!("enter: {}", n));

                    if let Some(name) = name {
                        packed_attrs.push((
                            self.get_or_create_event_attr_key("event.name".to_string())
                                .await?,
                            AttrVal::String(name.into()),
                        ));
                    }
                };

                packed_attrs.push((
                    self.get_or_create_event_attr_key("event.internal.rs.kind".to_string())
                        .await?,
                    AttrVal::String("span:enter".to_string().into()),
                ));

                packed_attrs.push((
                    self.get_or_create_event_attr_key("event.internal.rs.span_id".to_string())
                        .await?,
                    BigInt::new_attr_val(u64::from(id).into()),
                ));

                packed_attrs.push((
                    self.get_or_create_event_attr_key("event.internal.rs.tick".to_string())
                        .await?,
                    AttrVal::LogicalTime(LogicalTime::unary(pkt.tick)),
                ));

                self.client
                    .event(pkt.tick.into(), packed_attrs)
                    .await
                    .context("send packed event")?;
            }
            TracingWire::Exit(SerializeId { id }) => {
                let mut packed_attrs = Vec::new();

                {
                    // get stored span name
                    let name = SPAN_NAMES
                        .read()
                        .expect("span name lock poisoned, this is a bug")
                        .deref()
                        .get(&id.get())
                        .map(|n| format!("exit: {}", n));

                    if let Some(name) = name {
                        packed_attrs.push((
                            self.get_or_create_event_attr_key("event.name".to_string())
                                .await?,
                            AttrVal::String(name.into()),
                        ));
                    }
                };

                packed_attrs.push((
                    self.get_or_create_event_attr_key("event.internal.rs.kind".to_string())
                        .await?,
                    AttrVal::String("span:exit".to_string().into()),
                ));

                packed_attrs.push((
                    self.get_or_create_event_attr_key("event.internal.rs.span_id".to_string())
                        .await?,
                    BigInt::new_attr_val(u64::from(id).into()),
                ));

                packed_attrs.push((
                    self.get_or_create_event_attr_key("event.internal.rs.tick".to_string())
                        .await?,
                    AttrVal::LogicalTime(LogicalTime::unary(pkt.tick)),
                ));

                self.client
                    .event(pkt.tick.into(), packed_attrs)
                    .await
                    .context("send packed event")?;
            }
            TracingWire::Close(SerializeId { id }) => {
                SPAN_NAMES
                    .write()
                    .expect("span name lock poisoned, this is a bug")
                    .deref_mut()
                    .remove(&id.get());
            }
            TracingWire::IdClone { old, new } => {
                let mut span_names = SPAN_NAMES
                    .write()
                    .expect("span name lock poisoned, this is a bug");

                let name = span_names.deref().get(&old.id.get()).cloned();
                if let Some(name) = name {
                    span_names.deref_mut().insert(new.id.get(), name);
                }
            }
            TracingWire::Other(two) => {
                match two {
                    TWOther::MessageDiscarded => {
                        let mut packed_attrs = Vec::new();

                        packed_attrs.push((
                            self.get_or_create_event_attr_key("event.internal.rs.kind".to_string())
                                .await?,
                            AttrVal::String("message_discarded".to_string().into()),
                        ));
                        self.client
                            .event(pkt.tick.into(), packed_attrs)
                            .await
                            .context("send packed event")?;
                    }
                    TWOther::DeviceInfo {
                        clock_id,
                        ticks_per_sec,
                        device_id,
                    } => {
                        let mut packed_attrs = Vec::new();
                        packed_attrs.push((
                            self.get_or_create_timeline_attr_key(
                                "timeline.internal.rs.clock_id".to_string(),
                            )
                            .await?,
                            AttrVal::Integer(clock_id.into()),
                        ));
                        packed_attrs.push((
                            self.get_or_create_timeline_attr_key(
                                "timeline.ticks_per_sec".to_string(),
                            )
                            .await?,
                            AttrVal::Integer(ticks_per_sec.into()),
                        ));
                        packed_attrs.push((
                            self.get_or_create_timeline_attr_key(
                                "timeline.internal.rs.device_id".to_string(),
                            )
                            .await?,
                            // TODO: this includes array syntax in the ID
                            AttrVal::String(format!("{:x?}", device_id).into()),
                        ));
                        self.client
                            .timeline_metadata(packed_attrs)
                            .await
                            .context("send packed timeline metadata")?;
                    }
                }
            }
            _ => (),
        }

        Ok(())
    }

    async fn get_or_create_timeline_attr_key(
        &mut self,
        key: String,
    ) -> Result<InternedAttrKey, IngestError> {
        if let Some(id) = self.timeline_keys.get(&key) {
            return Ok(*id);
        }

        let interned_key = self
            .client
            .declare_attr_key(key.clone())
            .await
            .context("define timeline attr key")?;

        self.timeline_keys.insert(key, interned_key);

        Ok(interned_key)
    }

    async fn get_or_create_event_attr_key(
        &mut self,
        key: String,
    ) -> Result<InternedAttrKey, IngestError> {
        let key = if key.starts_with("event.") {
            key
        } else {
            format!("event.{key}")
        };

        if let Some(id) = self.event_keys.get(&key) {
            return Ok(*id);
        }

        let interned_key = self
            .client
            .declare_attr_key(key.clone())
            .await
            .context("define event attr key")?;

        self.event_keys.insert(key, interned_key);

        Ok(interned_key)
    }

    async fn pack_common_attrs<'a>(
        &mut self,
        packed_attrs: &mut Vec<(InternedAttrKey, AttrVal)>,
        metadata: SerializeMetadata<'a>,
        mut records: RecordMap<'a>,
        tick: u64,
    ) -> Result<(), IngestError> {
        let name = records
            .remove(&"name".into())
            .or_else(|| records.remove(&"message".into()))
            .and_then(tracing_value_to_attr_val)
            .unwrap_or_else(|| metadata.name.as_str().into());
        packed_attrs.push((
            self.get_or_create_event_attr_key("event.name".to_string())
                .await?,
            name,
        ));

        let severity = records
            .remove(&"severity".into())
            .and_then(tracing_value_to_attr_val)
            .unwrap_or_else(|| format!("{:?}", metadata.level).to_lowercase().into());
        packed_attrs.push((
            self.get_or_create_event_attr_key("event.severity".to_string())
                .await?,
            severity,
        ));

        let module_path = records
            .remove(&"source.module".into())
            .and_then(tracing_value_to_attr_val)
            .or_else(|| metadata.module_path.map(|mp| mp.as_str().into()));
        if let Some(module_path) = module_path {
            packed_attrs.push((
                self.get_or_create_event_attr_key("event.source.module".to_string())
                    .await?,
                module_path,
            ));
        }

        let source_file = records
            .remove(&"source.file".into())
            .and_then(tracing_value_to_attr_val)
            .or_else(|| metadata.file.map(|mp| mp.as_str().into()));
        if let Some(source_file) = source_file {
            packed_attrs.push((
                self.get_or_create_event_attr_key("event.source.file".to_string())
                    .await?,
                source_file,
            ));
        }

        let source_line = records
            .remove(&"source.line".into())
            .and_then(tracing_value_to_attr_val)
            .or_else(|| metadata.line.map(|mp| (mp as i64).into()));
        if let Some(source_line) = source_line {
            packed_attrs.push((
                self.get_or_create_event_attr_key("event.source.line".to_string())
                    .await?,
                source_line,
            ));
        }

        packed_attrs.push((
            self.get_or_create_event_attr_key("event.internal.rs.tick".to_string())
                .await?,
            AttrVal::LogicalTime(LogicalTime::unary(tick)),
        ));

        // handle manually to type the AttrVal correctly
        let remote_timeline_id = records
            .remove(&"interaction.remote_timeline_id".into())
            .and_then(tracing_value_to_attr_val);
        if let Some(attrval) = remote_timeline_id {
            let remote_timeline_id = if let AttrVal::String(string) = attrval {
                use std::str::FromStr;
                if let Ok(uuid) = Uuid::from_str(&string) {
                    AttrVal::TimelineId(Box::new(uuid.into()))
                } else {
                    AttrVal::String(string)
                }
            } else {
                attrval
            };

            packed_attrs.push((
                self.get_or_create_event_attr_key("event.interaction.remote_timeline_id".into())
                    .await?,
                remote_timeline_id,
            ));
        }

        // Manually retype the remote_timestamp
        let remote_timestamp = records
            .remove(&"interaction.remote_timestamp".into())
            .and_then(tracing_value_to_attr_val);
        if let Some(attrval) = remote_timestamp {
            let remote_timestamp = match attrval {
                AttrVal::Integer(i) if i >= 0 => AttrVal::Timestamp(Nanoseconds::from(i as u64)),
                AttrVal::BigInt(i) if *i >= 0 && *i <= u64::MAX as i128 => {
                    AttrVal::Timestamp(Nanoseconds::from(*i as u64))
                }
                AttrVal::Timestamp(t) => AttrVal::Timestamp(t),
                x => x,
            };

            packed_attrs.push((
                self.get_or_create_event_attr_key("event.interaction.remote_timestamp".into())
                    .await?,
                remote_timestamp,
            ));
        }

        // Manually retype the local timestamp
        let local_timestamp = records
            .remove(&"timestamp".into())
            .and_then(tracing_value_to_attr_val);
        if let Some(attrval) = local_timestamp {
            let remote_timestamp = match attrval {
                AttrVal::Integer(i) if i >= 0 => AttrVal::Timestamp(Nanoseconds::from(i as u64)),
                AttrVal::BigInt(i) if *i >= 0 && *i <= u64::MAX as i128 => {
                    AttrVal::Timestamp(Nanoseconds::from(*i as u64))
                }
                AttrVal::Timestamp(t) => AttrVal::Timestamp(t),
                x => x,
            };

            packed_attrs.push((
                self.get_or_create_event_attr_key("event.timestamp".into())
                    .await?,
                remote_timestamp,
            ));
        } else if let Ok(duration_since_epoch) =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        {
            let duration_since_epoch_in_nanos_res: Result<u64, _> =
                duration_since_epoch.as_nanos().try_into();
            if let Ok(duration_since_epoch_in_nanos) = duration_since_epoch_in_nanos_res {
                packed_attrs.push((
                    self.get_or_create_event_attr_key("event.timestamp".into())
                        .await?,
                    AttrVal::Timestamp(Nanoseconds::from(duration_since_epoch_in_nanos)),
                ));
            }
        }

        // pack any remaining records
        for (name, value) in records {
            let attrval = if let Some(attrval) = tracing_value_to_attr_val(value) {
                attrval
            } else {
                continue;
            };

            let key = if name.starts_with("event.") {
                name.to_string()
            } else {
                format!("event.{}", name.as_str())
            };

            packed_attrs.push((self.get_or_create_event_attr_key(key).await?, attrval));
        }

        Ok(())
    }
}

// `SerializeValue` is `#[nonexhaustive]`, returns `None` if they add a type we don't handle and
// fail to serialize it as a stringified json value
fn tracing_value_to_attr_val<'a, V: Borrow<SerializeValue<'a>>>(value: V) -> Option<AttrVal> {
    Some(match value.borrow() {
        SerializeValue::Debug(dr) => match dr {
            // TODO: there's an opertunity here to pull out message format
            // parameters raw here instead of shipping a formatted string
            DebugRecord::Ser(s) => AttrVal::String(s.to_string().into()),
            DebugRecord::De(s) => AttrVal::String(s.to_string().into()),
        },
        SerializeValue::Str(s) => AttrVal::String(s.to_string().into()),
        SerializeValue::F64(n) => AttrVal::Float((*n).into()),
        SerializeValue::I64(n) => AttrVal::Integer(*n),
        SerializeValue::U64(n) => BigInt::new_attr_val((*n).into()),
        SerializeValue::Bool(b) => AttrVal::Bool(*b),
        unknown_sv => {
            if let Ok(sval) = serde_json::to_string(&unknown_sv) {
                AttrVal::String(sval.into())
            } else {
                return None;
            }
        }
    })
}
