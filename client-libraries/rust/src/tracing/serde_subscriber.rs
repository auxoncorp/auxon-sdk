pub use tracing_serde_wire::Packet;

use std::{fmt::Debug, thread, thread_local, time::Instant};

use anyhow::Context as _;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use tokio::runtime::Runtime;
use tracing_core::{
    field::Visit,
    span::{Attributes, Id, Record},
    Field, Subscriber,
};
use tracing_subscriber::{
    layer::{Context, Layer},
    prelude::*,
    registry::{LookupSpan, Registry},
};
use uuid::Uuid;

use crate::{
    api::TimelineId,
    tracing::serde_modality_ingest::{options::Options, ConnectError, TracingModality},
};
use tracing_serde_structured::{AsSerde, CowString, RecordMap, SerializeValue};
use tracing_serde_wire::TracingWire;

static START: Lazy<Instant> = Lazy::new(Instant::now);
static GLOBAL_OPTIONS: RwLock<Option<Options>> = RwLock::new(None);

thread_local! {
    static HANDLER: LocalHandler = const { LocalHandler::new() };
}

struct LocalHandler(RwLock<Option<Result<TSHandler, ConnectError>>>);

impl LocalHandler {
    const fn new() -> Self {
        LocalHandler(RwLock::new(None))
    }

    fn manual_init(&self, new_handler: TSHandler) {
        let mut handler = self.0.write();
        *handler = Some(Ok(new_handler));
    }

    // ensures handler has been initialized, then runs the provided function on it if it has been
    // successfully initialized, otherwise does nothing
    fn with_read<R, F: FnOnce(&TSHandler) -> R>(&self, f: F) -> Option<R> {
        let mut handler = self.0.write();

        if handler.is_none() {
            *handler = Some(TSHandler::new());
        }

        if let Some(Ok(ref handler)) = *handler {
            Some(f(handler))
        } else {
            None
        }
    }

    // ensures handler has been initialized, then runs the provided function on it if it has been
    // successfully initialized, otherwise does nothing
    fn with_write<R, F: FnOnce(&mut TSHandler) -> R>(&self, f: F) -> Option<R> {
        let mut handler = self.0.write();

        if handler.is_none() {
            *handler = Some(TSHandler::new());
        }

        if let Some(Ok(ref mut handler)) = *handler {
            Some(f(handler))
        } else {
            None
        }
    }
}

impl LocalHandler {
    fn handle_message(&self, msg: TracingWire<'_>) {
        self.with_write(|h| h.handle_message(msg));
    }

    fn timeline_id(&self) -> TimelineId {
        self.with_read(|t| t.tracer.timeline_id())
            .unwrap_or_else(TimelineId::zero)
    }
}

pub fn timeline_id() -> TimelineId {
    HANDLER.with(|h| h.timeline_id())
}

pub struct TSHandler {
    tracer: TracingModality,
    rt: Runtime,
}

impl TSHandler {
    fn new() -> Result<Self, ConnectError> {
        let mut local_opts = GLOBAL_OPTIONS
            .read()
            .as_ref()
            .context("global options not initialized, but global logger was set to us somehow")?
            .clone();

        let cur = thread::current();
        let name = cur
            .name()
            .map(str::to_string)
            .unwrap_or_else(|| format!("Thread#{:?}", cur.id()));
        local_opts.set_name(name);

        let rt = Runtime::new().context("create local tokio runtime for sdk")?;
        let tracing_result = {
            let handle = rt.handle();
            handle.block_on(async { TracingModality::connect_with_options(local_opts).await })
        };

        match tracing_result {
            Ok(tracer) => Ok(TSHandler { rt, tracer }),
            Err(e) => Err(e),
        }
    }

    fn handle_message(&mut self, message: TracingWire<'_>) {
        let packet = Packet {
            message,
            // NOTE: will give inaccurate data if the program has run for more than 584942 years.
            tick: START.elapsed().as_micros() as u64,
        };
        self.rt
            .handle()
            .block_on(async { self.tracer.handle_packet(packet).await })
            .unwrap();
    }
}

pub struct TSSubscriber {
    _no_external_construct: (),
}

impl TSSubscriber {
    #[allow(clippy::new_ret_no_self)]
    // this doesn't technically build a `Self`, but that's the way people should think of it
    pub fn new() -> impl Subscriber {
        Self::new_with_options(Default::default())
    }

    pub fn new_with_options(opts: Options) -> impl Subscriber {
        Registry::default().with(TSLayer::new_with_options(opts))
    }

    // another bit of type-based lies, this doesn't take an &self because we never build a Self
    pub fn connect() -> Result<(), ConnectError> {
        let first_local_handler = TSHandler::new()?;
        HANDLER.with(|h| h.manual_init(first_local_handler));
        Ok(())
    }
}

pub struct TSLayer {
    _no_external_construct: (),
}

impl TSLayer {
    pub fn new() -> Self {
        Self::new_with_options(Default::default())
    }

    pub fn new_with_options(mut opts: Options) -> Self {
        let run_id = Uuid::new_v4();
        opts.add_metadata("run_id", run_id.to_string());

        {
            let mut global_opts = GLOBAL_OPTIONS.write();
            *global_opts = Some(opts);
        }

        TSLayer {
            _no_external_construct: (),
        }
    }

    /// This is an optional step that allows you to handle connection errors at initialization
    /// time. If this is not called it will be called implicitly during the handling of the first
    /// trace event.
    pub fn connect(&self) -> Result<(), ConnectError> {
        let first_local_handler = TSHandler::new()?;
        HANDLER.with(|h| h.manual_init(first_local_handler));
        Ok(())
    }

    /// Try to connect, and panic if that's not possible.
    pub fn connect_or_panic(&self) {
        if let Err(e) = self.connect() {
            panic!("Cannot connect to to modality: {e}")
        }
    }
}

impl Default for TSLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for TSLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, _metadata: &tracing_core::Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        // always enabled for all levels
        true
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _ctx: Context<'_, S>) {
        let mut visitor = RecordMapBuilder::new();

        attrs.record(&mut visitor);

        let msg = TracingWire::NewSpan {
            id: id.as_serde(),
            attrs: attrs.as_serde(),
            values: visitor.values().into(),
        };

        HANDLER.with(move |h| h.handle_message(msg));
    }

    fn on_record(&self, span: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        let msg = TracingWire::Record {
            span: span.as_serde(),
            values: values.as_serde().to_owned(),
        };

        HANDLER.with(move |h| h.handle_message(msg));
    }

    fn on_follows_from(&self, span: &Id, follows: &Id, _ctx: Context<'_, S>) {
        let msg = TracingWire::RecordFollowsFrom {
            span: span.as_serde(),
            follows: follows.as_serde().to_owned(),
        };

        HANDLER.with(move |h| h.handle_message(msg));
    }

    fn on_event(&self, event: &tracing_core::Event<'_>, _ctx: Context<'_, S>) {
        let msg = TracingWire::Event(event.as_serde().to_owned());

        HANDLER.with(move |h| h.handle_message(msg));
    }

    fn on_enter(&self, span: &Id, _ctx: Context<'_, S>) {
        let msg = TracingWire::Enter(span.as_serde());

        HANDLER.with(move |h| h.handle_message(msg));
    }

    fn on_exit(&self, span: &Id, _ctx: Context<'_, S>) {
        let msg = TracingWire::Exit(span.as_serde());

        HANDLER.with(move |h| h.handle_message(msg));
    }

    fn on_id_change(&self, old: &Id, new: &Id, _ctx: Context<'_, S>) {
        let msg = TracingWire::IdClone {
            old: old.as_serde(),
            new: new.as_serde(),
        };

        HANDLER.with(move |h| h.handle_message(msg));
    }

    fn on_close(&self, span: Id, _ctx: Context<'_, S>) {
        let msg = TracingWire::Close(span.as_serde());

        HANDLER.with(move |h| h.handle_message(msg));
    }
}

struct RecordMapBuilder<'a> {
    record_map: RecordMap<'a>,
}

impl<'a> RecordMapBuilder<'a> {
    fn values(self) -> RecordMap<'a> {
        self.record_map
    }
}

impl<'a> RecordMapBuilder<'a> {
    fn new() -> RecordMapBuilder<'a> {
        RecordMapBuilder {
            record_map: RecordMap::new(),
        }
    }
}

impl<'a> Visit for RecordMapBuilder<'a> {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.record_map.insert(
            CowString::Borrowed(field.name()),
            SerializeValue::Debug(CowString::Owned(format!("{:?}", value)).into()),
        );
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record_map.insert(
            CowString::Borrowed(field.name()),
            SerializeValue::F64(value),
        );
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_map.insert(
            CowString::Borrowed(field.name()),
            SerializeValue::I64(value),
        );
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_map.insert(
            CowString::Borrowed(field.name()),
            SerializeValue::U64(value),
        );
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_map.insert(
            CowString::Borrowed(field.name()),
            SerializeValue::Bool(value),
        );
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_map.insert(
            CowString::Borrowed(field.name()),
            SerializeValue::Str(CowString::Borrowed(value).to_owned()),
        );
    }
}
