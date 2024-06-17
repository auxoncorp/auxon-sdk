use crate::{
    api::{AttrVal, Nanoseconds, TimelineId},
    ingest_client::{
        dynamic::{DynamicIngestClient, DynamicIngestError},
        IngestClient, IngestStatus, ReadyState,
    },
    ingest_protocol::InternedAttrKey,
};
use std::{collections::BTreeMap, time::SystemTime};

// for backwards compatibility
pub use super::config::Config;

/// A high-level, convenient ingest client.
///
/// - Does attr key interning for you
///
/// - Automatically handles applying and updating timeline attrs in
///   standard ways:
///
///   - Sets `timeline.run_id` and `timeline.time_domain`
///
///   - Correctly applies `additional_timeline_attributes` anad
///     `override_timeline_attributes`
///
/// - Automatically sets `event.timestamp` if it's not given manually.
pub struct Client {
    inner: DynamicIngestClient,
    run_id: Option<String>,
    time_domain: Option<String>,

    timeline_keys: BTreeMap<String, InternedAttrKey>,
    event_keys: BTreeMap<String, InternedAttrKey>,

    additional_timeline_attributes: Vec<(InternedAttrKey, AttrVal)>,
    override_timeline_attributes: Vec<(InternedAttrKey, AttrVal)>,
    enable_auto_timestamp: bool,
}

impl Client {
    /// Create a new ingest client. Normally, you'll do this by
    /// calling [Config::connect_and_authenticate_ingest].
    ///
    /// * `client`: The underlying ingest client to use, which must be
    ///   in the `Ready` state (already authenticated).
    ///
    /// * `timeline_attr_cfg`: Configuration structure from the
    ///   reflector config which contains additional / ovverriding
    ///   timeline attributes.
    ///
    /// * `run_id`: If given, `timeline.run_id` will be set to this
    ///   value for all emitted timelines.
    ///
    /// * `time_domain`: If given, `timeline.time_domain` will be set
    ///   to this value for all emitted timelines.
    pub async fn new(
        client: IngestClient<ReadyState>,
        timeline_attr_cfg: crate::reflector_config::TimelineAttributes,
        run_id: Option<String>,
        time_domain: Option<String>,
    ) -> Result<Self, DynamicIngestError> {
        let mut client = Self {
            inner: client.into(),
            run_id,
            time_domain,
            timeline_keys: Default::default(),
            event_keys: Default::default(),
            additional_timeline_attributes: Default::default(),
            override_timeline_attributes: Default::default(),
            enable_auto_timestamp: true,
        };

        for kvp in timeline_attr_cfg.additional_timeline_attributes.into_iter() {
            let k = client.prep_timeline_attr(kvp.0.as_ref()).await?;
            client.additional_timeline_attributes.push((k, kvp.1));
        }

        for kvp in timeline_attr_cfg.override_timeline_attributes.into_iter() {
            let k = client.prep_timeline_attr(kvp.0.as_ref()).await?;
            client.override_timeline_attributes.push((k, kvp.1));
        }

        Ok(client)
    }

    /// Disable automatic `timestamp` attribute generation.
    ///
    /// By default, the client adds a `timestamp` attribute to every
    /// event, unless you have already provided such an attribute in
    /// the `event_attrs` parameter. This disables that behavior, so
    /// you'll only get a `timestamp` attribute if you explicitly
    /// provide one.
    pub fn disable_auto_timestamp(&mut self) {
        self.enable_auto_timestamp = false;
    }

    /// Set the current timeline to `id`. All subsequent timeline
    /// attrs and events will are attached to the current
    /// timeline.
    ///
    /// <div class="warning">
    /// You must call `Client::switch_timeline`  at least once before calling
    /// `Client::send_timeline_attrs` or `Client::send_event`.
    /// </div>
    pub async fn switch_timeline(&mut self, id: TimelineId) -> Result<(), DynamicIngestError> {
        self.inner.open_timeline(id).await?;
        Ok(())
    }

    /// Set timeline attributes for the current timeline. You typically only need
    /// to do this once for each timeline.
    ///
    /// <div class="warning">`Client::switch_timeline` must be called at least once before calling `Client::send_timeline_attrs`!</div>
    ///
    /// * `name`: The timeline name; sets the `timeline.name` attr.
    ///
    /// * `timeline_attrs`: The attributes to set. While you can use this with anything
    ///   that implements [IntoIterator], it's idiomatic to use a literal slice, and to
    ///   use `into()` to convert values to [AttrVal]`:
    ///   `client.send_timeline_attrs("tl", [("attr1", 42.into())]).await?;`
    ///
    ///   These keys are automatically normalized, so you prepending "timeline." is optional.
    pub async fn send_timeline_attrs(
        &mut self,
        name: &str,
        timeline_attrs: impl IntoIterator<Item = (&str, AttrVal)>,
    ) -> Result<(), DynamicIngestError> {
        let mut interned_attrs =
            vec![(self.prep_timeline_attr("timeline.name").await?, name.into())];

        if let Some(run_id) = self.run_id.clone() {
            let k = self.prep_timeline_attr("timeline.run_id").await?;
            interned_attrs.push((k, AttrVal::String(run_id.into())));
        }

        if let Some(time_domain) = self.time_domain.clone() {
            let k = self.prep_timeline_attr("timeline.time_domain").await?;
            interned_attrs.push((k, AttrVal::String(time_domain.into())));
        }

        interned_attrs.extend(self.additional_timeline_attributes.iter().cloned());
        interned_attrs.extend(self.override_timeline_attributes.iter().cloned());

        for (k, v) in timeline_attrs {
            let k = self.prep_timeline_attr(k).await?;
            if self
                .override_timeline_attributes
                .iter()
                .any(|(ko, _)| k == *ko)
            {
                continue;
            }

            interned_attrs.push((k, v));
        }

        self.inner.timeline_metadata(interned_attrs).await?;

        Ok(())
    }

    async fn prep_timeline_attr(&mut self, k: &str) -> Result<InternedAttrKey, DynamicIngestError> {
        let key = normalize_timeline_key(k);
        let int_key = if let Some(ik) = self.timeline_keys.get(&key) {
            *ik
        } else {
            let ik = self.inner.declare_attr_key(key.clone()).await?;
            self.timeline_keys.insert(key, ik);
            ik
        };

        Ok(int_key)
    }

    /// Create an event on the current timeline.
    ///
    /// <div class="warning">`Client::switch_timeline` must be called at least once before calling `Client::send_event`! </div>
    ///
    /// * `name`: The event name; sets the `event.name` attr.
    ///
    /// * `ordering`: The relative ordering of this event on its timeline. Most users will use a local counter
    ///   to populate this value, and increment it each time an event is sent on the timeline. Values to not
    ///   have to be consecutive, so you can use a single counter that is shared between all timelines if that makes
    ///   sense for your application.
    ///
    ///   <div class="warning">Avoid sending duplicate `ordering` values for the same timeline.</div>
    ///
    /// * `attrs`: The attributes to attach to the event. While you can use this with anything
    ///   that implements [IntoIterator], it's idiomatic to use a literal slice, and to
    ///   use `into()` to convert values to [AttrVal]:
    ///   `client.send_event("ev", [("attr1", 42.into())]).await?;`
    ///
    ///   * These keys are automatically normalized, so you prepending "event." is optional.
    ///
    ///   * If "timestamp" or "event.timestamp" is not given here, the
    ///     current system time (from [SystemTime::now]) will be used
    ///     to populate the `event.timestamp` attr. If you want to
    ///     handle timestamps completely manually, you can disable
    ///     this behavior using [Client::disable_auto_timestamp].
    pub async fn send_event(
        &mut self,
        name: &str,
        ordering: u128,
        attrs: impl IntoIterator<Item = (&str, AttrVal)>,
    ) -> Result<(), DynamicIngestError> {
        let mut interned_attrs = Vec::new();
        let mut have_timestamp = false;

        interned_attrs.push((self.prep_event_attr("event.name").await?, name.into()));

        for (k, v) in attrs {
            if self.enable_auto_timestamp && (k == "timestamp" || k == "event.timestamp") {
                have_timestamp = true;
            }

            interned_attrs.push((self.prep_event_attr(k).await?, v));
        }

        if self.enable_auto_timestamp && !have_timestamp {
            interned_attrs.push((
                self.prep_event_attr("event.timestamp").await?,
                Nanoseconds::from(
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64,
                )
                .into(),
            ));
        }

        self.inner.event(ordering, interned_attrs).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), DynamicIngestError> {
        self.inner.flush().await?;
        Ok(())
    }

    pub async fn status(&mut self) -> Result<IngestStatus, DynamicIngestError> {
        Ok(self.inner.status().await?)
    }

    async fn prep_event_attr(&mut self, k: &str) -> Result<InternedAttrKey, DynamicIngestError> {
        let key = normalize_event_key(k);
        let int_key = if let Some(ik) = self.event_keys.get(&key) {
            *ik
        } else {
            let ik = self.inner.declare_attr_key(key.clone()).await?;
            self.timeline_keys.insert(key, ik);
            ik
        };

        Ok(int_key)
    }
}

fn normalize_timeline_key(s: &str) -> String {
    if s.starts_with("timeline.") {
        s.to_owned()
    } else {
        format!("timeline.{s}")
    }
}

fn normalize_event_key(s: &str) -> String {
    if s.starts_with("event.") {
        s.to_owned()
    } else {
        format!("event.{s}")
    }
}
