//! A format definition and parser for the `modality-reflector`, to be used
//! by custom reflector plugins that can be hosted within the reflector itself.

pub mod resolve;

pub use refined::*;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use thiserror::Error;
pub use toml::Value as TomlValue;

pub const CONFIG_ENV_VAR: &str = "MODALITY_REFLECTOR_CONFIG";

pub const MODALITY_STORAGE_SERVICE_PORT_DEFAULT: u16 = 14182;
pub const MODALITY_STORAGE_SERVICE_TLS_PORT_DEFAULT: u16 = 14183;

pub const MODALITY_REFLECTOR_INGEST_CONNECT_PORT_DEFAULT: u16 = 14188;
pub const MODALITY_REFLECTOR_INGEST_CONNECT_TLS_PORT_DEFAULT: u16 = 14189;

pub const MODALITY_MUTATION_CONNECT_PORT_DEFAULT: u16 = 14192;
pub const MODALITY_MUTATION_CONNECT_TLS_PORT_DEFAULT: u16 = 14194;

pub const MODALITY_REFLECTOR_MUTATION_CONNECT_PORT_DEFAULT: u16 = 14198;
pub const MODALITY_REFLECTOR_MUTATION_CONNECT_TLS_PORT_DEFAULT: u16 = 14199;

/// Private, internal, raw representation of the TOML content
pub(crate) mod raw_toml {
    use super::*;
    use std::path::PathBuf;

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct Config {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) ingest: Option<TopLevelIngest>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) mutation: Option<TopLevelMutation>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) plugins: Option<TopLevelPlugins>,

        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        pub(crate) metadata: BTreeMap<String, TomlValue>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct TopLevelIngest {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) protocol_parent_url: Option<String>,

        #[serde(skip_serializing_if = "std::ops::Not::not")]
        pub(crate) allow_insecure_tls: bool,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) max_write_batch_staleness_millis: Option<u64>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) protocol_child_port: Option<u16>,

        #[serde(flatten)]
        pub(crate) timeline_attributes: TimelineAttributes,

        #[serde(skip_serializing_if = "Vec::is_empty", alias = "rollover-tracker")]
        pub(crate) rollover_trackers: Vec<IngestRolloverTracker>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct TopLevelMutation {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) protocol_parent_url: Option<String>,

        #[serde(skip_serializing_if = "std::ops::Not::not")]
        pub(crate) allow_insecure_tls: bool,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) protocol_child_port: Option<u16>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) mutator_http_api_port: Option<u16>,

        #[serde(flatten)]
        pub(crate) mutator_attributes: MutatorAttributes,

        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub(crate) external_mutator_urls: Vec<String>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct TopLevelPlugins {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) available_ports: Option<AvailablePorts>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) plugins_dir: Option<PathBuf>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) ingest: Option<PluginsIngest>,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) mutation: Option<PluginsMutation>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct AvailablePorts {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) any_local: Option<bool>,

        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub(crate) ranges: Vec<[u16; 2]>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct TimelineAttributes {
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub(crate) additional_timeline_attributes: Vec<String>,

        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub(crate) override_timeline_attributes: Vec<String>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct MutatorAttributes {
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub(crate) additional_mutator_attributes: Vec<String>,

        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub(crate) override_mutator_attributes: Vec<String>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct PluginsIngest {
        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        pub(crate) collectors: BTreeMap<String, PluginsIngestMember>,

        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        pub(crate) importers: BTreeMap<String, PluginsIngestMember>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct PluginsIngestMember {
        pub(crate) plugin: Option<String>,

        #[serde(flatten)]
        pub(crate) timeline_attributes: TimelineAttributes,

        #[serde(flatten)]
        pub(crate) shutdown: PluginShutdown,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) restart: Option<bool>,

        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        pub(crate) metadata: BTreeMap<String, TomlValue>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct PluginsMutation {
        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        pub(crate) mutators: BTreeMap<String, PluginsMutationMember>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct PluginsMutationMember {
        pub(crate) plugin: Option<String>,

        #[serde(flatten)]
        pub(crate) mutator_attributes: MutatorAttributes,

        #[serde(flatten)]
        pub(crate) shutdown: PluginShutdown,

        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) restart: Option<bool>,

        #[serde(skip_serializing_if = "BTreeMap::is_empty")]
        pub(crate) metadata: BTreeMap<String, TomlValue>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct PluginShutdown {
        pub(crate) shutdown_signal: Option<String>,
        pub(crate) shutdown_timeout_millis: Option<u64>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct IngestRolloverTracker {
        pub(crate) timeout_millis: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) sender: Option<RolloverTrackerParticipant>,
        #[serde(skip_serializing_if = "Vec::is_empty", alias = "receiver")]
        pub(crate) receivers: Vec<RolloverTrackerParticipant>,
    }

    #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "kebab-case", default)]
    pub(crate) struct RolloverTrackerParticipant {
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pub(crate) timeline_attributes: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) event_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub(crate) event_attribute_key: Option<String>,
    }

    #[cfg(test)]
    pub(crate) fn try_raw_to_string_pretty(config: &Config) -> Result<String, toml::ser::Error> {
        // Slightly unexpected detour through toml::Value to work around some
        // of the toml crate's touchy handling of the order of serialization of
        // fields.
        let toml_value = toml::Value::try_from(config)?;
        let content = toml::to_string_pretty(&toml_value)?;
        Ok(content)
    }

    impl PluginMemberExt for PluginsIngestMember {
        fn plugin(&self) -> Option<&str> {
            self.plugin.as_deref()
        }
    }

    impl PluginMemberExt for PluginsMutationMember {
        fn plugin(&self) -> Option<&str> {
            self.plugin.as_deref()
        }
    }

    #[cfg(feature = "modality")]
    impl PluginsIngest {
        pub(crate) fn find_collector_member_by_plugin_name<S: AsRef<str>>(
            &self,
            plugin_name: S,
        ) -> Option<&PluginsIngestMember> {
            find_member_by_plugin_name(&self.collectors, plugin_name)
        }

        pub(crate) fn find_importer_member_by_plugin_name<S: AsRef<str>>(
            &self,
            plugin_name: S,
        ) -> Option<&PluginsIngestMember> {
            find_member_by_plugin_name(&self.importers, plugin_name)
        }
    }

    #[cfg(feature = "modality")]
    impl PluginsMutation {
        pub(crate) fn find_mutator_member_by_plugin_name<S: AsRef<str>>(
            &self,
            plugin_name: S,
        ) -> Option<&PluginsMutationMember> {
            find_member_by_plugin_name(&self.mutators, plugin_name)
        }
    }

    impl From<refined::Config> for Config {
        fn from(value: refined::Config) -> Self {
            Self {
                ingest: value.ingest.map(Into::into),
                mutation: value.mutation.map(Into::into),
                plugins: value.plugins.map(Into::into),
                metadata: value.metadata,
            }
        }
    }

    impl From<refined::TopLevelIngest> for TopLevelIngest {
        fn from(value: refined::TopLevelIngest) -> Self {
            Self {
                protocol_parent_url: value.protocol_parent_url.map(Into::into),
                allow_insecure_tls: value.allow_insecure_tls,
                max_write_batch_staleness_millis: value.max_write_batch_staleness.map(|v| {
                    let millis = v.as_millis();
                    if millis >= u64::MAX as u128 {
                        u64::MAX
                    } else {
                        millis as u64
                    }
                }),
                protocol_child_port: value.protocol_child_port.map(Into::into),
                timeline_attributes: value.timeline_attributes.into(),
                rollover_trackers: value
                    .rollover_trackers
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            }
        }
    }
    impl From<refined::TopLevelMutation> for TopLevelMutation {
        fn from(value: refined::TopLevelMutation) -> Self {
            Self {
                protocol_parent_url: value.protocol_parent_url.map(Into::into),
                allow_insecure_tls: value.allow_insecure_tls,
                protocol_child_port: value.protocol_child_port.map(Into::into),
                mutator_http_api_port: value.mutator_http_api_port.map(Into::into),
                mutator_attributes: value.mutator_attributes.into(),
                external_mutator_urls: value
                    .external_mutator_urls
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            }
        }
    }
    impl From<refined::TopLevelPlugins> for TopLevelPlugins {
        fn from(value: refined::TopLevelPlugins) -> Self {
            Self {
                available_ports: value.available_ports.map(Into::into),
                plugins_dir: value.plugins_dir,
                ingest: value.ingest.map(Into::into),
                mutation: value.mutation.map(Into::into),
            }
        }
    }
    impl From<refined::TimelineAttributes> for TimelineAttributes {
        fn from(value: refined::TimelineAttributes) -> Self {
            Self {
                additional_timeline_attributes: value
                    .additional_timeline_attributes
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                override_timeline_attributes: value
                    .override_timeline_attributes
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            }
        }
    }
    impl From<refined::MutatorAttributes> for MutatorAttributes {
        fn from(value: refined::MutatorAttributes) -> Self {
            Self {
                additional_mutator_attributes: value
                    .additional_mutator_attributes
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                override_mutator_attributes: value
                    .override_mutator_attributes
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            }
        }
    }
    impl From<refined::PluginsIngest> for PluginsIngest {
        fn from(value: refined::PluginsIngest) -> Self {
            Self {
                collectors: value
                    .collectors
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
                importers: value
                    .importers
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
            }
        }
    }
    impl From<refined::PluginsMutation> for PluginsMutation {
        fn from(value: refined::PluginsMutation) -> Self {
            Self {
                mutators: value
                    .mutators
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect(),
            }
        }
    }
    impl From<refined::PluginsIngestMember> for PluginsIngestMember {
        fn from(value: refined::PluginsIngestMember) -> Self {
            Self {
                plugin: value.plugin,
                timeline_attributes: value.timeline_attributes.into(),
                shutdown: value.shutdown.into(),
                restart: value.restart,
                metadata: value.metadata,
            }
        }
    }
    impl From<refined::PluginsMutationMember> for PluginsMutationMember {
        fn from(value: refined::PluginsMutationMember) -> Self {
            Self {
                plugin: value.plugin,
                mutator_attributes: value.mutator_attributes.into(),
                shutdown: value.shutdown.into(),
                restart: value.restart,
                metadata: value.metadata,
            }
        }
    }

    impl From<refined::PluginShutdown> for PluginShutdown {
        fn from(value: refined::PluginShutdown) -> Self {
            Self {
                shutdown_signal: value.shutdown_signal,
                shutdown_timeout_millis: value.shutdown_timeout.map(|v| {
                    let millis = v.as_millis();
                    if millis >= u64::MAX as u128 {
                        u64::MAX
                    } else {
                        millis as u64
                    }
                }),
            }
        }
    }

    impl From<refined::AvailablePorts> for AvailablePorts {
        fn from(value: refined::AvailablePorts) -> Self {
            Self {
                any_local: value.any_local,
                ranges: value
                    .ranges
                    .into_iter()
                    .map(|inclusive_range| [inclusive_range.start(), inclusive_range.end()])
                    .collect(),
            }
        }
    }

    impl From<refined::IngestRolloverTracker> for IngestRolloverTracker {
        fn from(value: refined::IngestRolloverTracker) -> Self {
            Self {
                timeout_millis: value.timeout.map(|v| {
                    let millis = v.as_millis();
                    if millis >= u64::MAX as u128 {
                        u64::MAX
                    } else {
                        millis as u64
                    }
                }),
                sender: value.sender.map(Into::into),
                receivers: value.receivers.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl From<refined::RolloverTrackerParticipant> for RolloverTrackerParticipant {
        fn from(value: refined::RolloverTrackerParticipant) -> Self {
            Self {
                timeline_attributes: value
                    .timeline_attributes
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                event_name: value.event_name,
                event_attribute_key: value.event_attribute_key,
            }
        }
    }
}

/// Public-facing, more-semantically-enriched configuration types
mod refined {
    use super::TomlValue;
    use crate::api::types::{AttrKey, AttrVal};
    use lazy_static::lazy_static;
    use regex::{Captures, Regex};
    use std::collections::BTreeMap;
    use std::env;
    use std::fmt;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::time::Duration;
    use url::Url;

    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct Config {
        pub ingest: Option<TopLevelIngest>,
        pub mutation: Option<TopLevelMutation>,
        pub plugins: Option<TopLevelPlugins>,
        pub metadata: BTreeMap<String, TomlValue>,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct TopLevelIngest {
        pub protocol_parent_url: Option<Url>,
        pub allow_insecure_tls: bool,
        pub protocol_child_port: Option<u16>,
        pub timeline_attributes: TimelineAttributes,
        pub max_write_batch_staleness: Option<Duration>,
        pub rollover_trackers: Vec<IngestRolloverTracker>,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct TopLevelMutation {
        pub protocol_parent_url: Option<Url>,
        pub allow_insecure_tls: bool,
        pub protocol_child_port: Option<u16>,
        pub mutator_http_api_port: Option<u16>,
        pub mutator_attributes: MutatorAttributes,
        pub external_mutator_urls: Vec<Url>,
    }

    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct TopLevelPlugins {
        pub available_ports: Option<AvailablePorts>,
        pub plugins_dir: Option<PathBuf>,
        pub ingest: Option<PluginsIngest>,
        pub mutation: Option<PluginsMutation>,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct AvailablePorts {
        pub any_local: Option<bool>,
        pub ranges: Vec<InclusivePortRange>,
    }

    #[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
    pub struct InclusivePortRange {
        start: u16,
        end: u16,
    }

    impl InclusivePortRange {
        pub fn new(start: u16, end: u16) -> Result<Self, SemanticErrorExplanation> {
            if start > end {
                Err(SemanticErrorExplanation(format!("Port range start must <= end, but provided start {start} was > provided end {end}")))
            } else {
                Ok(InclusivePortRange { start, end })
            }
        }
        pub fn start(&self) -> u16 {
            self.start
        }
        pub fn end(&self) -> u16 {
            self.end
        }
        pub fn start_mut(&mut self) -> &mut u16 {
            &mut self.start
        }
        pub fn end_mut(&mut self) -> &mut u16 {
            &mut self.end
        }
    }
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct TimelineAttributes {
        pub additional_timeline_attributes: Vec<AttrKeyEqValuePair>,
        pub override_timeline_attributes: Vec<AttrKeyEqValuePair>,
    }
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct MutatorAttributes {
        pub additional_mutator_attributes: Vec<AttrKeyEqValuePair>,
        pub override_mutator_attributes: Vec<AttrKeyEqValuePair>,
    }

    impl MutatorAttributes {
        pub fn merge(
            &mut self,
            other: MutatorAttributes,
        ) -> Result<(), MergeMutatorAttributesError> {
            for AttrKeyEqValuePair(k, v) in other.additional_mutator_attributes.into_iter() {
                if self
                    .additional_mutator_attributes
                    .iter()
                    .any(|kvp| kvp.0 == k)
                {
                    return Err(MergeMutatorAttributesError::KeyConflict(k));
                }

                self.additional_mutator_attributes
                    .push(AttrKeyEqValuePair(k, v));
            }

            self.override_mutator_attributes
                .extend(other.override_mutator_attributes);

            Ok(())
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
    pub enum MergeMutatorAttributesError {
        #[error("Conflicting settings for mutator attribute key {0}")]
        KeyConflict(AttrKey),
    }

    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct PluginsIngest {
        pub collectors: BTreeMap<String, PluginsIngestMember>,
        pub importers: BTreeMap<String, PluginsIngestMember>,
    }
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct PluginsIngestMember {
        pub plugin: Option<String>,
        pub timeline_attributes: TimelineAttributes,
        pub shutdown: PluginShutdown,
        pub restart: Option<bool>,
        pub metadata: BTreeMap<String, TomlValue>,
    }
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct PluginsMutation {
        pub mutators: BTreeMap<String, PluginsMutationMember>,
    }
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct PluginsMutationMember {
        pub plugin: Option<String>,
        pub mutator_attributes: MutatorAttributes,
        pub shutdown: PluginShutdown,
        pub restart: Option<bool>,
        pub metadata: BTreeMap<String, TomlValue>,
    }
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct PluginShutdown {
        pub shutdown_signal: Option<String>,
        pub shutdown_timeout: Option<Duration>,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct IngestRolloverTracker {
        pub timeout: Option<Duration>,
        pub sender: Option<RolloverTrackerParticipant>,
        pub receivers: Vec<RolloverTrackerParticipant>,
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct RolloverTrackerParticipant {
        pub timeline_attributes: Vec<AttrKeyEqValuePair>,
        pub event_name: Option<String>,
        pub event_attribute_key: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
    pub enum AttrKeyValuePairParseError {
        #[error("'{0}' is not a valid attribute key=value string.")]
        Format(String),

        #[error("The key '{0}' starts with an invalid character.")]
        InvalidKey(String),

        #[error(transparent)]
        EnvSub(#[from] EnvSubError),
    }

    /// Parsing and representation for 'foo = "bar"' or "baz = true" or "whatever.anything = 42"
    /// type key value attribute pairs.
    ///
    /// The [`AttrKeyEqValuePair::from_str`] implementation supports the following
    /// environment variable substitution expressions:
    /// * `${NAME}`
    /// * `${NAME-default}`
    /// * `${NAME:-default}`
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd)]
    pub struct AttrKeyEqValuePair(pub AttrKey, pub AttrVal);

    impl From<(AttrKey, AttrVal)> for AttrKeyEqValuePair {
        fn from((k, v): (AttrKey, AttrVal)) -> Self {
            AttrKeyEqValuePair(k, v)
        }
    }

    impl FromStr for AttrKeyEqValuePair {
        type Err = AttrKeyValuePairParseError;

        fn from_str(input: &str) -> Result<Self, Self::Err> {
            // Do environment substitution first
            let s = envsub(input)?;

            let parts: Vec<&str> = s.trim().split('=').map(|p| p.trim()).collect();
            if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
                return Err(AttrKeyValuePairParseError::Format(s.to_string()));
            }

            let key = parts[0];
            let val_str = parts[1];

            if key.starts_with('.') {
                return Err(AttrKeyValuePairParseError::InvalidKey(key.to_string()));
            }

            let val: Result<_, std::convert::Infallible> = val_str.parse();
            let val = val.unwrap();

            Ok(AttrKeyEqValuePair(AttrKey::new(key.to_string()), val))
        }
    }

    impl TryFrom<String> for AttrKeyEqValuePair {
        type Error = AttrKeyValuePairParseError;

        fn try_from(s: String) -> Result<Self, Self::Error> {
            AttrKeyEqValuePair::from_str(&s)
        }
    }

    impl From<AttrKeyEqValuePair> for String {
        fn from(kv: AttrKeyEqValuePair) -> Self {
            kv.to_string()
        }
    }

    impl fmt::Display for AttrKeyEqValuePair {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            // N.B. When we standardize literal notation for more variants, will have to add here
            // or delegate to some shared serialization code
            // TODO - more standardized escaping?
            let val_s = match &self.1 {
                AttrVal::String(interned_string) => {
                    let mut s = String::new();
                    s.push('\"');
                    s.push_str(interned_string.as_ref());
                    s.push('\"');
                    s
                }
                AttrVal::TimelineId(timeline_id) => {
                    let mut s = String::new();
                    s.push('\"');
                    s.push_str(timeline_id.to_string().as_str());
                    s.push('\"');
                    s
                }
                v => v.to_string(),
            };
            write!(f, "{} = {}", self.0, val_s)
        }
    }

    #[derive(Debug)]
    pub struct SemanticErrorExplanation(pub String);

    use crate::reflector_config::raw_toml;
    impl TryFrom<raw_toml::Config> for Config {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::Config) -> Result<Self, Self::Error> {
            Ok(Self {
                ingest: if let Some(ingest) = value.ingest {
                    Some(ingest.try_into()?)
                } else {
                    None
                },
                mutation: if let Some(mutation) = value.mutation {
                    Some(mutation.try_into()?)
                } else {
                    None
                },
                plugins: if let Some(plugins) = value.plugins {
                    Some(plugins.try_into()?)
                } else {
                    None
                },
                metadata: value.metadata,
            })
        }
    }

    impl TryFrom<raw_toml::TopLevelIngest> for TopLevelIngest {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::TopLevelIngest) -> Result<Self, Self::Error> {
            Ok(Self {
                protocol_parent_url: if let Some(u) = value.protocol_parent_url {
                    Some(url::Url::from_str(&u).map_err(|parse_err| {
                        SemanticErrorExplanation(format!(
                            "ingest.protocol-parent-url could not be parsed. {parse_err}"
                        ))
                    })?)
                } else {
                    None
                },
                protocol_child_port: value.protocol_child_port,
                timeline_attributes: value.timeline_attributes.try_into()?,
                allow_insecure_tls: value.allow_insecure_tls,
                max_write_batch_staleness: value
                    .max_write_batch_staleness_millis
                    .map(Duration::from_millis),
                rollover_trackers: value
                    .rollover_trackers
                    .into_iter()
                    .map(IngestRolloverTracker::try_from)
                    .collect::<Result<Vec<_>, SemanticErrorExplanation>>()?,
            })
        }
    }
    impl TryFrom<raw_toml::TimelineAttributes> for TimelineAttributes {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::TimelineAttributes) -> Result<Self, Self::Error> {
            Ok(Self {
                additional_timeline_attributes: value
                    .additional_timeline_attributes
                    .into_iter()
                    .map(AttrKeyEqValuePair::try_from)
                    .collect::<Result<Vec<_>, AttrKeyValuePairParseError>>()
                    .map_err(|e| {
                        SemanticErrorExplanation(format!(
                            "Error in additional-timeline-attributes member. {e}"
                        ))
                    })?,
                override_timeline_attributes: value
                    .override_timeline_attributes
                    .into_iter()
                    .map(AttrKeyEqValuePair::try_from)
                    .collect::<Result<Vec<_>, AttrKeyValuePairParseError>>()
                    .map_err(|e| {
                        SemanticErrorExplanation(format!(
                            "Error in override-timeline-attributes member. {e}"
                        ))
                    })?,
            })
        }
    }
    impl TryFrom<raw_toml::MutatorAttributes> for MutatorAttributes {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::MutatorAttributes) -> Result<Self, Self::Error> {
            Ok(Self {
                additional_mutator_attributes: value
                    .additional_mutator_attributes
                    .into_iter()
                    .map(AttrKeyEqValuePair::try_from)
                    .collect::<Result<Vec<_>, AttrKeyValuePairParseError>>()
                    .map_err(|e| {
                        SemanticErrorExplanation(format!(
                            "Error in additional-mutator-attributes member. {e}"
                        ))
                    })?,
                override_mutator_attributes: value
                    .override_mutator_attributes
                    .into_iter()
                    .map(AttrKeyEqValuePair::try_from)
                    .collect::<Result<Vec<_>, AttrKeyValuePairParseError>>()
                    .map_err(|e| {
                        SemanticErrorExplanation(format!(
                            "Error in override-mutator-attributes member. {e}"
                        ))
                    })?,
            })
        }
    }

    impl TryFrom<raw_toml::TopLevelMutation> for TopLevelMutation {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::TopLevelMutation) -> Result<Self, Self::Error> {
            Ok(Self {
                protocol_parent_url: if let Some(u) = value.protocol_parent_url {
                    Some(url::Url::from_str(&u).map_err(|parse_err| SemanticErrorExplanation(format!("mutation.protocol-parent-url could not be parsed. {parse_err}")))?)
                } else {
                    None
                },
                allow_insecure_tls: value.allow_insecure_tls,
                protocol_child_port: value.protocol_child_port,
                mutator_http_api_port: value.mutator_http_api_port,
                mutator_attributes: value.mutator_attributes.try_into()?,
                external_mutator_urls: value.external_mutator_urls.into_iter().map(|v| url::Url::from_str(&v).map_err(|parse_err|SemanticErrorExplanation(format!("mutation.external-mutator-urls member {v} could not be parsed. {parse_err}")))).collect::<Result<Vec<url::Url>, SemanticErrorExplanation>>()?,
            })
        }
    }
    impl TryFrom<raw_toml::TopLevelPlugins> for TopLevelPlugins {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::TopLevelPlugins) -> Result<Self, Self::Error> {
            Ok(Self {
                available_ports: if let Some(v) = value.available_ports {
                    Some(v.try_into()?)
                } else {
                    None
                },
                plugins_dir: value.plugins_dir,
                ingest: if let Some(v) = value.ingest {
                    Some(v.try_into()?)
                } else {
                    None
                },
                mutation: if let Some(v) = value.mutation {
                    Some(v.try_into()?)
                } else {
                    None
                },
            })
        }
    }

    impl TryFrom<raw_toml::AvailablePorts> for AvailablePorts {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::AvailablePorts) -> Result<Self, Self::Error> {
            Ok(Self {
                any_local: value.any_local,
                ranges: value
                    .ranges
                    .into_iter()
                    .map(|v| InclusivePortRange::new(v[0], v[1]))
                    .collect::<Result<Vec<InclusivePortRange>, SemanticErrorExplanation>>()?,
            })
        }
    }
    impl TryFrom<raw_toml::PluginsIngest> for PluginsIngest {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::PluginsIngest) -> Result<Self, Self::Error> {
            Ok(
                Self {
                    collectors:
                        value
                            .collectors
                            .into_iter()
                            .map(|(k, v)| v.try_into().map(|vv| (k, vv)))
                            .collect::<Result<
                                BTreeMap<String, PluginsIngestMember>,
                                SemanticErrorExplanation,
                            >>()?,
                    importers:
                        value
                            .importers
                            .into_iter()
                            .map(|(k, v)| v.try_into().map(|vv| (k, vv)))
                            .collect::<Result<
                                BTreeMap<String, PluginsIngestMember>,
                                SemanticErrorExplanation,
                            >>()?,
                },
            )
        }
    }
    impl TryFrom<raw_toml::PluginsIngestMember> for PluginsIngestMember {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::PluginsIngestMember) -> Result<Self, Self::Error> {
            Ok(Self {
                plugin: value.plugin,
                timeline_attributes: value.timeline_attributes.try_into()?,
                shutdown: value.shutdown.into(),
                restart: value.restart,
                metadata: value.metadata,
            })
        }
    }
    impl TryFrom<raw_toml::PluginsMutation> for PluginsMutation {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::PluginsMutation) -> Result<Self, Self::Error> {
            Ok(
                Self {
                    mutators:
                        value
                            .mutators
                            .into_iter()
                            .map(|(k, v)| v.try_into().map(|vv| (k, vv)))
                            .collect::<Result<
                                BTreeMap<String, PluginsMutationMember>,
                                SemanticErrorExplanation,
                            >>()?,
                },
            )
        }
    }
    impl TryFrom<raw_toml::PluginsMutationMember> for PluginsMutationMember {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::PluginsMutationMember) -> Result<Self, Self::Error> {
            Ok(Self {
                plugin: value.plugin,
                mutator_attributes: value.mutator_attributes.try_into()?,
                shutdown: value.shutdown.into(),
                restart: value.restart,
                metadata: value.metadata,
            })
        }
    }

    impl From<raw_toml::PluginShutdown> for PluginShutdown {
        fn from(value: raw_toml::PluginShutdown) -> Self {
            Self {
                shutdown_signal: value.shutdown_signal,
                shutdown_timeout: value.shutdown_timeout_millis.map(Duration::from_millis),
            }
        }
    }

    impl TryFrom<raw_toml::IngestRolloverTracker> for IngestRolloverTracker {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::IngestRolloverTracker) -> Result<Self, Self::Error> {
            Ok(Self {
                timeout: value.timeout_millis.map(Duration::from_millis),
                sender: value.sender.map(TryInto::try_into).transpose()?,
                receivers: value
                    .receivers
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, SemanticErrorExplanation>>()?,
            })
        }
    }

    impl TryFrom<raw_toml::RolloverTrackerParticipant> for RolloverTrackerParticipant {
        type Error = SemanticErrorExplanation;

        fn try_from(value: raw_toml::RolloverTrackerParticipant) -> Result<Self, Self::Error> {
            Ok(Self {
                timeline_attributes: value
                    .timeline_attributes
                    .into_iter()
                    .map(AttrKeyEqValuePair::try_from)
                    .collect::<Result<Vec<_>, AttrKeyValuePairParseError>>()
                    .map_err(|e| {
                        SemanticErrorExplanation(format!(
                            "Error in rollover-tracker member timeline-attributes. {e}"
                        ))
                    })?,
                event_name: value.event_name,
                event_attribute_key: value.event_attribute_key,
            })
        }
    }

    impl Config {
        pub fn is_empty(&self) -> bool {
            self.ingest.is_none()
                && self.mutation.is_none()
                && self.plugins.is_none()
                && self.metadata.is_empty()
        }
    }

    impl PluginsIngest {
        pub fn find_collector_member_by_plugin_name<S: AsRef<str>>(
            &self,
            plugin_name: S,
        ) -> Option<&PluginsIngestMember> {
            find_member_by_plugin_name(&self.collectors, plugin_name)
        }

        pub fn find_importer_member_by_plugin_name<S: AsRef<str>>(
            &self,
            plugin_name: S,
        ) -> Option<&PluginsIngestMember> {
            find_member_by_plugin_name(&self.importers, plugin_name)
        }
    }

    impl PluginsMutation {
        pub fn find_mutator_member_by_plugin_name<S: AsRef<str>>(
            &self,
            plugin_name: S,
        ) -> Option<&PluginsMutationMember> {
            find_member_by_plugin_name(&self.mutators, plugin_name)
        }
    }

    pub(crate) fn find_member_by_plugin_name<T: PluginMemberExt, N: AsRef<str>>(
        members: &BTreeMap<String, T>,
        plugin_name: N,
    ) -> Option<&T> {
        members.iter().find_map(|(k, m)| {
            if member_matches_plugin_name(plugin_name.as_ref(), k, m.plugin()) {
                Some(m)
            } else {
                None
            }
        })
    }

    pub(crate) fn member_matches_plugin_name<N: AsRef<str>, K: AsRef<str>, P: AsRef<str>>(
        plugin_name: N,
        member_key: K,
        member_plugin: Option<P>,
    ) -> bool {
        if member_key.as_ref() == plugin_name.as_ref() {
            // Exact match on the key
            true
        } else if member_plugin
            .as_ref()
            .map(|p| p.as_ref() == plugin_name.as_ref())
            .unwrap_or(false)
        {
            // Exact match on the explicit plugin field
            true
        } else if member_key.as_ref().contains(plugin_name.as_ref()) {
            // Matched on the key (i.e. look for 'socketcan' in 'my-socketcan-entry')
            true
        } else if member_plugin
            .as_ref()
            .map(|p| p.as_ref().contains(plugin_name.as_ref()))
            .unwrap_or(false)
        {
            // Matched on the explicit plugin field (i.e. look for 'socketcan' in 'modality-socketcan-collector')
            true
        } else {
            false
        }
    }

    pub(crate) trait PluginMemberExt {
        fn plugin(&self) -> Option<&str>;
    }

    impl PluginMemberExt for PluginsIngestMember {
        fn plugin(&self) -> Option<&str> {
            self.plugin.as_deref()
        }
    }

    impl PluginMemberExt for PluginsMutationMember {
        fn plugin(&self) -> Option<&str> {
            self.plugin.as_deref()
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
    pub enum EnvSubError {
        #[error("The environment variable '{0}' contains invalid unicode")]
        EnvVarNotUnicode(String),

        #[error("The environment variable '{0}' is not set and no default value is specified")]
        EnvVarNotPresent(String),
    }

    /// Substitute the values of environment variables.
    /// Supports the following substitution style expressions:
    /// * `${NAME}`
    /// * `${NAME-default}`
    /// * `${NAME:-default}`
    fn envsub(input: &str) -> Result<String, EnvSubError> {
        lazy_static! {
            // Matches the following patterns with named capture groups:
            // * '${NAME}' : var = 'NAME'
            // * '${NAME-default}' : var = 'NAME', def = 'default'
            // * '${NAME:-default}' : var = 'NAME', def = 'default'
            static ref ENVSUB_RE: Regex =
                Regex::new(r"\$\{(?P<var>[a-zA-Z_][a-zA-Z0-9_]*)(:?-(?P<def>.*?))?\}")
                    .expect("Could not construct envsub Regex");
        }

        replace_all(&ENVSUB_RE, input, |caps: &Captures| {
            // SAFETY: the regex requires a match for capture group 'var'
            let env_var = &caps["var"];
            match env::var(env_var) {
                Ok(env_val_val) => Ok(env_val_val),
                Err(env::VarError::NotUnicode(_)) => {
                    Err(EnvSubError::EnvVarNotUnicode(env_var.to_owned()))
                }
                Err(env::VarError::NotPresent) => {
                    // Use the default value if one was provided
                    if let Some(def) = caps.name("def") {
                        Ok(def.as_str().to_string())
                    } else {
                        Err(EnvSubError::EnvVarNotPresent(env_var.to_owned()))
                    }
                }
            }
        })
    }

    // This is essentially a fallible version of Regex::replace_all
    fn replace_all(
        re: &Regex,
        input: &str,
        replacement: impl Fn(&Captures) -> Result<String, EnvSubError>,
    ) -> Result<String, EnvSubError> {
        let mut new = String::with_capacity(input.len());
        let mut last_match = 0;
        for caps in re.captures_iter(input) {
            let m = caps.get(0).unwrap();
            new.push_str(&input[last_match..m.start()]);
            new.push_str(&replacement(&caps)?);
            last_match = m.end();
        }
        new.push_str(&input[last_match..]);
        Ok(new)
    }
}

#[derive(Debug, Error)]
pub enum ConfigWriteError {
    #[error("TOML serialization error.")]
    Toml(#[from] toml::ser::Error),

    #[error("IO error")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum ConfigLoadError {
    #[error("Error in config file {} relating to TOML parsing. {error}", .path.display())]
    ConfigFileToml {
        path: PathBuf,
        #[source]
        error: toml::de::Error,
    },
    #[allow(unused)]
    #[error("Error in config content relating to TOML parsing. {error}")]
    ConfigToml {
        #[source]
        error: toml::de::Error,
    },

    #[error("IO Error")]
    Io(#[from] std::io::Error),

    #[error("Error in config content relating to semantics. {explanation}")]
    DefinitionSemantics { explanation: String },
}

pub fn try_from_file(path: &Path) -> Result<refined::Config, ConfigLoadError> {
    let content = &std::fs::read_to_string(path)?;
    let partial: raw_toml::Config =
        toml::from_str(content).map_err(|e| ConfigLoadError::ConfigFileToml {
            path: path.to_owned(),
            error: e,
        })?;
    let r: Result<refined::Config, SemanticErrorExplanation> = partial.try_into();
    r.map_err(|semantics| ConfigLoadError::DefinitionSemantics {
        explanation: semantics.0,
    })
}

pub fn try_from_str(content: &str) -> Result<refined::Config, ConfigLoadError> {
    let partial: raw_toml::Config =
        toml::from_str(content).map_err(|e| ConfigLoadError::ConfigToml { error: e })?;
    let r: Result<refined::Config, SemanticErrorExplanation> = partial.try_into();
    r.map_err(|semantics| ConfigLoadError::DefinitionSemantics {
        explanation: semantics.0,
    })
}

pub fn try_to_file(config: &refined::Config, path: &Path) -> Result<(), ConfigWriteError> {
    let content = try_to_string(config)?;
    std::fs::write(path, content)?;
    Ok(())
}

pub fn try_to_string(config: &refined::Config) -> Result<String, ConfigWriteError> {
    let raw: raw_toml::Config = config.clone().into();
    // Slightly unexpected detour through toml::Value to work around some
    // of the toml crate's touchy handling of the order of serialization of
    // fields.
    let toml_value = toml::Value::try_from(raw)?;
    let content = toml::to_string_pretty(&toml_value)?;
    Ok(content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::AttrKey;

    /// Note that this toml example is not nearly as compact as it could be
    /// with shorthand choices that will still parse equivalently.
    /// The current shape is meant to appease the toml pretty-printer for
    /// round-trip completeness testing.
    const FULLY_FILLED_IN_TOML: &str = r#"[ingest]
additional-timeline-attributes = [
    'a = 1',
    'b = "foo"',
]
override-timeline-attributes = ['c = true']
protocol-child-port = 9079
protocol-parent-url = 'modality-ingest://auxon.io:9077'

[[ingest.rollover-trackers]]
timeout-millis = 1000

[[ingest.rollover-trackers.receivers]]
event-attribute-key = 'event.seqnum'
event-name = 'rx'
timeline-attributes = ['timeline.name = "B"']

[[ingest.rollover-trackers.receivers]]
event-attribute-key = 'event.seqnum'
event-name = 'rx'
timeline-attributes = ['timeline.name = "C"']

[ingest.rollover-trackers.sender]
event-attribute-key = 'event.seqnum'
event-name = 'tx'
timeline-attributes = ['timeline.name = "A"']

[metadata]
bag = 42
grab = 24

[mutation]
additional-mutator-attributes = [
    'd = 100',
    'e = "oof"',
]
external-mutator-urls = ['http://some-other-process.com:8080/']
mutator-http-api-port = 9059
override-mutator-attributes = ['f = false']
protocol-child-port = 9080
protocol-parent-url = 'modality-ingest://localhost:9078'

[plugins]
plugins-dir = 'path/to/custom/plugins/dir'

[plugins.available-ports]
any-local = false
ranges = [
    [
    9081,
    9097,
],
    [
    10123,
    10123,
],
]
[plugins.ingest.collectors.foobar]
plugin = 'modality-socketcan-collector'

[plugins.ingest.collectors.foobar.metadata]
all-the-custom = false

[plugins.ingest.collectors.lttng-live]
additional-timeline-attributes = [
    'a = 2',
    'r = 3',
]
override-timeline-attributes = [
    'c = false',
    'q = 99',
]
restart = true
shutdown-signal = 'SIGINT'
shutdown-timeout-millis = 1000

[plugins.ingest.collectors.lttng-live.metadata]
all-the-custom = true
bag = 41
[plugins.ingest.collectors.my-dlt-cfg.metadata]
foo = 10
[plugins.ingest.importers.csv-yolo]
additional-timeline-attributes = ['s = 4']
override-timeline-attributes = ['t = "five"']

[plugins.ingest.importers.csv-yolo.metadata]
other-custom = 'yup'
[plugins.mutation.mutators.linux-network]
additional-mutator-attributes = ['u = "six"']
override-mutator-attributes = ['v = 7']

[plugins.mutation.mutators.linux-network.metadata]
moar-custom = [
    'ynot',
    'structured',
    2,
]
"#;

    #[test]
    fn raw_representation_round_trip() {
        let raw: raw_toml::Config = toml::from_str(FULLY_FILLED_IN_TOML).unwrap();
        let back_out = raw_toml::try_raw_to_string_pretty(&raw).unwrap();
        assert_eq!(FULLY_FILLED_IN_TOML, back_out.as_str());
    }

    #[test]
    fn refined_representation_round_trip() {
        let refined: refined::Config = try_from_str(FULLY_FILLED_IN_TOML).unwrap();
        let back_out = try_to_string(&refined).unwrap();
        let refined_prime: refined::Config = try_from_str(&back_out).unwrap();
        assert_eq!(refined, refined_prime);
        assert_eq!(FULLY_FILLED_IN_TOML, back_out.as_str());
    }

    #[test]
    fn everything_is_optional() {
        let empty = "";
        let refined: refined::Config = try_from_str(empty).unwrap();
        let back_out = try_to_string(&refined).unwrap();
        let refined_prime: refined::Config = try_from_str(&back_out).unwrap();
        assert_eq!(refined, refined_prime);
        assert_eq!(empty, back_out.as_str());
    }

    #[test]
    fn attr_kv_envsub_defaults() {
        let toml = r#"
[ingest]
additional-timeline-attributes = [
    '${NOT_SET_KEY:-foo} = ${NOT_SET_VAL-1}',
    '${NOT_SET_KEY-bar} = "${NOT_SET_VAL:-foo}"',
    '${NOT_SET_KEY-abc} = ${NOT_SET_VAL:-true}',
]"#;
        let cfg: refined::Config = try_from_str(toml).unwrap();
        let attrs = cfg
            .ingest
            .map(|i| i.timeline_attributes.additional_timeline_attributes)
            .unwrap();
        assert_eq!(
            attrs,
            vec![
                AttrKeyEqValuePair(AttrKey::new("foo".to_string()), 1_i64.into()),
                AttrKeyEqValuePair(AttrKey::new("bar".to_string()), "foo".into()),
                AttrKeyEqValuePair(AttrKey::new("abc".to_string()), true.into()),
            ]
        );
    }

    #[test]
    fn attr_kv_envsub() {
        let toml = r#"
[ingest]
additional-timeline-attributes = [
    '${CARGO_PKG_NAME} = "${CARGO_PKG_VERSION}"',
    'int_key = ${CARGO_PKG_VERSION_MINOR}',
]"#;
        let cfg: refined::Config = try_from_str(toml).unwrap();
        let attrs = cfg
            .ingest
            .map(|i| i.timeline_attributes.additional_timeline_attributes)
            .unwrap();
        assert_eq!(
            attrs,
            vec![
                AttrKeyEqValuePair(
                    AttrKey::new(env!("CARGO_PKG_NAME").to_string()),
                    env!("CARGO_PKG_VERSION").into()
                ),
                AttrKeyEqValuePair(
                    AttrKey::new("int_key".to_string()),
                    env!("CARGO_PKG_VERSION_MINOR")
                        .parse::<i64>()
                        .unwrap()
                        .into()
                ),
            ]
        );
    }

    #[test]
    fn attr_kv_envsub_errors() {
        let toml = r#"
[ingest]
additional-timeline-attributes = [
    '${NOT_SET_KEY} = 1',
]"#;
        match try_from_str(toml).unwrap_err() {
            ConfigLoadError::DefinitionSemantics { explanation } => {
                assert_eq!(explanation, "Error in additional-timeline-attributes member. The environment variable 'NOT_SET_KEY' is not set and no default value is specified".to_string())
            }
            _ => panic!(),
        }
    }

    #[test]
    fn config_member_lookups() {
        let cfg: refined::Config = try_from_str(FULLY_FILLED_IN_TOML).unwrap();
        let ingest = cfg
            .plugins
            .as_ref()
            .and_then(|c| c.ingest.as_ref())
            .unwrap();
        let mutation = cfg
            .plugins
            .as_ref()
            .and_then(|c| c.mutation.as_ref())
            .unwrap();
        assert!(ingest
            .find_collector_member_by_plugin_name("lttng-live")
            .is_some());
        assert!(ingest
            .find_collector_member_by_plugin_name("socketcan")
            .is_some());
        assert!(ingest.find_collector_member_by_plugin_name("dlt").is_some());
        assert!(ingest
            .find_importer_member_by_plugin_name("csv-yolo")
            .is_some());
        assert!(mutation
            .find_mutator_member_by_plugin_name("linux-network")
            .is_some());
    }
}
