//! A one-stop shop for reflector plugin configuration.  Automatically
//! handles configuration from a reflector config file (both running
//! standalone and as a managed child of the reflector) and
//! environment variables, via the
//! [envy](https://docs.rs/envy/latest/envy/) crate.
//!
//! All environment variable settings take precedence over config file
//! settings.
//!
//! Standard enviornment variable overrides are automatically
//! processed as well:
//!
//! * `MODALITY_REFLECTOR_CONFIG` indicates the path to a toml
//!   formatted reflector config file, which is read if given.
//!
//! * `MODALITY_AUTH_TOKEN` sets the authentication token to use
//!   for the backend connection. If not given, it is read from
//!   the user profile directory
//!
//! * `MODALITY_CLIENT_TIMEOUT` Backend connection timeout, in
//!   seconds. Defaults to 1 second if not given.
//!
//! * `MODALITY_RUN_ID` is attached as the `timeline.run_id` attribute
//!   to all timelines; a uuid is generated if not given.
//!
//! * `MODALITY_TIME_DOMAIN` is attached as the `timeline.time_domain`
//!   attribtue to all timelines, if given.
//!
//! * `MODALITY_INGEST_URL`: The modality-ingest connection url
//!   where the client will try to connect. If not given, falls back to a url formed from
//!   `MODALITY_HOST`, or else `modality-ingest://localhost`.
//!
//! * `MODALITY_HOST`: The name or ip of the host where modality is
//!   running. `MODALITY_INGEST_URL` takes precedence over
//!   this. Defaults to `localhost`. This will connect to localhost
//!   via plaintext, but use TLS connections and ports when connecting
//!   to any other host.
//!
//! * `ADDITIONAL_TIMELINE_ATTRIBUTES`: A
//!   comma-separated list of attr=value pairs, which will be attached
//!   to all timelines.
//!
//! * `OVERRIDE_TIMELINE_ATTRIBUTES`: A
//!   comma-separated list of attr=value pairs, which will be attached
//!   to all timelines, overriding any other attributes with the same
//!   names.
//!
//! # Example
//! ```no_run
//! use auxon_sdk::{init_tracing, api::TimelineId, plugin_utils::config::Config};
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize, Clone, Debug, Default)]
//! pub struct MyConfig {
//!     // This can be set with the MY_PLUGIN_SETTING environment variable
//!     pub setting: Option<u32>,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!   init_tracing!();
//!   let cfg = Config::<MyConfig>::load("MY_PLUGIN_")?;
//!   let mut client = cfg.connect_and_authenticate_ingest().await?;
//!
//!   let timeline_id = TimelineId::allocate();
//!   client.switch_timeline(timeline_id).await?;
//!   client.send_timeline_attrs("tl", [
//!     ("attr1", 42.into()),
//!     ("attr2", "hello".into())
//!   ]).await?;
//!
//!   client.send_event("ev", 0, [
//!     ("attr1", 42.into()),
//!     ("attr2", "hello".into())
//!   ]).await?;
//!
//!   Ok(())
//! }
//! ```

use crate::{
    auth_token::AuthToken,
    ingest_client::IngestClient,
    reflector_config::{
        AttrKeyEqValuePair, ConfigLoadError, SemanticErrorExplanation, TomlValue, TopLevelIngest,
        TopLevelMutation, CONFIG_ENV_VAR,
    },
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
    str::FromStr as _,
    time::Duration,
};
use url::Url;

/// Plugin configuration structure; contains both common elements, and
/// plugin-specific elements, based on the type param `T`.
pub struct Config<T> {
    /// Common ingest configuration; mostly connection-related.
    pub ingest: TopLevelIngest,

    /// Common mutation configuration; mostly connection-related.
    pub mutation: TopLevelMutation,

    /// The plugin-specific portion of the configuration.
    pub plugin: T,

    /// The client connection timeout. This is automatically used when
    /// you call [Config::connect_and_authenticate_ingest].
    pub client_timeout: Option<Duration>,

    /// `timeline.run_id` will be set to this value for all created
    /// timelines.
    pub run_id: String,

    /// If `Some(...)`, `timeline.time_domain` will be set to this value
    /// for all created timelines.
    pub time_domain: Option<String>,
}

#[derive(Deserialize)]
struct EnvConfig {
    // MODALITY_CLIENT_TIMEOUT Environment variable
    modality_client_timeout: Option<f32>,

    // MODALITY_RUN_ID Environment variable
    modality_run_id: Option<String>,

    // MODALITY_TIME_DOMAIN Environment variable
    modality_time_domain: Option<String>,
}

impl Config<()> {
    /// Load common config only. This is useful if you're writing a plugin
    /// with no specific configuartion options, or if you're connecting
    /// from a context other than a reflector plugin.
    pub fn load_common() -> Result<Config<()>, Box<dyn std::error::Error + Send + Sync>> {
        Self::load_custom("__NONE__", |_, _| Ok(None))
    }
}

impl<T: Serialize + DeserializeOwned> Config<T> {
    /// Load configuration from config file given in
    /// `MODALITY_REFLECTOR_CONFIG` as well as from other environment
    /// variables (see module documentation). The returned [Config]
    /// structure represents the fully reconcicled configuration.
    ///
    /// * `env_prefix`: The prefix used for environment variable based
    ///   settings for members of the configuration struct (type
    ///   param `T`).
    pub fn load(env_prefix: &str) -> Result<Config<T>, Box<dyn std::error::Error + Send + Sync>> {
        Self::load_custom(env_prefix, |_, _| Ok(None))
    }

    /// Load configuration, like [Config::load], but allows passing a
    /// `map_env_val` hook.  This can be used to implement
    /// non-standard environment deserialization, for value types
    /// which aren't correctly handled by the [envy](https://docs.rs/envy/latest/envy/) crate.
    ///
    /// * `map_env_val`: A function which will be called for every
    ///   environment variable. If it returns `Ok(Some((key, toml_value)))`,
    ///   a corresponding entry will be created in the
    ///   `metadata` toml table, which is then deserialized to the
    ///   custom config structure (type param `T`). This intermediate
    ///   form is used as a basis for merging values from the config
    ///   file and from the environment. Since this function returns
    ///   environment-provided values, they take precedence over the
    ///   config file.
    ///
    ///   For example:
    ///   ```
    ///   fn custom_map_val(env_key: &str, env_val: &str) -> Result<Option<(String, toml::Value)>, Box<dyn std::error::Error + Send + Sync>> {
    ///     // look for MY_PLUGIN_PREFIX_STRONGLY_ENCRYPTED_PASSWORD env var
    ///     if env_key == "STRONGLY_ENCRYPTED_PASSWORD" {
    ///       Ok(Some(("password".to_string(), toml::Value::String(env_val.to_owned()))))
    ///     } else {
    ///       // All other env vars use default deserialization
    ///       Ok(None)
    ///     }
    ///   }
    ///   ```
    pub fn load_custom(
        env_prefix: &str,
        map_env_val: impl Fn(
            &str,
            &str,
        ) -> Result<
            Option<(String, TomlValue)>,
            Box<dyn std::error::Error + Send + Sync>,
        >,
    ) -> Result<Config<T>, Box<dyn std::error::Error + Send + Sync>> {
        let mut cfg = None;

        // load from MODALITY_REFLECTOR_CONFIG
        if let Ok(env_path) = env::var(CONFIG_ENV_VAR) {
            let path = Path::new(&env_path);

            // Look at the file content to determine which section should be used.
            let content = &std::fs::read_to_string(path)?;
            let mut raw_toml: crate::reflector_config::raw_toml::Config =
                toml::from_str(content).map_err(|e| ConfigLoadError::ConfigFileToml {
                    path: path.to_owned(),
                    error: e,
                })?;

            // The 'metadata' entry is set up by the reflector on behalf of whatever plugin it's running,
            // so prefer it if it's present.
            if raw_toml.metadata.is_empty() {
                // if not, find the right plugin section and copy it to the top level
                copy_relevant_plugin_section_to_top_level_metadata(&mut raw_toml)?;
            }

            let r: Result<crate::reflector_config::Config, SemanticErrorExplanation> =
                raw_toml.try_into();
            cfg = Some(r.map_err(|semantics| ConfigLoadError::DefinitionSemantics {
                explanation: semantics.0,
            })?);
        }

        let cfg = cfg.unwrap_or_default();

        let mut ingest = cfg.ingest.clone().unwrap_or_default();
        override_ingest_config_from_env(&mut ingest)?;

        let mut mutation = cfg.mutation.clone().unwrap_or_default();
        override_mutation_config_from_env(&mut mutation)?;

        let env_config = envy::from_env::<EnvConfig>()?;

        // Load plugin-specific config from the 'metdata' entry
        let mut plugin_toml = cfg.metadata.clone();
        merge_plugin_config_from_env::<T>(env_prefix, map_env_val, &mut plugin_toml)?;

        // deserialize from merged toml values to the actual struct
        let plugin: T = TomlValue::Table(plugin_toml.into_iter().collect()).try_into()?;

        // syntheisze a uuid runid if none was given
        let run_id = env_config
            .modality_run_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let client_timeout = env_config
            .modality_client_timeout
            .map(Duration::from_secs_f32);

        Ok(Config {
            ingest,
            mutation,
            plugin,
            client_timeout,
            run_id,
            time_domain: env_config.modality_time_domain,
        })
    }

    #[deprecated = "Prefer the more explicit 'connect_and_authenticate_ingest'"]
    pub async fn connect_and_authenticate(
        &self,
    ) -> Result<super::ingest::Client, Box<dyn std::error::Error + Send + Sync>> {
        self.connect_and_authenticate_ingest().await
    }

    /// Connect to the configured Modality backend for ingest,
    /// authenticate, and return a high-level ingest client.
    pub async fn connect_and_authenticate_ingest(
        &self,
    ) -> Result<super::ingest::Client, Box<dyn std::error::Error + Send + Sync>> {
        let protocol_parent_url = if let Some(url) = &self.ingest.protocol_parent_url {
            url.clone()
        } else {
            Url::parse("modality-ingest://127.0.0.1")?
        };

        // load from MODALITY_AUTH_TOKEN or from the user profile
        let auth_token = AuthToken::load()?;

        let client = IngestClient::connect_with_timeout(
            &protocol_parent_url,
            self.ingest.allow_insecure_tls,
            self.client_timeout
                .unwrap_or_else(|| Duration::from_secs(1)),
        )
        .await?
        .authenticate(auth_token.into())
        .await?;

        Ok(super::ingest::Client::new(
            client,
            self.ingest.timeline_attributes.clone(),
            Some(self.run_id.clone()),
            self.time_domain.clone(),
        )
        .await?)
    }

    /// Connect to the configured Modality backend for mutation,
    /// authenticate, and return a connected MutatorHost.
    ///
    /// This also creates an ingest connection, which is used
    /// internally to log mutation-related events.
    #[cfg(feature = "deviant")]
    pub async fn connect_and_authenticate_mutation(
        &self,
    ) -> Result<super::mutation::MutatorHost, Box<dyn std::error::Error + Send + Sync>> {
        let ingest = self.connect_and_authenticate_ingest().await?;

        // TODO handle top level mutator_attributes configuration
        let protocol_parent_url = if let Some(url) = &self.mutation.protocol_parent_url {
            url.clone()
        } else {
            Url::parse("modality-mutation://127.0.0.1")?
        };

        // load from MODALITY_AUTH_TOKEN or from the user profile
        let auth_token = AuthToken::load()?;

        let client = super::mutation::MutatorHost::connect_and_authenticate(
            &protocol_parent_url,
            self.mutation.allow_insecure_tls,
            auth_token,
            Some(ingest),
        )
        .await?;

        Ok(client)
    }
}

/// We don't have a 'metadata' section, so we might be dealing with a reflector-style config file. Here we pull the confiruation from
/// one of the 'plugin.*' sections based on sniffing the executable name, and put that data in the 'metadata' section, so the caller
/// can find it all in that one place.
fn copy_relevant_plugin_section_to_top_level_metadata(
    raw_toml: &mut crate::reflector_config::raw_toml::Config,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(plugins) = &raw_toml.plugins {
        let file_stem = AliasablePluginFileStem::for_current_process()?;

        if let Some(ingest) = &plugins.ingest {
            let plugins_ingest_member = if file_stem.looks_like_collector() {
                ingest
                    .find_collector_member_by_plugin_name(file_stem.as_str()) // toml section: plugins.ingest.collectors.<full bin filename>
                    .or_else(|| ingest.find_collector_member_by_plugin_name(file_stem.alias()))
            // toml section: plugins.ingest.collectors.<stem filename>
            } else if file_stem.looks_like_importer() {
                ingest
                    .find_importer_member_by_plugin_name(file_stem.as_str()) // toml section: plugins.ingest.importers.<full bin filename>
                    .or_else(|| ingest.find_importer_member_by_plugin_name(file_stem.alias()))
            // toml section: plugins.ingest.importers.<stem filename>
            } else {
                None
            };

            if let Some(pim) = plugins_ingest_member {
                // If we identified a named toml entry, merge it in to the top level as 'metadata'.
                raw_toml.metadata = pim.metadata.clone();

                if raw_toml.ingest.is_none() {
                    raw_toml.ingest = Some(Default::default());
                }
                raw_toml.ingest.as_mut().unwrap().timeline_attributes =
                    pim.timeline_attributes.clone();
            }
        } else if let Some(mutation) = plugins.mutation.as_ref() {
            let mutations_ingest_member = if file_stem.looks_like_mutator() {
                mutation
                    .find_mutator_member_by_plugin_name(file_stem.as_str()) // toml section: plugins.mutation.mutators.<full bin filename>
                    .or_else(|| mutation.find_mutator_member_by_plugin_name(file_stem.alias()))
            // toml section: plugins.mutation.mutators.<stem filename>
            } else {
                None
            };

            if let Some(mim) = mutations_ingest_member {
                // If we identified a named toml entry, merge it in to the top level as 'metadata'.
                raw_toml.metadata = mim.metadata.clone();

                if raw_toml.ingest.is_none() {
                    raw_toml.ingest = Some(Default::default());
                }
            }
        }
    }

    Ok(())
}

/// Merge plugin-specific configuration values from environment
/// variables into the plugin_toml table from the config file (could
/// be an empty table, if no config file was given).
fn merge_plugin_config_from_env<T: Serialize + DeserializeOwned>(
    env_prefix: &str,
    map_env_val: impl Fn(
        &str,
        &str,
    ) -> Result<
        Option<(String, TomlValue)>,
        Box<dyn std::error::Error + Send + Sync>,
    >,
    plugin_toml: &mut BTreeMap<String, TomlValue>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut auto_vars = vec![];
    for (k, v) in env::vars() {
        let Some(k) = k.strip_prefix(env_prefix) else {
            continue;
        };

        if let Some((k, toml_val)) = map_env_val(k, &v)? {
            plugin_toml.insert(k.to_string(), toml_val);
            continue;
        } else {
            auto_vars.push((k.to_string(), v));
        }
    }

    let env_config = envy::from_iter::<_, T>(auto_vars.into_iter())?;
    let env_config_as_toml_str = toml::to_string(&env_config)?;
    let env_config_as_toml: BTreeMap<String, TomlValue> = toml::from_str(&env_config_as_toml_str)?;

    plugin_toml.extend(env_config_as_toml);
    Ok(())
}

#[derive(Deserialize)]
struct IngestEnvOverrides {
    // MODALITY_ingest_URL environment variable
    modality_ingest_url: Option<Url>,

    // MODALITY_HOST environment variable
    modality_host: Option<String>,

    // MODALITY_ALLOW_INSECURE_TLS environment variable
    modality_allow_insecure_tls: Option<bool>,

    // MODALITY_REFLECTOR_PROTOCOL_CHILD_PORT environment variable
    ingest_protocol_child_port: Option<u16>,

    // ADDITIONAL_TIMELINE_ATTRIBUTES environment variable
    additional_timeline_attributes: Option<Vec<String>>,

    // OVERRIDE_TIMELINE_ATTRIBUTES environment variable
    override_timeline_attributes: Option<Vec<String>>,
}

fn override_ingest_config_from_env(
    ingest: &mut TopLevelIngest,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ingest_env_overrides = envy::from_env::<IngestEnvOverrides>()?;
    if let Some(u) = ingest_env_overrides.modality_ingest_url {
        ingest.protocol_parent_url = Some(u);
    } else if ingest.protocol_parent_url.is_none() {
        if let Some(host) = ingest_env_overrides.modality_host {
            let scheme = if host == "localhost" {
                "modality-ingest"
            } else {
                "modality-ingest-tls"
            };
            ingest.protocol_parent_url =
                Some(url::Url::parse(&format!("{scheme}://{host}")).map_err(|e| e.to_string())?);
        }
    }
    if let Some(b) = ingest_env_overrides.modality_allow_insecure_tls {
        ingest.allow_insecure_tls = b;
    }
    if let Some(p) = ingest_env_overrides.ingest_protocol_child_port {
        ingest.protocol_child_port = Some(p);
    }

    if let Some(strs) = ingest_env_overrides.additional_timeline_attributes {
        for s in strs {
            let kvp = AttrKeyEqValuePair::from_str(&s)?;
            ingest
                .timeline_attributes
                .additional_timeline_attributes
                .push(kvp);
        }
    }

    if let Some(strs) = ingest_env_overrides.override_timeline_attributes {
        for s in strs {
            let kvp = AttrKeyEqValuePair::from_str(&s)?;
            ingest
                .timeline_attributes
                .override_timeline_attributes
                .push(kvp);
        }
    }

    Ok(())
}

#[derive(Deserialize)]
struct MutationEnvOverrides {
    // MODALITY_INGEST_URL environment variable
    modality_ingest_url: Option<Url>,

    // MODALITY_MUTATION_URL environment variable
    modality_mutation_url: Option<Url>,

    // MODALITY_HOST environment variable
    modality_host: Option<String>,

    // MODALITY_ALLOW_INSECURE_TLS environment variable
    modality_allow_insecure_tls: Option<bool>,

    // ADDITIONAL_TIMELINE_ATTRIBUTES environment variable
    additional_mutator_attributes: Option<Vec<String>>,
}

fn override_mutation_config_from_env(
    mutation: &mut TopLevelMutation,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mutation_env_overrides = envy::from_env::<MutationEnvOverrides>()?;
    if let Some(u) = mutation_env_overrides.modality_mutation_url {
        mutation.protocol_parent_url = Some(u);
    } else if let Some(u) = mutation_env_overrides.modality_ingest_url {
        let scheme = if u.scheme() == "modality-ingest-tls" {
            "modality-mutation-tls"
        } else {
            "modality-mutation"
        };
        let host = u
            .host()
            .ok_or_else(|| "Ingest url must have a host component".to_string())?;
        mutation.protocol_parent_url =
            Some(url::Url::parse(&format!("{scheme}://{host}")).map_err(|e| e.to_string())?);
    } else if mutation.protocol_parent_url.is_none() {
        if let Some(host) = mutation_env_overrides.modality_host {
            let scheme = if host == "localhost" {
                "modality-mutation"
            } else {
                "modality-mutation-tls"
            };
            mutation.protocol_parent_url =
                Some(url::Url::parse(&format!("{scheme}://{host}")).map_err(|e| e.to_string())?);
        }
    }
    if let Some(b) = mutation_env_overrides.modality_allow_insecure_tls {
        mutation.allow_insecure_tls = b;
    }

    if let Some(strs) = mutation_env_overrides.additional_mutator_attributes {
        for s in strs {
            let kvp = AttrKeyEqValuePair::from_str(&s)?;
            mutation
                .mutator_attributes
                .additional_mutator_attributes
                .push(kvp);
        }
    }

    Ok(())
}

/// Plugin file stem wrapper to allow aliasing (i.e. modality-foo-importer can be refered to with
/// the alias foo).
/// Supports our three plugin kind postfixes in a few different variants:
///   - collector\[s\]
///   - importer\[s\] | import
///   - mutator\[s\]
///
/// Also supports no postfix at all, since kind is implied by the plugin directory it lives in.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct AliasablePluginFileStem {
    filename: String,
    path: PathBuf,
}

impl AliasablePluginFileStem {
    #[cfg(not(test))]
    pub fn for_current_process() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::for_path(std::env::current_exe()?)
    }

    #[cfg(test)]
    pub fn for_current_process() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if let Ok(path) = std::env::var("TEST_CURRENT_EXE_PATH") {
            Self::for_path(path)
        } else {
            Self::for_path(std::env::current_exe()?)
        }
    }

    pub fn for_path(p: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let path = p.as_ref().to_owned();
        let filename = path
            .file_name()
            .ok_or("Plugin does not refer to a file")?
            .to_string_lossy()
            .to_string();
        Ok(Self { path, filename })
    }

    pub fn alias(&self) -> &str {
        self.filename
            .trim_start_matches("modality-")
            .trim_end_matches("-import")
            .trim_end_matches("-importer")
            .trim_end_matches("-importers")
            .trim_end_matches("-collector")
            .trim_end_matches("-collectors")
            .trim_end_matches("-mutator")
            .trim_end_matches("-mutators")
    }

    pub fn looks_like_importer(&self) -> bool {
        self.filename.ends_with("-import")
            || self.filename.ends_with("-importer")
            || self.filename.ends_with("-importers")
            || self
                .path
                .parent()
                .and_then(|p| p.components().last())
                .map(|c| c.as_os_str() == "importers")
                .unwrap_or(false)
    }

    pub fn looks_like_collector(&self) -> bool {
        self.filename.ends_with("-collector")
            || self.filename.ends_with("-collectors")
            || self
                .path
                .parent()
                .and_then(|p| p.components().last())
                .map(|c| c.as_os_str() == "collectors")
                .unwrap_or(false)
    }

    #[allow(unused)]
    pub fn looks_like_mutator(&self) -> bool {
        self.filename.ends_with("-mutator")
            || self.filename.ends_with("-mutators")
            || self
                .path
                .parent()
                .and_then(|p| p.components().last())
                .map(|c| c.as_os_str() == "mutators")
                .unwrap_or(false)
    }

    pub fn as_str(&self) -> &str {
        self.filename.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{AttrKey, AttrVal};
    use std::io::Write;

    fn apfs(p: impl AsRef<Path>) -> AliasablePluginFileStem {
        AliasablePluginFileStem::for_path(p).unwrap()
    }

    #[track_caller]
    fn check_alias(path: &str, expected: &str) {
        assert_eq!(expected, apfs(path).alias());
    }

    #[test]
    fn plugin_alias() {
        check_alias("/modality-foo", "foo");
        check_alias("/dir/modality-foo", "foo");
        check_alias("/dir/foo-import", "foo");
        check_alias("/dir/foo-importer", "foo");
        check_alias("/dir/foo-importers", "foo");
        check_alias("/dir/foo-collector", "foo");
        check_alias("/dir/foo-collectors", "foo");
        check_alias("/dir/foo-mutator", "foo");
        check_alias("/dir/foo-mutators", "foo");
        check_alias("/dir/foo", "foo");
    }

    #[test]
    fn type_heuristics() {
        assert!(apfs("/dir/foo-import").looks_like_importer());
        assert!(apfs("/dir/foo-importer").looks_like_importer());
        assert!(apfs("/dir/foo-importers").looks_like_importer());
        assert!(apfs("/dir/importers/foo").looks_like_importer());
        assert!(!apfs("/dir/collectors/foo").looks_like_importer());
        assert!(!apfs("/dir/mutators/foo").looks_like_importer());
        assert!(!apfs("/dir/foo-collector").looks_like_importer());
        assert!(!apfs("/dir/foo-mutator").looks_like_importer());

        assert!(apfs("/dir/foo-collector").looks_like_collector());
        assert!(apfs("/dir/foo-collectors").looks_like_collector());
        assert!(apfs("/dir/collectors/foo").looks_like_collector());
        assert!(!apfs("/dir/foo").looks_like_collector());
        assert!(!apfs("/dir/foo-importer").looks_like_collector());
        assert!(!apfs("/dir/foo-mutator").looks_like_collector());
        assert!(!apfs("/dir/importers/foo").looks_like_collector());
        assert!(!apfs("/dir/mutators/foo").looks_like_collector());

        assert!(apfs("/dir/foo-mutator").looks_like_mutator());
        assert!(apfs("/dir/foo-mutators").looks_like_mutator());
        assert!(apfs("/dir/mutators/foo").looks_like_mutator());
        assert!(!apfs("/dir/foo").looks_like_mutator());
        assert!(!apfs("/dir/foo-collector").looks_like_mutator());
        assert!(!apfs("/dir/foo-importer").looks_like_mutator());
        assert!(!apfs("/dir/collectors/foo").looks_like_mutator());
        assert!(!apfs("/dir/importers/foo").looks_like_mutator());
    }

    #[derive(Serialize, Deserialize)]
    struct CustomConfig {
        val: Option<u32>,
    }

    fn clear_relevant_env_vars() {
        env::remove_var("MODALITY_REFLECTOR_CONFIG");
        env::remove_var("MODALITY_CLIENT_TIMEOUT");
        env::remove_var("MODALITY_RUN_ID");
        env::remove_var("MODALITY_HOST");
        env::remove_var("MODALITY_INGEST_URL");
        env::remove_var("MODALITY_MUTATION_URL");
        env::remove_var("ADDITIONAL_TIMELINE_ATTRIBUTES");
        env::remove_var("OVERRIDE_TIMELINE_ATTRIBUTES");
        env::remove_var("MODALITY_ALLOW_INSECURE_TLS");
    }

    #[test]
    #[serial_test::serial]
    fn load_config_from_env() {
        // clear all the relevant env vars
        env::remove_var("TEST_VAL");
        clear_relevant_env_vars();

        // With no env shenanigans going on, we should get the default config
        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();
        assert_eq!(cfg.ingest, TopLevelIngest::default());
        assert!(cfg.client_timeout.is_none());
        assert!(cfg.time_domain.is_none());
        assert!(cfg.plugin.val.is_none());

        // Load custom val from the environment
        env::set_var("TEST_VAL", "42");
        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();
        assert_eq!(cfg.plugin.val, Some(42));
        env::remove_var("TEST_VAL");

        // Load client timeout from the environment
        env::set_var("MODALITY_CLIENT_TIMEOUT", "42");
        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();
        assert_eq!(cfg.client_timeout, Some(Duration::from_secs(42)));
        env::remove_var("MODALITY_CLIENT_TIMEOUT");

        // Load run id from the environment
        env::set_var("MODALITY_RUN_ID", "42");
        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();
        assert_eq!(cfg.run_id, "42");
        env::remove_var("MODALITY_RUN_ID");

        // Load time domain from the environment
        env::set_var("MODALITY_TIME_DOMAIN", "42");
        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();
        assert_eq!(cfg.time_domain.unwrap(), "42");
        env::remove_var("MODALITY_TIME_DOMAIN");

        // Load reflector protocol parent url from the environment
        env::set_var("MODALITY_INGEST_URL", "modality-ingest://foo");
        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();
        assert_eq!(
            cfg.ingest.protocol_parent_url,
            Url::parse("modality-ingest://foo").ok()
        );
        assert_eq!(
            cfg.mutation.protocol_parent_url,
            Url::parse("modality-mutation://foo").ok()
        );
        env::remove_var("MODALITY_INGEST_URL");

        // When it's TLS, mutation url should follow
        env::set_var("MODALITY_INGEST_URL", "modality-ingest-tls://foo");
        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();
        assert_eq!(
            cfg.ingest.protocol_parent_url,
            Url::parse("modality-ingest-tls://foo").ok()
        );
        assert_eq!(
            cfg.mutation.protocol_parent_url,
            Url::parse("modality-mutation-tls://foo").ok()
        );
        env::remove_var("MODALITY_INGEST_URL");

        // Load host from environment
        env::set_var("MODALITY_HOST", "foo");
        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();
        assert_eq!(
            cfg.ingest.protocol_parent_url,
            Url::parse("modality-ingest-tls://foo").ok()
        );
        assert_eq!(
            cfg.mutation.protocol_parent_url,
            Url::parse("modality-mutation-tls://foo").ok()
        );
        env::remove_var("MODALITY_HOST");

        // Load host from environment
        env::set_var("MODALITY_HOST", "localhost");
        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();
        assert_eq!(
            cfg.ingest.protocol_parent_url,
            Url::parse("modality-ingest://localhost").ok()
        );
        assert_eq!(
            cfg.mutation.protocol_parent_url,
            Url::parse("modality-mutation://localhost").ok()
        );
        env::remove_var("MODALITY_HOST");

        // reflector protocol parent url takes precedence over host
        env::set_var("MODALITY_INGEST_URL", "modality-ingest://foo");
        env::set_var("MODALITY_HOST", "bar");
        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();
        assert_eq!(
            cfg.ingest.protocol_parent_url,
            Url::parse("modality-ingest://foo").ok()
        );
        env::remove_var("MODALITY_HOST");
        env::remove_var("MODALITY_INGEST_URL");

        // Load additional timeline attrs from environment
        env::set_var("ADDITIONAL_TIMELINE_ATTRIBUTES", "foo=42,bar='yo'");
        env::set_var("OVERRIDE_TIMELINE_ATTRIBUTES", "foo=42,bar='yo'");
        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();
        assert_eq!(
            cfg.ingest
                .timeline_attributes
                .additional_timeline_attributes,
            vec![
                (AttrKey::from("foo"), AttrVal::from(42)).into(),
                (AttrKey::from("bar"), AttrVal::from("yo")).into(),
            ]
        );
        assert_eq!(
            cfg.ingest.timeline_attributes.override_timeline_attributes,
            vec![
                (AttrKey::from("foo"), AttrVal::from(42)).into(),
                (AttrKey::from("bar"), AttrVal::from("yo")).into(),
            ]
        );
        env::remove_var("ADDITIONAL_TIMELINE_ATTRIBUTES");
        env::remove_var("OVERRIDE_TIMELINE_ATTRIBUTES");

        // TODO test mutator section overrides

        clear_relevant_env_vars();
    }

    #[test]
    #[serial_test::serial]
    fn load_config_from_file() {
        clear_relevant_env_vars();

        let content = "
[ingest]
additional-timeline-attributes = ['a = 1']
override-timeline-attributes = ['c = true']
protocol-parent-url = 'modality-ingest-tls://auxon.io:9077'
allow-insecure-tls = true

[mutation]
additional-mutator-attributes = ['a = 1']
override-mutator-attributes = ['c = true']
protocol-parent-url = 'modality-mutation://auxon.io'
allow-insecure-tls = true

[metadata]
val = 42
";
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        write!(tmpfile, "{content}").unwrap();

        env::set_var("MODALITY_REFLECTOR_CONFIG", tmpfile.path());
        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();

        assert_eq!(
            cfg.ingest
                .timeline_attributes
                .additional_timeline_attributes,
            vec![(AttrKey::from("a"), AttrVal::from(1)).into(),]
        );
        assert_eq!(
            cfg.ingest.timeline_attributes.override_timeline_attributes,
            vec![(AttrKey::from("c"), AttrVal::from(true)).into(),]
        );
        assert_eq!(
            cfg.ingest.protocol_parent_url,
            Url::parse("modality-ingest-tls://auxon.io:9077").ok()
        );
        assert!(cfg.ingest.allow_insecure_tls);
        assert_eq!(cfg.plugin.val, Some(42));

        assert_eq!(
            cfg.mutation
                .mutator_attributes
                .additional_mutator_attributes,
            vec![(AttrKey::from("a"), AttrVal::from(1)).into(),]
        );
        assert_eq!(
            cfg.mutation.mutator_attributes.override_mutator_attributes,
            vec![(AttrKey::from("c"), AttrVal::from(true)).into(),]
        );
        assert_eq!(
            cfg.mutation.protocol_parent_url,
            Url::parse("modality-mutation://auxon.io").ok()
        );
        assert!(cfg.mutation.allow_insecure_tls);
        assert_eq!(cfg.plugin.val, Some(42));

        env::remove_var("MODALITY_REFLECTOR_CONFIG");
        clear_relevant_env_vars();
    }

    #[test]
    #[serial_test::serial]
    fn named_ingest_metadata_section_from_config_file() {
        clear_relevant_env_vars();

        let content = "
[plugins.ingest.collectors.test.metadata]
val = 42
";
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        write!(tmpfile, "{content}").unwrap();

        env::set_var("TEST_CURRENT_EXE_PATH", "/dir/test-collector");
        env::set_var("MODALITY_REFLECTOR_CONFIG", tmpfile.path());

        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();
        assert_eq!(cfg.plugin.val, Some(42));

        env::remove_var("MODALITY_REFLECTOR_CONFIG");
        env::remove_var("TEST_CURRENT_EXE_PATH");

        clear_relevant_env_vars();
    }

    #[test]
    #[serial_test::serial]
    fn env_overrides_config_file() {
        env::remove_var("TEST_VAL");
        clear_relevant_env_vars();

        let content = "
[ingest]
additional-timeline-attributes = ['a = 1']
override-timeline-attributes = ['c = true']
protocol-parent-url = 'modality-ingest-tls://auxon.io:9077'
allow-insecure-tls = true

[mutation]
protocol-parent-url = 'modality-mutation://auxon.io'
allow-insecure-tls = true


[metadata]
val = 42
";
        let mut tmpfile = tempfile::NamedTempFile::new().unwrap();
        write!(tmpfile, "{content}").unwrap();
        env::set_var("MODALITY_REFLECTOR_CONFIG", tmpfile.path());

        // Now set environment variables to override EVERYTHING
        env::set_var("ADDITIONAL_TIMELINE_ATTRIBUTES", "foo=42,bar='yo'");
        env::set_var("OVERRIDE_TIMELINE_ATTRIBUTES", "foo=42,bar='yo'");
        env::set_var("MODALITY_INGEST_URL", "modality-ingest://foo");
        env::set_var("MODALITY_ALLOW_INSECURE_TLS", "false");
        env::set_var("MODALITY_MUTATION_URL", "modality-mutation://foo");
        env::set_var("TEST_VAL", "99");

        let cfg = Config::<CustomConfig>::load("TEST_").unwrap();

        assert_eq!(
            cfg.ingest
                .timeline_attributes
                .additional_timeline_attributes,
            vec![
                (AttrKey::from("a"), AttrVal::from(1)).into(),
                (AttrKey::from("foo"), AttrVal::from(42)).into(),
                (AttrKey::from("bar"), AttrVal::from("yo")).into(),
            ]
        );
        assert_eq!(
            cfg.ingest.timeline_attributes.override_timeline_attributes,
            vec![
                (AttrKey::from("c"), AttrVal::from(true)).into(),
                (AttrKey::from("foo"), AttrVal::from(42)).into(),
                (AttrKey::from("bar"), AttrVal::from("yo")).into(),
            ]
        );
        assert_eq!(
            cfg.ingest.protocol_parent_url,
            Url::parse("modality-ingest://foo").ok()
        );
        assert!(!cfg.ingest.allow_insecure_tls);

        assert_eq!(
            cfg.mutation.protocol_parent_url,
            Url::parse("modality-mutation://foo").ok()
        );
        assert!(!cfg.mutation.allow_insecure_tls);

        assert_eq!(cfg.plugin.val, Some(99));

        env::remove_var("TEST_VAL");
        clear_relevant_env_vars();
    }
}
