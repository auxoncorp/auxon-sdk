use crate::auth_token::{
    decode_auth_token_hex, token_user_file::REFLECTOR_AUTH_TOKEN_DEFAULT_FILE_NAME, AuthToken,
};
use crate::reflector_config::{try_from_file, Config, ConfigLoadError, CONFIG_ENV_VAR};
use std::{
    env,
    path::{Path, PathBuf},
};

const CONFIG_FILE_NAME: &str = "config.toml";
const CONFIG_DIR: &str = "modality-reflector";
const SYS_CONFIG_BASE_PATH: &str = "/etc";

pub const MODALITY_HOST_ENV_VAR: &str = "MODALITY_HOST";
pub const MODALITY_REFLECTOR_PLUGINS_DIR_ENV_VAR: &str = "MODALITY_REFLECTOR_PLUGINS_DIR";

/// Load a Config and auth token. Either path may be given explicitly; if not, they are loaded from the
/// default system and user profile directories. (see `load_config` and `resolve_reflector_auth_token`).
/// Environment variable overrides are automatically incorporated. (See `ConfigContext::apply_environment_variable_overrides`)
pub fn load_config_and_auth_token(
    manually_provided_config_path: Option<PathBuf>,
    manually_provided_auth_token_path: Option<PathBuf>,
) -> Result<
    (crate::reflector_config::refined::Config, AuthToken),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let ConfigContext {
        config: cfg,
        config_file_parent_dir,
        ..
    } = ConfigContext::load_default(manually_provided_config_path)?;

    let auth_token =
        resolve_reflector_auth_token(manually_provided_auth_token_path, &config_file_parent_dir)?;
    Ok((cfg, auth_token))
}

/// Attempt to load a `config.toml` configuration file from the following locations:
/// - system configuration directory (i.e. /etc/modality-reflector/config.toml on Linux)
/// - `dirs::config_dir()` (i.e. ~/.config/modality-reflector/config.toml on Linux)
/// - Environment variable `MODALITY_REFLECTOR_CONFIG`
/// - Manually provided path (i.e. at the CLI with `--config file`)
///
/// The files are read in the order given above, with last file found
/// taking precedence over files read earlier.
///
/// If a configuration file doesn't exists in any of the locations, None is returned.
pub fn load_config(
    manually_provided_config_path: Option<PathBuf>,
) -> Result<Option<ConfigContext>, ExpandedConfigLoadError> {
    let mut cfg = load_system_config()?;
    if let Some(other_cfg) = load_user_config()? {
        cfg.replace(other_cfg);
    }
    if let Some(other_cfg) = load_env_config()? {
        cfg.replace(other_cfg);
    }
    if let Some(other_cfg_path) = manually_provided_config_path {
        if let Some(config_file_parent_dir) = other_cfg_path.parent().map(ToOwned::to_owned) {
            let other_cfg = ConfigContext {
                config: try_from_file(other_cfg_path.as_path())?,
                config_file: Some(other_cfg_path),
                config_file_parent_dir,
            };
            cfg.replace(other_cfg);
        }
    }
    Ok(cfg)
}

pub struct ConfigContext {
    pub config: Config,
    pub config_file: Option<PathBuf>,
    pub config_file_parent_dir: PathBuf,
}

impl ConfigContext {
    pub fn load_default(
        config_file_override: Option<PathBuf>,
    ) -> Result<Self, ExpandedConfigLoadError> {
        let mut cc = if let Some(cc) = load_config(config_file_override)? {
            cc
        } else {
            let config_file_parent_dir = env::current_dir().map_err(|ioerr| {
                ExpandedConfigLoadError::ConfigLoadError(ConfigLoadError::Io(ioerr))
            })?;
            ConfigContext {
                config: Default::default(),
                config_file: None,
                config_file_parent_dir,
            }
        };

        cc.apply_environment_variable_overrides()?;

        Ok(cc)
    }

    /// Override values in this configuration with values from environment variables, if they are set.
    ///
    /// * `MODALITY_HOST`: Overrides `ingest.protocol_parent_url` and
    ///   `mutation.protocol_parent_url`, to connect to this host, on
    ///   the default port.
    /// * `MODALITY_REFLECTOR_PLUGINS_DIR`: Overrides `ingest.plugins.plugins_dir` to load plugins
    ///   from the provided directory.
    pub fn apply_environment_variable_overrides(&mut self) -> Result<(), ExpandedConfigLoadError> {
        if let Some(modality_host) = env_str(MODALITY_HOST_ENV_VAR)? {
            if self.config.ingest.is_none() {
                self.config.ingest = Some(Default::default());
            }

            let ingest = self.config.ingest.as_mut().unwrap();
            ingest.protocol_parent_url = Some(
                url::Url::parse(&format!("modality-ingest://{modality_host}")).map_err(|_| {
                    ExpandedConfigLoadError::InvalidHostNameFromEnv {
                        var: MODALITY_HOST_ENV_VAR,
                        value: modality_host.clone(),
                    }
                })?,
            );

            if self.config.mutation.is_none() {
                self.config.mutation = Some(Default::default());
            }

            let mutation = self.config.mutation.as_mut().unwrap();
            mutation.protocol_parent_url = Some(
                url::Url::parse(&format!("modality-mutation://{modality_host}")).map_err(|_| {
                    ExpandedConfigLoadError::InvalidHostNameFromEnv {
                        var: MODALITY_HOST_ENV_VAR,
                        value: modality_host,
                    }
                })?,
            );
        }

        if let Some(plugins_dir) = env_str(MODALITY_REFLECTOR_PLUGINS_DIR_ENV_VAR)? {
            if self.config.plugins.is_none() {
                self.config.plugins = Some(Default::default());
            }
            let plugins = self.config.plugins.as_mut().unwrap();
            plugins.plugins_dir = Some(plugins_dir.into());
        }

        Ok(())
    }
}

fn load_system_config() -> Result<Option<ConfigContext>, ConfigLoadError> {
    let cfg_path = system_config_path();
    if cfg_path.exists() {
        tracing::trace!("Load system configuration file {}", cfg_path.display());
        if let Some(config_file_parent_dir) = cfg_path.parent().map(ToOwned::to_owned) {
            Ok(Some(ConfigContext {
                config: try_from_file(&cfg_path)?,
                config_file: Some(cfg_path),
                config_file_parent_dir,
            }))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

fn load_user_config() -> Result<Option<ConfigContext>, ExpandedConfigLoadError> {
    load_user_or_env_config(UserOrEnvPath::User)
}

fn load_env_config() -> Result<Option<ConfigContext>, ExpandedConfigLoadError> {
    load_user_or_env_config(UserOrEnvPath::Env)
}

fn load_user_or_env_config(
    loc: UserOrEnvPath,
) -> Result<Option<ConfigContext>, ExpandedConfigLoadError> {
    let cfg_path = match loc {
        UserOrEnvPath::User => user_config_path(),
        UserOrEnvPath::Env => env_config_path(),
    };
    match cfg_path {
        Some(p) if p.exists() => {
            tracing::trace!("Load {} configuration file {}", loc, p.display());
            if let Some(config_file_parent_dir) = p.as_path().parent().map(ToOwned::to_owned) {
                Ok(Some(ConfigContext {
                    config: try_from_file(&p)?,
                    config_file: Some(p),
                    config_file_parent_dir,
                }))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

fn system_config_path() -> PathBuf {
    PathBuf::from(SYS_CONFIG_BASE_PATH)
        .join(CONFIG_DIR)
        .join(CONFIG_FILE_NAME)
}

fn user_config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|d| d.join(CONFIG_DIR).join(CONFIG_FILE_NAME))
}

fn env_config_path() -> Option<PathBuf> {
    env::var_os(CONFIG_ENV_VAR).map(PathBuf::from)
}

fn env_str(var: &'static str) -> Result<Option<String>, ExpandedConfigLoadError> {
    match env::var(var) {
        Ok(s) => Ok(Some(s)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(ExpandedConfigLoadError::NonUnicodeEnvVar { var }),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExpandedConfigLoadError {
    #[error("Configuration environment variable '{var}' contained a non-unicode value")]
    NonUnicodeEnvVar { var: &'static str },

    #[error("Invalid hostname '{value}' specified in environment variable '{var}'")]
    InvalidHostNameFromEnv { var: &'static str, value: String },

    #[error("Config loading error.")]
    ConfigLoadError(
        #[source]
        #[from]
        ConfigLoadError,
    ),
}

#[derive(Copy, Clone, Debug)]
enum UserOrEnvPath {
    User,
    Env,
}

impl std::fmt::Display for UserOrEnvPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserOrEnvPath::User => f.write_str("user"),
            UserOrEnvPath::Env => f.write_str("environment"),
        }
    }
}

/// * CLI path override
/// * Env-Var MODALITY_AUTH_TOKEN
/// * file relative to process current working directory
/// * file relative to config file parent directory
pub fn resolve_reflector_auth_token(
    cli_override_auth_token_file_path: Option<PathBuf>,
    config_file_parent_directory: &Path,
) -> Result<AuthToken, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(path) = cli_override_auth_token_file_path {
        if !path.exists() {
            return Err(ReflectorAuthTokenLoadError::CliProvidedAuthTokenFileDidNotExist.into());
        }
        if let Some(token_file_contents) =
            crate::auth_token::token_user_file::read_user_auth_token_file(&path)?
        {
            return Ok(token_file_contents.auth_token);
        }
    }

    match env::var(crate::auth_token::MODALITY_AUTH_TOKEN_ENV_VAR) {
        Ok(val) => {
            tracing::trace!("Loading CLI env context auth token");
            return Ok(decode_auth_token_hex(&val)?);
        }
        Err(env::VarError::NotUnicode(_)) => {
            return Err(
                ReflectorAuthTokenLoadError::EnvVarSpecifiedModalityAuthTokenNonUtf8.into(),
            );
        }
        Err(env::VarError::NotPresent) => {
            // Fall through and try the next approach
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        let cwd_relative_path = cwd.join(REFLECTOR_AUTH_TOKEN_DEFAULT_FILE_NAME);
        if cwd_relative_path.exists() {
            if let Some(token_file_contents) =
                crate::auth_token::token_user_file::read_user_auth_token_file(&cwd_relative_path)?
            {
                return Ok(token_file_contents.auth_token);
            }
        }
    }

    let config_relative_path =
        config_file_parent_directory.join(REFLECTOR_AUTH_TOKEN_DEFAULT_FILE_NAME);

    if let Some(token_file_contents) =
        crate::auth_token::token_user_file::read_user_auth_token_file(&config_relative_path)?
    {
        return Ok(token_file_contents.auth_token);
    }

    // read the modality cli auth token as a fallback
    if let Ok(auth_token) = AuthToken::load() {
        return Ok(auth_token);
    }

    Err(ReflectorAuthTokenLoadError::Underspecified.into())
}

#[derive(Debug, thiserror::Error)]
pub enum ReflectorAuthTokenLoadError {
    #[error("CLI provided auth token file did not exist")]
    CliProvidedAuthTokenFileDidNotExist,

    #[error(
        "The MODALITY_AUTH_TOKEN environment variable contained a non-UTF-8-compatible string"
    )]
    EnvVarSpecifiedModalityAuthTokenNonUtf8,

    #[error("No auth token was specified.  Provide a path to a token file as a CLI argument or put the token hex contents into the MODALITY_AUTH_TOKEN environment path")]
    Underspecified,
}
