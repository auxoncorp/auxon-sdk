//! Library relating to the handling of modality's auth tokens:
//!
//! * Representation in memory
//! * Stringy-hexy serialization
//! * A tiny file format that pairs an auth token with a plaintext user name
use hex::FromHexError;
use std::{
    env,
    path::{Path, PathBuf},
    str::FromStr,
};
use thiserror::Error;
use token_user_file::{
    read_user_auth_token_file, TokenUserFileReadError, USER_AUTH_TOKEN_FILE_NAME,
};

pub mod token_user_file;

pub const MODALITY_AUTH_TOKEN_ENV_VAR: &str = "MODALITY_AUTH_TOKEN";

const DEFAULT_CONTEXT_DIR: &str = "modality_cli";
const MODALITY_CONTEXT_DIR_ENV_VAR: &str = "MODALITY_CONTEXT_DIR";

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[repr(transparent)]
pub struct AuthToken(Vec<u8>);

impl AuthToken {
    /// Load an auth token meant for user-api usage
    pub fn load() -> Result<Self, LoadAuthTokenError> {
        if let Ok(s) = std::env::var(MODALITY_AUTH_TOKEN_ENV_VAR) {
            return Ok(AuthTokenHexString(s).try_into()?);
        }

        let context_dir = Self::context_dir()?;
        let user_auth_token_path = context_dir.join(USER_AUTH_TOKEN_FILE_NAME);
        if user_auth_token_path.exists() {
            if let Some(file_contents) = read_user_auth_token_file(&user_auth_token_path)? {
                return Ok(file_contents.auth_token);
            } else {
                return Err(LoadAuthTokenError::NoTokenInFile(
                    user_auth_token_path.to_owned(),
                ));
            }
        }

        Err(LoadAuthTokenError::NoAuthToken)
    }

    fn context_dir() -> Result<PathBuf, LoadAuthTokenError> {
        match env::var(MODALITY_CONTEXT_DIR_ENV_VAR) {
            Ok(val) => Ok(PathBuf::from(val)),
            Err(env::VarError::NotUnicode(_)) => {
                Err(LoadAuthTokenError::EnvVarSpecifiedModalityContextDirNonUtf8)
            }
            Err(env::VarError::NotPresent) => {
                let config_dir = if cfg!(windows) {
                    // Attempt to use APPDATA env var on windows, it's the same as the
                    // underlying winapi call within config_dir but in env var form rather
                    // than a winapi call, it's not available on all versions like xp, apparently
                    if let Ok(val) = env::var("APPDATA") {
                        let dir = Path::new(&val);
                        dir.to_path_buf()
                    } else {
                        dirs::config_dir().ok_or(LoadAuthTokenError::ContextDir)?
                    }
                } else {
                    dirs::config_dir().ok_or(LoadAuthTokenError::ContextDir)?
                };
                Ok(config_dir.join(DEFAULT_CONTEXT_DIR))
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum LoadAuthTokenError {
    #[error(transparent)]
    AuthTokenStringDeserializationError(#[from] AuthTokenStringDeserializationError),

    #[error(transparent)]
    TokenUserFileReadError(#[from] TokenUserFileReadError),

    #[error("Auth token not found in token file at {0}")]
    NoTokenInFile(PathBuf),

    #[error(
        "The MODALITY_CONTEXT_DIR environment variable contained a non-UTF-8-compatible string"
    )]
    EnvVarSpecifiedModalityContextDirNonUtf8,

    #[error("Could not determine the user context configuration directory")]
    ContextDir,

    #[error("Cannot resolve config dir")]
    NoConfigDir,

    #[error("Couldn't find an auth token to load.")]
    NoAuthToken,
}

impl From<Vec<u8>> for AuthToken {
    fn from(v: Vec<u8>) -> Self {
        AuthToken(v)
    }
}

impl From<AuthToken> for Vec<u8> {
    fn from(v: AuthToken) -> Self {
        v.0
    }
}

impl AsRef<[u8]> for AuthToken {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// A possibly-human-readable UTF8 encoding of an auth token
/// as a series of lowercase case character pairs.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[repr(transparent)]
pub struct AuthTokenHexString(String);

impl std::fmt::Display for AuthTokenHexString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for AuthTokenHexString {
    type Err = AuthTokenStringDeserializationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        decode_auth_token_hex_str(s)
    }
}

impl AuthTokenHexString {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<AuthTokenHexString> for String {
    fn from(v: AuthTokenHexString) -> Self {
        v.0
    }
}

impl From<AuthToken> for AuthTokenHexString {
    fn from(v: AuthToken) -> Self {
        AuthTokenHexString(hex::encode(v.0))
    }
}

impl TryFrom<AuthTokenHexString> for AuthToken {
    type Error = AuthTokenStringDeserializationError;

    fn try_from(v: AuthTokenHexString) -> Result<Self, Self::Error> {
        decode_auth_token_hex(v.as_str())
    }
}

pub fn decode_auth_token_hex(s: &str) -> Result<AuthToken, AuthTokenStringDeserializationError> {
    hex::decode(s)
        .map_err(|hex_error|match hex_error {
            FromHexError::InvalidHexCharacter { .. } => AuthTokenStringDeserializationError::InvalidHexCharacter,
            FromHexError::OddLength => AuthTokenStringDeserializationError::OddLength,
            FromHexError::InvalidStringLength => {
                panic!("An audit of the hex crate showed that the InvalidStringLength error is impossible for the `decode` method call.");
            }
        })
        .map(AuthToken::from)
}

fn decode_auth_token_hex_str(
    s: &str,
) -> Result<AuthTokenHexString, AuthTokenStringDeserializationError> {
    decode_auth_token_hex(s).map(AuthTokenHexString::from)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Error, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum AuthTokenStringDeserializationError {
    #[error("Invalid character in the auth token hex representation. Characters ought to be '0' through '9', 'a' through 'f', or 'A' through 'F'")]
    InvalidHexCharacter,
    #[error("Auth token hex representation must contain an even number of hex-digits")]
    OddLength,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn decode_auth_token_hex_never_panics() {
        proptest!(|(s in ".*")| {
            match decode_auth_token_hex(&s) {
                Ok(at) => {
                    // If valid, must be round trippable
                    let aths = AuthTokenHexString::from(at.clone());
                    let at_two = AuthToken::try_from(aths).unwrap();
                    assert_eq!(at, at_two);
                },
                Err(AuthTokenStringDeserializationError::OddLength) => {
                    prop_assert!(s.len() % 2 == 1);
                }
                Err(AuthTokenStringDeserializationError::InvalidHexCharacter) => {
                    // Cool with this error
                }
            }
        });
    }
}
