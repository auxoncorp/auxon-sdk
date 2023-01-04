#![deny(warnings, clippy::all)]
//! Library relating to the handling of modality's auth tokens:
//!
//! * Representation in memory
//! * Stringy-hexy serialization
//! * A tiny file format that pairs an auth token with a plaintext user name
use hex::FromHexError;
use std::str::FromStr;
use thiserror::Error;

pub mod token_user_file;

pub const MODALITY_AUTH_TOKEN_ENV_VAR: &str = "MODALITY_AUTH_TOKEN";

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[repr(transparent)]
pub struct AuthToken(Vec<u8>);

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
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, Error)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
