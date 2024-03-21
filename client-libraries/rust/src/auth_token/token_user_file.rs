//! A tiny UTF8 file format consisting of the lowercase hex-string representation of an auth token
//! followed by a newline
//! followed optionally by user identity string content
use crate::auth_token::{
    decode_auth_token_hex, AuthToken, AuthTokenHexString, AuthTokenStringDeserializationError,
};
use std::collections::VecDeque;
use std::fs;
use std::path::Path;

/// A UTF8 file. The first line contains the auth token hex string representation and a line break.
/// All (optional) subsequent content represents the user identity
pub const USER_AUTH_TOKEN_FILE_NAME: &str = ".user_auth_token";

/// A UTF8 file. The first line contains the auth token hex string representation and a line break.
/// All (optional) subsequent content represents the authorizing user identity
pub const REFLECTOR_AUTH_TOKEN_DEFAULT_FILE_NAME: &str = ".modality-reflector-auth-token";

pub fn write_user_auth_token_file(
    path: &Path,
    auth_token: AuthToken,
) -> Result<(), std::io::Error> {
    let mut value: String = AuthTokenHexString::from(auth_token).into();
    value.push('\n');
    fs::write(path, value.as_bytes())
}

pub struct UserAuthTokenFileContents {
    pub auth_token: AuthToken,
}

/// Expects a UTF8 file.
/// The first line contains the auth token hex string representation followed by a line break.
/// If present, all subsequent content represents the user identity
pub fn read_user_auth_token_file(
    path: &Path,
) -> Result<Option<UserAuthTokenFileContents>, TokenUserFileReadError> {
    if path.exists() {
        let contents = fs::read_to_string(path)?;
        if contents.trim().is_empty() {
            return Ok(None);
        }
        let mut lines: VecDeque<&str> = contents.lines().collect();
        if let Some(hex_line) = lines.pop_front() {
            let auth_token = decode_auth_token_hex(hex_line)?;
            Ok(Some(UserAuthTokenFileContents { auth_token }))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TokenUserFileReadError {
    #[error("IO Error")]
    Io(
        #[source]
        #[from]
        std::io::Error,
    ),

    #[error("Auth token representation error")]
    AuthTokenRepresentation(
        #[source]
        #[from]
        AuthTokenStringDeserializationError,
    ),
}
