use crate::api::AttrVal;
use std::net::SocketAddr;

/// Initialization options.
#[derive(Clone)]
pub struct Options {
    pub(crate) auth: Option<Vec<u8>>,
    pub(crate) metadata: Vec<(String, AttrVal)>,
    pub(crate) server_addr: SocketAddr,
}

impl Options {
    pub fn new() -> Options {
        let auth = Self::resolve_auth_token();
        let server_addr = ([127, 0, 0, 1], 14182).into();
        Options {
            auth,
            metadata: Vec::new(),
            server_addr,
        }
    }

    fn resolve_auth_token() -> Option<Vec<u8>> {
        if let Some(from_env) = std::env::var("MODALITY_AUTH_TOKEN")
            .ok()
            .and_then(|t| hex::decode(t).ok())
        {
            return Some(from_env);
        }

        dirs::config_dir()
            .and_then(|config| {
                let file_path = config.join("modality_cli").join(".user_auth_token");
                std::fs::read_to_string(file_path).ok()
            })
            .and_then(|t| hex::decode(t.trim()).ok())
    }

    /// Set an auth token to be provided to modality. Tokens should be a hex stringish value.
    pub fn set_auth<S: AsRef<[u8]>>(&mut self, auth: S) {
        self.auth = hex::decode(auth).ok();
    }
    /// A chainable version of [set_auth](Self::set_auth).
    pub fn with_auth<S: AsRef<[u8]>>(mut self, auth: S) -> Self {
        self.auth = hex::decode(auth).ok();
        self
    }

    /// Set the name for the root timeline. By default this will be the name of the main thread as
    /// provided by the OS.
    pub fn set_name<S: AsRef<str>>(&mut self, name: S) {
        self.metadata.push((
            "timeline.name".to_string(),
            AttrVal::String(name.as_ref().to_string().into()),
        ));
    }
    /// A chainable version of [set_name](Self::set_name).
    pub fn with_name<S: AsRef<str>>(mut self, name: S) -> Self {
        self.metadata.push((
            "timeline.name".to_string(),
            AttrVal::String(name.as_ref().to_string().into()),
        ));
        self
    }

    /// Add arbitrary metadata to the root timeline.
    ///
    /// This can be called multiple times.
    pub fn add_metadata<K: AsRef<str>, V: Into<AttrVal>>(&mut self, key: K, value: V) {
        let key = key.as_ref();
        let key = if key.starts_with("timeline.") {
            key.to_string()
        } else {
            format!("timeline.{}", key)
        };

        self.metadata.push((key, value.into()));
    }
    /// A chainable version of [add_metadata](Self::add_metadata).
    pub fn with_metadata<K: AsRef<str>, V: Into<AttrVal>>(mut self, key: K, value: V) -> Self {
        let key = key.as_ref();
        let key = if key.starts_with("timeline.") {
            key.to_string()
        } else {
            format!("timeline.{}", key)
        };

        self.metadata.push((key, value.into()));
        self
    }

    /// Set the address of modalityd or a modality reflector where trace data should be sent.
    ///
    /// Defaults to `localhost:default_port`
    pub fn set_server_address(&mut self, addr: SocketAddr) {
        self.server_addr = addr;
    }
    /// A chainable version of [set_server_address](Self::set_server_address).
    pub fn with_server_address(mut self, addr: SocketAddr) -> Self {
        self.server_addr = addr;
        self
    }
}

impl Default for Options {
    fn default() -> Options {
        Options::new()
    }
}
