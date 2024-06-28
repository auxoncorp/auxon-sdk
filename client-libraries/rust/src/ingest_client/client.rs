use crate::api::types::{AttrKey, AttrVal, TimelineId};
use crate::ingest_protocol::{IngestMessage, IngestResponse, InternedAttrKey, PackedAttrKvs};
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
    time::timeout,
};
use tokio_rustls::client::TlsStream;
use url::Url;

pub struct IngestClient<S> {
    #[allow(unused)]
    pub(crate) state: S,
    pub(crate) common: IngestClientCommon,
}

pub struct UnauthenticatedState {}
pub struct ReadyState {}
pub struct BoundTimelineState {
    pub(crate) timeline_id: TimelineId,
}

/// Fields used by the client in every state
#[doc(hidden)]
pub struct IngestClientCommon {
    pub timeout: Duration,
    connection: IngestConnection,
    next_id: u32,
}

impl IngestClientCommon {
    #[doc(hidden)]
    pub fn new(timeout: Duration, connection: IngestConnection) -> Self {
        IngestClientCommon {
            timeout,
            connection,
            next_id: 0,
        }
    }

    /// Send a message and wait for a required response.
    #[doc(hidden)]
    pub async fn send_recv(&mut self, msg: &IngestMessage) -> Result<IngestResponse, IngestError> {
        self.connection.write_msg(msg).await?;
        timeout(self.timeout, self.connection.read_msg()).await?
    }

    /// Send a message.
    #[doc(hidden)]
    pub async fn send(&mut self, msg: &IngestMessage) -> Result<(), IngestError> {
        self.connection.write_msg(msg).await
    }

    pub(crate) async fn declare_attr_key<K: Into<AttrKey>>(
        &mut self,
        key_name: K,
    ) -> Result<InternedAttrKey, IngestError> {
        let key_name = key_name.into();

        if !(key_name.as_ref().starts_with("timeline.") || key_name.as_ref().starts_with("event."))
        {
            return Err(IngestError::AttrKeyNaming);
        }

        let wire_id = self.next_id;
        self.next_id += 1;
        let wire_id = wire_id.into();

        self.send(&IngestMessage::DeclareAttrKey {
            name: key_name.into(),
            wire_id,
        })
        .await?;

        Ok(wire_id)
    }
}

#[derive(Copy, Clone)]
pub enum TlsMode {
    Secure,
    Insecure,
}

pub enum IngestConnection {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl IngestConnection {
    pub async fn connect(
        endpoint: &Url,
        allow_insecure_tls: bool,
    ) -> Result<IngestConnection, IngestClientInitializationError> {
        let endpoint = IngestEndpoint::parse_and_resolve(endpoint, allow_insecure_tls).await?;

        // take the first addr, arbitrarily
        let remote_addr = endpoint
            .addrs
            .into_iter()
            .next()
            .ok_or(IngestClientInitializationError::NoIps)?;

        let local_addr: SocketAddr = if remote_addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()?;

        let socket = if remote_addr.is_ipv4() {
            TcpSocket::new_v4().map_err(IngestClientInitializationError::SocketInit)?
        } else {
            TcpSocket::new_v6().map_err(IngestClientInitializationError::SocketInit)?
        };

        socket
            .bind(local_addr)
            .map_err(IngestClientInitializationError::SocketInit)?;
        let stream = socket.connect(remote_addr).await.map_err(|error| {
            IngestClientInitializationError::SocketConnection { error, remote_addr }
        })?;

        if let Some(tls_mode) = endpoint.tls_mode {
            let config = match tls_mode {
                TlsMode::Insecure => crate::tls::INSECURE.clone(),
                TlsMode::Secure => crate::tls::SECURE.clone(),
            };

            let cx = tokio_rustls::TlsConnector::from(config);
            let stream = cx.connect(endpoint.cert_domain.try_into()?, stream).await?;
            Ok(IngestConnection::Tls(stream))
        } else {
            Ok(IngestConnection::Tcp(stream))
        }
    }

    pub async fn write_msg(&mut self, msg: &IngestMessage) -> Result<(), IngestError> {
        let msg_buf = minicbor::to_vec(msg)?;
        self.write_bytes(&msg_buf).await
    }

    /// Write already-encoded dagta directly to the ingest
    /// socket. `msg_buf` should NOT include the length prefix; that
    /// is added by this method.
    pub async fn write_bytes(&mut self, msg_buf: &[u8]) -> Result<(), IngestError> {
        let msg_len = msg_buf.len() as u32;

        match self {
            IngestConnection::Tcp(s) => {
                s.write_all(&msg_len.to_be_bytes())
                    .await
                    .map_err(minicbor::encode::Error::Write)?;
                s.write_all(msg_buf)
                    .await
                    .map_err(minicbor::encode::Error::Write)?;
            }
            IngestConnection::Tls(s) => {
                // We have to use write_all here, because https://github.com/tokio-rs/tls/issues/41
                s.write_all(&msg_len.to_be_bytes())
                    .await
                    .map_err(minicbor::encode::Error::Write)?;
                s.write_all(msg_buf)
                    .await
                    .map_err(minicbor::encode::Error::Write)?;
            }
        }

        Ok(())
    }

    pub async fn read_msg(&mut self) -> Result<IngestResponse, IngestError> {
        match self {
            IngestConnection::Tcp(s) => {
                let msg_len = s.read_u32().await?; // yes, this is big-endian
                let mut msg_buf = vec![0u8; msg_len as usize];
                s.read_exact(msg_buf.as_mut_slice()).await?;

                Ok(minicbor::decode::<IngestResponse>(&msg_buf)?)
            }
            IngestConnection::Tls(s) => {
                let msg_len = s.read_u32().await?; // yes, this is big-endian
                let mut msg_buf = vec![0u8; msg_len as usize];
                s.read_exact(msg_buf.as_mut_slice()).await?;

                Ok(minicbor::decode::<IngestResponse>(&msg_buf)?)
            }
        }
    }

    /// Copy data directly from `reader` to the ingest socket.
    pub async fn copy_from<'a, R>(&mut self, reader: &'a mut R) -> tokio::io::Result<u64>
    where
        R: tokio::io::AsyncRead + Unpin + ?Sized,
    {
        match self {
            IngestConnection::Tcp(s) => tokio::io::copy(reader, s).await,
            IngestConnection::Tls(s) => tokio::io::copy(reader, s).await,
        }
    }

    pub async fn flush(&mut self) -> tokio::io::Result<()> {
        match self {
            IngestConnection::Tcp(conn) => conn.flush().await,
            IngestConnection::Tls(conn) => conn.flush().await,
        }
    }
}

impl<T> IngestClient<T> {
    /// Consume this client and return the lower-level IngestConnection
    pub fn lower_to_connection(self) -> IngestConnection {
        self.common.connection
    }
}

impl IngestClient<UnauthenticatedState> {
    /// Create a new ingest client.
    pub async fn connect(
        endpoint: &Url,
        allow_insecure_tls: bool,
    ) -> Result<IngestClient<UnauthenticatedState>, IngestClientInitializationError> {
        let connection = IngestConnection::connect(endpoint, allow_insecure_tls).await?;
        let common = IngestClientCommon::new(Duration::from_secs(1), connection);

        Ok(IngestClient {
            state: UnauthenticatedState {},
            common,
        })
    }

    /// Create a new ingest client.
    pub async fn connect_with_timeout(
        endpoint: &Url,
        allow_insecure_tls: bool,
        timeout: Duration,
    ) -> Result<IngestClient<UnauthenticatedState>, IngestClientInitializationError> {
        let connection = IngestConnection::connect(endpoint, allow_insecure_tls).await?;
        let common = IngestClientCommon::new(timeout, connection);

        Ok(IngestClient {
            state: UnauthenticatedState {},
            common,
        })
    }

    pub async fn authenticate(
        mut self,
        token: Vec<u8>,
    ) -> Result<IngestClient<ReadyState>, IngestError> {
        let resp = self
            .common
            .send_recv(&IngestMessage::AuthRequest { token })
            .await?;

        match resp {
            IngestResponse::AuthResponse { ok, message } => {
                if ok {
                    Ok(IngestClient {
                        state: ReadyState {},
                        common: self.common,
                    })
                } else {
                    Err(IngestError::AuthenticationError {
                        message,
                        client: Box::new(self),
                    })
                }
            }
            _ => Err(IngestError::ProtocolError(
                "Invalid response received in the 'Unauthenticated' state.",
            )),
        }
    }
}

impl IngestClient<ReadyState> {
    /// Create a fully authorized client connection, using the
    /// standard config file location and environment variables.
    pub async fn connect_with_standard_config(
        timeout: Duration,
        manually_provided_config_path: Option<PathBuf>,
        manually_provided_auth_token: Option<PathBuf>,
    ) -> Result<IngestClient<ReadyState>, IngestError> {
        let (config, auth_token) = crate::reflector_config::resolve::load_config_and_auth_token(
            manually_provided_config_path,
            manually_provided_auth_token,
        )
        .map_err(IngestError::LoadConfigError)?;

        let mut endpoint = None;
        let mut allow_insecure_tls = false;
        if let Some(ingest) = config.ingest {
            allow_insecure_tls = ingest.allow_insecure_tls;
            endpoint = ingest.protocol_parent_url;
        };

        let endpoint =
            endpoint.unwrap_or_else(|| Url::parse("modality-ingest://127.0.0.1").unwrap());

        let client = IngestClient::<UnauthenticatedState>::connect_with_timeout(
            &endpoint,
            allow_insecure_tls,
            timeout,
        )
        .await?;

        client.authenticate(auth_token.into()).await
    }

    pub async fn open_timeline(
        mut self,
        id: TimelineId,
    ) -> Result<IngestClient<BoundTimelineState>, IngestError> {
        self.common
            .send(&IngestMessage::OpenTimeline { id })
            .await?;

        Ok(IngestClient {
            state: BoundTimelineState { timeline_id: id },
            common: self.common,
        })
    }

    pub async fn declare_attr_key(
        &mut self,
        key_name: String,
    ) -> Result<InternedAttrKey, IngestError> {
        self.common.declare_attr_key(key_name).await
    }
}

impl IngestClient<BoundTimelineState> {
    pub fn bound_timeline(&self) -> TimelineId {
        self.state.timeline_id
    }

    pub async fn open_timeline(&mut self, id: TimelineId) -> Result<(), IngestError> {
        self.common
            .send(&IngestMessage::OpenTimeline { id })
            .await?;
        self.state.timeline_id = id;
        Ok(())
    }

    /// This doesn't change the connection state, but it does require you to open_timeline again
    /// before you can do anything else.
    pub fn close_timeline(self) -> IngestClient<ReadyState> {
        IngestClient {
            state: ReadyState {},
            common: self.common,
        }
    }

    pub async fn declare_attr_key(
        &mut self,
        key_name: String,
    ) -> Result<InternedAttrKey, IngestError> {
        self.common.declare_attr_key(key_name).await
    }

    pub async fn timeline_metadata(
        &mut self,
        attrs: impl IntoIterator<Item = (InternedAttrKey, AttrVal)>,
    ) -> Result<(), IngestError> {
        self.common.timeline_metadata(attrs).await
    }

    pub async fn event(
        &mut self,
        ordering: u128,
        attrs: impl IntoIterator<Item = (InternedAttrKey, AttrVal)>,
    ) -> Result<(), IngestError> {
        self.common.event(ordering, attrs).await
    }

    pub async fn flush(&mut self) -> Result<(), IngestError> {
        self.common.flush().await
    }

    pub async fn status(&mut self) -> Result<IngestStatus, IngestError> {
        let resp = self
            .common
            .send_recv(&IngestMessage::IngestStatusRequest {})
            .await?;

        match resp {
            IngestResponse::IngestStatusResponse {
                current_timeline,
                events_received,
                events_written,
                events_pending,
                error_count,
            } => Ok(IngestStatus {
                current_timeline,
                events_received,
                events_written,
                events_pending,
                error_count: error_count.unwrap_or(0),
            }),
            _ => Err(IngestError::ProtocolError(
                "Invalid status response recieved",
            )),
        }
    }
}

impl IngestClientCommon {
    pub async fn timeline_metadata(
        &mut self,
        attrs: impl IntoIterator<Item = (InternedAttrKey, AttrVal)>,
    ) -> Result<(), IngestError> {
        let packed_attrs = PackedAttrKvs(attrs.into_iter().collect());

        self.send(&IngestMessage::TimelineMetadata {
            attrs: packed_attrs,
        })
        .await?;
        Ok(())
    }

    pub async fn event(
        &mut self,
        ordering: u128,
        attrs: impl IntoIterator<Item = (InternedAttrKey, AttrVal)>,
    ) -> Result<(), IngestError> {
        let packed_attrs = PackedAttrKvs(attrs.into_iter().collect());

        let be_ordering = ordering.to_be_bytes();
        let mut i = 0;
        while i < 15 {
            if be_ordering[i] != 0x00 {
                break;
            }
            i += 1;
        }
        let compact_be_ordering = be_ordering[i..16].to_vec();

        self.send(&IngestMessage::Event {
            be_ordering: compact_be_ordering,
            attrs: packed_attrs,
        })
        .await?;

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), IngestError> {
        self.send(&IngestMessage::Flush {}).await?;
        self.connection.flush().await?;
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[cfg_attr(feature = "pyo3", pyo3::pyclass)]
pub struct IngestStatus {
    pub current_timeline: Option<TimelineId>,
    pub events_received: u64,
    pub events_written: u64,
    pub events_pending: u64,
    pub error_count: u64,
}

#[cfg(feature = "pyo3")]
#[pyo3::pymethods]
impl IngestStatus {
    #[getter]
    fn current_timeline(&self) -> Option<TimelineId> {
        self.current_timeline
    }

    #[getter]
    fn events_received(&self) -> u64 {
        self.events_received
    }

    #[getter]
    fn events_written(&self) -> u64 {
        self.events_written
    }

    #[getter]
    fn events_pending(&self) -> u64 {
        self.events_pending
    }
}

#[derive(Debug, Error)]
pub enum IngestClientInitializationError {
    #[error("DNS Error: No IPs")]
    NoIps,

    #[error("Socket initialization error")]
    SocketInit(#[source] std::io::Error),

    #[error("Socket connection error. Remote address: {}", remote_addr)]
    SocketConnection {
        #[source]
        error: std::io::Error,
        remote_addr: SocketAddr,
    },

    #[error(transparent)]
    InvalidDnsName(#[from] tokio_rustls::rustls::pki_types::InvalidDnsNameError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Client local address parsing failed.")]
    ClientLocalAddrParse(#[from] std::net::AddrParseError),

    #[error("Error parsing endpoint")]
    ParseIngestEndpoint(#[from] ParseIngestEndpointError),
}

#[derive(Error)]
pub enum IngestError {
    #[error(transparent)]
    LoadConfigError(Box<dyn std::error::Error + Send + Sync>),

    #[error("Authentication Error: {message:?}")]
    AuthenticationError {
        message: Option<String>,
        client: Box<IngestClient<UnauthenticatedState>>,
    },

    #[error("Protocol Error: {0}")]
    ProtocolError(&'static str),

    #[error("Marshalling Error (Write)")]
    CborEncode(#[from] minicbor::encode::Error<std::io::Error>),

    #[error("Marshalling Error (Read)")]
    CborDecode(#[from] minicbor::decode::Error),

    #[error("Timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),

    #[error("Event attr keys must begin with 'event.', and timeline attr keys must begin with 'timeline.'")]
    AttrKeyNaming,

    #[error(transparent)]
    IngestClientInitializationError(#[from] IngestClientInitializationError),

    #[error("IO")]
    Io(#[from] std::io::Error),
}

// Manual impl so we can skip the embedded 'client'
impl std::fmt::Debug for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LoadConfigError(arg0) => f.debug_tuple("LoadConfigError").field(arg0).finish(),
            Self::AuthenticationError { message, .. } => f
                .debug_struct("AuthenticationError")
                .field("message", message)
                .finish(),
            Self::ProtocolError(arg0) => f.debug_tuple("ProtocolError").field(arg0).finish(),
            Self::CborEncode(arg0) => f.debug_tuple("CborEncode").field(arg0).finish(),
            Self::CborDecode(arg0) => f.debug_tuple("CborDecode").field(arg0).finish(),
            Self::Timeout(arg0) => f.debug_tuple("Timeout").field(arg0).finish(),
            Self::AttrKeyNaming => write!(f, "AttrKeyNaming"),
            Self::IngestClientInitializationError(arg0) => f
                .debug_tuple("IngestClientInitializationError")
                .field(arg0)
                .finish(),
            Self::Io(arg0) => f.debug_tuple("Io").field(arg0).finish(),
        }
    }
}

pub const MODALITY_STORAGE_SERVICE_PORT_DEFAULT: u16 = 14182;
pub const MODALITY_STORAGE_SERVICE_TLS_PORT_DEFAULT: u16 = 14184;
pub const MODALITY_INGEST_URL_SCHEME: &str = "modality-ingest";
pub const MODALITY_INGEST_TLS_URL_SCHEME: &str = "modality-ingest-tls";

struct IngestEndpoint {
    cert_domain: String,
    addrs: Vec<SocketAddr>,
    tls_mode: Option<TlsMode>,
}

impl IngestEndpoint {
    async fn parse_and_resolve(
        url: &Url,
        allow_insecure_tls: bool,
    ) -> Result<IngestEndpoint, ParseIngestEndpointError> {
        let host = match url.host() {
            Some(h) => h,
            None => return Err(ParseIngestEndpointError::MissingHost),
        };

        let is_tls = match url.scheme() {
            MODALITY_INGEST_URL_SCHEME => false,
            MODALITY_INGEST_TLS_URL_SCHEME => true,
            s => return Err(ParseIngestEndpointError::InvalidScheme(s.to_string())),
        };
        let port = match url.port() {
            Some(p) => p,
            _ => {
                if is_tls {
                    MODALITY_STORAGE_SERVICE_TLS_PORT_DEFAULT
                } else {
                    MODALITY_STORAGE_SERVICE_PORT_DEFAULT
                }
            }
        };

        let addrs = match host {
            url::Host::Domain(domain) => tokio::net::lookup_host((domain, port)).await?.collect(),
            url::Host::Ipv4(addr) => vec![SocketAddr::from((addr, port))],
            url::Host::Ipv6(addr) => vec![SocketAddr::from((addr, port))],
        };

        let tls_mode = match (is_tls, allow_insecure_tls) {
            (true, true) => Some(TlsMode::Insecure),
            (true, false) => Some(TlsMode::Secure),
            (false, _) => None,
        };

        Ok(IngestEndpoint {
            cert_domain: host.to_string(),
            addrs,
            tls_mode,
        })
    }
}

#[derive(Debug, Error)]
pub enum ParseIngestEndpointError {
    #[error("Url most contain a host")]
    MissingHost,

    // TODO update with the real thing
    #[error("Invalid URL scheme '{0}'. Must be one of 'modality-ingest' or 'modality-ingest-tls'")]
    InvalidScheme(String),

    #[error("IO Error")]
    Io(#[from] std::io::Error),
}
