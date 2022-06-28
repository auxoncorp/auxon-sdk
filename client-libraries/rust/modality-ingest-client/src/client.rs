use crate::{
    protocol::{IngestMessage, IngestResponse, PackedAttrKvs},
    types::{AttrKey, AttrVal, TimelineId},
};
use std::{net::SocketAddr, time::Duration};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
    time::timeout,
};
use tokio_native_tls::TlsStream;
use url::Url;

pub struct IngestClient<S> {
    #[allow(unused)]
    state: S,
    common: IngestClientCommon,
}

pub struct UnauthenticatedState {}
pub struct ReadyState {}
pub struct BoundTimelineState {
    timeline_id: TimelineId,
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

    async fn attr_key(&mut self, key_name: String) -> Result<AttrKey, IngestError> {
        if !(key_name.starts_with("timeline.") || key_name.starts_with("event.") ){
            return Err(IngestError::AttrKeyNaming);
        }

        let wire_id = self.next_id;
        self.next_id += 1;

        self.send(&IngestMessage::DeclareAttrKey {
            name: key_name,
            wire_id,
        })
        .await?;

        Ok(AttrKey(wire_id))
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
            let cx = native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(match tls_mode {
                    TlsMode::Secure => false,
                    TlsMode::Insecure => true,
                })
                .build()?;
            let cx = tokio_native_tls::TlsConnector::from(cx);
            let stream = cx.connect(&endpoint.cert_domain, stream).await?;
            Ok(IngestConnection::Tls(stream))
        } else {
            Ok(IngestConnection::Tcp(stream))
        }
    }

    async fn write_msg(&mut self, msg: &IngestMessage) -> Result<(), IngestError> {
        let msg_buf = minicbor::to_vec(msg)?;
        let msg_len = msg_buf.len() as u32;

        match self {
            IngestConnection::Tcp(s) => {
                s.write_all(&msg_len.to_be_bytes())
                    .await
                    .map_err(minicbor::encode::Error::Write)?;
                s.write_all(&msg_buf)
                    .await
                    .map_err(minicbor::encode::Error::Write)?;
            }
            IngestConnection::Tls(s) => {
                // We have to use write_all here, because https://github.com/tokio-rs/tls/issues/41
                s.write_all(&msg_len.to_be_bytes())
                    .await
                    .map_err(minicbor::encode::Error::Write)?;
                s.write_all(&msg_buf)
                    .await
                    .map_err(minicbor::encode::Error::Write)?;
            }
        }

        Ok(())
    }

    async fn read_msg(&mut self) -> Result<IngestResponse, IngestError> {
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

    pub async fn attr_key(&mut self, key_name: String) -> Result<AttrKey, IngestError> {
        self.common.attr_key(key_name).await
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

    pub async fn attr_key(&mut self, key_name: String) -> Result<AttrKey, IngestError> {
        self.common.attr_key(key_name).await
    }

    pub async fn timeline_metadata(
        &mut self,
        attrs: impl IntoIterator<Item = (AttrKey, AttrVal)>,
    ) -> Result<(), IngestError> {
        self.common.timeline_metadata(attrs).await
    }

    pub async fn event(
        &mut self,
        ordering: u128,
        attrs: impl IntoIterator<Item = (AttrKey, AttrVal)>,
    ) -> Result<(), IngestError> {
        self.common.event(ordering, attrs).await
    }

    // TODO make a blocking_flush as well, good for tests
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
            } => Ok(IngestStatus {
                current_timeline,
                events_received,
                events_written,
                events_pending,
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
        attrs: impl IntoIterator<Item = (AttrKey, AttrVal)>,
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
        attrs: impl IntoIterator<Item = (AttrKey, AttrVal)>,
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

    // TODO make a blocking_flush as well, good for tests
    pub async fn flush(&mut self) -> Result<(), IngestError> {
        self.send(&IngestMessage::Flush {}).await?;

        Ok(())
    }
}

pub struct IngestStatus {
    pub current_timeline: Option<TimelineId>,
    pub events_received: u64,
    pub events_written: u64,
    pub events_pending: u64,
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

    #[error("TLS Error")]
    Tls(#[from] native_tls::Error),

    #[error("Client local address parsing failed.")]
    ClientLocalAddrParse(#[from] std::net::AddrParseError),

    #[error("Error parsing endpoint")]
    ParseIngestEndpoint(#[from] ParseIngestEndpointError),
}

#[derive(Error)]
pub enum IngestError {
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

    #[error("IO")]
    Io(#[from] std::io::Error),
}

// Manual impl so we can skip the embedded 'client'
impl std::fmt::Debug for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AuthenticationError { message, .. } => f
                .debug_struct("AuthenticationError")
                .field("message", message)
                .finish(),
            Self::ProtocolError(e) => f.debug_tuple("ProtocolError").field(e).finish(),
            Self::CborEncode(e) => f.debug_tuple("CborEncode").field(e).finish(),
            Self::CborDecode(e) => f.debug_tuple("CborDecode").field(e).finish(),
            Self::Timeout(e) => f.debug_tuple("Timeout").field(e).finish(),
            Self::AttrKeyNaming => f.debug_tuple("AttrKeyNaming").finish(),
            Self::Io(e) => f.debug_tuple("Io").field(e).finish(),
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
