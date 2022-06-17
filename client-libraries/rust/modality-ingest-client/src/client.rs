use crate::{
    protocol::{IngestMessage, IngestResponse, PackedAttrKvs},
    types::{AttrKey, AttrVal, TimelineId},
};
use minicbor_io::{AsyncReader, AsyncWriter};
use std::collections::HashMap;
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
    id_packer: IdPacker,
}

impl IngestClientCommon {
    #[doc(hidden)]
    pub fn new(timeout: Duration, connection: IngestConnection) -> Self {
        IngestClientCommon {
            timeout,
            connection,
            id_packer: IdPacker::default(),
        }
    }

    /// Send a message and wait for a required response.
    #[doc(hidden)]
    pub async fn send_recv(&mut self, msg: &IngestMessage) -> Result<IngestMessage, IngestError> {
        self.connection.write_msg(msg).await?;
        timeout(self.timeout, self.connection.read_msg()).await?
    }

    /// Send a message.
    #[doc(hidden)]
    pub async fn send(&mut self, msg: &IngestMessage) -> Result<(), IngestError> {
        self.connection.write_msg(msg).await
    }

    /// Send a message.
    #[doc(hidden)]
    pub async fn recv(&mut self) -> Result<IngestMessage, IngestError> {
        self.connection.read_msg().await
    }
}

#[derive(Default)]
struct IdPacker {
    next_id: u32,
    attr_keys_ids: HashMap<AttrKey, u32>,
}

impl IdPacker {
    fn attr_key_id(&mut self, key: &AttrKey, newly_added: &mut Vec<(AttrKey, u32)>) -> u32 {
        if let Some(id) = self.attr_keys_ids.get(key) {
            *id
        } else {
            let id = self.next_id;
            self.next_id += 1;
            self.attr_keys_ids.insert(key.clone(), id);
            newly_added.push((key.clone(), id));
            id
        }
    }

    /// Return a tuple of:
    /// - Packed attr keys (int form) /vals
    /// - Set of any attrkeys with newly allocated ids
    fn pack_attrs(
        &mut self,
        attrs: impl IntoIterator<Item = (AttrKey, AttrVal)>,
    ) -> (PackedAttrKvs, Vec<(AttrKey, u32)>) {
        let mut new_attr_keys = vec![];
        let packed_attrs = PackedAttrKvs(
            attrs
                .into_iter()
                .map(|(k, v)| (self.attr_key_id(&k, &mut new_attr_keys), v))
                .collect::<Vec<_>>(),
        );

        (packed_attrs, new_attr_keys)
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

    async fn read_msg(&mut self) -> Result<IngestMessage, IngestError> {
        match self {
            IngestConnection::Tcp(s) => {
                let msg_len = s.read_u32().await?; // yes, this is big-endian
                let mut msg_buf = vec![0u8; msg_len as usize];
                s.read_exact(msg_buf.as_mut_slice()).await?;

                Ok(minicbor::decode::<IngestMessage>(&msg_buf)?)
            }
            IngestConnection::Tls(s) => {
                let msg_len = s.read_u32().await?; // yes, this is big-endian
                let mut msg_buf = vec![0u8; msg_len as usize];
                s.read_exact(msg_buf.as_mut_slice()).await?;

                Ok(minicbor::decode::<IngestMessage>(&msg_buf)?)
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
            IngestMessage::AuthResponse { ok, message } => {
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
        self.common
            .send(&IngestMessage::IngestStatusRequest {})
            .await?;

        let resp = timeout(self.common.timeout * 10, self.common.recv()).await??;

        match resp {
            IngestMessage::IngestStatusResponse {
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
        let (packed_attrs, new_attr_keys, packed_tags, new_tags) =
            self.id_packer.pack_attrs(attrs.into_iter());

        for (key, wire_id) in new_attr_keys {
            self.send(&IngestMessage::DeclareAttrKey {
                name: key.as_str().to_owned(),
                wire_id,
            })
            .await?;
        }

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
        let (packed_attrs, new_attr_keys, packed_tags, new_tags) =
            self.id_packer.pack_attrs(attrs.into_iter());

        for (key, wire_id) in new_attr_keys {
            self.send(&IngestMessage::DeclareAttrKey {
                name: key.as_str().to_owned(),
                wire_id,
            })
            .await?;
        }

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
            Self::Io(e) => f.debug_tuple("Io").field(e).finish(),
        }
    }
}
