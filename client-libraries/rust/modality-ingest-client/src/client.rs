use crate::{
    protocol::{IngestMessage, IngestResponse, PackedAttrKvs},
    types::{AttrVal, EventAttrKey, TimelineAttrKey, TimelineId},
};
use minicbor_io::{AsyncReader, AsyncWriter};
use std::{net::SocketAddr, time::Duration};
use thiserror::Error;
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpSocket,
    },
    time::timeout,
};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

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

// Fields used by the client in every state
struct IngestClientCommon {
    timeout: Duration,
    msg_reader: AsyncReader<Compat<OwnedReadHalf>>,
    msg_writer: AsyncWriter<Compat<OwnedWriteHalf>>,
    next_id: u32,
}

impl IngestClientCommon {
    async fn timeline_attr_key(
        &mut self,
        key_name: String,
    ) -> Result<TimelineAttrKey, IngestError> {
        if !key_name.starts_with("timeline.") {
            return Err(IngestError::AttrKeyNaming);
        }

        let wire_id = self.next_id;
        self.next_id += 1;

        self.msg_writer
            .write(&IngestMessage::DeclareAttrKey {
                name: key_name,
                wire_id,
            })
            .await?;

        Ok(TimelineAttrKey(wire_id))
    }

    async fn event_attr_key(&mut self, key_name: String) -> Result<EventAttrKey, IngestError> {
        if !key_name.starts_with("event.") {
            return Err(IngestError::AttrKeyNaming);
        }

        let wire_id = self.next_id;
        self.next_id += 1;

        self.msg_writer
            .write(&IngestMessage::DeclareAttrKey {
                name: key_name,
                wire_id,
            })
            .await?;

        Ok(EventAttrKey(wire_id))
    }
}

impl IngestClient<UnauthenticatedState> {
    pub async fn new(
        remote_addr: SocketAddr,
    ) -> Result<IngestClient<UnauthenticatedState>, IngestClientInitializationError> {
        let local_addr: SocketAddr = if remote_addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()?;

        let socket = if remote_addr.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };

        socket.bind(local_addr)?;
        let stream = socket.connect(remote_addr).await?;

        let (reader, writer) = stream.into_split();
        let msg_reader = AsyncReader::new(reader.compat());
        let msg_writer = AsyncWriter::new(writer.compat_write());

        Ok(IngestClient {
            state: UnauthenticatedState {},
            common: IngestClientCommon {
                timeout: Duration::from_secs(1),
                msg_reader,
                msg_writer,
                next_id: 0,
            },
        })
    }

    pub async fn authenticate(
        mut self,
        token: Vec<u8>,
    ) -> Result<IngestClient<ReadyState>, IngestError> {
        self.common
            .msg_writer
            .write(IngestMessage::AuthRequest { token })
            .await?;
        let resp = timeout(
            self.common.timeout,
            self.common.msg_reader.read::<IngestResponse>(),
        )
        .await??;
        match resp {
            Some(IngestResponse::AuthResponse { ok, message }) => {
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
                "Invalid response recieved in the 'Unauthenticated' state.",
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
            .msg_writer
            .write(&IngestMessage::OpenTimeline { id })
            .await?;

        Ok(IngestClient {
            state: BoundTimelineState { timeline_id: id },
            common: self.common,
        })
    }

    pub async fn timeline_attr_key(
        &mut self,
        key_name: String,
    ) -> Result<TimelineAttrKey, IngestError> {
        self.common.timeline_attr_key(key_name).await
    }

    pub async fn event_attr_key(&mut self, key_name: String) -> Result<EventAttrKey, IngestError> {
        self.common.event_attr_key(key_name).await
    }
}

impl IngestClient<BoundTimelineState> {
    pub fn bound_timeline(&self) -> TimelineId {
        self.state.timeline_id
    }

    pub async fn open_timeline(&mut self, id: TimelineId) -> Result<(), IngestError> {
        self.common
            .msg_writer
            .write(&IngestMessage::OpenTimeline { id })
            .await?;
        self.state.timeline_id = id;
        Ok(())
    }

    pub fn close_timeline(self) -> IngestClient<ReadyState> {
        IngestClient {
            state: ReadyState {},
            common: self.common,
        }
    }

    pub async fn timeline_attr_key(
        &mut self,
        key_name: String,
    ) -> Result<TimelineAttrKey, IngestError> {
        self.common.timeline_attr_key(key_name).await
    }

    pub async fn event_attr_key(&mut self, key_name: String) -> Result<EventAttrKey, IngestError> {
        self.common.event_attr_key(key_name).await
    }

    pub async fn timeline_metadata(
        &mut self,
        attrs: impl IntoIterator<Item = (TimelineAttrKey, AttrVal)>,
    ) -> Result<(), IngestError> {
        let attrs = PackedAttrKvs(attrs.into_iter().collect());

        self.common
            .msg_writer
            .write(&IngestMessage::TimelineMetadata { attrs })
            .await?;
        Ok(())
    }

    pub async fn event(
        &mut self,
        ordering: u128,
        attrs: impl IntoIterator<Item = (EventAttrKey, AttrVal)>,
    ) -> Result<(), IngestError> {
        let attrs = PackedAttrKvs(attrs.into_iter().collect());

        let be_ordering = ordering.to_be_bytes();
        let mut i = 0;
        while i < 15 {
            if be_ordering[i] != 0x00 {
                break;
            }
            i += 1;
        }
        let compact_be_ordering = be_ordering[i..16].to_vec();

        self.common
            .msg_writer
            .write(&IngestMessage::Event {
                be_ordering: compact_be_ordering,
                attrs,
            })
            .await?;

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), IngestError> {
        self.common
            .msg_writer
            .write(&IngestMessage::Flush {})
            .await?;

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum IngestClientInitializationError {
    #[error("Socket setup error")]
    SocketSetup(#[from] std::io::Error),
    #[error("Client local address parsing failed.")]
    ClientLocalAddrParse(#[from] std::net::AddrParseError),
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

    #[error("Marshalling Error")]
    Marshalling(#[from] minicbor_io::Error),

    #[error("Timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),

    #[error("Event attr keys must begin with 'event.', and timeline attr keys must begin with 'timeline.'")]
    AttrKeyNaming,
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
            Self::Marshalling(e) => f.debug_tuple("Marshalling").field(e).finish(),
            Self::Timeout(e) => f.debug_tuple("Timeout").field(e).finish(),
            Self::AttrKeyNaming => f.debug_tuple("AttrKeyNaming").finish(),
        }
    }
}
