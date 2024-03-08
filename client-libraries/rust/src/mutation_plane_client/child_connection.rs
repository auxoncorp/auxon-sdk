#![cfg_attr(not(unix), allow(unused))]

use crate::api::{AttrKey, AttrVal};
use crate::mutation_plane::protocol::{
    LeafwardsMessage, RootwardsMessage, MUTATION_PROTOCOL_VERSION,
};
use crate::mutation_plane::types::{ParticipantId, TriggerCRDT};
use minicbor_io::{AsyncReader, AsyncWriter};
use std::collections::BTreeMap;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

pub struct AuthReq {
    pub is_direct: bool,
    pub token: Vec<u8>,
    pub participant_id: ParticipantId,
    pub response_tx: oneshot::Sender<AuthResponse>,
}

#[derive(Debug)]
pub enum AuthResponse {
    DirectAuthOk {
        connection_id: ChildConnectionId,
        message: Option<String>,
        rootwards_tx: mpsc::Sender<Rootwards>,
        leafwards_rx: mpsc::Receiver<LeafwardsMessage>,
    },
    DelegatingAuthOk {
        message: Option<String>,
    },
    NotAuth {
        message: Option<String>,
    },
}
/// Opaque, internal-only id for child connections
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ChildConnectionId(pub uuid::Uuid);

impl std::fmt::Display for ChildConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        uuid::fmt::Hyphenated::from_uuid(self.0).fmt(f)
    }
}

/// Messages from an already-authenticated child connection.
#[derive(Debug)]
pub enum Rootwards {
    // Probably need to send ChildConnectionId along with all Rootwards messages
    MutatorAnnouncement {
        connection_id: ChildConnectionId,
        /// Id of the participant in charge of managing the mutator
        participant_id: ParticipantId,
        mutator_id: crate::mutation_plane::types::MutatorId,
        mutator_attrs: BTreeMap<AttrKey, AttrVal>,
    },
    MutatorRetirement {
        connection_id: ChildConnectionId,
        /// Id of the participant in charge of managing the mutator
        participant_id: ParticipantId,
        mutator_id: crate::mutation_plane::types::MutatorId,
    },
    UpdateTriggerState {
        connection_id: ChildConnectionId,

        mutator_id: crate::mutation_plane::types::MutatorId,
        mutation_id: crate::mutation_plane::types::MutationId,
        /// Interpret "None" as "clear your trigger state for this mutation, you don't need to
        /// track it (anymore)"
        maybe_trigger_crdt: Option<TriggerCRDT>,
    },
}
impl Rootwards {
    pub fn connection_id(&self) -> ChildConnectionId {
        match self {
            Rootwards::MutatorAnnouncement { connection_id, .. } => *connection_id,
            Rootwards::MutatorRetirement { connection_id, .. } => *connection_id,
            Rootwards::UpdateTriggerState { connection_id, .. } => *connection_id,
        }
    }
}

pub async fn mutation_protocol_child_tcp_connection(
    mut stream: TcpStream,
    shutdown: broadcast::Receiver<()>,
    auth_tx: mpsc::Sender<AuthReq>,
) -> (
    Option<ChildConnectionId>,
    Result<(), Box<dyn std::error::Error>>,
) {
    let (reader, writer) = stream.split();
    let msg_reader = AsyncReader::new(reader.compat());
    let msg_writer = AsyncWriter::new(writer.compat_write());
    mutation_protocol_child_connection(msg_reader, msg_writer, shutdown, auth_tx).await
}

/// This code LOOKS the same as tcp_connection, but none of the TYPES are common. So we get to
/// duplicate it.
#[cfg(unix)]
pub async fn mutation_protocol_child_uds_connection(
    mut stream: tokio::net::UnixStream,
    shutdown: broadcast::Receiver<()>,
    auth_tx: mpsc::Sender<AuthReq>,
) -> (
    Option<ChildConnectionId>,
    Result<(), Box<dyn std::error::Error>>,
) {
    let (reader, writer) = stream.split();
    let msg_reader = AsyncReader::new(reader.compat());
    let msg_writer = AsyncWriter::new(writer.compat_write());
    mutation_protocol_child_connection(msg_reader, msg_writer, shutdown, auth_tx).await
}

pub async fn mutation_protocol_child_connection<R, W>(
    mut msg_reader: AsyncReader<R>,
    mut msg_writer: AsyncWriter<W>,
    mut shutdown_rx: broadcast::Receiver<()>,
    auth_tx: mpsc::Sender<AuthReq>,
) -> (
    Option<ChildConnectionId>,
    Result<(), Box<dyn std::error::Error>>,
)
where
    R: futures::AsyncRead + Unpin,
    W: futures::AsyncWrite + Unpin,
{
    let mut unauth_state = UnauthenticatedConnectionState { auth_tx };
    let mut ready_state = loop {
        tokio::select! {
            msg = msg_reader.read::<RootwardsMessage>() => {
                let msg = match msg {
                    Ok(Some(msg)) => msg,
                    Ok(None) => return (None, Ok(())),
                    Err(minicbor_io::Error::Decode(e)) => {
                        tracing::error!(
                            error = &e as &dyn std::error::Error,
                            "Dropping invalid message during unauth state"
                        );
                        continue;
                    }
                    Err(e) => return (None, Err(e.into())),
                };

                match unauth_state.handle_rootwards_message(msg).await {
                    UnauthenticatedMessageOutcome::Proceed { state, reply } => {
                        if let Err(e) = msg_writer.write(reply).await {
                            return (Some(state.connection_id), Err(e.into()));
                        }
                        break state;
                    }
                    UnauthenticatedMessageOutcome::StayPut { state, reply } => {
                        if let Err(e) = msg_writer.write(reply).await {
                            return (None, Err(e.into()));
                        }
                        unauth_state = state;
                    }
                }
            },
            _ = shutdown_rx.recv() => {
                tracing::info!("Mutation protocol child connection received shutdown request while still unauthenticated.");
                return (None, Ok(()))
            }
        }
    };
    tracing::trace!("Mutation protocol client authenticated");

    loop {
        tokio::select! {
            // Pull from the channel coming from parent
            maybe_leafwards = ready_state.leafwards_rx.recv() => {
                match maybe_leafwards {
                    Some(leafwards) => {
                        if let Err(e) = msg_writer.write(leafwards).await {
                            return (Some(ready_state.connection_id), Err(e.into()));
                        }
                    },
                    None => {
                        tracing::warn!("Internal leafwards channel closed early unexpectedly for mutation protocol child connection.");
                        return (Some(ready_state.connection_id), Ok(()));
                    }
                }
            },
            // Pull from the network coming from child
            maybe_rootwards_result = msg_reader.read::<RootwardsMessage>() => {
                let msg: RootwardsMessage = match maybe_rootwards_result {
                    Ok(Some(msg)) => msg,
                    Ok(None) => return (Some(ready_state.connection_id), Ok(())),
                    Err(minicbor_io::Error::Decode(e)) => {
                        tracing::error!(error = &e as &dyn std::error::Error, "Dropping invalid message during ready state.");
                        continue;
                    }
                    Err(e) => return (Some(ready_state.connection_id), Err(e.into())),
                };
                let ReadyMessageOutcome {
                    reply_to_child, send_to_root
                } = ready_state.handle_rootwards_message(msg).await;
                if let Some(reply) = reply_to_child {
                    if let Err(e) = msg_writer.write(reply).await {
                        return (Some(ready_state.connection_id), Err(e.into()));
                    }
                }
                if let Some(rootwards) = send_to_root {
                    if let Err(e) = ready_state.rootwards_tx.send(rootwards).await {
                        tracing::error!(error = &e as &dyn std::error::Error, "Could not send rootwards message from child connection over internal channel.");
                    }
                }
            },
            _ = shutdown_rx.recv() => {
                tracing::info!("Mutation protocol child connection received shutdown request while in the ready state");
                return (Some(ready_state.connection_id), Ok(()))
            }
        }
    }
}
/// After handling a message, a connection can stay unauthenticated, or can move forward to 'ready'.
enum UnauthenticatedMessageOutcome {
    Proceed {
        state: ReadyConnectionState,
        reply: LeafwardsMessage,
    },
    StayPut {
        state: UnauthenticatedConnectionState,
        reply: LeafwardsMessage,
    },
}
struct UnauthenticatedConnectionState {
    auth_tx: tokio::sync::mpsc::Sender<AuthReq>,
}

impl UnauthenticatedConnectionState {
    async fn handle_rootwards_message(
        self,
        msg: RootwardsMessage,
    ) -> UnauthenticatedMessageOutcome {
        match msg {
            RootwardsMessage::ChildAuthAttempt {
                child_participant_id,
                version,
                token,
            } => {
                tracing::debug!(version = version, participant_id = %child_participant_id, "Auth attempt from unauthorized child connection");
                let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                if self
                    .auth_tx
                    .send(AuthReq {
                        // We are trying to auth the child participant
                        // sitting at the top of the participant tree directly
                        // on the other side of this network connection
                        is_direct: true,
                        token,
                        participant_id: child_participant_id,
                        response_tx,
                    })
                    .await
                    .is_err()
                {
                    UnauthenticatedMessageOutcome::StayPut {
                        state: self,
                        reply: LeafwardsMessage::ChildAuthOutcome {
                            child_participant_id,
                            version: MUTATION_PROTOCOL_VERSION,
                            ok: false,
                            message: Some(
                                "Could not send auth request over internal channel".to_owned(),
                            ),
                        },
                    }
                } else {
                    match response_rx.await {
                        Ok(resp) => {
                            match resp {
                                AuthResponse::DirectAuthOk { connection_id, message, rootwards_tx, leafwards_rx } => {
                                    UnauthenticatedMessageOutcome::Proceed {
                                        state: ReadyConnectionState {
                                            connection_id,
                                            auth_tx: self.auth_tx,
                                            leafwards_rx,
                                            rootwards_tx
                                        },
                                        reply: LeafwardsMessage::ChildAuthOutcome {
                                            child_participant_id,
                                            version: MUTATION_PROTOCOL_VERSION,
                                            ok: true,
                                            message
                                        }
                                    }
                                }
                                AuthResponse::DelegatingAuthOk { message } => {
                                    UnauthenticatedMessageOutcome::StayPut {
                                        state: self,
                                        reply: LeafwardsMessage::ChildAuthOutcome {
                                            child_participant_id,
                                            version: MUTATION_PROTOCOL_VERSION,
                                            ok: true,
                                            message
                                        }
                                    }
                                }
                                AuthResponse::NotAuth { message } => {
                                    UnauthenticatedMessageOutcome::StayPut { state: self, reply: LeafwardsMessage::ChildAuthOutcome {
                                        child_participant_id,
                                        version: MUTATION_PROTOCOL_VERSION,
                                        ok: false,
                                        message
                                    } }
                                }
                            }
                        },
                        Err(_recv_err) => {
                            UnauthenticatedMessageOutcome::StayPut { state: self, reply: LeafwardsMessage::ChildAuthOutcome {
                                child_participant_id,
                                version: MUTATION_PROTOCOL_VERSION,
                                ok: false,
                                message:Some("Mutation plane child connection could not receive auth request over internal channel.".to_owned())
                            } }
                        }
                    }
                }
            }
            _ => UnauthenticatedMessageOutcome::StayPut {
                state: self,
                reply: LeafwardsMessage::UnauthenticatedResponse {},
            },
        }
    }
}

struct ReadyConnectionState {
    connection_id: ChildConnectionId,
    auth_tx: tokio::sync::mpsc::Sender<AuthReq>,
    leafwards_rx: tokio::sync::mpsc::Receiver<LeafwardsMessage>,
    rootwards_tx: tokio::sync::mpsc::Sender<Rootwards>,
}

struct ReadyMessageOutcome {
    reply_to_child: Option<LeafwardsMessage>,
    send_to_root: Option<Rootwards>, // Maybe upwards things? Maybe deal with it all inline?
}

impl ReadyConnectionState {
    async fn handle_rootwards_message(&mut self, msg: RootwardsMessage) -> ReadyMessageOutcome {
        match msg {
            RootwardsMessage::ChildAuthAttempt {
                child_participant_id,
                version,
                token,
            } => {
                tracing::debug!(version = version, participant_id = %child_participant_id, "Auth attempt from already-authorized child connection");
                let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                if self
                    .auth_tx
                    .send(AuthReq {
                        // We are passing along an auth request from a further descendant in the
                        // participant tree.
                        is_direct: false,
                        token,
                        participant_id: child_participant_id,
                        response_tx,
                    })
                    .await
                    .is_err()
                {
                    ReadyMessageOutcome {
                        reply_to_child: Some(LeafwardsMessage::ChildAuthOutcome {
                            child_participant_id,
                            version: MUTATION_PROTOCOL_VERSION,
                            ok: false,
                            message: Some(
                                "Could not send auth request over internal channel".to_owned(),
                            ),
                        }),
                        send_to_root: None,
                    }
                } else {
                    match response_rx.await {
                        Ok(resp) => {
                            match resp {
                                AuthResponse::DirectAuthOk { connection_id: _, message, rootwards_tx: _, leafwards_rx : _} => {
                                    ReadyMessageOutcome {
                                        reply_to_child: Some(LeafwardsMessage::ChildAuthOutcome {
                                            child_participant_id,
                                            version: MUTATION_PROTOCOL_VERSION,
                                            ok: true,
                                            message
                                        }),
                                        send_to_root: None
                                    }
                                }
                                AuthResponse::DelegatingAuthOk { message } => {
                                    ReadyMessageOutcome {
                                        reply_to_child: Some(LeafwardsMessage::ChildAuthOutcome {
                                            child_participant_id,
                                            version: MUTATION_PROTOCOL_VERSION,
                                            ok: true,
                                            message
                                        }),
                                        send_to_root: None
                                    }
                                }
                                AuthResponse::NotAuth { message } => {
                                    ReadyMessageOutcome { reply_to_child: Some(LeafwardsMessage::ChildAuthOutcome {
                                        child_participant_id,
                                        version: MUTATION_PROTOCOL_VERSION,
                                        ok: false,
                                        message
                                    }),
                                        send_to_root: None
                                    }
                                }
                            }
                        },
                        Err(_recv_err) => {
                            ReadyMessageOutcome { reply_to_child: Some(LeafwardsMessage::ChildAuthOutcome {
                                child_participant_id,
                                version: MUTATION_PROTOCOL_VERSION,
                                ok: false,
                                message:Some("Mutation plane child connection could not receive auth request over internal channel.".to_owned())
                            }),
                                send_to_root: None
                            }
                        }
                    }
                }
            }
            RootwardsMessage::MutatorAnnouncement {
                participant_id,
                mutator_id,
                mutator_attrs,
            } => ReadyMessageOutcome {
                reply_to_child: None,
                send_to_root: Some(Rootwards::MutatorAnnouncement {
                    connection_id: self.connection_id,
                    participant_id,
                    mutator_id,
                    mutator_attrs: mutator_attrs
                        .0
                        .into_iter()
                        .map(|kv| (AttrKey::from(kv.key), kv.value))
                        .collect(),
                }),
            },
            RootwardsMessage::MutatorRetirement {
                participant_id,
                mutator_id,
            } => ReadyMessageOutcome {
                reply_to_child: None,
                send_to_root: Some(Rootwards::MutatorRetirement {
                    connection_id: self.connection_id,
                    participant_id,
                    mutator_id,
                }),
            },
            RootwardsMessage::UpdateTriggerState {
                mutator_id,
                mutation_id,
                maybe_trigger_crdt,
            } => ReadyMessageOutcome {
                reply_to_child: None,
                send_to_root: Some(Rootwards::UpdateTriggerState {
                    connection_id: self.connection_id,
                    mutator_id,
                    mutation_id,
                    maybe_trigger_crdt,
                }),
            },
        }
    }
}
