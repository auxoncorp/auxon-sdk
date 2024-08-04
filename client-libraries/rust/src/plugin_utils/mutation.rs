#![allow(unused)]

use tracing::{debug, error, info, trace, warn};
use url::Url;
use uuid::Uuid;

use crate::{
    api::{AttrKey, AttrVal, TimelineId},
    auth_token::AuthToken,
    mutation_plane::{
        protocol::{LeafwardsMessage, RootwardsMessage, MUTATION_PROTOCOL_VERSION},
        types::{MutationId, MutatorId, ParticipantId},
    },
    mutation_plane_client::parent_connection::{
        CommsError, MutationParentClientInitializationError, MutationParentConnection,
    },
    mutator_protocol::{
        actuator::MutatorActuator,
        descriptor::{
            owned::{
                MutatorLayer, MutatorOperation, MutatorStatefulness, OrganizationCustomMetadata,
                OwnedMutatorDescriptor,
            },
            MutatorDescriptor,
        },
        mutator::ActuatorDescriptor,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::Duration,
};

pub trait Mutator {
    fn id(&self) -> MutatorId;
    fn descriptor(&self) -> OwnedMutatorDescriptor;

    /// Return true on success, false on failure
    fn inject(&mut self, mutation_id: MutationId, params: BTreeMap<String, AttrVal>) -> bool;
    fn clear_mutation(&mut self, mutation_id: &MutationId);
    fn reset(&mut self);
}

pub struct MutatorHost {
    participant_id: ParticipantId,
    pub mutation_conn: MutationParentConnection,
    mutators: BTreeMap<MutatorId, Box<dyn Mutator + Send>>,
    active_mutations: HashMap<MutatorId, HashSet<MutationId>>,

    ingest: Option<super::ingest::Client>,
    ingest_ordering: u128,
    log_comms: bool,
    log_inject_and_clear: bool,
}

impl MutatorHost {
    pub async fn connect_and_authenticate(
        endpoint: &Url,
        allow_insecure_tls: bool,
        auth_token: AuthToken,
        mut ingest: Option<super::ingest::Client>,
    ) -> Result<MutatorHost, MutationParentClientInitializationError> {
        debug!(%endpoint, %allow_insecure_tls, "Connecting to mutation plane");

        let mut ingest_ordering = 0u128;
        if let Some(i) = ingest.as_mut() {
            let tl_id = TimelineId::allocate();
            i.switch_timeline(tl_id).await.unwrap();
            i.send_timeline_attrs("MutatorHost", []).await.unwrap();
            let _ = i
                .send_event("connecting_to_mutation_plane", ingest_ordering, [])
                .await;
            ingest_ordering += 1;
        }

        let mut mutation_conn =
            MutationParentConnection::connect(endpoint, allow_insecure_tls).await?;

        let mut_plane_pid = ParticipantId::allocate();
        debug!(%mut_plane_pid, "Authenticating");
        if let Some(i) = ingest.as_mut() {
            let _ = i
                .send_event(
                    "authenticating",
                    ingest_ordering,
                    [("participant_id", mut_plane_pid.to_string().into())],
                )
                .await;
            ingest_ordering += 1;
        }

        mutation_conn
            .write_msg(&RootwardsMessage::ChildAuthAttempt {
                child_participant_id: mut_plane_pid,
                version: MUTATION_PROTOCOL_VERSION,
                token: auth_token.as_ref().to_vec(),
            })
            .await;

        debug!("Awaiting authentication response");
        match mutation_conn.read_msg().await? {
            LeafwardsMessage::ChildAuthOutcome {
                child_participant_id,
                version: _,
                ok,
                message,
            } => {
                if child_participant_id == mut_plane_pid {
                    if ok {
                        if let Some(i) = ingest.as_mut() {
                            let _ = i.send_event("authenticated", ingest_ordering, []).await;
                            ingest_ordering += 1;
                        }
                    } else {
                        if let Some(i) = ingest.as_mut() {
                            let _ = i
                                .send_event(
                                    "authentication_failed",
                                    ingest_ordering,
                                    message.as_ref().map(|s| ("message", AttrVal::from(s))),
                                )
                                .await;
                        }
                        return Err(
                            MutationParentClientInitializationError::AuthenticationFailed(
                                message.unwrap_or_else(|| "(no message)".to_string()),
                            ),
                        );
                    }
                } else {
                    if let Some(i) = ingest.as_mut() {
                        let _ = i
                            .send_event(
                                "authentication_failed",
                                ingest_ordering,
                                message.as_ref().map(|s| ("message", AttrVal::from(s))),
                            )
                            .await;
                    }
                    error!("Mutation plane auth outcome received for a different participant");
                    return Err(MutationParentClientInitializationError::AuthWrongParticipant);
                }
            }
            resp => {
                error!(?resp, "Mutation plane unexpected auth response");
                return Err(MutationParentClientInitializationError::UnexpectedAuthResponse);
            }
        }

        debug!("Authenticated");
        let mut conn = MutatorHost {
            participant_id: mut_plane_pid,
            mutation_conn,
            mutators: Default::default(),
            active_mutations: Default::default(),

            ingest,
            ingest_ordering: 0,
            log_comms: true,
            log_inject_and_clear: true,
        };

        conn.send_event("mutation_plane_connected", []).await;
        Ok(conn)
    }

    /// Disable automatic logging of 'mutation communicated' events on the mutator timeline.
    pub fn disable_mutation_communicated_logging(&mut self) {
        self.log_comms = false;
    }

    /// Disable automatic logging of mutation injected/cleared events on the mutator host timeline.
    ///
    /// You might want to do this if you have arranged to log those events separately, on a timeline
    /// that is more directly relevant to system operation.
    pub fn disable_mutation_inject_and_clear_logging(&mut self) {
        self.log_inject_and_clear = false;
    }

    pub async fn register_mutator(
        &mut self,
        mutator: Box<dyn Mutator + Send>,
    ) -> Result<(), CommsError> {
        let mutator_id = mutator.id();
        let ann = mutator_announcement(self.participant_id, mutator.as_ref(), &mutator_id);
        self.mutators.insert(mutator.id(), mutator);
        self.mutation_conn.write_msg(&ann).await?;

        self.send_event(
            "modality.mutator.announced",
            [("event.mutator.id", mutator_id_to_attr_val(mutator_id))],
        )
        .await;

        Ok(())
    }

    pub async fn message_loop(&mut self) -> Result<(), CommsError> {
        loop {
            let msg = self.mutation_conn.read_msg().await?;
            self.handle_message(msg).await;
        }
    }

    pub async fn handle_message(&mut self, msg: LeafwardsMessage) {
        trace!(?msg, "handle_message");
        match msg {
            LeafwardsMessage::RequestForMutatorAnnouncements {} => {
                self.announce_all_mutators().await;
            }

            LeafwardsMessage::NewMutation {
                mutator_id,
                mutation_id,
                maybe_trigger_mask: _,
                params,
            } => {
                self.new_mutation(mutator_id, mutation_id, params).await;
            }

            LeafwardsMessage::ClearSingleMutation {
                mutator_id,
                mutation_id,
                reset_if_active,
            } => {
                self.clear_single_mutation(mutator_id, mutation_id, reset_if_active)
                    .await;
            }

            LeafwardsMessage::ClearMutationsForMutator {
                mutator_id,
                reset_if_active,
            } => {
                self.clear_mutations_for_mutator(mutator_id, reset_if_active)
                    .await;
            }

            LeafwardsMessage::ClearMutations {} => {
                self.clear_mutations().await;
            }

            LeafwardsMessage::UpdateTriggerState {
                mutator_id: _,
                mutation_id: _,
                maybe_trigger_crdt: _,
            } => {
                // Not yet implemented
            }

            _ => {
                warn!("Unexpected message");
                self.send_event("unexpected_message", []).await;
            }
        }
    }

    async fn announce_all_mutators(&mut self) {
        // We can't use the mutators iterator across an await point
        let mut announces = Vec::with_capacity(self.mutators.len());
        let mut mutator_ids = Vec::with_capacity(self.mutators.len());
        for (mutator_id, mutator) in self.mutators.iter() {
            let ann = mutator_announcement(self.participant_id, mutator.as_ref(), mutator_id);
            announces.push(ann);
            mutator_ids.push(*mutator_id);
        }

        for ann in announces.into_iter() {
            if let Err(e) = self.mutation_conn.write_msg(&ann).await {
                error!(
                    err = &e as &dyn std::error::Error,
                    "Failed to announce mutator; aborting batch announce"
                );
                // There's no reason to believe the next one would work
                return;
            }
        }

        for mutator_id in mutator_ids.into_iter() {
            self.send_event(
                "modality.mutator.announced",
                [("event.mutator.id", mutator_id_to_attr_val(mutator_id))],
            )
            .await;
        }
    }

    async fn clear_single_mutation(
        &mut self,
        mutator_id: MutatorId,
        mutation_id: MutationId,
        reset_if_active: bool,
    ) {
        self.send_event(
            "modality.mutation.clear_communicated",
            [
                ("event.mutator.id", mutator_id_to_attr_val(mutator_id)),
                ("event.mutation.id", mutation_id_to_attr_val(mutation_id)),
                ("event.mutation.success", true.into()),
            ],
        )
        .await;

        let Some(mutator) = self.mutators.get_mut(&mutator_id) else {
            warn!(
                %mutator_id,
                %mutation_id,
                "Cannot clear mutation, mutator is not hosted by this client"
            );
            return;
        };

        let Some(active_mutation_ids_for_mutator) = self.active_mutations.get_mut(&mutator_id)
        else {
            warn!(
                %mutator_id,
                %mutation_id,
                "Cannot clear mutation, no active mutations for mutator"
            );
            return;
        };

        if !active_mutation_ids_for_mutator.remove(&mutation_id) {
            warn!(
                %mutator_id,
                %mutation_id,
                "Cannot clear mutation, mutation not active"
            );
            return;
        }

        tracing::debug!(%mutator_id, %mutation_id, "Clearing mutation");

        mutator.clear_mutation(&mutation_id);
        if reset_if_active {
            mutator.reset();
        }
    }

    async fn clear_mutations_for_mutator(&mut self, mutator_id: MutatorId, reset_if_active: bool) {
        let Some(mutator) = self.mutators.get_mut(&mutator_id) else {
            warn!(
                %mutator_id,
                "Cannot clear mutations, mutator is not hosted by this client"
            );
            return;
        };

        let Some(active_mutation_ids_for_mutator) = self.active_mutations.remove(&mutator_id)
        else {
            warn!(
                %mutator_id,
                "Cannot clear mutations, no active mutations for mutator"
            );
            return;
        };

        let mut cleared_mutations = vec![];
        for mutation_id in active_mutation_ids_for_mutator.into_iter() {
            cleared_mutations.push(mutation_id);
            tracing::debug!(%mutator_id, %mutation_id, "Clearing mutation");
            mutator.clear_mutation(&mutation_id);

            if reset_if_active {
                mutator.reset();
            }
        }

        for mutation_id in cleared_mutations {
            self.send_event(
                "modality.mutation.clear_communicated",
                [
                    ("event.mutator.id", mutator_id_to_attr_val(mutator_id)),
                    ("event.mutation.id", mutation_id_to_attr_val(mutation_id)),
                ],
            )
            .await;
        }
    }

    async fn clear_mutations(&mut self) {
        let mut cleared_mutations = vec![];
        for (mutator_id, active_mutation_ids_for_mutator) in self.active_mutations.drain() {
            let Some(mutator) = self.mutators.get_mut(&mutator_id) else {
                warn!(
                    %mutator_id,
                    "Inconsistent internal state; cannot clear mutations for unregistered mutator'"
                );
                continue;
            };

            for mutation_id in active_mutation_ids_for_mutator.into_iter() {
                cleared_mutations.push((mutator_id, mutation_id));
                mutator.clear_mutation(&mutation_id);
                tracing::debug!(%mutator_id, %mutation_id, "Clearing mutation");
            }

            mutator.reset();
        }

        for (mutator_id, mutation_id) in cleared_mutations {
            self.send_event(
                "modality.mutation.clear_communicated",
                [
                    ("event.mutator.id", mutator_id_to_attr_val(mutator_id)),
                    ("event.mutation.id", mutation_id_to_attr_val(mutation_id)),
                ],
            )
            .await;
        }
    }

    async fn new_mutation(
        &mut self,
        mutator_id: MutatorId,
        mutation_id: crate::mutation_plane::types::MutationId,
        params: crate::mutation_plane::types::AttrKvs,
    ) {
        self.send_event(
            "modality.mutation.command_communicated",
            [
                ("event.mutator.id", mutator_id_to_attr_val(mutator_id)),
                ("event.mutation.id", mutation_id_to_attr_val(mutation_id)),
            ],
        )
        .await;

        let Some(mutator) = self.mutators.get_mut(&mutator_id) else {
            tracing::warn!(
                mutator_id = %mutator_id,
                "Failed to handle new mutation, mutator not hosted by this client");
            return;
        };

        let success = mutator.inject(mutation_id, attr_kvs_to_map(params));
        self.active_mutations
            .entry(mutator_id)
            .or_default()
            .insert(mutation_id);

        self.send_event(
            "modality.mutation.injected",
            [
                ("event.mutator.id", mutator_id_to_attr_val(mutator_id)),
                ("event.mutation.id", mutation_id_to_attr_val(mutation_id)),
                ("event.mutation.success", success.into()),
            ],
        )
        .await;
    }

    async fn send_event(&mut self, name: &str, attrs: impl IntoIterator<Item = (&str, AttrVal)>) {
        let Some(i) = self.ingest.as_mut() else {
            return;
        };

        let res = i.send_event(name, self.ingest_ordering, attrs).await;

        if let Err(e) = res {
            warn!(
                err = &e as &dyn std::error::Error,
                "Failed to send event to modality"
            )
        }

        self.ingest_ordering += 1;
    }
}

fn attr_kvs_to_map(
    params: crate::mutation_plane::types::AttrKvs,
) -> BTreeMap<String, crate::api::AttrVal> {
    let mut map = BTreeMap::new();
    for kv in params.0.into_iter() {
        map.insert(kv.key, kv.value);
    }
    map
}

fn mutator_announcement(
    participant_id: ParticipantId,
    m: &(impl Mutator + ?Sized),
    mutator_id: &MutatorId,
) -> RootwardsMessage {
    let mutator_attrs = m
        .descriptor()
        .get_description_attributes()
        .map(|(k, value)| crate::mutation_plane::types::AttrKv {
            key: k.to_string(),
            value,
        })
        .collect();
    RootwardsMessage::MutatorAnnouncement {
        participant_id,
        mutator_id: *mutator_id,
        mutator_attrs: crate::mutation_plane::types::AttrKvs(mutator_attrs),
    }
}

const MUTATION_PROTOCOL_PARENT_URL_ENV_VAR: &str = "MUTATION_PROTOCOL_PARENT_URL";
const MUTATION_PROTOCOL_PARENT_URL_DEFAULT: &str = "modality-mutation://127.0.0.1:14192";

fn mutation_proto_parent_url() -> Result<url::Url, MutationProtocolUrlError> {
    match std::env::var(MUTATION_PROTOCOL_PARENT_URL_ENV_VAR) {
        Ok(val) => Ok(Url::parse(&val)?),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(MutationProtocolUrlError::EnvVarSpecifiedMutationProtoParentUrlNonUtf8)
        }
        Err(std::env::VarError::NotPresent) => {
            Ok(Url::parse(MUTATION_PROTOCOL_PARENT_URL_DEFAULT)?)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MutationProtocolUrlError {
    #[error(
        "The MUTATION_PROTOCOL_PARENT_URL environment variable contained a non-UTF-8-compatible string"
    )]
    EnvVarSpecifiedMutationProtoParentUrlNonUtf8,

    #[error("Mutation protocol parent URL error")]
    MutationProtoParentUrl(#[from] url::ParseError),
}

fn mutation_id_to_attr_val(mutation_id: MutationId) -> AttrVal {
    uuid_to_integer_attr_val(mutation_id.as_ref())
}

pub fn mutator_id_to_attr_val(mutator_id: MutatorId) -> AttrVal {
    uuid_to_integer_attr_val(mutator_id.as_ref())
}

fn uuid_to_integer_attr_val(u: &Uuid) -> AttrVal {
    i128::from_le_bytes(*u.as_bytes()).into()
}
