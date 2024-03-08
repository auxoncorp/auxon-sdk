//! The modality mutation plane is comprised of a tree of communicating participants
//! sharing information about mutators and mutations using this protocol.
//!
//! # Participants
//!
//! * A participant has a globally unique identifier
//! * A participant may have zero to many child participants.
//! * The root participant has zero parents; all other participants have one parent
//! * A participant may manage zero to many mutators.
//!
//! # Mutators
//!
//! * A mutator has a globally unique identifier
//! * A mutator may only be managed by a single participant
//! * A mutator may have zero to many "staged" mutations
//!     * These are not-yet-actuated mutations
//!     * Staged mutations are only actuated when their triggering conditions are met
//!     * Staged mutations may be canceled
//! * A mutator may have zero to one active mutation
//!     * This is the most-recently-actuated mutation
//!     * A mutation is considered no longer active when either the mutator is reset
//!       or a new mutation is actuated at the same mutator.
//!
//! # Mutations
//!
//! * A mutation has a globally unique identifier
//! * A mutation has a map of zero to many parameters (key-value pairs)
//! * A mutation optionally has an associated set of triggering conditions and
//!   CRDT-like state tracking of those conditions
//! * A mutator and its managing participant are responsible for ensuring that a mutation
//!   is only ever actuated zero or one times.
//!
//! # Messages
//!
//! Messages typically travel in a single direction - either from the leaf/descendant participants
//! towards the root, or from the root/ancestor participants towards the leaf/descendants.
//!
//! When a participant receives a rootwards message from its children, it is expected to propagate
//! the message to its parent.
//!
//! When a participant receives a leafwards message from its parent either:
//!  * The message specifically only has meaning for the current participant or its managed mutators
//!    and must be handled locally and not propagated. E.G. `ClearMutationsForMutator`, `ChildAuthOutcome`
//!  * The message may be relevant to some other participant, and must be propagated to children.
//!
//! The exception to these rules is `UpdateTriggerState`, which travels in both directions.
//!
//! When a participant receives this message, it attempts to update its internal
//! triggering state for that mutation. If anything changed in the internal state as a result
//! of incorporating the contents of the message, an `UpdateTriggerState` message should be sent
//! to the participant's parent and any children.

use crate::mutation_plane::types::*;
use minicbor::{data::Tag, decode, encode, Decode, Decoder, Encode, Encoder};
use uuid::Uuid;

pub const MUTATION_PROTOCOL_VERSION: u32 = 1;

#[derive(Encode, Decode, Debug, PartialEq, Clone)]
pub enum RootwardsMessage {
    #[n(1001)]
    ChildAuthAttempt {
        /// Id of the child (or further descendant) requesting authorization
        #[n(0)]
        child_participant_id: ParticipantId,
        /// Protocol version supported by the child participant
        #[n(1)]
        version: u32,
        /// Proof the child is worthy
        #[n(2)]
        token: Vec<u8>,
    },
    #[n(1012)]
    MutatorAnnouncement {
        /// Id of the participant in charge of managing the mutator
        #[n(0)]
        participant_id: ParticipantId,
        #[n(1)]
        mutator_id: MutatorId,
        #[n(2)]
        mutator_attrs: AttrKvs,
    },
    #[n(1023)]
    MutatorRetirement {
        /// Id of the participant in charge of managing the mutator
        #[n(0)]
        participant_id: ParticipantId,
        #[n(1)]
        mutator_id: MutatorId,
    },
    #[n(1044)]
    UpdateTriggerState {
        #[n(0)]
        mutator_id: MutatorId,
        #[n(1)]
        mutation_id: MutationId,
        /// Interpret "None" as "clear your trigger state for this mutation, you don't need to
        /// track it (anymore)"
        #[n(2)]
        maybe_trigger_crdt: Option<TriggerCRDT>,
    },
}

impl RootwardsMessage {
    pub fn name(&self) -> &'static str {
        match self {
            RootwardsMessage::ChildAuthAttempt { .. } => "ChildAuthAttempt",
            RootwardsMessage::MutatorAnnouncement { .. } => "MutatorAnnouncement",
            RootwardsMessage::MutatorRetirement { .. } => "MutatorRetirement",
            RootwardsMessage::UpdateTriggerState { .. } => "UpdateTriggerState",
        }
    }
}

#[derive(Encode, Decode, Debug, PartialEq, Clone)]
pub enum LeafwardsMessage {
    #[n(2001)]
    ChildAuthOutcome {
        /// Id of the child (or further descendant) that requested authorization
        #[n(0)]
        child_participant_id: ParticipantId,
        /// Protocol version supported by the ancestors
        #[n(1)]
        version: u32,
        /// Did the authorization succeed?
        #[n(2)]
        ok: bool,

        /// Possible explanation for outcome
        #[n(3)]
        message: Option<String>,
    },
    #[n(2002)]
    UnauthenticatedResponse {},
    #[n(2013)]
    RequestForMutatorAnnouncements {},
    #[n(2024)]
    NewMutation {
        #[n(0)]
        mutator_id: MutatorId,
        #[n(1)]
        mutation_id: MutationId,
        /// If Some, the mutation should not be actuated immediately,
        /// and instead should only be actuated when accumulated TriggerCRDT
        /// state (as updated by UpdateTriggerState messages) matches this value.
        /// If None, actuate the mutation immediately.
        #[n(2)]
        maybe_trigger_mask: Option<TriggerCRDT>,
        #[n(3)]
        params: AttrKvs,
    },
    #[n(2035)]
    ClearSingleMutation {
        #[n(0)]
        mutator_id: MutatorId,
        #[n(1)]
        mutation_id: MutationId,
        #[n(2)]
        reset_if_active: bool,
    },
    #[n(2036)]
    ClearMutationsForMutator {
        #[n(0)]
        mutator_id: MutatorId,
        #[n(2)]
        reset_if_active: bool,
    },
    #[n(2037)]
    ClearMutations {},
    #[n(2044)]
    UpdateTriggerState {
        #[n(0)]
        mutator_id: MutatorId,
        #[n(1)]
        mutation_id: MutationId,
        /// Interpret "None" as "clear your trigger state for this mutation, you don't need to
        /// track it (anymore)"
        #[n(2)]
        maybe_trigger_crdt: Option<TriggerCRDT>,
    },
}

impl LeafwardsMessage {
    pub fn name(&self) -> &'static str {
        match self {
            LeafwardsMessage::ChildAuthOutcome { .. } => "ChildAuthOutcome",
            LeafwardsMessage::UnauthenticatedResponse { .. } => "UnauthenticatedResponse",
            LeafwardsMessage::RequestForMutatorAnnouncements { .. } => {
                "RequestForMutatorAnnouncements"
            }
            LeafwardsMessage::NewMutation { .. } => "NewMutation",
            LeafwardsMessage::ClearSingleMutation { .. } => "ClearSingleMutation",
            LeafwardsMessage::ClearMutationsForMutator { .. } => "ClearMutationsForMutator",
            LeafwardsMessage::ClearMutations { .. } => "ClearMutations",
            LeafwardsMessage::UpdateTriggerState { .. } => "UpdateTriggerState",
        }
    }
}

const TAG_PARTICIPANT_ID: Tag = Tag::Unassigned(40200);
const TAG_MUTATOR_ID: Tag = Tag::Unassigned(40201);
const TAG_MUTATION_ID: Tag = Tag::Unassigned(40202);

impl Encode for ParticipantId {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_PARTICIPANT_ID)?.bytes(self.as_ref().as_bytes())?;
        Ok(())
    }
}

impl<'b> Decode<'b> for ParticipantId {
    fn decode(d: &mut Decoder<'b>) -> Result<Self, decode::Error> {
        let t = d.tag()?;
        if t != TAG_PARTICIPANT_ID {
            return Err(decode::Error::Message("Expected TAG_PARTICIPANT_ID"));
        }

        Uuid::from_slice(d.bytes()?)
            .map(Into::into)
            .map_err(|_uuid_err| decode::Error::Message("Error decoding uuid for ParticipantId"))
    }
}

impl Encode for MutatorId {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_MUTATOR_ID)?.bytes(self.as_ref().as_bytes())?;
        Ok(())
    }
}

impl<'b> Decode<'b> for MutatorId {
    fn decode(d: &mut Decoder<'b>) -> Result<Self, decode::Error> {
        let t = d.tag()?;
        if t != TAG_MUTATOR_ID {
            return Err(decode::Error::Message("Expected TAG_MUTATOR_ID"));
        }

        Uuid::from_slice(d.bytes()?)
            .map(Into::into)
            .map_err(|_uuid_err| decode::Error::Message("Error decoding uuid for MutatorId"))
    }
}

impl Encode for MutationId {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.tag(TAG_MUTATION_ID)?.bytes(self.as_ref().as_bytes())?;
        Ok(())
    }
}

impl<'b> Decode<'b> for MutationId {
    fn decode(d: &mut Decoder<'b>) -> Result<Self, decode::Error> {
        let t = d.tag()?;
        if t != TAG_MUTATION_ID {
            return Err(decode::Error::Message("Expected TAG_MUTATION_ID"));
        }

        Uuid::from_slice(d.bytes()?)
            .map(Into::into)
            .map_err(|_uuid_err| decode::Error::Message("Error decoding uuid for MutationId"))
    }
}

impl Encode for TriggerCRDT {
    fn encode<W: encode::Write>(&self, e: &mut Encoder<W>) -> Result<(), encode::Error<W::Error>> {
        e.array(self.as_ref().len() as u64)?;
        for byte in self.as_ref().iter() {
            e.u8(*byte)?;
        }

        Ok(())
    }
}

impl<'b> Decode<'b> for TriggerCRDT {
    fn decode(d: &mut Decoder<'b>) -> Result<Self, decode::Error> {
        let arr_len = d.array()?;

        if let Some(len) = arr_len {
            let mut bytes = Vec::with_capacity(len as usize);
            for _ in 0..len {
                bytes.push(d.u8()?);
            }
            Ok(TriggerCRDT::new(bytes))
        } else {
            Err(decode::Error::Message(
                "missing array length for TriggerCRDT",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn participant_id() -> impl Strategy<Value = ParticipantId> {
        any::<[u8; 16]>().prop_map(|arr| Uuid::from_bytes(arr).into())
    }
    fn mutator_id() -> impl Strategy<Value = MutatorId> {
        any::<[u8; 16]>().prop_map(|arr| Uuid::from_bytes(arr).into())
    }
    fn mutation_id() -> impl Strategy<Value = MutationId> {
        any::<[u8; 16]>().prop_map(|arr| Uuid::from_bytes(arr).into())
    }
    fn trigger_crdt() -> impl Strategy<Value = TriggerCRDT> {
        proptest::collection::vec(any::<u8>(), 1..=5).prop_map(|v| v.into_iter().into())
    }

    fn attr_kv() -> impl Strategy<Value = AttrKv> {
        (".+", crate::api::proptest_strategies::attr_val())
            .prop_map(|(k, v)| AttrKv { key: k, value: v })
    }
    fn attr_kvs() -> impl Strategy<Value = AttrKvs> {
        proptest::collection::vec(attr_kv(), 1..=5).prop_map(AttrKvs)
    }

    fn rootwards_message() -> impl Strategy<Value = RootwardsMessage> {
        prop_oneof![
            (
                any::<u32>(),
                participant_id(),
                proptest::collection::vec(any::<u8>(), 0..100)
            )
                .prop_map(|(version, participant, token)| {
                    RootwardsMessage::ChildAuthAttempt {
                        child_participant_id: participant,
                        version,
                        token,
                    }
                }),
            (participant_id(), mutator_id(), attr_kvs()).prop_map(
                |(participant_id, mutator_id, mutator_attrs)| {
                    RootwardsMessage::MutatorAnnouncement {
                        participant_id,
                        mutator_id,
                        mutator_attrs,
                    }
                }
            ),
            (participant_id(), mutator_id()).prop_map(|(participant_id, mutator_id)| {
                RootwardsMessage::MutatorRetirement {
                    participant_id,
                    mutator_id,
                }
            }),
            (
                mutator_id(),
                mutation_id(),
                proptest::option::of(trigger_crdt())
            )
                .prop_map(|(mutator_id, mutation_id, maybe_trigger_crdt)| {
                    RootwardsMessage::UpdateTriggerState {
                        mutator_id,
                        mutation_id,
                        maybe_trigger_crdt,
                    }
                }),
        ]
    }
    fn leafwards_message() -> impl Strategy<Value = LeafwardsMessage> {
        prop_oneof![
            (
                any::<u32>(),
                participant_id(),
                any::<bool>(),
                proptest::option::of(".+")
            )
                .prop_map(|(version, child_participant_id, ok, message)| {
                    LeafwardsMessage::ChildAuthOutcome {
                        version,
                        child_participant_id,
                        ok,
                        message,
                    }
                }),
            (mutator_id(), any::<bool>()).prop_map(|(mutator_id, reset_if_active)| {
                LeafwardsMessage::ClearMutationsForMutator {
                    mutator_id,
                    reset_if_active,
                }
            }),
            (mutator_id(), mutation_id(), any::<bool>()).prop_map(
                |(mutator_id, mutation_id, reset_if_active)| {
                    LeafwardsMessage::ClearSingleMutation {
                        mutator_id,
                        mutation_id,
                        reset_if_active,
                    }
                }
            ),
            (
                mutator_id(),
                mutation_id(),
                proptest::option::of(trigger_crdt()),
                attr_kvs()
            )
                .prop_map(|(mutator_id, mutation_id, maybe_trigger_mask, params)| {
                    LeafwardsMessage::NewMutation {
                        mutator_id,
                        mutation_id,
                        maybe_trigger_mask,
                        params,
                    }
                }),
            Just(LeafwardsMessage::RequestForMutatorAnnouncements {}),
            (
                mutator_id(),
                mutation_id(),
                proptest::option::of(trigger_crdt())
            )
                .prop_map(|(mutator_id, mutation_id, maybe_trigger_crdt)| {
                    LeafwardsMessage::UpdateTriggerState {
                        mutator_id,
                        mutation_id,
                        maybe_trigger_crdt,
                    }
                }),
        ]
    }

    #[test]
    fn round_trip_rootwards() {
        proptest!(|(msg in rootwards_message())| {
            let mut buf = vec![];
            minicbor::encode(&msg , &mut buf)?;

            let msg_prime: RootwardsMessage = minicbor::decode(&buf)?;
            prop_assert_eq!(msg, msg_prime);
        });
    }
    #[test]
    fn round_trip_leafwards() {
        proptest!(|(msg in leafwards_message())| {
            let mut buf = vec![];
            minicbor::encode(&msg , &mut buf)?;

            let msg_prime: LeafwardsMessage = minicbor::decode(&buf)?;
            prop_assert_eq!(msg, msg_prime);
        });
    }

    #[test]
    fn round_trip_update_trigger_state_bidirectional() {
        proptest!(|((mutator_id, mutation_id, maybe_trigger_crdt) in (mutator_id(), mutation_id(), proptest::option::of(trigger_crdt())))| {
            let mut rootwards_buf = vec![];
            let rootwards_msg = RootwardsMessage::UpdateTriggerState{
                mutator_id, mutation_id, maybe_trigger_crdt
            };
            minicbor::encode(&rootwards_msg , &mut rootwards_buf)?;

            let rootwards_msg_prime: RootwardsMessage = minicbor::decode(&rootwards_buf)?;
            if let RootwardsMessage::UpdateTriggerState{
                mutator_id, mutation_id, maybe_trigger_crdt
            } = rootwards_msg_prime {
                let mut leafwards_buf = vec![];
                let leafwards_msg = LeafwardsMessage::UpdateTriggerState{
                    mutator_id, mutation_id, maybe_trigger_crdt
                };
                minicbor::encode(&leafwards_msg, &mut leafwards_buf)?;
                let leafwards_msg_prime: LeafwardsMessage = minicbor::decode(&leafwards_buf)?;
                if let LeafwardsMessage::UpdateTriggerState{
                    mutator_id, mutation_id, maybe_trigger_crdt
                } = leafwards_msg_prime {
                    let rootwards_via_leafwards = RootwardsMessage::UpdateTriggerState { mutator_id, mutation_id, maybe_trigger_crdt };
                    prop_assert_eq!(rootwards_msg, rootwards_via_leafwards);
                } else {
                    panic!("Wrong leafwards variant");
                }
            } else {
                panic!("Wrong rootwards variant");
            }
        });
    }
}
