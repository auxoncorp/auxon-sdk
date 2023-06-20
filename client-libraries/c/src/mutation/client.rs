use crate::{
    capi_result,
    mutation::{attr_kv, mutator},
    runtime, Error, NullPtrExt,
};
use modality_mutation_plane::{
    protocol::{LeafwardsMessage, RootwardsMessage, MUTATION_PROTOCOL_VERSION},
    types::{AttrKv, AttrKvs, MutationId, MutatorId, ParticipantId},
};
use modality_mutation_plane_client::parent_connection::MutationParentConnection;
use modality_mutator_protocol::descriptor::MutatorDescriptor;
use std::ffi::{c_char, c_int, CStr};
use std::{mem, slice, time::Duration};
use tokio::{runtime::Runtime, time::timeout};
use url::Url;
use uuid::Uuid;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);

pub struct mutation_client {
    rt: &'static Runtime,
    timeout: Duration,
    pid: ParticipantId,
    state: State,
    mutators: Vec<CapiMutator>,
}

struct CapiMutator {
    id: MutatorId,
    m: mutator,
    active_mutation: Option<MutationId>,
    description_attributes: AttrKvs,
    capi_params_storage: Vec<attr_kv>,
}

impl CapiMutator {
    fn new(m: &mutator) -> Result<Self, Error> {
        if !m.has_get_description() {
            return Err(Error::NullMutatorInterfaceFunction);
        }
        if !m.has_inject() {
            return Err(Error::NullMutatorInterfaceFunction);
        }
        if !m.has_reset() {
            return Err(Error::NullMutatorInterfaceFunction);
        }

        let desc = m.get_description()?;
        let description_attributes = desc
            .get_description_attributes()
            .map(|(k, v)| AttrKv {
                key: k.into(),
                value: v,
            })
            .collect();

        Ok(Self {
            id: Uuid::new_v4().into(),
            m: m.clone(),
            active_mutation: None,
            description_attributes: AttrKvs(description_attributes),
            capi_params_storage: Vec::new(),
        })
    }

    fn announcement(&self, participant_id: ParticipantId) -> RootwardsMessage {
        RootwardsMessage::MutatorAnnouncement {
            participant_id,
            mutator_id: self.id,
            mutator_attrs: self.description_attributes.clone(),
        }
    }
}

enum State {
    Init,
    Connected(MutationParentConnection),
    Authed(MutationParentConnection),
}

impl State {
    fn is_init(&self) -> bool {
        matches!(self, State::Init)
    }

    fn as_authed(&mut self) -> Result<&mut MutationParentConnection, Error> {
        use State::*;
        match self {
            Init => Err(Error::ClientNotConnected),
            Connected(_) => Err(Error::ClientNotAuthenticated),
            Authed(s) => Ok(s),
        }
    }
}

#[no_mangle]
pub extern "C" fn modality_mutation_client_new(
    rt: *const runtime,
    out: *mut *mut mutation_client,
) -> c_int {
    capi_result(|| unsafe { internal_client_new(rt, DEFAULT_TIMEOUT, out) })
}

unsafe fn internal_client_new(
    rt: *const runtime,
    timeout: Duration,
    out: *mut *mut mutation_client,
) -> Result<(), Error> {
    out.null_check()?;
    let rt = rt.as_ref().ok_or(Error::NullPointer)?;
    *out = Box::into_raw(Box::new(mutation_client {
        rt: &rt.0,
        timeout,
        pid: Uuid::new_v4().into(),
        state: State::Init,
        mutators: Vec::new(),
    }));
    Ok(())
}

#[no_mangle]
pub extern "C" fn modality_mutation_client_set_timeout_ms(
    client: *mut mutation_client,
    timeout_ms: u64,
) -> c_int {
    capi_result(|| unsafe {
        let c = client.as_mut().ok_or(Error::NullPointer)?;
        c.timeout = Duration::from_millis(timeout_ms);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_mutation_client_set_timeout_us(
    client: *mut mutation_client,
    timeout_us: u64,
) -> c_int {
    capi_result(|| unsafe {
        let c = client.as_mut().ok_or(Error::NullPointer)?;
        c.timeout = Duration::from_micros(timeout_us);
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_mutation_client_free(client: *mut mutation_client) {
    if !client.is_null() {
        let _ = unsafe { Box::from_raw(client) };
    }
}

#[no_mangle]
pub extern "C" fn modality_mutation_client_connect(
    client: *mut mutation_client,
    endpoint_url: *const c_char,
    allow_insecure_tls: c_int,
) -> c_int {
    capi_result(|| unsafe {
        endpoint_url.null_check()?;
        let c = client.as_mut().ok_or(Error::NullPointer)?;
        if !c.state.is_init() {
            Err(Error::ClientAlreadyConnected)
        } else {
            let url_str = CStr::from_ptr(endpoint_url)
                .to_str()
                .map_err(|_| Error::InvalidUtf8)?;
            let url = Url::parse(url_str).map_err(|_| Error::InvalidUrl)?;
            let connected_client = c.rt.block_on(MutationParentConnection::connect(
                &url,
                allow_insecure_tls != 0,
            ))?;
            let _ = mem::replace(&mut c.state, State::Connected(connected_client));
            Ok(())
        }
    })
}

#[no_mangle]
pub extern "C" fn modality_mutation_client_authenticate_bytes(
    client: *mut mutation_client,
    token: *const u8,
    token_len: usize,
) -> c_int {
    capi_result(|| unsafe { internal_client_authenticate_bytes(client, token, token_len) })
}

#[no_mangle]
pub extern "C" fn modality_mutation_client_authenticate(
    client: *mut mutation_client,
    token_hex: *const c_char,
) -> c_int {
    capi_result(|| unsafe {
        token_hex.null_check()?;
        let hex = CStr::from_ptr(token_hex)
            .to_str()
            .map_err(|_| Error::InvalidUtf8)?;
        let token = hex::decode(hex).map_err(|_| Error::InvalidAuthTokenHex)?;
        internal_client_authenticate_bytes(client, token.as_ptr(), token.len())
    })
}

unsafe fn internal_client_authenticate_bytes(
    client: *mut mutation_client,
    token: *const u8,
    token_len: usize,
) -> Result<(), Error> {
    let token = if token.is_null() || token_len == 0 {
        &[]
    } else {
        slice::from_raw_parts(token, token_len)
    };
    if token.is_empty() {
        return Err(Error::InvalidAuthToken);
    }
    let c = client.as_mut().ok_or(Error::NullPointer)?;

    // Drop the connection on failure, requires a connect-retry on failure
    let mut state = match mem::replace(&mut c.state, State::Init) {
        State::Connected(s) => Ok(s),
        State::Init => Err(Error::ClientNotConnected),
        State::Authed(s) => {
            let _ = mem::replace(&mut c.state, State::Authed(s));
            Err(Error::ClientAlreadyAuthenticated)
        }
    }?;

    c.rt.block_on(state.write_msg(&RootwardsMessage::ChildAuthAttempt {
        child_participant_id: c.pid,
        version: MUTATION_PROTOCOL_VERSION,
        token: token.to_vec(),
    }))?;

    match c
        .rt
        .block_on(async { timeout(c.timeout, state.read_msg()).await })??
    {
        LeafwardsMessage::ChildAuthOutcome {
            child_participant_id,
            version,
            ok,
            message,
        } => {
            if child_participant_id == c.pid {
                if ok {
                    tracing::debug!(
                        auth_ok = true,
                        msg = message.unwrap_or_default().as_str(),
                        version = version,
                        parent_version = version,
                        "Mutation plane authorization with parent complete"
                    );
                } else {
                    tracing::error!(
                        auth_ok = false,
                        msg = message.unwrap_or_default().as_str(),
                        "Mutation plane authorization failed"
                    );

                    return Err(Error::AuthenticationError);
                }
            } else {
                tracing::error!(
                    auth_ok = ok,
                    msg = message.unwrap_or_default().as_str(),
                    "Mutation plane auth outcome received for a different participant"
                );

                return Err(Error::AuthenticationError);
            }
        }
        _ => {
            tracing::error!("Invalid response from parent");

            return Err(Error::AuthenticationError);
        }
    }

    let _ = mem::replace(&mut c.state, State::Authed(state));

    Ok(())
}

#[no_mangle]
pub extern "C" fn modality_mutation_client_register_mutators(
    client: *mut mutation_client,
    mutators: *const mutator,
    mutators_length: usize,
) -> c_int {
    capi_result(|| unsafe {
        let mutators = if mutators.is_null() || mutators_length == 0 {
            &[]
        } else {
            slice::from_raw_parts(mutators, mutators_length)
        };

        if !mutators.is_empty() {
            let c = client.as_mut().ok_or(Error::NullPointer)?;
            let state = c.state.as_authed()?;
            for m in mutators {
                let m = CapiMutator::new(m)?;
                let res = c.rt.block_on(state.write_msg(&m.announcement(c.pid)));
                c.mutators.push(m);
                res?;
            }
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_mutation_client_poll(client: *mut mutation_client) -> c_int {
    capi_result(|| unsafe {
        let c = client.as_mut().ok_or(Error::NullPointer)?;
        let state = c.state.as_authed()?;
        let res =
            c.rt.block_on(async { timeout(c.timeout, state.read_msg()).await });
        // Timeouts are ignored when polling
        if let Ok(comms_res) = res {
            let msg = comms_res?;
            match msg {
                LeafwardsMessage::RequestForMutatorAnnouncements {} => {
                    for m in c.mutators.iter() {
                        c.rt.block_on(state.write_msg(&m.announcement(c.pid)))?;
                    }
                }
                LeafwardsMessage::NewMutation {
                    mutator_id,
                    mutation_id,
                    maybe_trigger_mask: _, // triggers not supported yet
                    params,
                } => {
                    match c
                        .mutators
                        .iter_mut()
                        .find(|mutator| mutator.id == mutator_id)
                    {
                        None => {
                            tracing::warn!(
                                mutator_id = %mutator_id,
                                "Failed to handle new mutation, mutator not hosted by this client")
                        }
                        Some(mutator) => {
                            tracing::debug!(
                                        mutator_id = %mutator_id,
                                        mutation_id = %mutation_id,
                                        "Handling new mutation");

                            // Reset active mutation first, if any
                            if let Some(active_mutation_id) = mutator.active_mutation.take() {
                                tracing::debug!(
                                            mutator_id = %mutator_id,
                                            mutation_id = %active_mutation_id,
                                            "Clearing currently active mutation");
                                if let Err(e) = mutator.m.reset() {
                                    tracing::error!(
                                            mutator_id = %mutator.id,
                                            mutator_error = %e,
                                            "Mutator reset returned an error");
                                    return Err(e);
                                }
                            }

                            let params = params
                                .0
                                .into_iter()
                                .map(|kv| (kv.key.into(), kv.value))
                                .collect();
                            match mutator.m.inject(
                                mutation_id.into(),
                                params,
                                &mut mutator.capi_params_storage,
                            ) {
                                Ok(()) => {
                                    mutator.active_mutation = Some(mutation_id);
                                }
                                Err(e) => {
                                    tracing::error!(
                                            mutator_id = %mutator.id,
                                            mutation_id = %mutation_id,
                                            mutator_error = %e,
                                            "Mutation inject returned an error");
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
                LeafwardsMessage::ClearSingleMutation {
                    mutator_id,
                    mutation_id,
                    reset_if_active,
                } => {
                    match c
                        .mutators
                        .iter_mut()
                        .find(|mutator| mutator.id == mutator_id)
                    {
                        Some(mutator) if Some(mutation_id) == mutator.active_mutation => {
                            tracing::debug!(
                                        mutator_id = %mutator_id,
                                        mutation_id = %mutation_id,
                                        reset = reset_if_active,
                                        "Clearing mutation");
                            mutator.active_mutation.take();
                            if reset_if_active {
                                if let Err(e) = mutator.m.reset() {
                                    tracing::error!(
                                            mutator_id = %mutator.id,
                                            mutator_error = %e,
                                            "Mutator reset returned an error");
                                    return Err(e);
                                }
                            }
                        }
                        Some(_) => {
                            tracing::warn!(
                                        mutator_id = %mutator_id,
                                        mutation_id = %mutation_id,
                                        "Mutation not active");
                        }
                        None => {
                            tracing::warn!(mutator_id = %mutator_id, "Mutator not hosted by this client")
                        }
                    }
                }
                LeafwardsMessage::ClearMutationsForMutator {
                    mutator_id,
                    reset_if_active,
                } => {
                    match c
                        .mutators
                        .iter_mut()
                        .find(|mutator| mutator.id == mutator_id)
                    {
                        Some(mutator) => {
                            if let Some(mutation_id) = mutator.active_mutation.take() {
                                tracing::debug!(
                                        mutator_id = %mutator_id,
                                        mutation_id = %mutation_id,
                                        reset = reset_if_active,
                                        "Clearing mutation");
                                if reset_if_active {
                                    if let Err(e) = mutator.m.reset() {
                                        tracing::error!(
                                            mutator_id = %mutator.id,
                                            mutator_error = %e,
                                            "Mutator reset returned an error");
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        None => tracing::warn!(
                                        mutator_id = %mutator_id,
                                        "Mutator not hosted by this client"),
                    }
                }
                LeafwardsMessage::ClearMutations {} => {
                    tracing::debug!("Clearing all mutations");

                    // This method will clear all mutations, and only returns
                    // the first mutator-reset-error (if any) to ensure every
                    // mutator gets reset
                    let mut first_mutator_reset_err = None;

                    // We dont' have staged mutations yet, so just clear any active mutations
                    for mutator in c.mutators.iter_mut() {
                        if mutator.active_mutation.take().is_some() {
                            if let Err(e) = mutator.m.reset() {
                                tracing::error!(
                                    mutator_id = %mutator.id,
                                    mutator_error = %e,
                                    "Mutator reset returned an error");
                                let _ = first_mutator_reset_err.get_or_insert(e);
                            }
                        }
                    }

                    if let Some(e) = first_mutator_reset_err.take() {
                        return Err(e);
                    }
                }
                msg => tracing::debug!(message = msg.name(), "Ignoring leafwards message"),
            }
        }
        Ok(())
    })
}
