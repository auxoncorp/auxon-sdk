use crate::{attr_val, capi_result, runtime, timeline_id, Error, NullPtrExt};
use modality_ingest_client::{dynamic::DynamicIngestClient, IngestClient, UnauthenticatedState};
use std::ffi::{c_char, c_int, CStr};
use std::{mem, slice, time::Duration};
use tokio::runtime::Runtime;
use url::Url;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct interned_attr_key(pub u32);

impl From<interned_attr_key> for modality_ingest_protocol::InternedAttrKey {
    fn from(k: interned_attr_key) -> Self {
        modality_ingest_protocol::InternedAttrKey::from(k.0)
    }
}

impl From<modality_ingest_protocol::InternedAttrKey> for interned_attr_key {
    fn from(k: modality_ingest_protocol::InternedAttrKey) -> Self {
        interned_attr_key(u32::from(k))
    }
}

#[repr(C)]
pub struct attr {
    pub key: interned_attr_key,
    pub val: attr_val,
}

impl From<&attr>
    for (
        modality_ingest_protocol::InternedAttrKey,
        modality_api::AttrVal,
    )
{
    fn from(attr: &attr) -> Self {
        let val = modality_api::AttrVal::from(&attr.val);
        (attr.key.into(), val)
    }
}

pub struct ingest_client {
    rt: &'static Runtime,
    state: InnerState,
}

enum InnerState {
    Init,
    Connected(IngestClient<UnauthenticatedState>),
    Authed(DynamicIngestClient),
}

impl InnerState {
    fn is_init(&self) -> bool {
        matches!(self, InnerState::Init)
    }

    fn swap_to_connected(&mut self) -> Result<IngestClient<UnauthenticatedState>, Error> {
        match mem::replace(self, InnerState::Init) {
            InnerState::Connected(s) => Ok(s),
            InnerState::Init => Err(Error::ClientNotConnected),
            InnerState::Authed(s) => {
                let _ = mem::replace(self, InnerState::Authed(s));
                Err(Error::ClientAlreadyAuthenticated)
            }
        }
    }

    fn as_authed(&mut self) -> Result<&mut DynamicIngestClient, Error> {
        use InnerState::*;
        match self {
            Init => Err(Error::ClientNotConnected),
            Connected(_) => Err(Error::ClientNotAuthenticated),
            Authed(s) => Ok(s),
        }
    }
}

#[no_mangle]
pub extern "C" fn modality_ingest_client_new(
    rt: *const runtime,
    out: *mut *mut ingest_client,
) -> c_int {
    capi_result(|| unsafe {
        out.null_check()?;
        let rt = rt.as_ref().ok_or(Error::NullPointer)?;
        *out = Box::into_raw(Box::new(ingest_client {
            rt: &rt.0,
            state: InnerState::Init,
        }));
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_ingest_client_free(client: *mut ingest_client) {
    if !client.is_null() {
        let _ = unsafe { Box::from_raw(client) };
    }
}

#[no_mangle]
pub extern "C" fn modality_ingest_client_connect(
    client: *mut ingest_client,
    endpoint_url: *const c_char,
    allow_insecure_tls: c_int,
) -> c_int {
    capi_result(|| unsafe {
        internal_client_connect(client, endpoint_url, allow_insecure_tls, None)
    })
}

#[no_mangle]
pub extern "C" fn modality_ingest_client_connect_with_timeout(
    client: *mut ingest_client,
    endpoint_url: *const c_char,
    allow_insecure_tls: c_int,
    timeout_seconds: u64,
) -> c_int {
    capi_result(|| unsafe {
        internal_client_connect(
            client,
            endpoint_url,
            allow_insecure_tls,
            timeout_seconds.into(),
        )
    })
}

unsafe fn internal_client_connect(
    client: *mut ingest_client,
    endpoint_url: *const c_char,
    allow_insecure_tls: c_int,
    timeout_seconds: Option<u64>,
) -> Result<(), Error> {
    endpoint_url.null_check()?;
    let c = client.as_mut().ok_or(Error::NullPointer)?;
    if !c.state.is_init() {
        Err(Error::ClientAlreadyConnected)
    } else {
        let url_str = CStr::from_ptr(endpoint_url)
            .to_str()
            .map_err(|_| Error::InvalidUtf8)?;
        let url = Url::parse(url_str).map_err(|_| Error::InvalidUrl)?;
        let connected_client = match timeout_seconds {
            Some(to_sec) => c.rt.block_on(IngestClient::connect_with_timeout(
                &url,
                allow_insecure_tls != 0,
                Duration::from_secs(to_sec),
            ))?,

            None => {
                c.rt.block_on(IngestClient::connect(&url, allow_insecure_tls != 0))?
            }
        };
        let _ = mem::replace(&mut c.state, InnerState::Connected(connected_client));
        Ok(())
    }
}

#[no_mangle]
pub extern "C" fn modality_ingest_client_authenticate_bytes(
    client: *mut ingest_client,
    token: *const u8,
    token_len: usize,
) -> c_int {
    capi_result(|| unsafe { internal_client_authenticate_bytes(client, token, token_len) })
}

#[no_mangle]
pub extern "C" fn modality_ingest_client_authenticate(
    client: *mut ingest_client,
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
    client: *mut ingest_client,
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
    let state = c.state.swap_to_connected()?;
    let authed_client = c.rt.block_on(state.authenticate(token.to_vec()))?;
    let _ = mem::replace(&mut c.state, InnerState::Authed(authed_client.into()));
    Ok(())
}

#[no_mangle]
pub extern "C" fn modality_ingest_client_declare_attr_key(
    client: *mut ingest_client,
    key_name: *const c_char,
    out: *mut interned_attr_key,
) -> c_int {
    capi_result(|| unsafe {
        key_name.null_check()?;
        out.null_check()?;
        let key_name = CStr::from_ptr(key_name)
            .to_str()
            .map_err(|_| Error::InvalidUtf8)?;
        let c = client.as_mut().ok_or(Error::NullPointer)?;
        let state = c.state.as_authed()?;
        let key =
            c.rt.block_on(state.declare_attr_key(key_name.to_string()))?;
        *out = key.into();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_ingest_client_open_timeline(
    client: *mut ingest_client,
    id: *const timeline_id,
) -> c_int {
    capi_result(|| unsafe {
        let tid = id.as_ref().ok_or(Error::NullPointer)?;
        let c = client.as_mut().ok_or(Error::NullPointer)?;
        let state = c.state.as_authed()?;
        c.rt.block_on(state.open_timeline(tid.into()))?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_ingest_client_close_timeline(client: *mut ingest_client) -> c_int {
    capi_result(|| unsafe {
        let c = client.as_mut().ok_or(Error::NullPointer)?;
        let state = c.state.as_authed()?;
        state.close_timeline();
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_ingest_client_timeline_metadata(
    client: *mut ingest_client,
    attrs: *const attr,
    attrs_len: usize,
) -> c_int {
    capi_result(|| unsafe {
        let attrs = if attrs.is_null() || attrs_len == 0 {
            &[]
        } else {
            slice::from_raw_parts(attrs, attrs_len)
        };
        if attrs.is_empty() {
            return Err(Error::InvalidAttrList);
        }
        let c = client.as_mut().ok_or(Error::NullPointer)?;
        let state = c.state.as_authed()?;
        c.rt.block_on(state.timeline_metadata(attrs.iter().map(|attr| attr.into())))?;
        Ok(())
    })
}

#[no_mangle]
pub extern "C" fn modality_ingest_client_event(
    client: *mut ingest_client,
    ordering_lower: u64,
    ordering_upper: u64,
    attrs: *const attr,
    attrs_len: usize,
) -> c_int {
    capi_result(|| unsafe {
        let attrs = if attrs.is_null() || attrs_len == 0 {
            &[]
        } else {
            slice::from_raw_parts(attrs, attrs_len)
        };
        if attrs.is_empty() {
            return Err(Error::InvalidAttrList);
        }
        let c = client.as_mut().ok_or(Error::NullPointer)?;
        let state = c.state.as_authed()?;

        let ord_lsb = ordering_lower.to_le_bytes();
        let ord_msb = ordering_upper.to_le_bytes();
        let mut ord_bytes = [0_u8; 16];
        ord_bytes[..8].copy_from_slice(&ord_lsb);
        ord_bytes[8..16].copy_from_slice(&ord_msb);
        let ordering = u128::from_le_bytes(ord_bytes);

        c.rt.block_on(state.event(ordering, attrs.iter().map(|attr| attr.into())))?;
        Ok(())
    })
}
