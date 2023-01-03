use modality_ingest_client::{
    dynamic::DynamicIngestError, IngestClientInitializationError, IngestError,
};
use std::ffi::c_int;

/// cbindgen::ignore
pub type Error = error;

#[repr(C)]
#[non_exhaustive]
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, thiserror::Error)]
#[error("{:?}", self)]
pub enum error {
    Ok = 0,
    NullPointer = -1,
    InvalidAuthToken = -2,
    InvalidAuthTokenHex = -3,
    InvalidUtf8 = -4,
    InvalidUrl = -5,
    InvalidIpAddress = -6,
    InvalidAttrList = -7,
    InvalidNameSegment = -8,
    // Reserved
    TracingSubscriber = -20,
    AsyncRuntime = -21,
    ClientNotConnected = -22,
    ClientAlreadyConnected = -23,
    ClientNotAuthenticated = -24,
    ClientAlreadyAuthenticated = -25,
    // Reserved
    NullMutatorInterfaceFunction = -40,
    MutatorInterfaceError = -41,
    // Reserved
    NoIps = -60,
    SocketInit = -61,
    SocketConnection = -62,
    Tls = -63,
    ClientLocalAddrParse = -64,
    ParseIngestEndpoint = -65,
    // Reserved
    AuthenticationError = -80,
    ProtocolError = -81,
    CborEncode = -82,
    CborDecode = -83,
    Timeout = -84,
    AttrKeyNaming = -85,
    Io = -86,
    // Reserved
    NoBoundTimeline = -100,
}

impl From<Error> for c_int {
    fn from(e: Error) -> Self {
        e as _
    }
}

impl From<IngestClientInitializationError> for Error {
    fn from(e: IngestClientInitializationError) -> Self {
        use IngestClientInitializationError::*;
        match e {
            NoIps => Error::NoIps,
            SocketInit(_) => Error::SocketInit,
            SocketConnection { .. } => Error::SocketConnection,
            Tls(_) => Error::Tls,
            ClientLocalAddrParse(_) => Error::ClientLocalAddrParse,
            ParseIngestEndpoint(_) => Error::ParseIngestEndpoint,
        }
    }
}

impl From<IngestError> for Error {
    fn from(e: IngestError) -> Self {
        use IngestError::*;
        match e {
            AuthenticationError { .. } => Error::AuthenticationError,
            ProtocolError(_) => Error::ProtocolError,
            CborEncode(_) => Error::CborEncode,
            CborDecode(_) => Error::CborDecode,
            Timeout(_) => Error::Timeout,
            AttrKeyNaming => Error::AttrKeyNaming,
            Io(_) => Error::Io,
        }
    }
}

impl From<DynamicIngestError> for Error {
    fn from(e: DynamicIngestError) -> Self {
        use DynamicIngestError::*;
        match e {
            IngestError(e) => Error::from(e),
            NoBoundTimeline => Error::NoBoundTimeline,
        }
    }
}

pub(crate) fn capi_result<F>(f: F) -> c_int
where
    F: FnOnce() -> Result<(), Error>,
{
    match f() {
        Err(e) => e,
        Ok(_) => Error::Ok,
    }
    .into()
}

pub(crate) trait NullPtrExt {
    fn null_check(self) -> Result<(), Error>;
}

impl<T> NullPtrExt for *const T {
    fn null_check(self) -> Result<(), Error> {
        if self.is_null() {
            Err(Error::NullPointer)
        } else {
            Ok(())
        }
    }
}
