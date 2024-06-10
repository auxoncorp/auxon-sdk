use crate::mutation_plane::protocol::{LeafwardsMessage, RootwardsMessage};
use std::net::SocketAddr;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use url::Url;

#[derive(Copy, Clone)]
pub enum TlsMode {
    Secure,
    Insecure,
}

pub enum MutationParentConnection {
    Tcp(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl MutationParentConnection {
    pub async fn connect(
        endpoint: &Url,
        allow_insecure_tls: bool,
    ) -> Result<MutationParentConnection, MutationParentClientInitializationError> {
        let endpoint = RootwardsEndpoint::parse_and_resolve(endpoint, allow_insecure_tls).await?;

        // take the first addr, arbitrarily
        let remote_addr = endpoint
            .addrs
            .into_iter()
            .next()
            .ok_or(MutationParentClientInitializationError::NoIps)?;

        let local_addr: SocketAddr = if remote_addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()?;

        let socket = if remote_addr.is_ipv4() {
            TcpSocket::new_v4().map_err(MutationParentClientInitializationError::SocketInit)?
        } else {
            TcpSocket::new_v6().map_err(MutationParentClientInitializationError::SocketInit)?
        };

        socket
            .bind(local_addr)
            .map_err(MutationParentClientInitializationError::SocketInit)?;
        let stream = socket.connect(remote_addr).await.map_err(|error| {
            MutationParentClientInitializationError::SocketConnection { error, remote_addr }
        })?;

        if let Some(tls_mode) = endpoint.tls_mode {
            let config = match tls_mode {
                TlsMode::Secure => crate::tls::SECURE.clone(),
                TlsMode::Insecure => crate::tls::INSECURE.clone(),
            };
            let cx = TlsConnector::from(config);
            let stream = cx.connect(endpoint.cert_domain.try_into()?, stream).await?;
            Ok(MutationParentConnection::Tls(stream))
        } else {
            Ok(MutationParentConnection::Tcp(stream))
        }
    }

    pub async fn write_msg(&mut self, msg: &RootwardsMessage) -> Result<(), CommsError> {
        let msg_buf = minicbor::to_vec(msg)?;
        let msg_len = msg_buf.len() as u32;

        match self {
            MutationParentConnection::Tcp(s) => {
                s.write_all(&msg_len.to_be_bytes())
                    .await
                    .map_err(minicbor::encode::Error::Write)?;
                s.write_all(&msg_buf)
                    .await
                    .map_err(minicbor::encode::Error::Write)?;
            }
            MutationParentConnection::Tls(s) => {
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

    pub async fn read_msg(&mut self) -> Result<LeafwardsMessage, CommsError> {
        match self {
            MutationParentConnection::Tcp(s) => {
                let msg_len = s.read_u32().await?; // yes, this is big-endian
                let mut msg_buf = vec![0u8; msg_len as usize];
                s.read_exact(msg_buf.as_mut_slice()).await?;

                Ok(minicbor::decode::<LeafwardsMessage>(&msg_buf)?)
            }
            MutationParentConnection::Tls(s) => {
                let msg_len = s.read_u32().await?; // yes, this is big-endian
                let mut msg_buf = vec![0u8; msg_len as usize];
                s.read_exact(msg_buf.as_mut_slice()).await?;

                Ok(minicbor::decode::<LeafwardsMessage>(&msg_buf)?)
            }
        }
    }
}
pub const MODALITY_MUTATION_CONNECT_PORT_DEFAULT: u16 = 14192;
pub const MODALITY_MUTATION_CONNECT_TLS_PORT_DEFAULT: u16 = 14194;

pub const MODALITY_MUTATION_URL_SCHEME: &str = "modality-mutation";
pub const MODALITY_MUTATION_TLS_URL_SCHEME: &str = "modality-mutation-tls";

struct RootwardsEndpoint {
    cert_domain: String,
    addrs: Vec<SocketAddr>,
    tls_mode: Option<TlsMode>,
}

impl RootwardsEndpoint {
    async fn parse_and_resolve(
        url: &Url,
        allow_insecure_tls: bool,
    ) -> Result<RootwardsEndpoint, ParseRootwardsEndpointError> {
        let host = match url.host() {
            Some(h) => h,
            None => return Err(ParseRootwardsEndpointError::MissingHost),
        };

        let is_tls = match url.scheme() {
            MODALITY_MUTATION_URL_SCHEME => false,
            MODALITY_MUTATION_TLS_URL_SCHEME => true,
            s => return Err(ParseRootwardsEndpointError::InvalidScheme(s.to_string())),
        };
        let port = match url.port() {
            Some(p) => p,
            _ => {
                if is_tls {
                    MODALITY_MUTATION_CONNECT_TLS_PORT_DEFAULT
                } else {
                    MODALITY_MUTATION_CONNECT_PORT_DEFAULT
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

        Ok(RootwardsEndpoint {
            cert_domain: host.to_string(),
            addrs,
            tls_mode,
        })
    }
}

#[derive(Debug, Error)]
pub enum MutationParentClientInitializationError {
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
    ParseIngestEndpoint(#[from] ParseRootwardsEndpointError),

    #[error("Mutation plane authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Mutation plane auth outcome received for a different participant")]
    AuthWrongParticipant,

    #[error("Unexpected auth response")]
    UnexpectedAuthResponse,

    #[error(transparent)]
    CommsError(#[from] CommsError),
}

#[derive(Debug, Error)]
pub enum CommsError {
    #[error("Marshalling Error (Write)")]
    CborEncode(#[from] minicbor::encode::Error<std::io::Error>),

    #[error("Marshalling Error (Read)")]
    CborDecode(#[from] minicbor::decode::Error),

    #[error("IO")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum ParseRootwardsEndpointError {
    #[error("Url most contain a host")]
    MissingHost,

    // TODO update with the real thing
    #[error(
        "Invalid URL scheme '{0}'. Must be one of 'modality-mutation' or 'modality-mutation-tls'"
    )]
    InvalidScheme(String),

    #[error("IO Error")]
    Io(#[from] std::io::Error),
}
