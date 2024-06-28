//! Various helpful utilities for writing modality-reflector plugins.

pub mod config;
pub mod serde;

#[cfg(feature = "modality")]
pub mod ingest;

#[cfg(feature = "deviant")]
pub mod mutation;

use crate::api::types::{AttrKey, AttrVal};
use crate::auth_token::{self, AuthToken, MODALITY_AUTH_TOKEN_ENV_VAR};
use crate::reflector_config::{self, AttrKeyEqValuePair, ConfigLoadError, TopLevelIngest};
use clap::Parser;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use url::Url;

pub const MODALITY_STORAGE_SERVICE_PORT_DEFAULT: u16 = 14182;

pub const CLI_TEMPLATE: &str = "\
            {about}\n\n\
            USAGE:\n    {usage}\n\
            \n\
            {all-args}\
        ";

/// Handles boilerplate setup for:
/// * tracing_subscriber configuration
/// * Signal pipe fixup
/// * Printing out errors
/// * Exit code management
///
/// The server constructor function consumes config, custom cli args, and a shutdown signal future,
/// then returns an indefinitely-running future that represents the server.
///
/// This function blocks waiting for either the constructed server future to finish
/// or a CTRL+C style signal.
///
/// Returns the process's desired exit code.
#[deprecated]
pub fn server_main<Opts, ServerFuture, ServerConstructor>(
    server_constructor: ServerConstructor,
) -> i32
where
    Opts: Parser,
    Opts: BearingConfigFilePath,
    ServerFuture: Future<Output = Result<(), Box<dyn std::error::Error + 'static>>> + 'static,
    ServerConstructor: FnOnce(
        reflector_config::Config,
        AuthToken,
        Opts,
        Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) -> ServerFuture,
{
    let _ = reset_signal_pipe_handler();
    let opts = match Opts::try_parse_from(std::env::args()) {
        Ok(opts) => opts,
        Err(e)
            if e.kind() == clap::error::ErrorKind::DisplayHelp
                || e.kind() == clap::error::ErrorKind::DisplayVersion =>
        {
            // Need to print to stdout for these command variants in support of manual generation
            if let Err(e) = e.print() {
                error_print(&e);
                return exitcode::SOFTWARE;
            }
            return exitcode::OK;
        }
        Err(e) => {
            error_print(&e);
            return exitcode::SOFTWARE;
        }
    };

    let config = if let Some(config_file) = opts.config_file_path() {
        match reflector_config::try_from_file(config_file) {
            Ok(c) => c,
            Err(config_load_error) => {
                // N.B. tracing subscriber is not configured yet, this may disappear
                tracing::error!(
                    err = &config_load_error as &dyn std::error::Error,
                    "Failed to load config file provided by command line args, exiting."
                );
                let exit_code = match &config_load_error {
                    ConfigLoadError::Io(_) => exitcode::IOERR,
                    _ => exitcode::CONFIG,
                };
                error_print(&config_load_error);
                return exit_code;
            }
        }
    } else if let Ok(config_file) = std::env::var(reflector_config::CONFIG_ENV_VAR) {
        match reflector_config::try_from_file(&PathBuf::from(config_file)) {
            Ok(c) => c,
            Err(config_load_error) => {
                // N.B. tracing subscriber is not configured yet, this may disappear
                tracing::error!(
                    err = &config_load_error as &dyn std::error::Error,
                    "Failed to load config file provided by environment variable, exiting."
                );
                let exit_code = match &config_load_error {
                    ConfigLoadError::Io(_) => exitcode::IOERR,
                    _ => exitcode::CONFIG,
                };
                error_print(&config_load_error);
                return exit_code;
            }
        }
    } else {
        // N.B. tracing subscriber is not configured yet, this may disappear
        tracing::warn!("No config file specified, using default configuration.");
        reflector_config::Config::default()
    };

    // setup custom tracer including ModalityLayer
    #[cfg(feature = "modality_tracing")]
    let maybe_modality = {
        let mut modality_tracing_options = crate::tracing::Options::default();
        let maybe_preferred_ingest_parent_socket = if let Some(ingest_parent_url) = config
            .ingest
            .as_ref()
            .and_then(|ing| ing.protocol_parent_url.as_ref())
        {
            ingest_parent_url
                .socket_addrs(|| Some(14182))
                .ok()
                .and_then(|sockets| sockets.into_iter().next())
        } else {
            None
        };
        if let Some(socket) = maybe_preferred_ingest_parent_socket {
            modality_tracing_options = modality_tracing_options.with_server_address(socket);
        }

        use tracing_subscriber::layer::{Layer, SubscriberExt};

        use tracing_subscriber::filter::{EnvFilter, LevelFilter};
        let (disp, maybe_modality_ingest_handle) =
            match crate::tracing::blocking::ModalityLayer::init_with_options(
                modality_tracing_options,
            ) {
                Ok((modality_layer, modality_ingest_handle)) => {
                    // Trace output through both the stdout formatter and modality's ingest pipeline
                    (
                        tracing::Dispatch::new(
                            tracing_subscriber::Registry::default()
                                .with(
                                    modality_layer.with_filter(
                                        EnvFilter::builder()
                                            .with_default_directive(LevelFilter::INFO.into())
                                            .from_env_lossy(),
                                    ),
                                )
                                .with(
                                    tracing_subscriber::fmt::Layer::default().with_filter(
                                        EnvFilter::builder()
                                            .with_default_directive(LevelFilter::INFO.into())
                                            .from_env_lossy(),
                                    ),
                                ),
                        ),
                        Some(modality_ingest_handle),
                    )
                }
                Err(modality_init_err) => {
                    eprintln!("Modality tracing layer initialization error.");
                    error_print(&modality_init_err);
                    // Only do trace output through the stdout formatter
                    (
                        tracing::Dispatch::new(
                            tracing_subscriber::Registry::default().with(
                                tracing_subscriber::fmt::Layer::default().with_filter(
                                    EnvFilter::builder()
                                        .with_default_directive(LevelFilter::INFO.into())
                                        .from_env_lossy(),
                                ),
                            ),
                        ),
                        None,
                    )
                }
            };

        tracing::dispatcher::set_global_default(disp).expect("set global tracer");

        maybe_modality_ingest_handle
    };

    let auth_token = if let Ok(auth_token_env_str) = std::env::var(MODALITY_AUTH_TOKEN_ENV_VAR) {
        match auth_token::decode_auth_token_hex(auth_token_env_str.as_str()) {
            Ok(at) => at,
            Err(auth_token_deserialization_err) => {
                tracing::error!(
                    err = &auth_token_deserialization_err as &dyn std::error::Error,
                    "Failed to interpret auth token provide by environment variable, exiting."
                );
                error_print(&auth_token_deserialization_err);
                return exitcode::CONFIG;
            }
        }
    } else {
        tracing::warn!(
            "No auth token provided by environment variable {}, falling back to empty auth token",
            MODALITY_AUTH_TOKEN_ENV_VAR
        );
        AuthToken::from(vec![])
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Could not construct tokio runtime");

    let ctrlc = tokio::signal::ctrl_c();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_done = server_constructor(
        config,
        auth_token,
        opts,
        Box::pin(async {
            let _ = shutdown_rx.await.map_err(|_recv_err| {
                tracing::error!("Shutdown signal channel unexpectedly closed early.");
            });
        }),
    );

    let mut maybe_shutdown_tx = Some(shutdown_tx);
    let out_exit_code = runtime.block_on(async {
        tokio::select! {
            signal_result = ctrlc => {
                match signal_result {
                    Ok(()) => {
                        if let Some(shutdown_tx) = maybe_shutdown_tx.take() {
                            let _ = shutdown_tx.send(());
                        }
                        tracing::info!("Received ctrl+c, exiting.");
                        exitcode::OK
                    },
                    Err(io_err) => {
                        if let Some(shutdown_tx) = maybe_shutdown_tx.take() {
                            let _ = shutdown_tx.send(());
                        }
                        error_print(&io_err);
                        tracing::error!("Failed to install ctrl+c handler, exiting.");
                        exitcode::IOERR
                    }
                }
            }
            server_result = server_done => {
                match server_result {
                    Ok(()) => {
                        tracing::info!("Done.");
                        exitcode::OK
                    },
                    Err(e) => {
                        tracing::error!("Server crashed early, exiting.");
                        error_print(e.as_ref());
                        exitcode::SOFTWARE
                    }
                }
            }
        }
    });
    // Drop the runtime a little ahead of function exit
    // in order to ensure that the shutdown_tx side of
    // the shutdown signal channel does not drop first.
    std::mem::drop(runtime);
    #[cfg(feature = "modality_tracing")]
    {
        if let Some(modality_ingest_handle) = maybe_modality {
            modality_ingest_handle.finish();
        }
    }
    let _maybe_shutdown_tx = maybe_shutdown_tx;
    out_exit_code
}

pub(crate) fn error_print(err: &dyn std::error::Error) {
    fn print_err_node(err: &dyn std::error::Error) {
        eprintln!("{err}");
    }

    print_err_node(err);

    let mut cause = err.source();
    while let Some(err) = cause {
        eprint!("Caused by: ");
        print_err_node(err);
        cause = err.source();
    }
}

// Used to prevent panics on broken pipes.
// See:
//   https://github.com/rust-lang/rust/issues/46016#issuecomment-605624865
fn reset_signal_pipe_handler() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(target_family = "unix")]
    {
        use nix::sys::signal;

        unsafe {
            signal::signal(signal::Signal::SIGPIPE, signal::SigHandler::SigDfl)?;
        }
    }

    Ok(())
}

pub trait BearingConfigFilePath {
    fn config_file_path(&self) -> Option<&Path>;
}

pub fn merge_ingest_protocol_parent_url(
    cli_provided: Option<&Url>,
    cfg: &reflector_config::Config,
) -> Url {
    if let Some(parent_url) = cli_provided {
        parent_url.clone()
    } else if let Some(TopLevelIngest {
        protocol_parent_url: Some(parent_url),
        ..
    }) = &cfg.ingest
    {
        parent_url.clone()
    } else {
        let fallback = Url::from_str("modality-ingest://127.0.0.1").unwrap();
        tracing::warn!(
            "Plugin falling back to an ingest protocol parent URL of {}",
            &fallback
        );
        fallback
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolParentError {
    #[error("Failed to provide an ingest protocol parent URL.")]
    IngestProtocolParentUrlMissing,

    #[error("Failed to resolve ingest protocol parent URL to an address '{0}'.")]
    IngestProtocolParentAddressResolution(Url),
}

pub fn merge_timeline_attrs(
    cli_provided_attrs: &[AttrKeyEqValuePair],
    cfg: &reflector_config::Config,
) -> BTreeMap<AttrKey, AttrVal> {
    // Merge additional and override timeline attrs from cfg and opts
    // TODO deal with conflicting reserved attrs in #2098
    let mut timeline_attrs = BTreeMap::new();

    fn ensure_timeline_prefix(k: AttrKey) -> AttrKey {
        if k.as_ref().starts_with("timeline.") {
            k
        } else if k.as_ref().starts_with('.') {
            AttrKey::from("timeline".to_owned() + k.as_ref())
        } else {
            AttrKey::from("timeline.".to_owned() + k.as_ref())
        }
    }
    if let Some(tli) = &cfg.ingest {
        for kvp in tli
            .timeline_attributes
            .additional_timeline_attributes
            .iter()
            .cloned()
        {
            let _ = timeline_attrs.insert(ensure_timeline_prefix(kvp.0), kvp.1);
        }
        for kvp in tli
            .timeline_attributes
            .override_timeline_attributes
            .iter()
            .cloned()
        {
            let _ = timeline_attrs.insert(ensure_timeline_prefix(kvp.0), kvp.1);
        }
    }
    // The CLI-provided attrs will take precedence over config
    for kvp in cli_provided_attrs.iter().cloned() {
        let _ = timeline_attrs.insert(ensure_timeline_prefix(kvp.0), kvp.1);
    }
    timeline_attrs
}

/// Initialize the `tracing` crate with `tracing_subscriber::EnvFilter`. If
/// `RUST_LOG` is not set, default to setting the current module to 'info'.
/// Will panic if the tracing subscriber cannot be initialized.
#[macro_export]
macro_rules! init_tracing {
    () => {
        let builder = ::tracing_subscriber::fmt::Subscriber::builder();
        let env_filter = ::std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
            .map(::tracing_subscriber::EnvFilter::new)
            .unwrap_or_else(|_| {
                ::tracing_subscriber::EnvFilter::new(format!(
                    "{}={}",
                    env!("CARGO_PKG_NAME").replace('-', "_"),
                    ::tracing::Level::INFO
                ))
            });
        let builder = builder.with_env_filter(env_filter);
        let subscriber = builder.finish();
        use ::tracing_subscriber::util::SubscriberInitExt;
        subscriber
            .try_init()
            .expect("Unable to initialize tracing subscriber");
    };
    ($env_filter:expr) => {
        let builder = ::tracing_subscriber::fmt::Subscriber::builder();
        let env_filter = ::std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
            .map(::tracing_subscriber::EnvFilter::new)
            .unwrap_or_else(|_| $env_filter);
        let builder = builder.with_env_filter(env_filter);
        let subscriber = builder.finish();
        use ::tracing_subscriber::util::SubscriberInitExt;
        subscriber
            .try_init()
            .expect("Unable to initialize tracing subscriber");
    };
}
