use std::net::Ipv4Addr;
use tokio::sync::oneshot::Sender;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let (shutdown_tx, shutdown_rx): (Sender<_>, _) = tokio::sync::oneshot::channel();
    auxon_sdk::mutator_server::server::serve_mutators(
        Default::default(),
        None,
        (Ipv4Addr::UNSPECIFIED, 8080),
        async {
            shutdown_rx.await.ok();
        },
    )
    .await;
    let _ = shutdown_tx.send(());
}
