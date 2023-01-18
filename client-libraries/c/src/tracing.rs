use crate::{capi_result, Error};
use std::ffi::c_int;
use tracing_subscriber::util::SubscriberInitExt;

#[no_mangle]
pub extern "C" fn modality_tracing_subscriber_init() -> c_int {
    capi_result(|| {
        try_init_tracing_subscriber().map_err(|_| Error::TracingSubscriber)?;
        Ok(())
    })
}

fn try_init_tracing_subscriber() -> Result<(), Box<dyn std::error::Error>> {
    let builder = tracing_subscriber::fmt::Subscriber::builder();
    let env_filter = std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV)
        .map(tracing_subscriber::EnvFilter::new)
        .unwrap_or_else(|_| {
            tracing_subscriber::EnvFilter::new(format!("modality={}", tracing::Level::WARN))
        });
    let builder = builder.with_env_filter(env_filter);
    let subscriber = builder.finish();
    subscriber.try_init()?;
    Ok(())
}
