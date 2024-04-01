use auxon_sdk::tracing::blocking::{Options, TracingModality};
use tracing::{debug, error, event, info, span, trace, warn, Level};

fn main() {
    let modality = TracingModality::init_with_options(
        Options::new()
            .with_name("basic example")
            .with_metadata("build_id", 0i64),
    )
    .expect("init tracing");

    {
        let span = span!(Level::TRACE, "outer_span");
        let _span = span.enter();
        do_thing::doit();
    }

    modality.finish();
}

pub mod do_thing {
    use super::*;
    pub fn doit() {
        let span = span!(Level::TRACE, "my span");
        span.in_scope(|| {
            event!(Level::INFO, "something has happened!");
        });

        event!(Level::TRACE, foo = 1, "a trace thing");

        trace!(foo = 1, "a trace thing");
        debug!(foo = 2, "a debug thing");
        info!(foo = 3, "a info thing");
        warn!(foo = 4, "a warn thing");
        error!(foo = 5, "a error thing");
    }
}
