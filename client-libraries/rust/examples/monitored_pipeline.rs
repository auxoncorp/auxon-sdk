use auxon_sdk::tracing::blocking::{timeline_id, ModalityLayer, TimelineId};
use rand::prelude::*;
use std::error::Error;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{channel, sync_channel, Receiver, RecvTimeoutError, Sender, SyncSender},
    Arc,
};
use std::time::{Duration, Instant};
use std::{fmt, thread};
use tracing_core::Dispatch;
use tracing_subscriber::{fmt::Layer, layer::SubscriberExt, Registry};

fn main() {
    // setup custom tracer including ModalityLayer
    let modality = {
        let (modality_layer, modality_ingest_handle) =
            ModalityLayer::init().expect("initialize ModalityLayer");

        let subscriber = Registry::default()
            .with(modality_layer)
            .with(Layer::default());

        let disp = Dispatch::new(subscriber);
        tracing::dispatcher::set_global_default(disp).expect("set global tracer");

        modality_ingest_handle
    };

    // Constant seed so we get predictable output, which matches the docs.
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);

    // Enable signal handling for convenient process termination.
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let shutdown_requested_for_handler = shutdown_requested.clone();
    if let Err(e) = ctrlc::set_handler(move || {
        shutdown_requested_for_handler.store(true, Ordering::SeqCst);
    }) {
        tracing::error!(
            err = &e as &dyn Error,
            "Could not establish a process signal handler."
        );
    };

    // If there is an integer argument provided, run this process for that duration in seconds
    // and then intentionally shut down.
    let args: Vec<_> = std::env::args().collect();
    if args.len() >= 2 {
        if let Ok(run_duration_in_seconds) = args[1].as_str().parse::<u64>() {
            let shutdown_requested_for_timed_run = shutdown_requested.clone();
            let _ = thread::spawn(move || {
                for _ in 0..run_duration_in_seconds {
                    // Check if something else (like the signal handler) has requested shutdown
                    if shutdown_requested_for_timed_run.load(Ordering::SeqCst) {
                        return;
                    }
                    thread::sleep(Duration::from_secs(1))
                }
                shutdown_requested_for_timed_run.store(true, Ordering::SeqCst);
            });
            println!("Running a pipeline of collaborating processes for {} second{}. Sending traces to modality.", run_duration_in_seconds, if run_duration_in_seconds == 0 { "" } else {"s"})
        } else {
            println!("Running a pipeline of collaborating processes indefinitely. Sending traces to modality.")
        }
    }
    {
        println!(
            "Running pipeline of collaborating processes indefinitely. Sending traces to modality."
        )
    }
    let is_shutdown_requested = move || shutdown_requested.load(Ordering::SeqCst);

    let (consumer_tx, consumer_rx) = sync_channel(CONSUMER_CHANNEL_SIZE);
    let (monitor_tx, monitor_rx) = channel();

    let monitor_tx_for_producer = monitor_tx.clone();
    let is_shutdown_requested_for_producer = is_shutdown_requested.clone();
    let producer_rng = StdRng::from_rng(&mut rng).unwrap();
    let producer_join_handle = thread::Builder::new()
        .name(Component::Producer.name().into())
        .spawn(|| {
            producer::run_producer(
                consumer_tx,
                monitor_tx_for_producer,
                is_shutdown_requested_for_producer,
                producer_rng,
            )
        })
        .expect("Could not start producer");

    let monitor_tx_for_consumer = monitor_tx;
    let is_shutdown_requested_for_consumer = is_shutdown_requested.clone();
    let consumer_rng = StdRng::from_rng(&mut rng).unwrap();
    let consumer_join_handle = thread::Builder::new()
        .name(Component::Consumer.name().into())
        .spawn(|| {
            consumer::run_consumer(
                consumer_rx,
                monitor_tx_for_consumer,
                is_shutdown_requested_for_consumer,
                consumer_rng,
            )
        })
        .expect("Could not start consumer");

    let monitor_join_handle = thread::Builder::new()
        .name(Component::Monitor.name().into())
        .spawn(|| monitor::run_monitor(monitor_rx, is_shutdown_requested))
        .expect("Could not start monitor");

    // Wait for all the threads to finish
    for (jh, component) in [
        (producer_join_handle, Component::Producer),
        (consumer_join_handle, Component::Consumer),
        (monitor_join_handle, Component::Monitor),
    ] {
        if let Err(_e) = jh.join() {
            tracing::error!(component = component.name(), "Failed to join thread");
        } else {
            tracing::info!(component = component.name(), "Joined thread");
        }
    }

    modality.finish();
}

const CONSUMER_CHANNEL_SIZE: usize = 64;

pub struct HeartbeatMessage {
    source: Component,
    meta: MessageMetadata,
}

pub struct MeasurementMessage {
    /// The measurement sample generated by the producer
    sample: i8,
    meta: MessageMetadata,
}

/// Information about the source of the message
pub struct MessageMetadata {
    /// When was this message from?
    #[allow(unused)]
    timestamp: NanosecondsSinceUnixEpoch,

    /// Which tracing timeline was this from?
    timeline_id: TimelineId,

    /// A correlation nonce for precisely matching
    /// the source event related to this message.
    nonce: i64,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub struct NanosecondsSinceUnixEpoch(pub u64);
impl NanosecondsSinceUnixEpoch {
    fn now() -> Result<Self, TimeCrime> {
        let d = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(d) => d,
            Err(_e) => return Err(TimeCrime::TimeRanBackwards),
        };
        let fit_nanos: u64 = match d.as_nanos().try_into() {
            Ok(n) => n,
            Err(_) => return Err(TimeCrime::TooFarInTheFuture),
        };
        Ok(NanosecondsSinceUnixEpoch(fit_nanos))
    }
}
#[derive(Debug)]
enum TimeCrime {
    TimeRanBackwards,
    TooFarInTheFuture,
}
impl fmt::Display for TimeCrime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TimeCrime::TimeRanBackwards => "System time ran backwards",
            TimeCrime::TooFarInTheFuture => {
                "We've gone too far in the future to represent time since epoch in nanoseconds"
            }
        };
        f.write_str(s)
    }
}
impl Error for TimeCrime {}

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
enum Component {
    Producer,
    Consumer,
    Monitor,
}
impl Component {
    fn name(self) -> &'static str {
        match self {
            Component::Producer => "producer",
            Component::Consumer => "consumer",
            Component::Monitor => "monitor",
        }
    }
}

mod producer {
    use super::*;
    pub fn run_producer(
        consumer_tx: SyncSender<MeasurementMessage>,
        monitor_tx: Sender<HeartbeatMessage>,
        is_shutdown_requested: impl Fn() -> bool,
        mut rng: impl Rng,
    ) {
        tracing::info!("Starting up");

        let timeline_id = timeline_id();

        // This is the imaginary physically-derived value that the producer is tracking and sampling
        let mut measurement: i8 = 0;
        for i in std::iter::repeat(0..u64::MAX).flatten() {
            if is_shutdown_requested() {
                tracing::info!("Shutting down");
                return;
            }
            let sample = update_and_sample_measurement(&mut rng, &mut measurement);
            send_measurement(sample, &consumer_tx, timeline_id, rng.gen());

            if i % 5 == 0 {
                send_heartbeat(&monitor_tx, Component::Producer, timeline_id, &mut rng);
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn update_and_sample_measurement(rng: &mut impl Rng, measurement: &mut i8) -> i8 {
        let update: i8 = rng.gen_range(-1..=1);
        *measurement = measurement.wrapping_add(update);
        let sample = *measurement;
        tracing::info!(sample, "Producer sampled raw measurement");
        sample
    }

    fn send_measurement(
        sample: i8,
        consumer_tx: &SyncSender<MeasurementMessage>,
        timeline_id: TimelineId,
        nonce: i64,
    ) {
        // The measurement sample value must be in the range [-50, 50]
        let sample = clamp(sample, -50, 50);

        let timestamp = match NanosecondsSinceUnixEpoch::now() {
            Ok(timestamp) => timestamp,
            Err(e) => {
                tracing::error!(
                    err = &e as &dyn Error,
                    "Could not produce a valid timestamp"
                );
                return;
            }
        };

        tracing::info!(
            sample,
            nonce,
            destination = Component::Consumer.name(),
            "Producer sending measurement message"
        );
        if let Err(_e) = consumer_tx.send(MeasurementMessage {
            sample,
            meta: MessageMetadata {
                timestamp,
                timeline_id,
                nonce,
            },
        }) {
            tracing::warn!(
                sample,
                "Producer failed to send sample measurement downstream"
            );
        }
    }

    fn clamp(x: i8, low: i8, high: i8) -> i8 {
        // There's something fishy about this implementation on purpose.
        // It's a surprise for later.
        // This is also the reason we're not using std::cmp::Ord::clamp.
        if x > high {
            x
        } else if x < low {
            low
        } else {
            x
        }
    }
}

mod consumer {
    use super::*;
    pub fn run_consumer(
        consumer_rx: Receiver<MeasurementMessage>,
        monitor_tx: Sender<HeartbeatMessage>,
        is_shutdown_requested: impl Fn() -> bool,
        mut rng: impl Rng,
    ) {
        tracing::info!("Starting up");

        let timeline_id = timeline_id();

        let mut last_heartbeat_tx: Instant = Instant::now();
        const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);

        loop {
            let timed_recv_result = consumer_rx.recv_timeout(Duration::from_millis(50));
            if is_shutdown_requested() {
                tracing::info!("Shutting down");
                return;
            }
            match timed_recv_result {
                Ok(msg) => {
                    tracing::info!(
                        sample = msg.sample,
                        interaction.remote_timeline_id = %msg.meta.timeline_id.get_raw(),
                        interaction.remote_nonce= msg.meta.nonce,
                        "Received measurement message");

                    expensive_task(msg.sample, &is_shutdown_requested);

                    if last_heartbeat_tx.elapsed() > HEARTBEAT_INTERVAL {
                        send_heartbeat(&monitor_tx, Component::Consumer, timeline_id, &mut rng);
                        last_heartbeat_tx = Instant::now();
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    // Loop around and try again
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    tracing::info!("Detected producer closed.");
                    tracing::info!("Shutting down");
                    return;
                }
            }
        }
    }

    // The imaginary spec says value must be in the range [-50, 50], we assume it's correct.
    // Fishy.   ><>  °ﾟº❍｡  ><>
    fn expensive_task(sample: i8, is_shutdown_requested: impl Fn() -> bool) {
        let abs_value: u8 = if sample <= 0x32 { 0x3 } else { sample as u8 };
        let count: u32 = if (abs_value as u32 * 3) > 0x168 {
            0x168
        } else {
            abs_value as u32 * 3
        };

        for i in 0..count {
            // TODO - RESTORE
            //tracing::trace!("Expensive task loop iteration");
            thread::sleep(Duration::from_millis(5));
            if i % 80 == 0 && is_shutdown_requested() {
                return;
            }
        }
    }
}

mod monitor {
    use super::*;
    use std::collections::HashMap;
    pub fn run_monitor(
        monitor_rx: Receiver<HeartbeatMessage>,
        is_shutdown_requested: impl Fn() -> bool,
    ) {
        tracing::info!("Starting up");
        let mut component_to_last_rx: HashMap<Component, Instant> = Default::default();
        loop {
            let timed_recv_result = monitor_rx.recv_timeout(Duration::from_millis(10));
            if is_shutdown_requested() {
                tracing::info!("Shutting down");
                return;
            }
            match timed_recv_result {
                Ok(msg) => {
                    tracing::info!(
                        source = msg.source.name(),
                        interaction.remote_timeline_id = %msg.meta.timeline_id.get_raw(),
                        interaction.remote_nonce = msg.meta.nonce,
                        "Received heartbeat message");
                    let prev = component_to_last_rx.insert(msg.source, Instant::now());
                    if prev.is_none() {
                        tracing::info!(
                            source = msg.source.name(),
                            "Monitor has observed a component for the first time for that component");
                    }
                    check_for_timeouts(&component_to_last_rx);
                }
                Err(RecvTimeoutError::Timeout) => {
                    check_for_timeouts(&component_to_last_rx);
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    tracing::info!("Detected all monitor transmission channels closed.");
                    tracing::info!("Shutting down");
                    return;
                }
            }
        }
    }

    fn check_for_timeouts(component_to_last_rx: &HashMap<Component, Instant>) {
        let now = Instant::now();
        const TIMEOUT: Duration = Duration::from_millis(600);
        for (component, last_received_heartbeat_at) in component_to_last_rx.iter() {
            if now.duration_since(*last_received_heartbeat_at) > TIMEOUT {
                tracing::error!(component = component.name(), "Detected heartbeat timeout");
            }
        }
    }
}

fn send_heartbeat(
    monitor_tx: &Sender<HeartbeatMessage>,
    // Who is sending the heartbeat?
    source: Component,
    // What is the timeline id of the component sending the heartbeat?
    // Instead of passing this around, for a small cost, one could use the timeline_id() fn
    timeline_id: TimelineId,
    rng: &mut impl Rng,
) {
    let timestamp = match NanosecondsSinceUnixEpoch::now() {
        Ok(timestamp) => timestamp,
        Err(e) => {
            tracing::error!(
                err = &e as &dyn Error,
                "Could not produce a valid timestamp"
            );
            return;
        }
    };

    let nonce: i64 = rng.gen();
    tracing::info!(
        destination = Component::Monitor.name(),
        nonce,
        "Sending heartbeat message"
    );
    if let Err(_e) = monitor_tx.send(HeartbeatMessage {
        source,
        meta: MessageMetadata {
            timestamp,
            timeline_id,
            nonce,
        },
    }) {
        tracing::warn!("Failed to send heartbeat message");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn can_make_a_timestamp() {
        NanosecondsSinceUnixEpoch::now().expect("Time crime");
    }
}
