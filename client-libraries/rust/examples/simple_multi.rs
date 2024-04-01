use auxon_sdk::tracing::blocking::{timeline_id, TimelineId, TracingModality};
use rand::{thread_rng, Rng};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::{Builder, JoinHandle};
use tracing::{info, info_span};

const THREADS: usize = 2;

enum Message {
    Data(Job),
}

struct Job {
    nonce: u32,
    num: u32,
    timeline_id: TimelineId,
}

fn main() {
    let modality = TracingModality::init().expect("init tracing");
    let mut rng = thread_rng();

    let (terminal_tx, terminal_rx): (Sender<Message>, Receiver<Message>) = channel();
    let mut channels: Vec<(usize, Sender<Message>, Receiver<Message>)> = (0..THREADS)
        .map(|i| {
            let (tx, rx) = channel();
            (i, tx, rx)
        })
        .collect();
    let tx_chans: Vec<Sender<Message>> = channels.iter().map(|(_i, tx, _rx)| tx.clone()).collect();

    let threads: Vec<JoinHandle<()>> = channels
        .drain(..)
        .map(|(i, _tx, rx)| {
            let term_tx = terminal_tx.clone();
            Builder::new()
                .name(format!("worker{:02}", i))
                .spawn(move || {
                    let timeline_id = timeline_id();
                    while let Ok(msg) = rx.recv() {
                        match msg {
                            Message::Data(job) => {
                                info!(
                                    interaction.remote_nonce=job.nonce,
                                    interaction.remote_timeline_id=?job.timeline_id.get_raw(),
                                    job.num,
                                    "received",
                                );

                                let comp_span = info_span!(
                                    "computation",
                                    abouttodothething = true,
                                    foo = "bar"
                                );

                                let result = comp_span.in_scope(|| {
                                    job.num * 2
                                });
                                //let nonce = job.nonce + THREADS as u32;
                                let nonce = job.nonce;
                                info!(nonce = nonce, source = ?timeline_id.get_raw(), result, "sending");
                                term_tx
                                    .send(Message::Data(Job {
                                        nonce,
                                        num: result,
                                        timeline_id,
                                    }))
                                    .unwrap();
                            }
                        }
                        std::thread::yield_now();
                    }
                })
                .unwrap()
        })
        .collect();

    let timeline_id = timeline_id();

    for i in 0..3 {
        // Don't start with 0 or 1
        let start = rng.gen_range(0..=100) + 2;
        let target = (start as usize) as usize % THREADS;
        info!(
            nonce = i,
            worker = target,
            input = start,
            source = ?timeline_id.get_raw(),
            "sending",
        );
        tx_chans[target]
            .send(Message::Data(Job {
                nonce: i,
                num: start,
                timeline_id,
            }))
            .unwrap();
    }

    for _ in 0..3 {
        let result = terminal_rx.recv().unwrap();
        match result {
            Message::Data(job) => {
                info!(
                    interaction.remote_nonce=job.nonce,
                    interaction.remote_timeline_id=?job.timeline_id.get_raw(),
                    job.num,
                    "result",
                );
            }
        }
    }

    drop(tx_chans);

    for t in threads {
        t.join().unwrap();
    }

    // end the modality ingest thread, flushing all already written events to modality
    modality.finish();
}
