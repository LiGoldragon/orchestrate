//! Deadline-driven reclamation for expired lane records.
//!
//! The worker receives a new deadline only when lane lifecycle state changes.
//! It sleeps until that exact deadline, then submits one ordinary observation
//! through the daemon boundary. It never scans on an interval.

use std::{
    path::PathBuf,
    sync::mpsc::{self, Receiver, RecvTimeoutError, Sender},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use signal_orchestrate::schema::lib::{Input, Observation};

use crate::{OrdinarySignalTransport, TimestampNanos};

enum LaneReclamationSignal {
    Reschedule(Option<TimestampNanos>),
    Shutdown,
}

/// One lifecycle-owned deadline worker. The worker has no store access: its
/// expiry event re-enters the daemon through the normal ordinary Signal path,
/// preserving the engine actor as the single durable-state writer.
pub struct LaneReclaimer {
    sender: Sender<LaneReclamationSignal>,
}

impl LaneReclaimer {
    pub fn spawn(socket_path: PathBuf, initial_deadline: Option<TimestampNanos>) -> Self {
        let (sender, receiver) = mpsc::channel();
        thread::Builder::new()
            .name("orchestrate-lane-reclaimer".to_string())
            .spawn(move || {
                LaneReclamationWorker::new(socket_path, receiver, initial_deadline).run();
            })
            .expect("spawn orchestrate lane reclamation worker");
        Self { sender }
    }

    pub fn reschedule(&self, deadline: Option<TimestampNanos>) {
        let _ = self
            .sender
            .send(LaneReclamationSignal::Reschedule(deadline));
    }
}

impl Drop for LaneReclaimer {
    fn drop(&mut self) {
        let _ = self.sender.send(LaneReclamationSignal::Shutdown);
    }
}

struct LaneReclamationWorker {
    socket_path: PathBuf,
    receiver: Receiver<LaneReclamationSignal>,
    deadline: Option<TimestampNanos>,
}

impl LaneReclamationWorker {
    fn new(
        socket_path: PathBuf,
        receiver: Receiver<LaneReclamationSignal>,
        deadline: Option<TimestampNanos>,
    ) -> Self {
        Self {
            socket_path,
            receiver,
            deadline,
        }
    }

    fn run(mut self) {
        loop {
            let Some(deadline) = self.deadline else {
                if !self.receive_next_signal() {
                    return;
                }
                continue;
            };
            let wait = Self::wait_until(deadline);
            match self.receiver.recv_timeout(wait) {
                Ok(signal) => {
                    if !self.apply_signal(signal) {
                        return;
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    // A deadline is an OS wait for one known lifecycle fact, not
                    // a ticker. The following request is processed by the engine
                    // actor and re-arms this worker from current durable state.
                    self.deadline = None;
                    self.submit_expiry_event();
                }
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn receive_next_signal(&mut self) -> bool {
        match self.receiver.recv() {
            Ok(signal) => self.apply_signal(signal),
            Err(_) => false,
        }
    }

    fn apply_signal(&mut self, signal: LaneReclamationSignal) -> bool {
        match signal {
            LaneReclamationSignal::Reschedule(deadline) => {
                self.deadline = deadline;
                true
            }
            LaneReclamationSignal::Shutdown => false,
        }
    }

    fn wait_until(deadline: TimestampNanos) -> Duration {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        Duration::from_nanos(deadline.value().saturating_sub(now))
    }

    fn submit_expiry_event(&self) {
        let input = Input::observe(Observation::Lanes);
        let _ = OrdinarySignalTransport::connect(&self.socket_path)
            .and_then(|mut transport| transport.exchange(&input).map(|_| ()));
    }
}
