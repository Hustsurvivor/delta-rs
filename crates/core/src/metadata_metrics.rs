//! Opt-in timing primitives for metadata-only Delta Lake experiments.
//!
//! A collector is owned by one benchmark run and is explicitly passed to the
//! measured operation.  This deliberately avoids task-local or global state:
//! concurrent async callers cannot attribute each other's timings to the same
//! `run_id`.  Stage durations are *inclusive*; consumers must not add nested
//! stages together.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// One completed metadata stage.
#[derive(Clone, Debug)]
pub struct MetadataStage {
    /// Stable stage identifier, such as `snapshot_reconstruction`.
    pub name: &'static str,
    /// Inclusive wall-clock duration for this stage.
    pub elapsed: Duration,
}

/// Per-run collector for stable metadata stage names.
#[derive(Clone, Debug)]
pub struct MetadataMetrics {
    run_id: String,
    stages: Arc<Mutex<Vec<MetadataStage>>>,
    candidate_files: Arc<Mutex<Option<usize>>>,
}

impl MetadataMetrics {
    /// Create an empty collector associated with one logical benchmark run.
    pub fn new(run_id: impl Into<String>) -> Self {
        Self {
            run_id: run_id.into(),
            stages: Arc::new(Mutex::new(Vec::new())),
            candidate_files: Arc::new(Mutex::new(None)),
        }
    }

    /// Identifier supplied by the caller to correlate all stages in a run.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Start an inclusive stage. Dropping the returned guard records it.
    pub fn stage(&self, name: &'static str) -> MetadataStageGuard {
        MetadataStageGuard {
            metrics: self.clone(),
            name,
            started: Instant::now(),
        }
    }

    /// Return a snapshot of stages recorded so far, in completion order.
    pub fn stages(&self) -> Vec<MetadataStage> {
        self.stages
            .lock()
            .expect("metadata metrics lock poisoned")
            .clone()
    }

    /// Candidate files selected by this run's own transaction-log replay.
    pub fn candidate_files(&self) -> Option<usize> {
        *self.candidate_files.lock().expect("metadata metrics lock poisoned")
    }

    pub(crate) fn record_candidate_files(&self, count: usize) {
        *self.candidate_files.lock().expect("metadata metrics lock poisoned") = Some(count);
    }

    fn record(&self, name: &'static str, elapsed: Duration) {
        self.stages
            .lock()
            .expect("metadata metrics lock poisoned")
            .push(MetadataStage { name, elapsed });
    }
}

/// RAII timer used around an async operation; it covers polling until the
/// future completes rather than merely timing future construction.
pub struct MetadataStageGuard {
    metrics: MetadataMetrics,
    name: &'static str,
    started: Instant,
}

impl Drop for MetadataStageGuard {
    fn drop(&mut self) {
        self.metrics.record(self.name, self.started.elapsed());
    }
}
