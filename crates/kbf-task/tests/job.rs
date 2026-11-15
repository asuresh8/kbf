//! A job's Command is started directly (`Run::job`), and the task ends and reaps what the Command
//! left (`src/run.rs`, `Job`).
//!
//! Each case runs the `socket_probe` helper as a real Command and takes the lock its `linger`
//! mode's leftover holds: the lock is free once that process is gone. `Job::Subreaper` reaps every
//! child of its process, so the cases that use it are one test (the tests of a file share a
//! process), and `Job::Group` is in `job_group.rs`.

#![cfg(target_os = "linux")]

use std::time::Duration;

use kbf_task::network::Layers;
use kbf_task::run::Job;

mod job_support;
use job_support::{Leftover, lock_is_free, run_probe, scratch};

/// `linger` leaves a process in a group of its own: only the subreaper reaches it, as it is handed
/// to this process when its parent ends. It is gone and reaped when the run returns, also when
/// the run ended at its deadline.
#[test]
fn a_process_that_left_the_group_is_gone_when_the_run_returns() {
    for (name, deadline) in [
        ("ends", Duration::from_secs(600)),
        ("deadline", Duration::ZERO),
    ] {
        let dir = scratch(name);
        let ran = run_probe(&dir, "linger", Job::Subreaper, deadline);
        let _left = Leftover(&dir.join("pid"));
        assert_eq!(ran.layers, Layers::Job);
        assert_eq!(ran.timed_out, deadline.is_zero());
        // With a deadline of zero the Command may be killed before it started anything.
        if dir.join("lock").exists() {
            assert!(
                lock_is_free(&dir),
                "{name}: a process of the Command is running"
            );
        }
    }
}
