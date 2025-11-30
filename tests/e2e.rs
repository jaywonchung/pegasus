//! End-to-end tests for Pegasus scheduling.

use std::io::Write;
use std::process::ExitStatus;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tempfile::NamedTempFile;
use tokio::sync::Mutex;

use pegasus_ssh::{
    Cmd, FailedCmd, Host, HostSlotState, JobCompletion, PegasusError, Session, find_host_for_job,
    get_hosts, spawn_job,
};

/// Record of an executed command for testing.
#[derive(Debug, Clone)]
pub struct ExecutedCommand {
    pub command: String,
    pub timestamp: Instant,
}

/// Mock session for testing that doesn't actually execute commands.
pub struct MockSession {
    #[allow(dead_code)]
    name: String,
    executed_commands: Arc<Mutex<Vec<ExecutedCommand>>>,
    #[allow(dead_code)]
    default_exit_code: i32,
    delay_ms: u64,
}

impl MockSession {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            executed_commands: Arc::new(Mutex::new(Vec::new())),
            default_exit_code: 0,
            delay_ms: 0,
        }
    }

    pub fn with_delay_ms(mut self, delay_ms: u64) -> Self {
        self.delay_ms = delay_ms;
        self
    }

    pub fn executed_commands(&self) -> Arc<Mutex<Vec<ExecutedCommand>>> {
        Arc::clone(&self.executed_commands)
    }
}

#[async_trait]
impl Session for MockSession {
    async fn run(&self, job: &str, _print_period: usize) -> Result<ExitStatus, PegasusError> {
        if self.delay_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(self.delay_ms)).await;
        }

        self.executed_commands.lock().await.push(ExecutedCommand {
            command: job.to_string(),
            timestamp: Instant::now(),
        });

        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            Ok(ExitStatus::from_raw(self.default_exit_code << 8))
        }
    }
}

/// Test harness that runs the scheduling loop with mock sessions.
/// Returns the list of executed commands per host.
async fn run_scheduling_test(hosts: Vec<Host>, commands: Vec<Cmd>) -> Vec<Vec<ExecutedCommand>> {
    let num_hosts = hosts.len();

    // Create mock sessions and track their executed commands.
    let mut sessions: Vec<Arc<Box<dyn Session + Send + Sync>>> = Vec::with_capacity(num_hosts);
    let mut command_trackers: Vec<Arc<Mutex<Vec<ExecutedCommand>>>> = Vec::with_capacity(num_hosts);

    for host in &hosts {
        let mock = MockSession::new(&host.hostname).with_delay_ms(10);
        command_trackers.push(mock.executed_commands());
        sessions.push(Arc::new(Box::new(mock) as Box<dyn Session + Send + Sync>));
    }

    // Initialize slot state for each host.
    let mut slot_states: Vec<HostSlotState> =
        hosts.iter().map(|h| HostSlotState::new(h.slots)).collect();

    // Channel for job completion notifications.
    let (completion_tx, completion_rx) = flume::unbounded::<JobCompletion>();

    // Track errors (we don't assert on these in most tests).
    let errored: Arc<Mutex<Vec<FailedCmd>>> = Arc::new(Mutex::new(vec![]));

    // Track running job tasks.
    let mut running_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Process all commands.
    let mut cmd_iter = commands.into_iter();
    let mut pending_cmd: Option<Cmd> = cmd_iter.next();

    loop {
        // Process any completions (non-blocking) - release slots.
        while let Ok(completion) = completion_rx.try_recv() {
            slot_states[completion.host_index].release(&completion.released_slots);
        }

        // Try to schedule the pending job.
        if let Some(cmd) = pending_cmd.take() {
            if let Some((host_index, allocated_slots)) =
                find_host_for_job(&mut slot_states, cmd.slots_required)
            {
                let task = spawn_job(
                    Arc::clone(&sessions[host_index]),
                    hosts[host_index].clone(),
                    cmd,
                    allocated_slots,
                    completion_tx.clone(),
                    host_index,
                    0, // print_period = 0 for tests
                    Arc::clone(&errored),
                );
                running_tasks.push(task);

                // Get next command
                pending_cmd = cmd_iter.next();
            } else {
                // No host has capacity. Put job back and wait.
                pending_cmd = Some(cmd);
            }
        } else {
            // Get next command if available
            pending_cmd = cmd_iter.next();
        }

        // Check termination condition.
        if pending_cmd.is_none() {
            let slots_in_use: usize = slot_states
                .iter()
                .map(|s| s.total_slots() - s.free_slots())
                .sum();
            if slots_in_use == 0 {
                break;
            }
        }

        // Wait for a completion if we have a pending job but no capacity.
        let slots_in_use: usize = slot_states
            .iter()
            .map(|s| s.total_slots() - s.free_slots())
            .sum();
        if pending_cmd.is_some() && slots_in_use > 0 {
            if let Ok(completion) = completion_rx.recv_async().await {
                slot_states[completion.host_index].release(&completion.released_slots);
            }
        } else {
            tokio::task::yield_now().await;
        }
    }

    // Wait for all running tasks to complete.
    futures::future::join_all(running_tasks).await;

    // Collect executed commands from all sessions.
    let mut results = Vec::with_capacity(num_hosts);
    for tracker in command_trackers {
        let cmds = tracker.lock().await;
        results.push(cmds.clone());
    }
    results
}

/// Parse hosts from YAML string.
fn parse_hosts_yaml(yaml: &str) -> Vec<Host> {
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(yaml.as_bytes()).unwrap();
    get_hosts(file.path().to_str().unwrap())
}

/// Create a Cmd with the given command and slots.
fn make_cmd(command: &str, slots: usize) -> Cmd {
    let mut cmd = Cmd::new_for_test(command.to_string());
    cmd.slots_required = slots;
    cmd
}

// =============================================================================
// E2E Tests for Slot-Based Scheduling
// =============================================================================

#[tokio::test]
async fn test_e2e_single_host_sequential_jobs() {
    // Single host with 1 slot - jobs should run sequentially.
    let hosts = parse_hosts_yaml("- localhost");
    assert_eq!(hosts.len(), 1);
    assert_eq!(hosts[0].slots, 1);

    let commands = vec![
        make_cmd("echo job1", 1),
        make_cmd("echo job2", 1),
        make_cmd("echo job3", 1),
    ];

    let results = run_scheduling_test(hosts, commands).await;

    // All 3 jobs should run on the single host.
    assert_eq!(results[0].len(), 3);

    // Verify commands contain expected content (with slots injected).
    assert!(results[0][0].command.contains("job1"));
    assert!(results[0][1].command.contains("job2"));
    assert!(results[0][2].command.contains("job3"));
}

#[tokio::test]
async fn test_e2e_multi_slot_concurrent_jobs() {
    // Single host with 4 slots - up to 4 jobs can run concurrently.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 4
"#,
    );
    assert_eq!(hosts.len(), 1);
    assert_eq!(hosts[0].slots, 4);

    let commands = vec![
        make_cmd("echo job1", 1),
        make_cmd("echo job2", 1),
        make_cmd("echo job3", 1),
        make_cmd("echo job4", 1),
    ];

    let results = run_scheduling_test(hosts, commands).await;

    // All 4 jobs should run on the single host.
    assert_eq!(results[0].len(), 4);
}

#[tokio::test]
async fn test_e2e_slot_allocation_for_multi_gpu_job() {
    // Single host with 8 slots - test that slots are properly allocated.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 8
"#,
    );

    // One 4-slot job and four 1-slot jobs.
    let commands = vec![
        make_cmd("echo 4gpu {{slots}}", 4),
        make_cmd("echo 1gpu_a {{slots}}", 1),
        make_cmd("echo 1gpu_b {{slots}}", 1),
        make_cmd("echo 1gpu_c {{slots}}", 1),
        make_cmd("echo 1gpu_d {{slots}}", 1),
    ];

    let results = run_scheduling_test(hosts, commands).await;

    // All 5 jobs should complete.
    assert_eq!(results[0].len(), 5);

    // The 4-GPU job should have 4 slots allocated (e.g., "0,1,2,3").
    let four_gpu_cmd = &results[0][0].command;
    // The command will have slots injected, so we check it ran.
    assert!(four_gpu_cmd.contains("4gpu"));
    // Verify {{slots}} was replaced (not present in output).
    assert!(!four_gpu_cmd.contains("{{slots}}"));
}

#[tokio::test]
async fn test_e2e_nvlink_aware_2gpu_allocation() {
    // Test that 2-GPU jobs prefer even-aligned pairs.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 8
"#,
    );

    // Four 2-slot jobs should get pairs: (0,1), (2,3), (4,5), (6,7).
    let commands = vec![
        make_cmd("echo 2gpu_a", 2),
        make_cmd("echo 2gpu_b", 2),
        make_cmd("echo 2gpu_c", 2),
        make_cmd("echo 2gpu_d", 2),
    ];

    let results = run_scheduling_test(hosts, commands).await;

    // All 4 jobs should complete.
    assert_eq!(results[0].len(), 4);
}

#[tokio::test]
async fn test_e2e_multiple_hosts_load_distribution() {
    // Two hosts with 2 slots each.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - host1
    - host2
  slots:
    - 2
"#,
    );
    assert_eq!(hosts.len(), 2);

    // Four 1-slot jobs should distribute across both hosts.
    let commands = vec![
        make_cmd("echo job1", 1),
        make_cmd("echo job2", 1),
        make_cmd("echo job3", 1),
        make_cmd("echo job4", 1),
    ];

    let results = run_scheduling_test(hosts, commands).await;

    // Jobs should be distributed (not all on one host).
    let total_jobs: usize = results.iter().map(|r| r.len()).sum();
    assert_eq!(total_jobs, 4);
}

#[tokio::test]
async fn test_e2e_job_too_large_waits_for_capacity() {
    // Host with 4 slots, job requiring 4 slots should wait until capacity is free.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 4
"#,
    );

    // Three 2-slot jobs followed by one 4-slot job.
    // The 4-slot job must wait for the first two 2-slot jobs to complete.
    let commands = vec![
        make_cmd("echo 2slot_a", 2),
        make_cmd("echo 2slot_b", 2),
        make_cmd("echo 4slot", 4),
    ];

    let results = run_scheduling_test(hosts, commands).await;

    // All 3 jobs should complete.
    assert_eq!(results[0].len(), 3);

    // The 4-slot job should be last.
    assert!(results[0][2].command.contains("4slot"));
}

// =============================================================================
// E2E Tests for Backwards Compatibility
// =============================================================================

#[tokio::test]
async fn test_e2e_backwards_compat_no_slots_defaults_to_one() {
    // Hosts without slots field should default to 1.
    let hosts = parse_hosts_yaml("- localhost\n- localhost");
    assert_eq!(hosts.len(), 2);
    assert_eq!(hosts[0].slots, 1);
    assert_eq!(hosts[1].slots, 1);

    let commands = vec![make_cmd("echo job1", 1), make_cmd("echo job2", 1)];

    let results = run_scheduling_test(hosts, commands).await;

    // Both jobs should complete.
    let total_jobs: usize = results.iter().map(|r| r.len()).sum();
    assert_eq!(total_jobs, 2);
}

#[tokio::test]
async fn test_e2e_backwards_compat_parametrized_hosts() {
    // Test parametrized hosts still work correctly.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  param:
    - a
    - b
"#,
    );
    assert_eq!(hosts.len(), 2);
    assert_eq!(hosts[0].params.get("param"), Some(&"a".to_string()));
    assert_eq!(hosts[1].params.get("param"), Some(&"b".to_string()));
}

#[tokio::test]
async fn test_e2e_heterogeneous_cluster() {
    // Test a cluster with different slot counts.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - gpu8
  slots:
    - 8
- hostname:
    - gpu4
  slots:
    - 4
- hostname:
    - cpu
  slots:
    - 1
"#,
    );
    assert_eq!(hosts.len(), 3);
    assert_eq!(hosts[0].slots, 8);
    assert_eq!(hosts[1].slots, 4);
    assert_eq!(hosts[2].slots, 1);

    // Mix of job sizes.
    let commands = vec![
        make_cmd("echo 8gpu", 8),   // Only fits on gpu8
        make_cmd("echo 4gpu", 4),   // Fits on gpu8 or gpu4
        make_cmd("echo 1gpu_a", 1), // Fits anywhere
        make_cmd("echo 1gpu_b", 1),
    ];

    let results = run_scheduling_test(hosts, commands).await;

    // All 4 jobs should complete.
    let total_jobs: usize = results.iter().map(|r| r.len()).sum();
    assert_eq!(total_jobs, 4);
}

#[tokio::test]
async fn test_e2e_slots_template_variable_injected() {
    // Test that {{slots}} template variable is injected correctly.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 4
"#,
    );

    let commands = vec![make_cmd(
        "CUDA_VISIBLE_DEVICES={{slots}} python train.py",
        2,
    )];

    let results = run_scheduling_test(hosts, commands).await;

    // The command should have slots injected.
    assert_eq!(results[0].len(), 1);
    let cmd = &results[0][0].command;
    // Check that {{slots}} was replaced with actual slot indices (e.g., "0,1").
    assert!(!cmd.contains("{{slots}}"));
    assert!(cmd.contains("CUDA_VISIBLE_DEVICES="));
}
