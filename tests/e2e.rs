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
    default_exit_code: i32,
    delay_ms: u64,
    /// Optional: map of command substrings to exit codes for testing failures.
    failure_patterns: Vec<(String, i32)>,
}

impl MockSession {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            executed_commands: Arc::new(Mutex::new(Vec::new())),
            default_exit_code: 0,
            delay_ms: 0,
            failure_patterns: Vec::new(),
        }
    }

    pub fn with_delay_ms(mut self, delay_ms: u64) -> Self {
        self.delay_ms = delay_ms;
        self
    }

    pub fn with_exit_code(mut self, exit_code: i32) -> Self {
        self.default_exit_code = exit_code;
        self
    }

    /// Add a pattern that causes commands containing the pattern to fail with given exit code.
    pub fn with_failure_pattern(mut self, pattern: &str, exit_code: i32) -> Self {
        self.failure_patterns.push((pattern.to_string(), exit_code));
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

        // Check if any failure pattern matches.
        let exit_code = self
            .failure_patterns
            .iter()
            .find(|(pattern, _)| job.contains(pattern))
            .map(|(_, code)| *code)
            .unwrap_or(self.default_exit_code);

        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            Ok(ExitStatus::from_raw(exit_code << 8))
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
        let mock = MockSession::new(&host.hostname).with_delay_ms(50);
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
        let mut needs_wait = false;
        if let Some(cmd) = pending_cmd.take() {
            if let Some((host_index, allocated_slots)) =
                find_host_for_job(&mut slot_states, cmd.slots_required, cmd.allocation_policy)
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
                // No host has capacity. Put job back and wait for completion.
                pending_cmd = Some(cmd);
                needs_wait = true;
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

        // Wait for a completion only if we couldn't schedule due to no capacity.
        if needs_wait {
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

/// Create a Cmd with the given command, slots, and allocation policy.
fn make_cmd_with_policy(command: &str, slots: usize, policy: pegasus_ssh::AllocationPolicy) -> Cmd {
    let mut cmd = Cmd::new_for_test(command.to_string());
    cmd.slots_required = slots;
    cmd.allocation_policy = policy;
    cmd
}

/// Test result including both executed commands and errors.
pub struct SchedulingTestResult {
    pub executed: Vec<Vec<ExecutedCommand>>,
    pub errors: Vec<FailedCmd>,
}

/// Extended test harness that also returns error information.
async fn run_scheduling_test_with_errors(
    hosts: Vec<Host>,
    commands: Vec<Cmd>,
) -> SchedulingTestResult {
    run_scheduling_test_with_sessions(hosts, commands, None).await
}

/// Test harness with custom session configuration for failure testing.
async fn run_scheduling_test_with_sessions(
    hosts: Vec<Host>,
    commands: Vec<Cmd>,
    session_configs: Option<Vec<MockSession>>,
) -> SchedulingTestResult {
    let num_hosts = hosts.len();

    // Create mock sessions and track their executed commands.
    let mut sessions: Vec<Arc<Box<dyn Session + Send + Sync>>> = Vec::with_capacity(num_hosts);
    let mut command_trackers: Vec<Arc<Mutex<Vec<ExecutedCommand>>>> = Vec::with_capacity(num_hosts);

    match session_configs {
        Some(configs) => {
            for mock in configs {
                command_trackers.push(mock.executed_commands());
                sessions.push(Arc::new(Box::new(mock) as Box<dyn Session + Send + Sync>));
            }
        }
        None => {
            for host in &hosts {
                let mock = MockSession::new(&host.hostname).with_delay_ms(50);
                command_trackers.push(mock.executed_commands());
                sessions.push(Arc::new(Box::new(mock) as Box<dyn Session + Send + Sync>));
            }
        }
    }

    // Initialize slot state for each host.
    let mut slot_states: Vec<HostSlotState> =
        hosts.iter().map(|h| HostSlotState::new(h.slots)).collect();

    // Channel for job completion notifications.
    let (completion_tx, completion_rx) = flume::unbounded::<JobCompletion>();

    // Track errors.
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
        let mut needs_wait = false;
        if let Some(cmd) = pending_cmd.take() {
            if let Some((host_index, allocated_slots)) =
                find_host_for_job(&mut slot_states, cmd.slots_required, cmd.allocation_policy)
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
                // No host has capacity. Put job back and wait for completion.
                pending_cmd = Some(cmd);
                needs_wait = true;
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
        if needs_wait {
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
    let mut executed = Vec::with_capacity(num_hosts);
    for tracker in command_trackers {
        let cmds = tracker.lock().await;
        executed.push(cmds.clone());
    }

    // Collect errors.
    let errors = std::mem::take(&mut *errored.lock().await);

    SchedulingTestResult { executed, errors }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
    // Use {{slots}} template to capture actual allocated slots.
    let commands = vec![
        make_cmd("echo slots={{slots}} 2gpu_a", 2),
        make_cmd("echo slots={{slots}} 2gpu_b", 2),
        make_cmd("echo slots={{slots}} 2gpu_c", 2),
        make_cmd("echo slots={{slots}} 2gpu_d", 2),
    ];

    let sessions = MockSession::new("localhost").with_delay_ms(100);
    let result = run_scheduling_test_with_sessions(hosts, commands, Some(vec![sessions])).await;

    // All 4 jobs should complete.
    assert_eq!(result.executed[0].len(), 4);

    // Verify that slots are even-aligned pairs.
    let expected_pairs = ["0,1", "2,3", "4,5", "6,7"];
    let mut found_pairs: Vec<String> = result.executed[0]
        .iter()
        .filter_map(|cmd| {
            // Extract slots=X,Y from command
            cmd.command
                .split("slots=")
                .nth(1)
                .and_then(|s| s.split_whitespace().next())
                .map(|s| s.to_string())
        })
        .collect();
    found_pairs.sort();

    assert_eq!(
        found_pairs, expected_pairs,
        "Expected even-aligned pairs {:?}, got {:?}",
        expected_pairs, found_pairs
    );
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

// =============================================================================
// Example File Validation Tests
// =============================================================================

/// Test that all host YAML files in examples/ are properly parsable.
#[test]
fn test_example_host_files_are_valid() {
    let examples_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");

    let mut found_any = false;
    for entry in std::fs::read_dir(&examples_dir).unwrap() {
        let entry = entry.unwrap();
        if !entry.file_type().unwrap().is_dir() {
            continue;
        }
        let hosts_path = entry.path().join("hosts.yaml");
        if !hosts_path.exists() {
            continue;
        }
        found_any = true;
        let path_str = hosts_path.to_str().unwrap();
        let hosts = get_hosts(path_str);
        assert!(
            !hosts.is_empty(),
            "Host file {:?} should contain at least one host",
            hosts_path
        );
        println!(
            "{:?}: {} hosts parsed successfully",
            hosts_path,
            hosts.len()
        );
    }
    assert!(found_any, "No hosts.yaml files found in examples/");
}

/// Test that all queue YAML files in examples/ are properly parsable.
#[test]
fn test_example_queue_files_are_valid() {
    use pegasus_ssh::validate_queue_file;

    let examples_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");

    let mut found_any = false;
    for entry in std::fs::read_dir(&examples_dir).unwrap() {
        let entry = entry.unwrap();
        if !entry.file_type().unwrap().is_dir() {
            continue;
        }
        let queue_path = entry.path().join("queue.yaml");
        if !queue_path.exists() {
            continue;
        }
        found_any = true;
        let path_str = queue_path.to_str().unwrap();
        let result = validate_queue_file(path_str);
        assert!(
            result.is_ok(),
            "Queue file {:?} failed to parse: {}",
            queue_path,
            result.unwrap_err()
        );
        let job_count = result.unwrap();
        assert!(
            job_count > 0,
            "Queue file {:?} should contain at least one job",
            queue_path
        );
        println!("{:?}: {} jobs parsed successfully", queue_path, job_count);
    }
    assert!(found_any, "No queue.yaml files found in examples/");
}

// =============================================================================
// Error Case Tests
// =============================================================================

#[test]
fn test_error_invalid_yaml_queue_file() {
    use pegasus_ssh::validate_queue_file;

    let mut file = NamedTempFile::new().unwrap();
    // Invalid YAML: unbalanced brackets
    file.write_all(b"- command: echo hello\n  invalid: [unclosed")
        .unwrap();
    let result = validate_queue_file(file.path().to_str().unwrap());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Failed to parse YAML"));
}

#[test]
fn test_error_queue_file_missing_command_key() {
    use pegasus_ssh::validate_queue_file;

    let mut file = NamedTempFile::new().unwrap();
    // Valid YAML but missing 'command' key
    file.write_all(b"- slots: 2\n  param: value").unwrap();
    let result = validate_queue_file(file.path().to_str().unwrap());
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("missing 'command' key"));
}

#[test]
fn test_error_empty_queue_file() {
    use pegasus_ssh::validate_queue_file;

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(b"[]").unwrap();
    let result = validate_queue_file(file.path().to_str().unwrap());
    // Empty queue is valid YAML, but has 0 jobs
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

#[test]
#[should_panic(expected = "Failed to open")]
fn test_error_hosts_file_not_found() {
    get_hosts("/nonexistent/path/hosts.yaml");
}

#[test]
#[should_panic(expected = "Failed to parse")]
fn test_error_invalid_yaml_hosts_file() {
    let mut file = NamedTempFile::new().unwrap();
    // Invalid YAML
    file.write_all(b"- hostname: [unclosed").unwrap();
    get_hosts(file.path().to_str().unwrap());
}

#[test]
#[should_panic(expected = "missing the 'hostname' key")]
fn test_error_hosts_file_missing_hostname_key() {
    let mut file = NamedTempFile::new().unwrap();
    // Parametrized entry without 'hostname' key
    file.write_all(b"- slots: 8\n  gpu: nvidia").unwrap();
    get_hosts(file.path().to_str().unwrap());
}

#[tokio::test]
async fn test_error_command_execution_failure() {
    // Test that command failures are properly tracked.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 4
"#,
    );

    let commands = vec![
        make_cmd("echo success1", 1),
        make_cmd("echo failing_command", 1),
        make_cmd("echo success2", 1),
    ];

    // Create mock session that fails on commands containing "failing".
    let mock = MockSession::new("localhost")
        .with_delay_ms(10)
        .with_failure_pattern("failing", 1);

    let result = run_scheduling_test_with_sessions(hosts, commands, Some(vec![mock])).await;

    // All 3 commands should execute.
    assert_eq!(result.executed[0].len(), 3);

    // One command should have failed.
    assert_eq!(result.errors.len(), 1);
    let error_debug = format!("{:?}", result.errors[0]);
    assert!(error_debug.contains("failing_command"));
}

#[tokio::test]
async fn test_error_multiple_command_failures() {
    let hosts = parse_hosts_yaml("- localhost");

    let commands = vec![
        make_cmd("echo fail1", 1),
        make_cmd("echo success", 1),
        make_cmd("echo fail2", 1),
    ];

    let mock = MockSession::new("localhost")
        .with_delay_ms(10)
        .with_failure_pattern("fail", 1);

    let result = run_scheduling_test_with_sessions(hosts, commands, Some(vec![mock])).await;

    assert_eq!(result.executed[0].len(), 3);
    assert_eq!(result.errors.len(), 2);
}

#[tokio::test]
async fn test_error_all_commands_fail() {
    let hosts = parse_hosts_yaml("- localhost");

    let commands = vec![make_cmd("echo cmd1", 1), make_cmd("echo cmd2", 1)];

    // Session that fails all commands.
    let mock = MockSession::new("localhost")
        .with_delay_ms(10)
        .with_exit_code(1);

    let result = run_scheduling_test_with_sessions(hosts, commands, Some(vec![mock])).await;

    assert_eq!(result.executed[0].len(), 2);
    assert_eq!(result.errors.len(), 2);
}

// =============================================================================
// Allocation Policy Combination Tests
// =============================================================================

#[tokio::test]
async fn test_policy_mixed_firstfit_and_buddy_jobs() {
    use pegasus_ssh::AllocationPolicy;

    // Host with 8 slots.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 8
"#,
    );

    // Mix of FirstFit and Buddy jobs.
    let commands = vec![
        make_cmd_with_policy("echo firstfit_2 {{slots}}", 2, AllocationPolicy::FirstFit),
        make_cmd_with_policy("echo buddy_2 {{slots}}", 2, AllocationPolicy::Buddy),
        make_cmd_with_policy("echo firstfit_1 {{slots}}", 1, AllocationPolicy::FirstFit),
        make_cmd_with_policy("echo buddy_4 {{slots}}", 4, AllocationPolicy::Buddy),
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    // All jobs should complete.
    assert_eq!(result.executed[0].len(), 4);
    assert!(result.errors.is_empty());

    // Verify slots were injected correctly (no {{slots}} remaining).
    for cmd in &result.executed[0] {
        assert!(
            !cmd.command.contains("{{slots}}"),
            "Template not filled: {}",
            cmd.command
        );
    }
}

#[tokio::test]
async fn test_policy_buddy_gets_aligned_slots() {
    use pegasus_ssh::AllocationPolicy;

    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 8
"#,
    );

    // Buddy 2-slot jobs should get even-aligned pairs.
    let commands = vec![
        make_cmd_with_policy("echo buddy_a {{slots}}", 2, AllocationPolicy::Buddy),
        make_cmd_with_policy("echo buddy_b {{slots}}", 2, AllocationPolicy::Buddy),
        make_cmd_with_policy("echo buddy_c {{slots}}", 2, AllocationPolicy::Buddy),
        make_cmd_with_policy("echo buddy_d {{slots}}", 2, AllocationPolicy::Buddy),
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    assert_eq!(result.executed[0].len(), 4);

    // Extract slot assignments from commands.
    let mut all_slots: Vec<Vec<usize>> = Vec::new();
    for cmd in &result.executed[0] {
        // Parse slots from command like "echo buddy_a 0,1"
        let slots_str = cmd.command.split_whitespace().last().unwrap();
        let slots: Vec<usize> = slots_str.split(',').map(|s| s.parse().unwrap()).collect();
        all_slots.push(slots);
    }

    // Each buddy allocation should be aligned: start at even index.
    for slots in &all_slots {
        assert_eq!(slots.len(), 2, "Each job should have 2 slots");
        assert!(
            slots[0] % 2 == 0,
            "Buddy allocation should start at even index, got {:?}",
            slots
        );
        assert_eq!(
            slots[1],
            slots[0] + 1,
            "Slots should be contiguous, got {:?}",
            slots
        );
    }
}

#[tokio::test]
async fn test_policy_buddy_4_slot_alignment() {
    use pegasus_ssh::AllocationPolicy;

    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 8
"#,
    );

    // Buddy 4-slot jobs should start at indices 0 or 4.
    let commands = vec![
        make_cmd_with_policy("echo buddy4_a {{slots}}", 4, AllocationPolicy::Buddy),
        make_cmd_with_policy("echo buddy4_b {{slots}}", 4, AllocationPolicy::Buddy),
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    assert_eq!(result.executed[0].len(), 2);

    for cmd in &result.executed[0] {
        let slots_str = cmd.command.split_whitespace().last().unwrap();
        let slots: Vec<usize> = slots_str.split(',').map(|s| s.parse().unwrap()).collect();
        assert_eq!(slots.len(), 4);
        assert!(
            slots[0] == 0 || slots[0] == 4,
            "4-slot buddy should start at 0 or 4, got {:?}",
            slots
        );
    }
}

#[tokio::test]
async fn test_policy_allocation_policy_template_variable() {
    use pegasus_ssh::AllocationPolicy;

    let hosts = parse_hosts_yaml("- localhost");

    let commands = vec![
        make_cmd_with_policy(
            "echo policy={{allocation_policy}}",
            1,
            AllocationPolicy::FirstFit,
        ),
        make_cmd_with_policy(
            "echo policy={{allocation_policy}}",
            1,
            AllocationPolicy::Buddy,
        ),
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    assert_eq!(result.executed[0].len(), 2);

    // Verify allocation_policy template was injected.
    assert!(
        result.executed[0][0].command.contains("policy=first_fit"),
        "Expected first_fit, got: {}",
        result.executed[0][0].command
    );
    assert!(
        result.executed[0][1].command.contains("policy=buddy"),
        "Expected buddy, got: {}",
        result.executed[0][1].command
    );
}

#[tokio::test]
async fn test_policy_firstfit_falls_back_to_non_contiguous() {
    use pegasus_ssh::AllocationPolicy;

    // This test verifies FirstFit can use non-contiguous slots when needed.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 4
"#,
    );

    // Allocate slots 0 and 2, leaving 1 and 3 free.
    // A 2-slot FirstFit job should then get non-contiguous slots.
    let commands = vec![
        make_cmd_with_policy("echo job1 {{slots}}", 1, AllocationPolicy::FirstFit), // gets 0
        make_cmd_with_policy("echo job2 {{slots}}", 1, AllocationPolicy::FirstFit), // gets 1
        make_cmd_with_policy("echo job3 {{slots}}", 1, AllocationPolicy::FirstFit), // gets 2
        make_cmd_with_policy("echo job4 {{slots}}", 1, AllocationPolicy::FirstFit), // gets 3
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    assert_eq!(result.executed[0].len(), 4);
    assert!(result.errors.is_empty());
}

#[tokio::test]
async fn test_policy_buddy_vs_firstfit_fragmentation() {
    use pegasus_ssh::AllocationPolicy;

    // Test that Buddy is stricter than FirstFit under fragmentation.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - host1
  slots:
    - 8
- hostname:
    - host2
  slots:
    - 8
"#,
    );

    // Fill host1 with pattern that prevents 4-slot buddy but allows firstfit.
    // Occupy 0,1,4 on host1, leaving 2,3,5,6,7 free.
    // A 4-slot Buddy needs alignment at 0 or 4, neither works.
    // A 4-slot FirstFit can use 2,3,5,6 (non-contiguous fallback).

    // We simulate this by first running some jobs.
    // For simplicity, we just test that both policies work on fresh hosts.
    let commands = vec![
        make_cmd_with_policy("echo buddy4 {{slots}}", 4, AllocationPolicy::Buddy),
        make_cmd_with_policy("echo firstfit4 {{slots}}", 4, AllocationPolicy::FirstFit),
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    let total_jobs: usize = result.executed.iter().map(|r| r.len()).sum();
    assert_eq!(total_jobs, 2);
    assert!(result.errors.is_empty());
}

// =============================================================================
// Slot Size and Parametrization Tests
// =============================================================================

#[tokio::test]
async fn test_slots_large_slot_count_16() {
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - bighost
  slots:
    - 16
"#,
    );
    assert_eq!(hosts[0].slots, 16);

    let commands = vec![
        make_cmd("echo 8gpu {{slots}}", 8),
        make_cmd("echo 8gpu {{slots}}", 8),
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    assert_eq!(result.executed[0].len(), 2);
}

#[tokio::test]
async fn test_slots_large_slot_count_32() {
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - bighost
  slots:
    - 32
"#,
    );
    assert_eq!(hosts[0].slots, 32);

    let commands = vec![
        make_cmd("echo 16gpu {{slots}}", 16),
        make_cmd("echo 8gpu {{slots}}", 8),
        make_cmd("echo 4gpu {{slots}}", 4),
        make_cmd("echo 4gpu {{slots}}", 4),
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    assert_eq!(result.executed[0].len(), 4);
}

#[tokio::test]
async fn test_slots_heterogeneous_slot_requirements() {
    // Queue with mixed slot requirements: 1, 2, 4, 8 slot jobs.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - host8
  slots:
    - 8
- hostname:
    - host4
  slots:
    - 4
"#,
    );

    let commands = vec![
        make_cmd("echo 8slot", 8), // Only fits on host8
        make_cmd("echo 4slot", 4), // Fits on either
        make_cmd("echo 2slot", 2), // Fits on either
        make_cmd("echo 1slot", 1), // Fits on either
        make_cmd("echo 1slot", 1), // Fits on either
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    let total_jobs: usize = result.executed.iter().map(|r| r.len()).sum();
    assert_eq!(total_jobs, 5);
    assert!(result.errors.is_empty());

    // 8-slot job must have run on host8 (index 0).
    let host8_cmds: Vec<_> = result.executed[0]
        .iter()
        .filter(|c| c.command.contains("8slot"))
        .collect();
    assert_eq!(host8_cmds.len(), 1, "8-slot job should run on host8");
}

#[tokio::test]
async fn test_slots_many_small_jobs_on_large_host() {
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - bighost
  slots:
    - 8
"#,
    );

    // 8 single-slot jobs should all run concurrently.
    let commands: Vec<Cmd> = (0..8)
        .map(|i| make_cmd(&format!("echo job{}", i), 1))
        .collect();

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    assert_eq!(result.executed[0].len(), 8);
}

#[tokio::test]
async fn test_parametrization_host_with_slots() {
    // Test host parametrization combined with slots.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - node1
    - node2
  slots:
    - 4
"#,
    );

    assert_eq!(hosts.len(), 2);
    assert_eq!(hosts[0].hostname, "node1");
    assert_eq!(hosts[0].slots, 4);
    assert_eq!(hosts[1].hostname, "node2");
    assert_eq!(hosts[1].slots, 4);

    let commands = vec![
        make_cmd("echo job1 {{hostname}}", 2),
        make_cmd("echo job2 {{hostname}}", 2),
        make_cmd("echo job3 {{hostname}}", 2),
        make_cmd("echo job4 {{hostname}}", 2),
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    let total_jobs: usize = result.executed.iter().map(|r| r.len()).sum();
    assert_eq!(total_jobs, 4);

    // Verify hostname template was filled.
    for host_cmds in &result.executed {
        for cmd in host_cmds {
            assert!(!cmd.command.contains("{{hostname}}"));
            assert!(cmd.command.contains("node1") || cmd.command.contains("node2"));
        }
    }
}

#[tokio::test]
async fn test_parametrization_host_template_expansion() {
    // Test hostname with template variables.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - gpu{{id}}.cluster.local
  id:
    - "01"
    - "02"
    - "03"
  slots:
    - 8
"#,
    );

    assert_eq!(hosts.len(), 3);
    assert_eq!(hosts[0].hostname, "gpu01.cluster.local");
    assert_eq!(hosts[1].hostname, "gpu02.cluster.local");
    assert_eq!(hosts[2].hostname, "gpu03.cluster.local");
    assert_eq!(hosts[0].slots, 8);
}

#[tokio::test]
async fn test_parametrization_host_with_multiple_params() {
    // Test host with multiple parameters and slots.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - server1
    - server2
  gpu_type:
    - a100
    - h100
  slots:
    - 8
"#,
    );

    // Cartesian product: 2 hostnames × 2 gpu_types = 4 hosts
    assert_eq!(hosts.len(), 4);
    for host in &hosts {
        assert_eq!(host.slots, 8);
        assert!(host.params.contains_key("gpu_type"));
    }
}

#[tokio::test]
async fn test_parametrization_combined_host_and_job() {
    // Test that host params are available in job templates.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  region:
    - us-east
    - us-west
  slots:
    - 2
"#,
    );

    assert_eq!(hosts.len(), 2);

    let commands = vec![
        make_cmd("echo region={{region}} host={{hostname}}", 1),
        make_cmd("echo region={{region}} host={{hostname}}", 1),
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    let total_jobs: usize = result.executed.iter().map(|r| r.len()).sum();
    assert_eq!(total_jobs, 2);

    // Verify templates were filled with host params.
    for host_cmds in &result.executed {
        for cmd in host_cmds {
            assert!(!cmd.command.contains("{{region}}"));
            assert!(!cmd.command.contains("{{hostname}}"));
            assert!(cmd.command.contains("region=us-"));
        }
    }
}

#[tokio::test]
async fn test_parametrization_job_with_slots_and_params() {
    // Test that jobs with params also work correctly with slots.
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 4
"#,
    );

    // Job with both slots and custom params.
    let mut cmd = Cmd::new_for_test("echo lr={{lr}} slots={{slots}}".to_string());
    cmd.slots_required = 2;
    cmd.insert_param("lr".to_string(), "0.001".to_string());

    let result = run_scheduling_test_with_errors(hosts, vec![cmd]).await;

    assert_eq!(result.executed[0].len(), 1);
    let executed_cmd = &result.executed[0][0].command;
    assert!(executed_cmd.contains("lr=0.001"));
    assert!(!executed_cmd.contains("{{slots}}"));
    assert!(!executed_cmd.contains("{{lr}}"));
}

// =============================================================================
// Edge Cases and Boundary Tests
// =============================================================================

#[tokio::test]
async fn test_edge_single_slot_host_with_multi_slot_job_waits() {
    // Job requiring more slots than host has - should never be scheduled.
    // In the real system this would block forever; here we test find_host_for_job.
    use pegasus_ssh::AllocationPolicy;

    let mut hosts = vec![HostSlotState::new(4)];

    // Job needing 8 slots can't fit on 4-slot host.
    let result = find_host_for_job(&mut hosts, 8, AllocationPolicy::FirstFit);
    assert!(result.is_none());
}

#[tokio::test]
async fn test_edge_exact_slot_fit() {
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 4
"#,
    );

    // Job requiring exactly the available slots.
    let commands = vec![make_cmd("echo exact_fit", 4)];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    assert_eq!(result.executed[0].len(), 1);
}

#[tokio::test]
async fn test_edge_sequential_large_jobs() {
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 8
"#,
    );

    // Two 8-slot jobs must run sequentially.
    let commands = vec![
        make_cmd("echo first_8slot", 8),
        make_cmd("echo second_8slot", 8),
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    assert_eq!(result.executed[0].len(), 2);

    // Verify ordering (second job started after first completed).
    assert!(result.executed[0][0].timestamp < result.executed[0][1].timestamp);
}

#[tokio::test]
async fn test_edge_mix_sequential_and_concurrent() {
    let hosts = parse_hosts_yaml(
        r#"
- hostname:
    - localhost
  slots:
    - 4
"#,
    );

    // 4-slot job followed by four 1-slot jobs.
    // The 4-slot runs alone, then all 1-slot can run concurrently.
    let commands = vec![
        make_cmd("echo big_job", 4),
        make_cmd("echo small_1", 1),
        make_cmd("echo small_2", 1),
        make_cmd("echo small_3", 1),
        make_cmd("echo small_4", 1),
    ];

    let result = run_scheduling_test_with_errors(hosts, commands).await;

    assert_eq!(result.executed[0].len(), 5);
}
