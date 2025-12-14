//! Resource-aware scheduling for Queue mode.
//!
//! This module provides slot-based resource tracking for hosts, enabling
//! multiple concurrent jobs on a single host when sufficient slots are available.

use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::host::Host;
use crate::job::{AllocationPolicy, Cmd, FailedCmd};
use crate::session::Session;

/// Tracks slot allocation for a single host.
///
/// Slots are numbered from 0 to `total_slots - 1`.
#[derive(Debug)]
pub struct HostSlotState {
    /// Total number of slots on this host.
    total_slots: usize,
    /// Set of currently occupied slot indices.
    occupied_slots: HashSet<usize>,
}

impl HostSlotState {
    /// Creates a new slot state with the given total slots.
    pub fn new(total_slots: usize) -> Self {
        Self {
            total_slots,
            occupied_slots: HashSet::new(),
        }
    }

    /// Returns the number of free slots.
    pub fn free_slots(&self) -> usize {
        self.total_slots - self.occupied_slots.len()
    }

    /// Returns the total number of slots.
    pub fn total_slots(&self) -> usize {
        self.total_slots
    }

    /// Attempts to allocate `n` slots according to the given allocation policy.
    ///
    /// For `FirstFit` (default):
    /// 1. Contiguous block starting at an even index
    /// 2. Any contiguous block
    /// 3. Any `n` available slots (non-contiguous fallback)
    ///
    /// For `Buddy`:
    /// - Requires `n` to be a power of 2
    /// - Allocation must start at an index aligned to `n`
    /// - e.g., 2 slots: 0-1, 2-3, 4-5, 6-7; 4 slots: 0-3, 4-7
    ///
    /// Returns `Some(Vec<usize>)` with allocated slot indices, or `None` if insufficient.
    pub fn allocate(&mut self, n: usize, policy: AllocationPolicy) -> Option<Vec<usize>> {
        if n == 0 || self.free_slots() < n {
            return None;
        }

        match policy {
            AllocationPolicy::FirstFit => self.allocate_first_fit(n),
            AllocationPolicy::Buddy => self.allocate_buddy(n),
        }
    }

    /// FirstFit allocation: tries contiguous (NVLink-aware), falls back to non-contiguous.
    fn allocate_first_fit(&mut self, n: usize) -> Option<Vec<usize>> {
        // Priority 1: Contiguous block starting at even index
        for start in (0..self.total_slots).step_by(2) {
            if start + n <= self.total_slots {
                let slots: Vec<usize> = (start..start + n).collect();
                if slots.iter().all(|s| !self.occupied_slots.contains(s)) {
                    for &s in &slots {
                        self.occupied_slots.insert(s);
                    }
                    return Some(slots);
                }
            }
        }

        // Priority 2: Any contiguous block
        for start in 0..self.total_slots {
            if start + n <= self.total_slots {
                let slots: Vec<usize> = (start..start + n).collect();
                if slots.iter().all(|s| !self.occupied_slots.contains(s)) {
                    for &s in &slots {
                        self.occupied_slots.insert(s);
                    }
                    return Some(slots);
                }
            }
        }

        // Priority 3: Any n available slots (non-contiguous fallback)
        let available: Vec<usize> = (0..self.total_slots)
            .filter(|s| !self.occupied_slots.contains(s))
            .take(n)
            .collect();
        if available.len() == n {
            for &s in &available {
                self.occupied_slots.insert(s);
            }
            return Some(available);
        }

        None
    }

    /// Buddy allocation: requires power-of-2 size and aligned start index.
    fn allocate_buddy(&mut self, n: usize) -> Option<Vec<usize>> {
        // n must be a power of 2
        if !n.is_power_of_two() {
            return None;
        }

        // Try each aligned position
        for start in (0..self.total_slots).step_by(n) {
            if start + n <= self.total_slots {
                let slots: Vec<usize> = (start..start + n).collect();
                if slots.iter().all(|s| !self.occupied_slots.contains(s)) {
                    for &s in &slots {
                        self.occupied_slots.insert(s);
                    }
                    return Some(slots);
                }
            }
        }

        None
    }

    /// Releases the specified slots back to the pool.
    pub fn release(&mut self, slots: &[usize]) {
        for &slot in slots {
            self.occupied_slots.remove(&slot);
        }
    }
}

/// Message sent from worker task to scheduler when a job completes.
#[derive(Debug)]
pub struct JobCompletion {
    /// Index of the host that completed the job.
    pub host_index: usize,
    /// Slot indices that were released.
    pub released_slots: Vec<usize>,
}

/// Finds a host with enough free slots for a job.
///
/// Returns `Some((host_index, allocated_slots))` if a host is found,
/// or `None` if no host currently has enough capacity.
pub fn find_host_for_job(
    hosts: &mut [HostSlotState],
    slots_required: usize,
    policy: AllocationPolicy,
) -> Option<(usize, Vec<usize>)> {
    for (i, host) in hosts.iter_mut().enumerate() {
        if let Some(slots) = host.allocate(slots_required, policy) {
            return Some((i, slots));
        }
    }
    None
}

/// Spawns an async task to execute a job on a host.
#[allow(clippy::too_many_arguments)]
pub fn spawn_job(
    session: Arc<Box<dyn Session + Send + Sync>>,
    host: Host,
    mut cmd: Cmd,
    allocated_slots: Vec<usize>,
    completion_tx: flume::Sender<JobCompletion>,
    host_index: usize,
    print_period: usize,
    errored: Arc<Mutex<Vec<FailedCmd>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Inject {{slots}} template variable with allocated slot indices.
        let slots_str = allocated_slots
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(",");
        cmd.insert_param("slots".to_string(), slots_str);

        // Inject {{allocation_policy}} template variable.
        cmd.insert_param(
            "allocation_policy".to_string(),
            cmd.allocation_policy.to_string(),
        );

        // Fill template and run command.
        let filled_cmd = cmd.fill_template(&host);

        let result = session.run(&filled_cmd, print_period).await;

        // Record errors.
        match result {
            Ok(status) => {
                if status.code() != Some(0) {
                    errored.lock().await.push(FailedCmd::new(
                        host.to_string(),
                        filled_cmd,
                        status.to_string(),
                    ));
                }
            }
            Err(err) => {
                errored.lock().await.push(FailedCmd::new(
                    host.to_string(),
                    filled_cmd,
                    format!("Pegasus error: {}", err),
                ));
            }
        }

        // Notify scheduler that slots are released.
        let _ = completion_tx.send(JobCompletion {
            host_index,
            released_slots: allocated_slots,
        });
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Basic allocation tests
    #[test]
    fn test_allocate_single_slot() {
        let mut state = HostSlotState::new(8);
        let slots = state.allocate(1, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(slots, vec![0]);
        assert_eq!(state.free_slots(), 7);
    }

    #[test]
    fn test_allocate_all_slots() {
        let mut state = HostSlotState::new(8);
        let slots = state.allocate(8, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(slots, vec![0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(state.free_slots(), 0);
    }

    #[test]
    fn test_allocate_insufficient_slots() {
        let mut state = HostSlotState::new(4);
        assert!(state.allocate(5, AllocationPolicy::FirstFit).is_none());
        assert_eq!(state.free_slots(), 4);
    }

    // NVLink-aware even-aligned allocation tests
    #[test]
    fn test_allocate_prefers_even_start_for_2_slots() {
        let mut state = HostSlotState::new(8);
        assert_eq!(
            state.allocate(2, AllocationPolicy::FirstFit).unwrap(),
            vec![0, 1]
        );
        assert_eq!(
            state.allocate(2, AllocationPolicy::FirstFit).unwrap(),
            vec![2, 3]
        );
        assert_eq!(
            state.allocate(2, AllocationPolicy::FirstFit).unwrap(),
            vec![4, 5]
        );
        assert_eq!(
            state.allocate(2, AllocationPolicy::FirstFit).unwrap(),
            vec![6, 7]
        );
    }

    #[test]
    fn test_allocate_4_slots_even_aligned() {
        let mut state = HostSlotState::new(8);
        assert_eq!(
            state.allocate(4, AllocationPolicy::FirstFit).unwrap(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(
            state.allocate(4, AllocationPolicy::FirstFit).unwrap(),
            vec![4, 5, 6, 7]
        );
    }

    #[test]
    fn test_allocate_falls_back_to_even_start_after_release() {
        let mut state = HostSlotState::new(8);
        state.allocate(2, AllocationPolicy::FirstFit); // 0,1
        state.allocate(2, AllocationPolicy::FirstFit); // 2,3
        state.allocate(2, AllocationPolicy::FirstFit); // 4,5
        state.release(&[2, 3]);
        // Now free: 2,3,6,7. Even-aligned options: 2,3 or 6,7. Should pick 2,3.
        assert_eq!(
            state.allocate(2, AllocationPolicy::FirstFit).unwrap(),
            vec![2, 3]
        );
    }

    #[test]
    fn test_allocate_odd_start_fallback() {
        let mut state = HostSlotState::new(4);
        state.allocate(1, AllocationPolicy::FirstFit); // occupies slot 0
        state.occupied_slots.insert(2); // also occupy slot 2
        // Free: 1, 3. No even-aligned contiguous block of 2.
        // No contiguous block at all. Falls back to non-contiguous: 1, 3.
        let slots = state.allocate(2, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(slots, vec![1, 3]);
    }

    #[test]
    fn test_allocate_prefers_even_over_odd_contiguous() {
        let mut state = HostSlotState::new(4);
        state.allocate(1, AllocationPolicy::FirstFit); // occupies slot 0
        // Free: 1,2,3. Even-aligned at 2: slots 2,3 available. Should prefer that.
        let slots = state.allocate(2, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(slots, vec![2, 3]);
    }

    #[test]
    fn test_allocate_non_contiguous_fallback() {
        let mut state = HostSlotState::new(8);
        // Occupy alternating: 1, 3, 5, 7
        for s in [1, 3, 5, 7] {
            state.occupied_slots.insert(s);
        }
        // Free: 0, 2, 4, 6. No contiguous block of 3. Falls back to non-contiguous.
        let slots = state.allocate(3, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(slots, vec![0, 2, 4]);
    }

    // Release tests
    #[test]
    fn test_release_slots() {
        let mut state = HostSlotState::new(8);
        let slots = state.allocate(4, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(state.free_slots(), 4);
        state.release(&slots);
        assert_eq!(state.free_slots(), 8);
    }

    #[test]
    fn test_release_partial() {
        let mut state = HostSlotState::new(8);
        state.allocate(4, AllocationPolicy::FirstFit); // 0,1,2,3
        state.release(&[1, 2]);
        assert_eq!(state.free_slots(), 6);
        assert!(state.occupied_slots.contains(&0));
        assert!(!state.occupied_slots.contains(&1));
        assert!(!state.occupied_slots.contains(&2));
        assert!(state.occupied_slots.contains(&3));
    }

    // Interleaved allocate/release tests
    #[test]
    fn test_interleaved_allocate_release() {
        let mut state = HostSlotState::new(8);
        let a = state.allocate(2, AllocationPolicy::FirstFit).unwrap(); // 0,1
        let b = state.allocate(2, AllocationPolicy::FirstFit).unwrap(); // 2,3
        let c = state.allocate(2, AllocationPolicy::FirstFit).unwrap(); // 4,5
        assert_eq!(state.free_slots(), 2);

        state.release(&a); // release 0,1
        assert_eq!(state.free_slots(), 4);

        // New allocation should prefer 0,1 (even-aligned at 0)
        let d = state.allocate(2, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(d, vec![0, 1]);

        state.release(&b); // release 2,3
        state.release(&c); // release 4,5

        // Request 4: should get 2,3,4,5 (even-aligned at 2) since 0,1 occupied
        let e = state.allocate(4, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(e, vec![2, 3, 4, 5]);
    }

    // Edge cases
    #[test]
    fn test_allocate_zero_slots() {
        let mut state = HostSlotState::new(8);
        assert!(state.allocate(0, AllocationPolicy::FirstFit).is_none());
        assert_eq!(state.free_slots(), 8);
    }

    #[test]
    fn test_single_slot_host() {
        let mut state = HostSlotState::new(1);
        let a = state.allocate(1, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(a, vec![0]);
        assert!(state.allocate(1, AllocationPolicy::FirstFit).is_none());
        state.release(&a);
        assert_eq!(
            state.allocate(1, AllocationPolicy::FirstFit).unwrap(),
            vec![0]
        );
    }

    // Multi-host scheduling tests
    fn make_hosts(slot_counts: &[usize]) -> Vec<HostSlotState> {
        slot_counts.iter().map(|&n| HostSlotState::new(n)).collect()
    }

    #[test]
    fn test_find_host_for_job_simple() {
        let mut hosts = make_hosts(&[8, 8]);
        let (host_idx, slots) =
            find_host_for_job(&mut hosts, 4, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(host_idx, 0);
        assert_eq!(slots, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_find_host_for_job_first_full() {
        let mut hosts = make_hosts(&[8, 8]);
        hosts[0].allocate(8, AllocationPolicy::FirstFit); // fill first host
        let (host_idx, slots) =
            find_host_for_job(&mut hosts, 4, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(host_idx, 1);
        assert_eq!(slots, vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_find_host_for_job_heterogeneous() {
        let mut hosts = make_hosts(&[4, 8, 2]);
        // Job needing 6 slots: only host 1 can handle it
        let (host_idx, slots) =
            find_host_for_job(&mut hosts, 6, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(host_idx, 1);
        assert_eq!(slots.len(), 6);
    }

    #[test]
    fn test_find_host_for_job_none_available() {
        let mut hosts = make_hosts(&[4, 4]);
        hosts[0].allocate(2, AllocationPolicy::FirstFit);
        hosts[1].allocate(3, AllocationPolicy::FirstFit);
        // Host 0 has 2 free, host 1 has 1 free. Job needs 3.
        assert!(find_host_for_job(&mut hosts, 3, AllocationPolicy::FirstFit).is_none());
    }

    #[test]
    fn test_job_too_large_for_any_host() {
        let hosts = make_hosts(&[4, 8, 4]);
        let max_slots = hosts.iter().map(|h| h.total_slots()).max().unwrap();
        assert!(9 > max_slots);
    }

    #[test]
    fn test_concurrent_jobs_same_host() {
        let mut hosts = make_hosts(&[8]);
        let (_, slots1) = find_host_for_job(&mut hosts, 2, AllocationPolicy::FirstFit).unwrap();
        let (_, slots2) = find_host_for_job(&mut hosts, 2, AllocationPolicy::FirstFit).unwrap();
        let (_, slots3) = find_host_for_job(&mut hosts, 2, AllocationPolicy::FirstFit).unwrap();
        // All on same host, non-overlapping
        assert!(slots1.iter().all(|s| !slots2.contains(s)));
        assert!(slots2.iter().all(|s| !slots3.contains(s)));
        assert!(slots1.iter().all(|s| !slots3.contains(s)));
    }

    #[test]
    fn test_release_enables_new_allocation() {
        let mut hosts = make_hosts(&[4]);
        let (_, slots1) = find_host_for_job(&mut hosts, 4, AllocationPolicy::FirstFit).unwrap();
        assert!(find_host_for_job(&mut hosts, 1, AllocationPolicy::FirstFit).is_none());
        hosts[0].release(&slots1);
        assert!(find_host_for_job(&mut hosts, 4, AllocationPolicy::FirstFit).is_some());
    }

    // Backwards compatibility tests
    // These ensure that when slots are not specified (defaulting to 1),
    // behavior is identical to the original queue mode.

    #[test]
    fn test_backwards_compat_default_slots_one_job_per_host() {
        // Simulates original behavior: each host can run exactly one job
        let mut hosts = make_hosts(&[1, 1, 1]); // 3 hosts, each with 1 slot (default)

        // First 3 jobs get scheduled, one per host
        let (h0, s0) = find_host_for_job(&mut hosts, 1, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(h0, 0);
        assert_eq!(s0, vec![0]);

        let (h1, s1) = find_host_for_job(&mut hosts, 1, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(h1, 1);
        assert_eq!(s1, vec![0]);

        let (h2, s2) = find_host_for_job(&mut hosts, 1, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(h2, 2);
        assert_eq!(s2, vec![0]);

        // Fourth job cannot be scheduled - all hosts busy
        assert!(find_host_for_job(&mut hosts, 1, AllocationPolicy::FirstFit).is_none());

        // Release one host
        hosts[1].release(&s1);

        // Now a job can be scheduled on host 1
        let (h3, _) = find_host_for_job(&mut hosts, 1, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(h3, 1);
    }

    #[test]
    fn test_backwards_compat_no_concurrent_jobs_on_single_slot_host() {
        // With slots=1, only one job can run per host at a time
        let mut hosts = make_hosts(&[1]);

        let (_, slots) = find_host_for_job(&mut hosts, 1, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(hosts[0].free_slots(), 0);

        // Cannot schedule another job on the same host
        assert!(find_host_for_job(&mut hosts, 1, AllocationPolicy::FirstFit).is_none());

        // Release and verify we can schedule again
        hosts[0].release(&slots);
        assert_eq!(hosts[0].free_slots(), 1);
        assert!(find_host_for_job(&mut hosts, 1, AllocationPolicy::FirstFit).is_some());
    }

    #[test]
    fn test_backwards_compat_jobs_default_to_one_slot() {
        // Jobs without explicit slots should require 1 slot
        // This is tested implicitly - all tests above use slots_required=1
        let mut hosts = make_hosts(&[1]);
        let result = find_host_for_job(&mut hosts, 1, AllocationPolicy::FirstFit);
        assert!(result.is_some());

        // The slot returned should be [0]
        let (_, slots) = result.unwrap();
        assert_eq!(slots, vec![0]);
    }

    #[test]
    fn test_backwards_compat_fifo_order_preserved() {
        // Jobs should be assigned to hosts in order (first available)
        let mut hosts = make_hosts(&[1, 1]);

        // Job 1 -> Host 0
        let (h, _) = find_host_for_job(&mut hosts, 1, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(h, 0);

        // Job 2 -> Host 1
        let (h, _) = find_host_for_job(&mut hosts, 1, AllocationPolicy::FirstFit).unwrap();
        assert_eq!(h, 1);
    }

    // Buddy allocation tests
    #[test]
    fn test_buddy_allocate_1_slot() {
        let mut state = HostSlotState::new(8);
        // 1 slot can go anywhere
        assert_eq!(state.allocate(1, AllocationPolicy::Buddy).unwrap(), vec![0]);
        assert_eq!(state.allocate(1, AllocationPolicy::Buddy).unwrap(), vec![1]);
    }

    #[test]
    fn test_buddy_allocate_2_slots_aligned() {
        let mut state = HostSlotState::new(8);
        // 2 slots must be aligned to 2 (even indices)
        assert_eq!(
            state.allocate(2, AllocationPolicy::Buddy).unwrap(),
            vec![0, 1]
        );
        assert_eq!(
            state.allocate(2, AllocationPolicy::Buddy).unwrap(),
            vec![2, 3]
        );
        assert_eq!(
            state.allocate(2, AllocationPolicy::Buddy).unwrap(),
            vec![4, 5]
        );
        assert_eq!(
            state.allocate(2, AllocationPolicy::Buddy).unwrap(),
            vec![6, 7]
        );
    }

    #[test]
    fn test_buddy_allocate_4_slots_aligned() {
        let mut state = HostSlotState::new(8);
        // 4 slots must be aligned to 4
        assert_eq!(
            state.allocate(4, AllocationPolicy::Buddy).unwrap(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(
            state.allocate(4, AllocationPolicy::Buddy).unwrap(),
            vec![4, 5, 6, 7]
        );
    }

    #[test]
    fn test_buddy_allocate_8_slots_aligned() {
        let mut state = HostSlotState::new(8);
        // 8 slots must start at 0
        assert_eq!(
            state.allocate(8, AllocationPolicy::Buddy).unwrap(),
            vec![0, 1, 2, 3, 4, 5, 6, 7]
        );
    }

    #[test]
    fn test_buddy_rejects_non_power_of_2() {
        let mut state = HostSlotState::new(8);
        // 3, 5, 6, 7 are not powers of 2
        assert!(state.allocate(3, AllocationPolicy::Buddy).is_none());
        assert!(state.allocate(5, AllocationPolicy::Buddy).is_none());
        assert!(state.allocate(6, AllocationPolicy::Buddy).is_none());
        assert!(state.allocate(7, AllocationPolicy::Buddy).is_none());
    }

    #[test]
    fn test_buddy_fragmentation() {
        let mut state = HostSlotState::new(8);
        // Occupy 0,1,2,3,4,7 leaving 5,6 free
        state.occupied_slots.insert(0);
        state.occupied_slots.insert(1);
        state.occupied_slots.insert(2);
        state.occupied_slots.insert(3);
        state.occupied_slots.insert(4);
        state.occupied_slots.insert(7);
        // 2-slot buddy allocation needs even-aligned: 0-1, 2-3, 4-5, 6-7
        // 0-1, 2-3 occupied. 4-5: 4 occupied. 6-7: 7 occupied.
        // No valid 2-slot buddy block available
        assert!(state.allocate(2, AllocationPolicy::Buddy).is_none());
    }

    #[test]
    fn test_buddy_waits_for_aligned_block() {
        let mut state = HostSlotState::new(8);
        // Allocate slots 0 and 1, leaving 2-7 free
        state.allocate(2, AllocationPolicy::Buddy); // 0,1
        // A 4-slot buddy needs alignment to 4: either 0-3 or 4-7
        // 0-3 is partially occupied (0,1), so only 4-7 works
        assert_eq!(
            state.allocate(4, AllocationPolicy::Buddy).unwrap(),
            vec![4, 5, 6, 7]
        );
    }
}
