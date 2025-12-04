# Pegasus Examples

Each directory contains a `hosts.yaml` and `queue.yaml` pair demonstrating a specific feature.

## Broadcast Mode

- **`terraform/`** - Mass node setup/provisioning before running jobs

## Queue Mode

- **`queue-simple/`** - Basic job queue with 1 slot per host
- **`queue-gpu/`** - GPU cluster with multiple slots for concurrent jobs
- **`queue-heterogeneous/`** - Mixed cluster with different slot counts per host

## Parametrization

- **`host-parametrization/`** - Expand one physical host into multiple logical hosts with different parameters
- **`job-parametrization/`** - Expand jobs via Cartesian product (hyperparameter sweeps)
- **`combined-parametrization/`** - Both host and job parametrization together

## Slots + Parametrization

- **`multi-slot-jobs/`** - Jobs requiring multiple slots with host parameters

## Usage

```bash
# Broadcast mode
pegasus b --hosts-file examples/terraform/hosts.yaml --queue-file examples/terraform/queue.yaml

# Queue mode
pegasus q --hosts-file examples/queue-gpu/hosts.yaml --queue-file examples/queue-gpu/queue.yaml

# Daemon mode
pegasus q --hosts-file examples/queue-gpu/hosts.yaml --queue-file examples/queue-gpu/queue.yaml --daemon
```
