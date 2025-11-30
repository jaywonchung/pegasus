# Pegasus Examples

This directory contains example host and queue YAML files demonstrating various Pegasus features.

## Directory Structure

### Broadcast Mode Examples
- `broadcast-hosts.yaml` + `broadcast-queue.yaml` - Basic broadcast (run same command on all hosts)
- `terraform-hosts.yaml` + `terraform-queue.yaml` - Node setup/terraforming example

### Queue Mode Examples
- `queue-hosts-simple.yaml` + `queue-simple.yaml` - Basic job queue (1 slot per host)
- `queue-hosts-gpu.yaml` + `queue-gpu.yaml` - GPU cluster with slots for concurrent jobs
- `queue-hosts-heterogeneous.yaml` + `queue-heterogeneous.yaml` - Mixed cluster (different slot counts)

### Parametrization Examples
- `param-hosts.yaml` + `param-queue.yaml` - Host parametrization (same host, different params)
- `param-queue-sweep.yaml` - Job parametrization (hyperparameter sweep)
- `param-combined.yaml` - Combined host + job parametrization

### Resource Slots + Parametrization
- `slots-param-hosts.yaml` + `slots-param-queue.yaml` - Multi-GPU jobs with parametrization

## Usage

```bash
# Broadcast mode
pegasus b --hosts-file examples/broadcast-hosts.yaml --queue-file examples/broadcast-queue.yaml

# Queue mode
pegasus q --hosts-file examples/queue-hosts-gpu.yaml --queue-file examples/queue-gpu.yaml

# Daemon mode (keeps running, watches for new jobs)
pegasus q --hosts-file examples/queue-hosts-gpu.yaml --queue-file examples/queue-gpu.yaml --daemon
```
