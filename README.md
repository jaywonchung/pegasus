# Pegasus

A simple multi-node command runner.

## TODO

- Host and job parametrization
  - A single host split into multiple hosts based on some parameter.
  - For the use case right now, add 'container'.

```yaml
# hosts.yaml
- clgpu012
  - container
    - zeus0
    - zeus1
- clgpu013
  - container
    - zeus0
    - zeus1
```

```yaml
# jobs.yaml
- docker exec '{{ container }} python train.py | tee log-run{{ run }}'
  - run
    - 1
    - 2
    - 3
```


```rust
//! Users supply Pegasus by populating `hosts.yaml`. Basic use:
//!
//! ```yaml
//! # hosts.yaml
//! - node-1
//! - node-2
//! - node-3
//! - node-4
//! ```
//!
//! This will create four hosts, which means that four commands will run in parallel, one in each
//! host.
//!
//! You may also parametrize hosts:
//!
//! ```yaml
//! # hosts.yaml
//! - hostname:
//!     - node-1
//!     - node-2
//!   container:
//!     - zeus0
//!     - zeus1
//! ```
//!
//! This specifies `2 * 2 = 4` hosts: (node-1, zeus0), (node-1, zeus1), (node-2, zeus0), (node-2, zeus1)
//! Values under the required `hostname` key will serve as the SSH hostname, and all other parameters,
//! along with command parameters, fill in command templates.
//!
//! ```yaml
//! # queue.yaml
//! - command:
//!     - docker exec {{ container }} python train.py -bs {{ bs }} | tee /nfs/logs/train-bs{{ bs }}-run{{ run }}.log
//!   run:
//!     - 1
//!     - 2
//!     - 3
//!   bs:
//!     - 32
//!     - 64
//! ```
//!
//! This will run `3 * 2 = 6` jobs on 4 hosts.
```
