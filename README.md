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
