This directory is a non-runtime placeholder for Docker Compose project parsing.

The orchestrator service must be started with `SWARM_CONTRACTS_HOST_DIR` set to a real
contract bundle. The service entrypoint fails closed before `swarm serve` if the
variable is not set.
