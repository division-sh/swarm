#!/usr/bin/env bash
set -euo pipefail
out=test-results/diagnostic-evidence
mkdir -p "$out"
go env -json GOVERSION GOOS GOARCH CGO_ENABLED GOWORK GOFLAGS > "$out/go-environment.json"
jq -n --arg name "${RUNNER_NAME:-unknown}" --arg os "$RUNNER_OS" --arg arch "$RUNNER_ARCH" \
  --arg image "${ImageOS:-unknown}" --arg version "${ImageVersion:-unknown}" \
  --arg slots "${SWARM_TEST_RUN_SLOTS:-unset}" --argjson cpus "$(nproc)" \
  '{name:$name,os:$os,arch:$arch,image:$image,image_version:$version,cpus:$cpus,
    slot_setting:$slots,runner_class:"ubuntu-latest",gateway:"empty"}' > "$out/runner-environment.json"
docker version --format '{{.Server.Version}}' > "$out/docker-version.txt"
