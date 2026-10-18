#!/usr/bin/env bash
set -euo pipefail

# Preparation only until the orchestrator grants server2 after G releases it.
root=$HOME/dev/wt/agent-d
tree=$root/scratch
evidence=$root/evidence-2735-memo-20261018
candidate=55adb583b9de0572f69e6e4adb23776ac0851ed1
expected_image=sha256:0c09f37b09828ac8460ef68fccb8aea5891a9cbcdc1c73c91f6b1e13da8073b1
test -z "$(git -C "$tree" status --porcelain)"
test "$(docker image inspect swarm-workspace:latest --format '{{.Id}}')" = "$expected_image"
old=$(git -C "$tree" rev-parse HEAD)
exec > >(tee "$evidence/preparation-and-invocation.log") 2>&1
printf 'INHERITED_GOMAXPROCS=%s\n' "${GOMAXPROCS-<unset>}"
cleanup() {
  git -C "$tree" switch --detach "$old"
  printf 'RESTORED_SCRATCH=%s\n' "$(git -C "$tree" rev-parse HEAD)"
  printf 'UNMODIFIED_IMAGE=%s\n' "$(docker image inspect swarm-workspace:latest --format '{{.Id}}')"
  date -u +CLEANUP_UTC=%Y-%m-%dT%H:%M:%S.%NZ
}
trap cleanup EXIT
git -C "$tree" switch --detach "$candidate"
export GOTOOLCHAIN=go1.26.8 GOMAXPROCS=2 SWARM_TEST_WORKSPACE_MCP_DOCKER=1
export SWARM_TEST_BUILD_CACHE=$evidence/product-cache
unset GOFLAGS SWARM_TEST_PROOF_PROFILE TEST_POSTGRES_DSN DATABASE_URL
go version
go env GOVERSION GOOS GOARCH CGO_ENABLED GOCACHE GOMODCACHE
docker image inspect "$expected_image" > "$evidence/image-inspect.json"
sha256sum "$tree/Dockerfile.workspace" "$evidence/candidate-overlay.json" "$evidence/run-candidate.sh"
mkdir -p "$evidence/candidate/trace" "$evidence/candidate/tmp"
cd "$tree"
export GOFLAGS="-overlay=$evidence/candidate-overlay.json"
{
  go build -o "$evidence/candidate/swarm" ./cmd/swarm
  go test -c -o "$evidence/candidate/workspace-inspection.test" ./internal/store/construction
  go test -c -o "$evidence/candidate/releasee2e.test" ./internal/releasee2e
  sha256sum "$evidence/candidate/swarm" "$evidence/candidate/workspace-inspection.test" "$evidence/candidate/releasee2e.test"
  go version -m "$evidence/candidate/swarm"
} > "$evidence/candidate/build-preparation.log" 2>&1
test "$(git rev-parse HEAD)" = "$candidate"
test -z "$(git status --porcelain)"
test "$(docker image inspect swarm-workspace:latest --format '{{.Id}}')" = "$expected_image"
(cd "$evidence"; sha256sum -c overlay-content.sha256) > "$evidence/candidate/overlay-check.log"
export TMPDIR=$evidence/candidate/tmp RELEASE_E2E_DIAG_DIR=$evidence/candidate/trace
{
  hostname
  date -u +ARM_START_UTC=%Y-%m-%dT%H:%M:%S.%NZ
  cat /proc/loadavg
  taskset -pc $$
  git rev-parse HEAD HEAD^{tree}
  go version
  printf 'ENV_HASH_EXCLUDING_ARM_PATHS='
  env | grep -Ev '^(PWD|OLDPWD|TMPDIR|GOFLAGS|RELEASE_E2E_DIAG_DIR)=' | sort | sha256sum
  printf 'GOMAXPROCS=%s\nGOTOOLCHAIN=%s\nGOFLAGS=%s\nTMPDIR=%s\nIMAGE=%s\n' "$GOMAXPROCS" "$GOTOOLCHAIN" "$GOFLAGS" "$TMPDIR" "$expected_image"
  sha256sum "$evidence/candidate/swarm" "$evidence/candidate/workspace-inspection.test" "$evidence/candidate/releasee2e.test"
  printf '%s\n' "COMMAND: taskset -c 0,1 go test ./internal/releasee2e -run '^TestWorkspaceMCPCompiledDockerConformance$/^disconnect=false$' -count=1 -timeout=4m -v"
} > "$evidence/candidate/identity-and-environment.txt"
set +e
taskset -c 0,1 go test ./internal/releasee2e -run '^TestWorkspaceMCPCompiledDockerConformance$/^disconnect=false$' -count=1 -timeout=4m -v > "$evidence/candidate/test.log" 2>&1 &
pid=$!
taskset -pc "$pid" >> "$evidence/candidate/identity-and-environment.txt"
wait "$pid"
code=$?
set -e
printf 'EXIT=%s\n' "$code" > "$evidence/candidate/result.txt"
date -u +ARM_END_UTC=%Y-%m-%dT%H:%M:%S.%NZ >> "$evidence/candidate/result.txt"
cat /proc/loadavg >> "$evidence/candidate/result.txt"
cat "$evidence/candidate/result.txt"
grep -E '^(--- FAIL|FAIL|ok|panic)|DATA RACE|build failed' "$evidence/candidate/test.log" | head -40 || true
find "$evidence/candidate/trace" -maxdepth 1 -type f -print0 | sort -z | xargs -0 -r sha256sum > "$evidence/candidate/retained-files.sha256"
printf 'AUTHORIZED_DIAGNOSTIC_COMPLETE=1/1; no replacement or base resample\n'
