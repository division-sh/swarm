#!/usr/bin/env bash
set -euo pipefail

root=$HOME/dev/wt/agent-d
evidence=$root/evidence-2735-corrected-20261018
base=d092f96b466a9b7564587e6a6caf0478580dfb59
candidate=80b43c1a238ad360363c6217b2a269094a333258
expected_image=sha256:0c09f37b09828ac8460ef68fccb8aea5891a9cbcdc1c73c91f6b1e13da8073b1
exec > >(tee "$evidence/preparation-and-invocations.log") 2>&1
main_old=$(git -C "$root/main" rev-parse HEAD)
scratch_old=$(git -C "$root/scratch" rev-parse HEAD)
test -z "$(git -C "$root/main" status --porcelain)"
test -z "$(git -C "$root/scratch" status --porcelain)"
test "$(docker image inspect swarm-workspace:latest --format '{{.Id}}')" = "$expected_image"
cleanup() {
  git -C "$root/main" switch --detach "$main_old"
  git -C "$root/scratch" switch --detach "$scratch_old"
  printf 'RESTORED_MAIN=%s\nRESTORED_SCRATCH=%s\n' "$(git -C "$root/main" rev-parse HEAD)" "$(git -C "$root/scratch" rev-parse HEAD)"
  printf 'UNMODIFIED_IMAGE=%s\n' "$(docker image inspect swarm-workspace:latest --format '{{.Id}}')"
  date -u +CLEANUP_UTC=%Y-%m-%dT%H:%M:%S.%NZ
}
trap cleanup EXIT
git -C "$root/main" switch --detach "$base"
git -C "$root/scratch" switch --detach "$candidate"
export GOTOOLCHAIN=go1.26.8 GOMAXPROCS=2 SWARM_TEST_WORKSPACE_MCP_DOCKER=1
export SWARM_TEST_BUILD_CACHE=$evidence/product-cache
unset GOFLAGS SWARM_TEST_PROOF_PROFILE TEST_POSTGRES_DSN DATABASE_URL
go version
go env GOVERSION GOOS GOARCH CGO_ENABLED GOCACHE GOMODCACHE
docker image inspect "$expected_image" > "$evidence/image-inspect.json"
cmp "$root/main/Dockerfile.workspace" "$root/scratch/Dockerfile.workspace"
sha256sum "$root/main/Dockerfile.workspace" "$root/scratch/Dockerfile.workspace"
sha256sum "$evidence/base-overlay.json" "$evidence/candidate-overlay.json" "$evidence/run-pair.sh"

prepare() {
  local arm=$1 tree=$2
  mkdir -p "$evidence/$arm/trace" "$evidence/$arm/tmp"
  (
    cd "$tree"
    export GOFLAGS="-overlay=$evidence/$arm-overlay.json"
    go build -o "$evidence/$arm/swarm" ./cmd/swarm
    go test -c -o "$evidence/$arm/workspace-inspection.test" ./internal/store/construction
    go test -c -o "$evidence/$arm/releasee2e.test" ./internal/releasee2e
    sha256sum "$evidence/$arm/swarm" "$evidence/$arm/workspace-inspection.test" "$evidence/$arm/releasee2e.test"
    go version -m "$evidence/$arm/swarm"
  ) > "$evidence/$arm/build-preparation.log" 2>&1
}
prepare base "$root/main"
prepare candidate "$root/scratch"

invoke() {
  local arm=$1 tree=$2 expected=$3 code pid
  (
    cd "$tree"
    test "$(git rev-parse HEAD)" = "$expected"
    test -z "$(git status --porcelain)"
    test "$(docker image inspect swarm-workspace:latest --format '{{.Id}}')" = "$expected_image"
    (cd "$evidence"; sha256sum -c overlay-content.sha256) > "$evidence/$arm/overlay-check.log"
    export GOFLAGS="-overlay=$evidence/$arm-overlay.json"
    export TMPDIR=$evidence/$arm/tmp
    export RELEASE_E2E_DIAG_DIR=$evidence/$arm/trace
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
      sha256sum "$evidence/$arm/swarm" "$evidence/$arm/workspace-inspection.test" "$evidence/$arm/releasee2e.test"
      printf '%s\n' "COMMAND: taskset -c 0,1 go test ./internal/releasee2e -run '^TestWorkspaceMCPCompiledDockerConformance$/^disconnect=false$' -count=1 -timeout=4m -v"
    } > "$evidence/$arm/identity-and-environment.txt"
    set +e
    taskset -c 0,1 go test ./internal/releasee2e -run '^TestWorkspaceMCPCompiledDockerConformance$/^disconnect=false$' -count=1 -timeout=4m -v > "$evidence/$arm/test.log" 2>&1 &
    pid=$!
    taskset -pc "$pid" >> "$evidence/$arm/identity-and-environment.txt"
    wait "$pid"
    code=$?
    set -e
    printf 'EXIT=%s\n' "$code" > "$evidence/$arm/result.txt"
    date -u +ARM_END_UTC=%Y-%m-%dT%H:%M:%S.%NZ >> "$evidence/$arm/result.txt"
    cat /proc/loadavg >> "$evidence/$arm/result.txt"
    cat "$evidence/$arm/result.txt"
    grep -E '^(--- FAIL|FAIL|ok|panic)|DATA RACE|build failed' "$evidence/$arm/test.log" | head -40 || true
    find "$evidence/$arm/trace" -maxdepth 1 -type f -print0 | sort -z | xargs -0 -r sha256sum > "$evidence/$arm/retained-files.sha256"
  )
}
invoke base "$root/main" "$base"
invoke candidate "$root/scratch" "$candidate"
printf 'AUTHORIZED_DIAGNOSTIC_COMPLETE=2/2; no replacements\n'
