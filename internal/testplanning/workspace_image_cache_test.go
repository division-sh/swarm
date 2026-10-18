package testplanning

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

type workspaceCacheStep struct {
	Name, ID, Uses, If, Run, Shell string
	With, Env                      map[string]string
}

func workspaceCacheSteps(raw []byte) ([]workspaceCacheStep, error) {
	var workflow struct {
		Env  map[string]string
		Jobs map[string]struct {
			Env   map[string]string
			Steps []workspaceCacheStep
		}
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		return nil, err
	}
	job := workflow.Jobs["sqlite-local-dev"]
	for _, env := range []map[string]string{workflow.Env, job.Env} {
		for name := range env {
			if strings.HasPrefix(name, "DOCKER_") || strings.HasPrefix(name, "BUILDKIT_") {
				return nil, fmt.Errorf("unkeyed inherited workspace build setting %s", name)
			}
		}
	}
	return job.Steps, nil
}

func validateWorkspaceImageCache(raw []byte) error {
	steps, err := workspaceCacheSteps(raw)
	if err != nil {
		return err
	}
	positions := map[string]int{}
	for index, step := range steps {
		if step.Name == "" {
			continue
		}
		if _, duplicate := positions[step.Name]; duplicate {
			return fmt.Errorf("duplicate workspace job step %q", step.Name)
		}
		positions[step.Name] = index
	}
	for _, name := range []string{"Restore workspace image", "Prepare workspace image", "Save workspace image", "Prove real container MCP emission and successor refusal", "Prove no-selector SQLite local run"} {
		if _, ok := positions[name]; !ok {
			return fmt.Errorf("missing workspace cache/proof step %q", name)
		}
	}
	restore := steps[positions["Restore workspace image"]]
	prepare := steps[positions["Prepare workspace image"]]
	save := steps[positions["Save workspace image"]]
	const cachePath = "${{ runner.temp }}/swarm-workspace-image"
	const cacheKey = "workspace-image-v1-${{ runner.os }}-${{ runner.arch }}-${{ hashFiles('Dockerfile.workspace') }}"
	if restore.Uses != "actions/cache/restore@v4" || restore.ID != "workspace-image-cache" ||
		restore.If != "hashFiles('Dockerfile.workspace') != ''" || len(restore.With) != 2 ||
		restore.With["path"] != cachePath || restore.With["key"] != cacheKey {
		return fmt.Errorf("workspace image restore lost exact input/scoped key")
	}
	if save.Uses != "actions/cache/save@v4" || len(save.With) != 2 || save.With["path"] != cachePath ||
		save.With["key"] != "${{ steps.workspace-image-cache.outputs.cache-primary-key }}" ||
		save.If != "steps.workspace-image-cache.outputs.cache-hit != 'true' && success()" {
		return fmt.Errorf("workspace image publication lost successful-proof/exact-key binding")
	}
	if prepare.If != "" || prepare.Shell != "bash" || len(prepare.Env) != 3 ||
		prepare.Env["CACHE_HIT"] != "${{ steps.workspace-image-cache.outputs.cache-hit }}" ||
		prepare.Env["DOCKERFILE_HASH"] != "${{ hashFiles('Dockerfile.workspace') }}" ||
		prepare.Env["WORKSPACE_IMAGE_CACHE"] != cachePath || !strings.HasPrefix(prepare.Run, "set -euo pipefail\n") {
		return fmt.Errorf("workspace preparation lost unconditional fail-closed hit/miss owner")
	}
	if !(positions[restore.Name] < positions[prepare.Name] &&
		positions[prepare.Name] < positions["Prove real container MCP emission and successor refusal"] &&
		positions["Prove real container MCP emission and successor refusal"] < positions["Prove no-selector SQLite local run"] &&
		positions["Prove no-selector SQLite local run"] < positions[save.Name]) {
		return fmt.Errorf("workspace archive must be captured before proofs and published only afterward")
	}
	builds := 0
	for _, required := range []string{
		`manifest_error='https://public.ecr.aws/v2/docker/library/golang/manifests/sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437: 429 Too Many Requests'`,
		`for backoff in 5 10 20 40 0; do`,
		`terminal_error=$(awk '/^ERROR:/ { errors++; terminal=$0 } NF { last=$0 } END { if (errors == 1 && terminal == last) print terminal }' "$build_log")`,
		`"$error_prefix$manifest_error"|"$error_prefix$manifest_error - Server message: toomanyrequests: Data limit exceeded") retryable=true ;;`,
		`if [ "$build_status" -ne 1 ] || [ "$backoff" -eq 0 ] || [ "$retryable" != true ]; then`,
		`exit "$build_status"`,
		`sleep "$backoff"`,
		`test "$built" = true`,
	} {
		if !strings.Contains(prepare.Run, required) {
			return fmt.Errorf("workspace acquisition lost finite classified retry: %s", required)
		}
	}
	for _, line := range strings.Split(prepare.Run, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "docker build") {
			if line != `if docker build -t swarm-workspace:latest -f Dockerfile.workspace . > "$build_log" 2>&1; then` {
				return fmt.Errorf("unkeyed workspace build arguments/platform/context")
			}
			builds++
		}
	}
	if builds != 1 {
		return fmt.Errorf("workspace image requires exactly one keyed miss-build path")
	}
	return nil
}

func TestWorkspaceImageCacheHostedContract(t *testing.T) {
	raw := readWorkspaceCacheWorkflow(t)
	if err := validateWorkspaceImageCache(raw); err != nil {
		t.Fatal(err)
	}
	for name, pair := range map[string][2]string{
		"restore omitted":     {"name: Restore workspace image", "name: omitted restore"},
		"load omitted":        {"name: Prepare workspace image", "name: omitted prepare"},
		"save omitted":        {"name: Save workspace image", "name: omitted save"},
		"partial key":         {"workspace-image-v1-${{ runner.os }}-${{ runner.arch }}-", "workspace-image-v1-"},
		"per-head key":        {"workspace-image-v1-${{ runner.os }}", "workspace-image-v1-${{ github.sha }}-${{ runner.os }}"},
		"empty hash allowed":  {"if: hashFiles('Dockerfile.workspace') != ''", "if: always()"},
		"prefix fallback":     {"key: workspace-image-v1-${{ runner.os }}", "restore-keys: workspace-image-v1-\n          key: workspace-image-v1-${{ runner.os }}"},
		"wrong restore owner": {"id: workspace-image-cache", "id: other-cache"},
		"wrong archive path":  {"path: ${{ runner.temp }}/swarm-workspace-image", "path: ${{ runner.temp }}/other-image"},
		"wrong save key":      {"key: ${{ steps.workspace-image-cache.outputs.cache-primary-key }}", "key: other-image"},
		"save after failure":  {"if: steps.workspace-image-cache.outputs.cache-hit != 'true' && success()", "if: always()"},
		"conditional prepare": {"name: Prepare workspace image\n        shell: bash", "name: Prepare workspace image\n        if: false\n        shell: bash"},
		"no pipefail":         {"set -euo pipefail\n          [[ \"$DOCKERFILE_HASH\"", "set -eu\n          [[ \"$DOCKERFILE_HASH\""},
		"unkeyed build arg":   {"docker build -t swarm-workspace:latest", "docker build --build-arg INSTALL_CLAUDE_CLI=true -t swarm-workspace:latest"},
		"unkeyed platform":    {"docker build -t swarm-workspace:latest", "docker build --platform linux/arm64 -t swarm-workspace:latest"},
		"inherited platform":  {"  sqlite-local-dev:\n", "  sqlite-local-dev:\n    env:\n      DOCKER_DEFAULT_PLATFORM: linux/arm64\n"},
		"unbounded retry":     {"for backoff in 5 10 20 40 0; do", "while true; do"},
		"larger retry budget": {"for backoff in 5 10 20 40 0; do", "for backoff in 5 10 20 40 80 0; do"},
		"unclassified retry":  {`[ "$retryable" != true ]`, "false"},
		"accumulated errors":  {`errors == 1 && terminal == last`, `errors >= 1`},
		"build fallthrough":   {`test "$built" = true`, "true"},
	} {
		t.Run(name, func(t *testing.T) {
			changed := strings.Replace(string(raw), pair[0], pair[1], 1)
			if changed == string(raw) || validateWorkspaceImageCache([]byte(changed)) == nil {
				t.Fatal("cache contract mutation was not discriminating")
			}
		})
	}
	steps, err := workspaceCacheSteps(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"Save workspace image", "Prepare workspace image"}, {"Save workspace image", "Prove real container MCP emission and successor refusal"}} {
		t.Run("reordered "+pair[1], func(t *testing.T) {
			var first, second int
			for i, step := range steps {
				if step.Name == pair[0] {
					first = i
				}
				if step.Name == pair[1] {
					second = i
				}
			}
			changed := append([]workspaceCacheStep(nil), steps...)
			changed[first], changed[second] = changed[second], changed[first]
			mutated, err := yaml.Marshal(map[string]any{"jobs": map[string]any{"sqlite-local-dev": map[string]any{"steps": changed}}})
			if err != nil {
				t.Fatal(err)
			}
			if validateWorkspaceImageCache(mutated) == nil {
				t.Fatal("premature cache publication accepted")
			}
		})
	}
}

func readWorkspaceCacheWorkflow(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("../..", ".github/workflows/ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestWorkspaceImageCacheExecutesActualHitMissAndRefusals(t *testing.T) {
	steps, err := workspaceCacheSteps(readWorkspaceCacheWorkflow(t))
	if err != nil {
		t.Fatal(err)
	}
	var script string
	for _, step := range steps {
		if step.Name == "Prepare workspace image" {
			script = step.Run
		}
	}
	if script == "" {
		t.Fatal("missing actual preparation script")
	}
	id := "sha256:" + strings.Repeat("a", 64)
	for _, test := range []struct {
		name, hit, archive, savedID, actualID, failure string
		wantError                                      bool
		wantBuilds                                     int
		wantSleeps                                     string
		wantStatus                                     int
	}{
		{name: "exact hit", hit: "true", archive: "workspace-image", savedID: id},
		{name: "miss"},
		{name: "partial restore", hit: "false", archive: "obsolete", savedID: "obsolete"},
		{name: "nonliteral hit", hit: "TRUE", archive: "obsolete", savedID: "obsolete"},
		{name: "missing archive", hit: "true", savedID: id, wantError: true},
		{name: "corrupt archive", hit: "true", archive: "corrupt", savedID: id, wantError: true},
		{name: "missing ID", hit: "true", archive: "workspace-image", wantError: true},
		{name: "empty ID", hit: "true", archive: "workspace-image", savedID: "\n", wantError: true},
		{name: "malformed ID", hit: "true", archive: "workspace-image", savedID: "sha256:bad", wantError: true},
		{name: "multiline ID", hit: "true", archive: "workspace-image", savedID: id + "\n" + id, wantError: true},
		{name: "wrong tagged ID", hit: "true", archive: "workspace-image", savedID: "sha256:" + strings.Repeat("b", 64), wantError: true},
		{name: "decompress failure", hit: "true", archive: "workspace-image", savedID: id, failure: "decompress", wantError: true},
		{name: "load failure", hit: "true", archive: "workspace-image", savedID: id, failure: "load", wantError: true},
		{name: "build failure", failure: "build", wantError: true},
		{name: "registry 429 then success", failure: "registry_429_transient", wantBuilds: 3, wantSleeps: "5\n10\n"},
		{name: "success on fifth attempt", failure: "registry_429_last", wantBuilds: 5, wantSleeps: "5\n10\n20\n40\n"},
		{name: "permanent registry 429", failure: "registry_429", wantError: true, wantBuilds: 5, wantSleeps: "5\n10\n20\n40\n"},
		{name: "data allowance exhausted", failure: "registry_data_limit", wantError: true, wantBuilds: 5, wantSleeps: "5\n10\n20\n40\n"},
		{name: "registry forbidden", failure: "registry_403", wantError: true, wantBuilds: 1},
		{name: "apt 429 is not registry throttling", failure: "apt_429", wantError: true, wantBuilds: 1},
		{name: "mixed endpoint and status", failure: "mixed_status", wantError: true, wantBuilds: 1},
		{name: "ambiguous terminal errors", failure: "mixed_errors", wantError: true, wantBuilds: 1},
		{name: "later unrelated failure", failure: "registry_429_then_403", wantError: true, wantBuilds: 2, wantSleeps: "5\n"},
		{name: "docker status retained", failure: "registry_status_7", wantError: true, wantBuilds: 1, wantStatus: 7},
		{name: "earlier 429 is not terminal", failure: "terminal_apt", wantError: true, wantBuilds: 1},
		{name: "save failure", failure: "save", wantError: true},
		{name: "compress failure", failure: "compress", wantError: true},
		{name: "invalid built ID", actualID: "not-an-image", wantError: true},
		{name: "inspect failure", failure: "inspect", wantError: true},
		{name: "empty Dockerfile hash", failure: "hash", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			cache := filepath.Join(root, "image-cache")
			if err := os.Mkdir(cache, 0o700); err != nil {
				t.Fatal(err)
			}
			write := func(name, data string, mode os.FileMode) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(root, name), []byte(data), mode); err != nil {
					t.Fatal(err)
				}
			}
			write("image-cache/stale", "must be discarded on miss", 0o600)
			if test.archive != "" {
				write("image-cache/image.tar.zst", test.archive, 0o600)
			}
			if test.savedID != "" {
				write("image-cache/image.id", test.savedID, 0o600)
			}
			write("docker", workspaceCacheDockerStub, 0o700)
			write("zstd", workspaceCacheZstdStub, 0o700)
			write("sleep", "#!/bin/bash\nset -euo pipefail\nprintf '%s\\n' \"$*\" >> \"$CALLS.sleep\"\n", 0o700)
			actualID := test.actualID
			if actualID == "" {
				actualID = id
			}
			hash := strings.Repeat("c", 64)
			if test.failure == "hash" {
				hash = ""
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "-c", script)
			cmd.Env = append(os.Environ(), "PATH="+root+":"+os.Getenv("PATH"), "CACHE_HIT="+test.hit,
				"WORKSPACE_IMAGE_CACHE="+cache, "DOCKERFILE_HASH="+hash, "ACTUAL_ID="+actualID,
				"FAIL_STAGE="+test.failure, "CALLS="+filepath.Join(root, "calls"))
			out, err := cmd.CombinedOutput()
			if (err != nil) != test.wantError {
				t.Fatalf("error=%v wantError=%v output=%s", err, test.wantError, out)
			}
			calls, readErr := os.ReadFile(filepath.Join(root, "calls"))
			if readErr != nil && !os.IsNotExist(readErr) {
				t.Fatal(readErr)
			}
			if test.hit == "true" && (strings.Contains(string(calls), "build") || strings.Contains(string(calls), "save")) {
				t.Fatalf("exact hit rebuilt/fell back: %s", calls)
			}
			sleeps, sleepErr := os.ReadFile(filepath.Join(root, "calls.sleep"))
			if sleepErr != nil && !os.IsNotExist(sleepErr) {
				t.Fatal(sleepErr)
			}
			if string(sleeps) != test.wantSleeps {
				t.Fatalf("backoffs=%q want=%q", sleeps, test.wantSleeps)
			}
			if test.wantBuilds != 0 && strings.Count(string(calls), "build -t ") != test.wantBuilds {
				t.Fatalf("build attempts=%q want=%d", calls, test.wantBuilds)
			}
			if test.wantError {
				if test.wantBuilds != 0 {
					status := test.wantStatus
					if status == 0 {
						status = 1
					}
					if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != status {
						t.Fatalf("docker failure status lost: %v want=%d", err, status)
					}
				}
				if test.wantBuilds != 0 && (strings.Contains(string(calls), "image inspect") || strings.Contains(string(calls), "save ")) {
					t.Fatalf("failed build fell through to inspection/save: %s", calls)
				}
				return
			}
			want := "load\nimage inspect --format {{.Id}} swarm-workspace:latest\n"
			if test.hit != "true" {
				want = "build -t swarm-workspace:latest -f Dockerfile.workspace .\nimage inspect --format {{.Id}} swarm-workspace:latest\nsave swarm-workspace:latest\nimage inspect --format {{.Id}} swarm-workspace:latest\n"
				if test.wantBuilds > 1 {
					want = strings.Repeat("build -t swarm-workspace:latest -f Dockerfile.workspace .\n", test.wantBuilds-1) + want
				}
				if _, err := os.Stat(filepath.Join(cache, "stale")); !os.IsNotExist(err) {
					t.Fatal("miss retained partial restore payload")
				}
			}
			if string(calls) != want {
				t.Fatalf("docker calls=%q want=%q", calls, want)
			}
			for name, want := range map[string]string{"image.id": id, "image.tar.zst": "workspace-image"} {
				got, err := os.ReadFile(filepath.Join(cache, name))
				if err != nil || strings.TrimSpace(string(got)) != want {
					t.Fatalf("saved %s=%q err=%v", name, got, err)
				}
			}
		})
	}
}

const workspaceCacheDockerStub = `#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> "$CALLS"
base_error() {
  ref='public.ecr.aws/docker/library/golang:1.25-bookworm@sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437'
  endpoint='https://public.ecr.aws/v2/docker/library/golang/manifests/sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437'
  printf 'ERROR: failed to build: failed to solve: %s: failed to resolve source metadata for %s: failed to copy: httpReadSeeker: failed open: unexpected status code %s: %s\n' "$ref" "$ref" "$endpoint" "$1" >&2
}
case "$*" in
  'build -t swarm-workspace:latest -f Dockerfile.workspace .')
    attempt=$(grep -c '^build ' "$CALLS")
    if [ "$FAIL_STAGE" = registry_429 ] ||
       { [ "$FAIL_STAGE" = registry_429_transient ] && [ "$attempt" -le 2 ]; } ||
       { [ "$FAIL_STAGE" = registry_429_last ] && [ "$attempt" -le 4 ]; } ||
       { [ "$FAIL_STAGE" = registry_429_then_403 ] && [ "$attempt" -eq 1 ]; }; then
      base_error '429 Too Many Requests'
      exit 1
    fi
    if [ "$FAIL_STAGE" = registry_403 ] || [ "$FAIL_STAGE" = registry_429_then_403 ]; then
      base_error '403 Forbidden'
      exit 1
    fi
    if [ "$FAIL_STAGE" = apt_429 ]; then
      echo '#2 load metadata for public.ecr.aws/docker/library/golang' >&2
      echo 'E: Failed to fetch https://deb.debian.org/debian/dists/bookworm/InRelease 429 Too Many Requests' >&2
      exit 1
    fi
    if [ "$FAIL_STAGE" = mixed_status ]; then
      echo 'https://deb.debian.org/debian/dists/bookworm/InRelease: 429 Too Many Requests' >&2
      base_error '403 Forbidden'
      exit 1
    fi
    if [ "$FAIL_STAGE" = mixed_errors ]; then
      echo 'ERROR: apt install failed with status 100' >&2
      base_error '429 Too Many Requests'
      exit 1
    fi
    if [ "$FAIL_STAGE" = registry_status_7 ]; then
      base_error '429 Too Many Requests'
      exit 7
    fi
    if [ "$FAIL_STAGE" = terminal_apt ]; then
      base_error '429 Too Many Requests'
      echo 'E: apt install failed with status 100' >&2
      exit 1
    fi
    if [ "$FAIL_STAGE" = registry_data_limit ]; then
      base_error '429 Too Many Requests - Server message: toomanyrequests: Data limit exceeded'
      exit 1
    fi
    test "$FAIL_STAGE" != build ;;
  'save swarm-workspace:latest') printf workspace-image; test "$FAIL_STAGE" != save ;;
  load) test "$(cat)" = workspace-image; test "$FAIL_STAGE" != load ;;
  'image inspect --format {{.Id}} swarm-workspace:latest') test "$FAIL_STAGE" != inspect; printf '%s\n' "$ACTUAL_ID" ;;
  *) echo 'unexpected Docker operation (including registry traffic)' >&2; exit 99 ;;
esac
`

const workspaceCacheZstdStub = `#!/bin/bash
set -euo pipefail
case "$1" in
  -dc) test "$FAIL_STAGE" != decompress; test "$(cat "$2")" = workspace-image; cat "$2" ;;
  -T0) test "$2" = -o; cat > "$3"; test "$FAIL_STAGE" != compress ;;
  *) exit 99 ;;
esac
`

func TestWorkspaceImageCacheDockerfileInputsAreComplete(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("../..", "Dockerfile.workspace"))
	if err != nil {
		t.Fatal(err)
	}
	validate := func(raw []byte) error {
		if regexp.MustCompile(`(?im)^\s*#\s*(syntax|escape)\s*=`).Match(raw) || strings.Contains(string(raw), "--mount") {
			return fmt.Errorf("unkeyed frontend or mounted build input")
		}
		from := regexp.MustCompile(`^FROM public\.ecr\.aws/docker/library/golang:1\.25-bookworm@sha256:[a-f0-9]{64}$`)
		bases := 0
		for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\\\n", " "), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			switch strings.ToUpper(strings.Fields(line)[0]) {
			case "FROM":
				if !from.MatchString(line) {
					return fmt.Errorf("workspace base is not the pinned public mirror")
				}
				bases++
			case "ARG", "RUN", "USER", "WORKDIR":
			default:
				return fmt.Errorf("new Dockerfile dependency requires cache-key review: %s", line)
			}
		}
		if bases != 1 {
			return fmt.Errorf("workspace base count %d", bases)
		}
		return nil
	}
	if err := validate(raw); err != nil {
		t.Fatal(err)
	}
	for _, extra := range []string{"COPY . /workspace", "ADD https://example.invalid/source /workspace", "RUN --mount=type=bind,target=/input true", "FROM golang:1.25-bookworm", "# syntax=docker/dockerfile:1", "#syntax = docker/dockerfile:1", "# escape=`"} {
		if validate(append(append([]byte(nil), raw...), []byte("\n"+extra+"\n")...)) == nil {
			t.Fatalf("unkeyed input accepted: %s", extra)
		}
	}
}
