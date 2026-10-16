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
	for _, line := range strings.Split(prepare.Run, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "docker build") {
			if line != "docker build -t swarm-workspace:latest -f Dockerfile.workspace ." {
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
			if test.wantError {
				return
			}
			want := "load\nimage inspect --format {{.Id}} swarm-workspace:latest\n"
			if test.hit != "true" {
				want = "build -t swarm-workspace:latest -f Dockerfile.workspace .\nimage inspect --format {{.Id}} swarm-workspace:latest\nsave swarm-workspace:latest\nimage inspect --format {{.Id}} swarm-workspace:latest\n"
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
case "$*" in
  'build -t swarm-workspace:latest -f Dockerfile.workspace .') test "$FAIL_STAGE" != build ;;
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
