package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestWorkspaceBuildClaudeCLIUsesEmbeddedBuildPlanFromTempCWD(t *testing.T) {
	sourceRoot := repoRoot()
	tempCWD := t.TempDir()
	chdirForTest(t, tempCWD)
	callsPath := configureWorkspaceBuildDockerStub(t)
	t.Setenv("SWARM_WORKSPACE_IMAGE", "")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), tempCWD, []string{"workspace", "build", "--backend", "claude_cli"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitOK {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	for _, want := range []string{
		"Building workspace image swarm-workspace:latest for backend claude_cli",
		"Validating workspace image swarm-workspace:latest can execute claude",
		"Workspace image swarm-workspace:latest is ready for claude_cli",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout missing %q:\n%s", want, stdout.String())
		}
	}

	calls := readWorkspaceBuildDockerCalls(t, callsPath)
	if len(calls) != 5 {
		t.Fatalf("docker call count = %d, want 5:\n%s", len(calls), strings.Join(calls, "\n"))
	}
	if calls[0] != "version --format {{.Server.Version}}" {
		t.Fatalf("docker version call = %q", calls[0])
	}
	build := calls[1]
	for _, want := range []string{
		"build -t swarm-workspace-build-",
		"--build-arg INSTALL_CLAUDE_CLI=true",
		"--build-arg INSTALL_CODEX_CLI=false",
		"swarm-workspace-build-context-",
	} {
		if !strings.Contains(build, want) {
			t.Fatalf("docker build call missing %q:\n%s", want, build)
		}
	}
	if strings.Contains(build, "build -t swarm-workspace:latest") {
		t.Fatalf("docker build call published directly to runtime image tag:\n%s", build)
	}
	tempImage := workspaceBuildTaggedImageFromCall(t, build)
	if strings.Contains(build, sourceRoot) || strings.Contains(build, tempCWD) {
		t.Fatalf("docker build call used source checkout or current directory:\n%s", build)
	}
	if !strings.Contains(build, "Dockerfile.workspace-") {
		t.Fatalf("docker build call did not use materialized embedded Dockerfile:\n%s", build)
	}
	validate := calls[2]
	for _, want := range []string{
		"run --rm --entrypoint sh " + tempImage,
		"command -v -- \"$1\" >/dev/null && \"$1\" --version >/dev/null",
		"swarm-cli-proof claude",
	} {
		if !strings.Contains(validate, want) {
			t.Fatalf("docker validation call missing %q:\n%s", want, validate)
		}
	}
	if calls[3] != "tag "+tempImage+" swarm-workspace:latest" {
		t.Fatalf("docker publish call = %q, want tag from temp image to runtime image", calls[3])
	}
	if calls[4] != "image rm "+tempImage {
		t.Fatalf("docker temp cleanup call = %q, want image rm %s", calls[4], tempImage)
	}
}

func TestWorkspaceBuildClaudeCLIImageOverride(t *testing.T) {
	callsPath := configureWorkspaceBuildDockerStub(t)
	t.Setenv("SWARM_WORKSPACE_IMAGE", "env-image:latest")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"workspace", "build", "--backend", "claude_cli", "--image", "custom/image:test"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitOK {
		t.Fatalf("code = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	calls := strings.Join(readWorkspaceBuildDockerCalls(t, callsPath), "\n")
	if !strings.Contains(calls, "tag swarm-workspace-build-") || !strings.Contains(calls, " custom/image:test") {
		t.Fatalf("docker calls did not use --image override:\n%s", calls)
	}
	if strings.Contains(calls, "build -t custom/image:test") || strings.Contains(calls, "run --rm --entrypoint sh custom/image:test") {
		t.Fatalf("docker calls published or validated final image before validation completed:\n%s", calls)
	}
	if strings.Contains(calls, "env-image:latest") {
		t.Fatalf("docker calls used env image despite --image override:\n%s", calls)
	}
}

func TestWorkspaceBuildClaudeCLIFailsOnDockerBuildError(t *testing.T) {
	configureWorkspaceBuildDockerStub(t)
	t.Setenv("SWARM_TEST_DOCKER_FAIL_BUILD", "1")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"workspace", "build", "--backend", "claude_cli"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
	}
	for _, want := range []string{"workspace image build failed for image", "docker build failed"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
		}
	}
}

func TestWorkspaceBuildClaudeCLIFailsOnValidationError(t *testing.T) {
	callsPath := configureWorkspaceBuildDockerStub(t)
	t.Setenv("SWARM_TEST_DOCKER_FAIL_VALIDATE", "1")

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"workspace", "build", "--backend", "claude_cli"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
	}
	for _, want := range []string{"workspace image validation failed", "configured Claude CLI command \"claude\" cannot execute", "claude missing"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
		}
	}
	calls := strings.Join(readWorkspaceBuildDockerCalls(t, callsPath), "\n")
	if strings.Contains(calls, "\ntag ") || strings.HasPrefix(calls, "tag ") {
		t.Fatalf("docker calls published final image after validation failure:\n%s", calls)
	}
	if !strings.Contains(calls, "\nimage rm swarm-workspace-build-") {
		t.Fatalf("docker calls did not clean up temp validation image:\n%s", calls)
	}
}

func TestWorkspaceBuildClaudeCLIFailsOnUnavailableDocker(t *testing.T) {
	t.Setenv("SWARM_DOCKER_BIN", filepath.Join(t.TempDir(), "missing-docker"))

	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"workspace", "build", "--backend", "claude_cli"}, &stdout, &stderr, defaultRootCommandOptions())
	if code != cliExitRuntime {
		t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitRuntime, stdout.String(), stderr.String())
	}
	for _, want := range []string{"Docker is not available", "SWARM_DOCKER_BIN"} {
		if !strings.Contains(stderr.String(), want) {
			t.Fatalf("stderr missing %q:\n%s", want, stderr.String())
		}
	}
}

func TestWorkspaceBuildClaudeCLIValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing backend", args: []string{"workspace", "build"}, want: "requires --backend claude_cli"},
		{name: "unsupported backend", args: []string{"workspace", "build", "--backend", "host"}, want: "unsupported workspace build backend"},
		{name: "empty image", args: []string{"workspace", "build", "--backend", "claude_cli", "--image", " "}, want: "workspace image from --image must be non-empty"},
		{name: "image whitespace", args: []string{"workspace", "build", "--backend", "claude_cli", "--image", "bad image"}, want: "must not contain whitespace"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), tc.args, &stdout, &stderr, defaultRootCommandOptions())
			if code != cliExitValidation {
				t.Fatalf("code = %d, want %d stdout=%s stderr=%s", code, cliExitValidation, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr missing %q:\n%s", tc.want, stderr.String())
			}
		})
	}
}

func TestWorkspaceBuildSpecAuthorityPromoted(t *testing.T) {
	var spec struct {
		WorkspaceModel struct {
			BuildAuthority struct {
				PromotedBy           string         `yaml:"promoted_by"`
				ImplementationStatus string         `yaml:"implementation_status"`
				CanonicalOwner       string         `yaml:"canonical_owner"`
				BuildPlanRule        string         `yaml:"build_plan_rule"`
				BuildProfile         map[string]any `yaml:"claude_cli_build_profile"`
				ImageTargetRule      string         `yaml:"image_target_rule"`
				ValidationRule       string         `yaml:"validation_rule"`
				ConsumerBoundaries   []string       `yaml:"consumer_boundaries"`
			} `yaml:"local_workspace_image_build_authority"`
		} `yaml:"workspace_model"`
		CLISpecification struct {
			CommandCatalog struct {
				WorkspaceBuild struct {
					Command              string   `yaml:"command"`
					ImplementationStatus string   `yaml:"implementation_status"`
					Owner                string   `yaml:"owner"`
					Behavior             string   `yaml:"behavior"`
					Boundaries           []string `yaml:"boundaries"`
				} `yaml:"workspace_build"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	data, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(data, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	owner := spec.WorkspaceModel.BuildAuthority
	if owner.PromotedBy != "#1566" || owner.ImplementationStatus != "implemented_first_slice" || owner.CanonicalOwner != "platform-spec.yaml#workspace_model.local_workspace_image_build_authority" {
		t.Fatalf("workspace image build authority = %#v", owner)
	}
	for _, want := range []string{"embedded/materialized", "source checkout", "Dockerfile.workspace", "go.mod"} {
		if !strings.Contains(owner.BuildPlanRule, want) {
			t.Fatalf("build plan rule missing %q:\n%s", want, owner.BuildPlanRule)
		}
	}
	for _, want := range []string{"SWARM_WORKSPACE_IMAGE", "swarm-workspace:latest", "--image"} {
		if !strings.Contains(owner.ImageTargetRule, want) {
			t.Fatalf("image target rule missing %q:\n%s", want, owner.ImageTargetRule)
		}
	}
	for _, want := range []string{"temporary image tag", "command -v", "--version", "lookup alone are insufficient"} {
		if !strings.Contains(owner.ValidationRule, want) {
			t.Fatalf("validation rule missing %q:\n%s", want, owner.ValidationRule)
		}
	}
	buildArgs, ok := owner.BuildProfile["docker_build_args"].(map[string]any)
	if !ok || buildArgs["INSTALL_CLAUDE_CLI"] != "true" || buildArgs["INSTALL_CODEX_CLI"] != "false" {
		t.Fatalf("build args = %#v", owner.BuildProfile["docker_build_args"])
	}
	for _, want := range []string{"MUST NOT build images", "MUST NOT auto-build images", "Host workspace backend"} {
		if !stringSliceContains(owner.ConsumerBoundaries, want) {
			t.Fatalf("consumer boundaries missing %q: %#v", want, owner.ConsumerBoundaries)
		}
	}
	row := spec.CLISpecification.CommandCatalog.WorkspaceBuild
	if row.Command != "swarm workspace build --backend claude_cli [--image <tag>]" || row.ImplementationStatus != "implemented_first_slice" || row.Owner != "workspace_model.local_workspace_image_build_authority" {
		t.Fatalf("workspace_build command catalog row = %#v", row)
	}
	for _, want := range []string{"embedded/materialized", "temporary image tag", "SWARM_DOCKER_BIN", "SWARM_WORKSPACE_IMAGE", "claude --version"} {
		if !strings.Contains(row.Behavior, want) {
			t.Fatalf("workspace_build behavior missing %q:\n%s", want, row.Behavior)
		}
	}
	if !stringSliceContains(row.Boundaries, "no runtime startup auto-build") || !stringSliceContains(row.Boundaries, "no source-checkout Dockerfile lookup or go.mod walk") {
		t.Fatalf("workspace_build boundaries incomplete: %#v", row.Boundaries)
	}
}

func configureWorkspaceBuildDockerStub(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	callsPath := filepath.Join(dir, "docker.calls")
	dockerPath := filepath.Join(dir, "docker")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$SWARM_TEST_DOCKER_CALLS"
case "$1" in
  version)
    exit 0
    ;;
  build)
    if [ "$SWARM_TEST_DOCKER_FAIL_BUILD" = "1" ]; then
      echo "docker build failed" >&2
      exit 42
    fi
    exit 0
    ;;
  run)
    if [ "$SWARM_TEST_DOCKER_FAIL_VALIDATE" = "1" ]; then
      echo "claude missing" >&2
      exit 43
    fi
    exit 0
    ;;
  tag)
    exit 0
    ;;
  image)
    if [ "$2" = "rm" ]; then
      exit 0
    fi
    echo "unexpected docker image command: $*" >&2
    exit 45
    ;;
  *)
    echo "unexpected docker command: $*" >&2
    exit 44
    ;;
esac
`
	if err := os.WriteFile(dockerPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake docker: %v", err)
	}
	t.Setenv("SWARM_DOCKER_BIN", dockerPath)
	t.Setenv("SWARM_TEST_DOCKER_CALLS", callsPath)
	t.Setenv("SWARM_TEST_DOCKER_FAIL_BUILD", "")
	t.Setenv("SWARM_TEST_DOCKER_FAIL_VALIDATE", "")
	return callsPath
}

func workspaceBuildTaggedImageFromCall(t *testing.T, call string) string {
	t.Helper()
	fields := strings.Fields(call)
	for i := 0; i < len(fields)-1; i++ {
		if fields[i] == "-t" {
			return fields[i+1]
		}
	}
	t.Fatalf("docker call missing -t image: %s", call)
	return ""
}

func readWorkspaceBuildDockerCalls(t *testing.T, callsPath string) []string {
	t.Helper()
	data, err := os.ReadFile(callsPath)
	if err != nil {
		t.Fatalf("read docker calls: %v", err)
	}
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}
