package testplanning

import (
	"context"
	"crypto/sha256"
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

type workspaceCacheWorkflow struct {
	Env         map[string]string
	Permissions map[string]string
	On          struct {
		Dispatch struct {
			Inputs map[string]struct {
				Type    string
				Default any
			}
		} `yaml:"workflow_dispatch"`
	}
	Jobs map[string]struct {
		If, Name    string
		Env         map[string]string
		Permissions map[string]string
		Outputs     map[string]string
		Needs       []string
		Steps       []workspaceCacheStep
	}
}

func workspaceSetupExcluded(condition string) bool {
	const exclusion = "github.event_name != 'workflow_dispatch' || inputs.workspace_base_setup != true"
	condition = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(condition, "${{"), "}}"))
	if condition == exclusion {
		return true
	}
	condition = strings.TrimPrefix(strings.TrimPrefix(condition, "always() && "), "!cancelled() && ")
	if condition == "("+exclusion+")" {
		return true
	}
	rest, ok := strings.CutPrefix(condition, "("+exclusion+") && ")
	if !ok {
		return false
	}
	// Only check dominance of the explicit setup exclusion, not GHA semantics.
	depth, quoted := 0, false
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '\'':
			quoted = !quoted
		case '(':
			if !quoted {
				depth++
			}
		case ')':
			if !quoted {
				depth--
			}
		case '|':
			if !quoted && depth == 0 {
				return false
			}
		}
		if depth < 0 {
			return false
		}
	}
	return !quoted && depth == 0
}

func workspaceBaseSetupContract(raw []byte) error {
	workflow, err := workspaceCacheDocument(raw)
	if err != nil {
		return err
	}
	input := workflow.On.Dispatch.Inputs["workspace_base_setup"]
	if input.Type != "boolean" || input.Default != false || workflow.Permissions["packages"] != "" {
		return fmt.Errorf("setup must be explicit default-off without workflow-wide package permission")
	}
	setup, ok := workflow.Jobs["workspace-base-setup"]
	if !ok || setup.If != "github.event_name == 'workflow_dispatch' && inputs.workspace_base_setup == true && github.repository == 'division-sh/swarm'" ||
		len(setup.Permissions) != 2 || setup.Permissions["contents"] != "read" || setup.Permissions["packages"] != "write" ||
		len(setup.Needs) != 0 || len(setup.Outputs) != 0 || len(setup.Steps) != 4 {
		return fmt.Errorf("isolated manual upstream setup or exact write scope changed")
	}
	for name, job := range workflow.Jobs {
		if name == "workspace-base-setup" {
			continue
		}
		if !workspaceSetupExcluded(job.If) || job.Permissions["packages"] == "write" {
			return fmt.Errorf("normal job %s can execute or write packages in setup mode", name)
		}
		for _, dependency := range job.Needs {
			if dependency == "workspace-base-setup" {
				return fmt.Errorf("normal proof depends on setup instead of independent qualification")
			}
		}
	}
	job := workflow.Jobs["sqlite-local-dev"]
	if len(job.Permissions) != 2 || job.Permissions["contents"] != "read" || job.Permissions["packages"] != "read" {
		return fmt.Errorf("workspace preparation lacks job-scoped read-only permission")
	}
	if setup.Steps[0].Uses != "actions/checkout@v4" || setup.Steps[1].Env["REVIEWED_HEAD"] != "${{ inputs.workspace_base_setup_head }}" ||
		!strings.Contains(setup.Steps[1].Run, `test "$GITHUB_SHA" = "$REVIEWED_HEAD"`) ||
		!strings.Contains(setup.Steps[1].Run, `test "$(git rev-parse HEAD)" = "$REVIEWED_HEAD"`) ||
		setup.Steps[2].Env["GH_TOKEN"] != "${{ github.token }}" {
		return fmt.Errorf("setup source/token is not bound to the reviewed dispatch")
	}
	if setup.Steps[1].If != "" || setup.Steps[2].If != "" || setup.Steps[3].If != "" ||
		setup.Steps[1].Shell != "bash" || setup.Steps[2].Shell != "bash" {
		return fmt.Errorf("setup may bypass failed binding/verification or shell ownership")
	}
	for _, command := range []string{
		`printf '%s' "$GH_TOKEN" | docker login ghcr.io -u "$GITHUB_ACTOR" --password-stdin`,
		`trap 'rm -r -- "$credential_root"' EXIT`,
		`python3 .github/scripts/verify-workspace-base.py --before-publication`,
		`docker build -t "$mirror:repository-link" -f Dockerfile.workspace-base-link .`,
		`docker push "$mirror:repository-link"`,
		`python3 .github/scripts/verify-workspace-base.py --association-only`,
		`docker buildx imagetools create --prefer-index=false --tag "$mirror:1.25-bookworm" "$source"`,
		`python3 .github/scripts/verify-workspace-base.py > "$RUNNER_TEMP/workspace-base-setup.json"`,
		`docker pull "$mirror@$digest"`,
	} {
		if !strings.Contains(setup.Steps[2].Run, command) {
			return fmt.Errorf("setup lost immutable source/association/authentication check: %s", command)
		}
	}
	result := setup.Steps[3]
	if result.Uses != "actions/upload-artifact@v4" || len(result.With) != 3 ||
		result.With["name"] != "workspace-base-setup-${{ github.sha }}" ||
		result.With["path"] != "${{ runner.temp }}/workspace-base-setup.json" || result.With["if-no-files-found"] != "error" {
		return fmt.Errorf("setup may publish a qualification or credential artifact")
	}
	for _, step := range setup.Steps {
		for _, forbidden := range []string{"go test ", "-plan-ci", "-record-evidence", "-record-tier", "gate.sh"} {
			if strings.Contains(step.Run, forbidden) {
				return fmt.Errorf("setup executes a proof or emits qualification")
			}
		}
	}
	return nil
}

func validateWorkspaceBaseSetup(t *testing.T, raw []byte) {
	t.Helper()
	if err := workspaceBaseSetupContract(raw); err != nil {
		t.Fatal(err)
	}
	assertWorkspaceSetupShell(t, raw)
	for name, mutation := range map[string][2]string{
		"enabled by default":      {"default: false\n        type: boolean\n      workspace_base_setup_head:", "default: true\n        type: boolean\n      workspace_base_setup_head:"},
		"PR setup":                {"github.event_name == 'workflow_dispatch' && inputs.workspace_base_setup == true && github.repository == 'division-sh/swarm'", "github.event_name == 'pull_request'"},
		"unbound head":            {`test "$GITHUB_SHA" = "$REVIEWED_HEAD"`, "true"},
		"wrong copy":              {"imagetools create --prefer-index=false", "imagetools create --annotation changed=true"},
		"association selected":    {`docker pull "$mirror@$digest"`, `docker pull "$mirror:repository-link"`},
		"credential artifact":     {"path: ${{ runner.temp }}/workspace-base-setup.json", "path: ${{ runner.temp }}"},
		"proof artifact":          {"name: workspace-base-setup-${{ github.sha }}", "name: ci-tier-proof"},
		"write on normal build":   {"packages: read", "packages: write"},
		"summary still qualifies": {"if: always() && (github.event_name != 'workflow_dispatch' || inputs.workspace_base_setup != true)", "if: always()"},
	} {
		t.Run("setup/"+name, func(t *testing.T) {
			changed := strings.Replace(string(raw), mutation[0], mutation[1], 1)
			if changed == string(raw) || workspaceBaseSetupContract([]byte(changed)) == nil {
				t.Fatal("setup-mode boundary change was not refused")
			}
		})
	}
	workflow, err := workspaceCacheDocument(raw)
	if err != nil {
		t.Fatal(err)
	}
	for name, job := range workflow.Jobs {
		if name == "workspace-base-setup" {
			continue
		}
		t.Run("setup/all-jobs/"+name, func(t *testing.T) {
			if workspaceSetupExcluded("always()") || workspaceSetupExcluded(job.If+" || true") {
				t.Fatal("normal-job exclusion admitted an always/disjunction bypass")
			}
		})
	}
}

func assertWorkspaceSetupShell(t *testing.T, raw []byte) {
	t.Helper()
	workflow, err := workspaceCacheDocument(raw)
	if err != nil {
		t.Fatal(err)
	}
	steps := workflow.Jobs["workspace-base-setup"].Steps
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, reviewed, eventHead, checkout, repository, ref string }{
		{"exact", strings.Repeat("a", 40), strings.Repeat("a", 40), strings.Repeat("a", 40), "division-sh/swarm", "refs/heads/reviewed"},
		{"missing-reviewed", "", strings.Repeat("a", 40), strings.Repeat("a", 40), "division-sh/swarm", "refs/heads/reviewed"},
		{"crossed-dispatch", strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("a", 40), "division-sh/swarm", "refs/heads/reviewed"},
		{"crossed-checkout", strings.Repeat("a", 40), strings.Repeat("a", 40), strings.Repeat("b", 40), "division-sh/swarm", "refs/heads/reviewed"},
		{"foreign-repository", strings.Repeat("a", 40), strings.Repeat("a", 40), strings.Repeat("a", 40), "outside/swarm", "refs/heads/reviewed"},
		{"tag-ref", strings.Repeat("a", 40), strings.Repeat("a", 40), strings.Repeat("a", 40), "division-sh/swarm", "refs/tags/not-reviewed-branch"},
	} {
		t.Run("setup/binding/"+test.name, func(t *testing.T) {
			bin := t.TempDir()
			if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/bin/bash\nset -euo pipefail\ntest \"$*\" = 'rev-parse HEAD'\nprintf '%s\\n' \"$CHECKOUT_HEAD\"\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "-c", steps[1].Run)
			cmd.Dir = repo
			cmd.Env = append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "REVIEWED_HEAD="+test.reviewed,
				"GITHUB_SHA="+test.eventHead, "CHECKOUT_HEAD="+test.checkout, "GITHUB_REPOSITORY="+test.repository, "GITHUB_REF="+test.ref)
			out, err := cmd.CombinedOutput()
			if (err == nil) != (test.name == "exact") {
				t.Fatalf("setup source binding result=%v output=%s", err, out)
			}
		})
	}
	for _, fault := range []string{"", "login", "before-publication", "association-only", "link-build", "link-inspect", "link-push", "copy", "verify", "pull", "link-layers", "link-label", "link-executable"} {
		t.Run("setup/execution/"+fault, func(t *testing.T) {
			root := t.TempDir()
			for name, script := range map[string]string{"docker": workspaceSetupDockerStub, "python3": workspaceSetupPythonStub} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(script), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "-c", steps[2].Run)
			cmd.Dir = repo
			cmd.Env = append(os.Environ(), "PATH="+root+":"+os.Getenv("PATH"), "RUNNER_TEMP="+root,
				"GITHUB_WORKSPACE="+repo, "GH_TOKEN=fixture-only-token", "GITHUB_ACTOR=workspace-fixture",
				"FAIL_STAGE="+fault, "CALLS="+filepath.Join(root, "calls"))
			out, err := cmd.CombinedOutput()
			if (err != nil) != (fault != "") || strings.Contains(string(out), "fixture-only-token") {
				t.Fatalf("setup execution=%v fault=%q output=%s", err, fault, out)
			}
			calls, readErr := os.ReadFile(filepath.Join(root, "calls"))
			if readErr != nil {
				t.Fatal(readErr)
			}
			want := "login\nbefore-publication\nlink-build\nlink-inspect\nlink-push\nassociation-only\ncopy\nverify\npull\n"
			cut := fault
			if strings.HasPrefix(fault, "link-") && fault != "link-build" && fault != "link-push" {
				cut = "link-inspect"
			}
			if cut != "" {
				before, _, found := strings.Cut(want, cut+"\n")
				if !found {
					t.Fatal("invalid setup fault cut")
				}
				want = before + cut + "\n"
			}
			if string(calls) != want {
				t.Fatalf("setup advanced past failure or changed ordered work: got=%q want=%q", calls, want)
			}
			credentials, err := os.ReadFile(filepath.Join(root, "calls.credentials"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(strings.TrimSpace(string(credentials))); !os.IsNotExist(err) {
				t.Fatalf("setup credentials survived failure/success: %v", err)
			}
		})
	}
}

const workspaceSetupDockerStub = `#!/bin/bash
set -euo pipefail
operation=''
case "$*" in
  'login ghcr.io -u workspace-fixture --password-stdin')
    operation=login
    test "$(cat)" = fixture-only-token
    test -d "$DOCKER_CONFIG"
    printf '%s\n' "$DOCKER_CONFIG" > "$CALLS.credentials"
    printf '%s' fixture-only-token > "$DOCKER_CONFIG/config.json" ;;
  'build -t ghcr.io/division-sh/swarm-workspace-golang:repository-link -f Dockerfile.workspace-base-link .') operation=link-build ;;
  'image inspect ghcr.io/division-sh/swarm-workspace-golang:repository-link') operation=link-inspect ;;
  'push ghcr.io/division-sh/swarm-workspace-golang:repository-link') operation=link-push ;;
  'buildx imagetools create --prefer-index=false --tag ghcr.io/division-sh/swarm-workspace-golang:1.25-bookworm public.ecr.aws/docker/library/golang:1.25-bookworm@sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437') operation=copy ;;
  'pull ghcr.io/division-sh/swarm-workspace-golang@sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437') operation=pull ;;
  *) echo unexpected-setup-Docker-operation >&2; exit 99 ;;
esac
printf '%s\n' "$operation" >> "$CALLS"
test "$FAIL_STAGE" != "$operation"
if [ "$operation" = link-inspect ]; then
  label=https://github.com/division-sh/swarm
  layers='[]'
  command=null
  if [ "$FAIL_STAGE" = link-label ]; then label=https://github.com/outside/swarm; fi
  if [ "$FAIL_STAGE" = link-layers ]; then layers='["unapproved-layer"]'; fi
  if [ "$FAIL_STAGE" = link-executable ]; then command='["unapproved-program"]'; fi
  printf '[{"Config":{"Labels":{"org.opencontainers.image.source":"%s"},"Cmd":%s,"Entrypoint":null},"RootFS":{"Layers":%s}}]\n' "$label" "$command" "$layers"
fi
`

const workspaceSetupPythonStub = `#!/bin/bash
set -euo pipefail
case "$*" in
  '.github/scripts/verify-workspace-base.py') operation=verify ;;
  '.github/scripts/verify-workspace-base.py --before-publication') operation=before-publication ;;
  '.github/scripts/verify-workspace-base.py --association-only') operation=association-only ;;
  *) exit 99 ;;
esac
printf '%s\n' "$operation" >> "$CALLS"
test "$FAIL_STAGE" != "$operation"
printf '{"qualification":false}\n'
`

func workspaceCacheDocument(raw []byte) (workspaceCacheWorkflow, error) {
	var workflow workspaceCacheWorkflow
	err := yaml.Unmarshal(raw, &workflow)
	return workflow, err
}

func workspaceCacheSteps(raw []byte) ([]workspaceCacheStep, error) {
	workflow, err := workspaceCacheDocument(raw)
	if err != nil {
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
	for _, name := range []string{"Select workspace base", "Restore workspace image", "Prepare workspace image", "Save workspace image", "Prove real container MCP emission and successor refusal", "Prove no-selector SQLite local run"} {
		if _, ok := positions[name]; !ok {
			return fmt.Errorf("missing workspace cache/proof step %q", name)
		}
	}
	restore := steps[positions["Restore workspace image"]]
	prepare := steps[positions["Prepare workspace image"]]
	save := steps[positions["Save workspace image"]]
	const cachePath = "${{ runner.temp }}/swarm-workspace-image"
	const cacheKey = "workspace-image-v2-${{ runner.os }}-${{ runner.arch }}-${{ hashFiles('Dockerfile.workspace') }}-${{ steps.workspace-base.outputs.hash }}"
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
	if prepare.If != "" || prepare.Shell != "bash" || len(prepare.Env) != 7 ||
		prepare.Env["CACHE_HIT"] != "${{ steps.workspace-image-cache.outputs.cache-hit }}" ||
		prepare.Env["DOCKERFILE_HASH"] != "${{ hashFiles('Dockerfile.workspace') }}" ||
		prepare.Env["BASE_REF"] != "${{ steps.workspace-base.outputs.ref }}" ||
		prepare.Env["BASE_REF_HASH"] != "${{ steps.workspace-base.outputs.hash }}" ||
		prepare.Env["GH_TOKEN"] != "${{ github.token }}" ||
		prepare.Env["HEAD_REPOSITORY"] != "${{ github.event.pull_request.head.repo.full_name }}" ||
		prepare.Env["WORKSPACE_IMAGE_CACHE"] != cachePath || !strings.HasPrefix(prepare.Run, "set -euo pipefail\n") {
		return fmt.Errorf("workspace preparation lost unconditional fail-closed hit/miss owner")
	}
	if !(positions["Select workspace base"] < positions[restore.Name] && positions[restore.Name] < positions[prepare.Name] &&
		positions[prepare.Name] < positions["Prove real container MCP emission and successor refusal"] &&
		positions["Prove real container MCP emission and successor refusal"] < positions["Prove no-selector SQLite local run"] &&
		positions["Prove no-selector SQLite local run"] < positions[save.Name]) {
		return fmt.Errorf("workspace archive must be captured before proofs and published only afterward")
	}
	builds := 0
	for _, required := range []string{
		`manifest_error='https://public.ecr.aws/v2/docker/library/golang/manifests/sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437: 429 Too Many Requests'`,
		`manifest_error='https://ghcr.io/v2/division-sh/swarm-workspace-golang/manifests/sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437: 429 Too Many Requests'`,
		`test "$(printf '%s' "$BASE_REF" | sha256sum | cut -d ' ' -f 1)" = "$BASE_REF_HASH"`,
		`printf '%s' "$GH_TOKEN" | docker login ghcr.io -u "$GITHUB_ACTOR" --password-stdin`,
		`trap 'rm -r -- "$credential_root"' EXIT`,
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
			if line != `if docker build -t swarm-workspace:latest --build-arg WORKSPACE_GOLANG_BASE="$base_ref" -f Dockerfile.workspace . > "$build_log" 2>&1; then` {
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
	validateWorkspaceBaseSetup(t, raw)
	for name, pair := range map[string][2]string{
		"restore omitted":     {"name: Restore workspace image", "name: omitted restore"},
		"load omitted":        {"name: Prepare workspace image", "name: omitted prepare"},
		"save omitted":        {"name: Save workspace image", "name: omitted save"},
		"partial key":         {"workspace-image-v2-${{ runner.os }}-${{ runner.arch }}-", "workspace-image-v2-"},
		"per-head key":        {"workspace-image-v2-${{ runner.os }}", "workspace-image-v2-${{ github.sha }}-${{ runner.os }}"},
		"unkeyed registry":    {"-${{ steps.workspace-base.outputs.hash }}", ""},
		"empty hash allowed":  {"if: hashFiles('Dockerfile.workspace') != ''", "if: always()"},
		"prefix fallback":     {"key: workspace-image-v2-${{ runner.os }}", "restore-keys: workspace-image-v2-\n          key: workspace-image-v2-${{ runner.os }}"},
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
	assertWorkspaceBaseSelector(t, steps)
	id := "sha256:" + strings.Repeat("a", 64)
	for _, scope := range []struct{ name, repository, event, head, base string }{
		{"fork-ecr", "division-sh/swarm", "pull_request", "outside/swarm", workspaceECRBase},
		{"upstream-ghcr", "division-sh/swarm", "push", "", workspaceGHCRBase},
	} {
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
			{name: "foreign registry 429", failure: "foreign_registry", wantError: true, wantBuilds: 1},
			{name: "mirror authentication failure", failure: "login", wantError: true},
		} {
			if test.failure == "login" && scope.base != workspaceGHCRBase {
				continue
			}
			t.Run(scope.name+"/"+test.name, func(t *testing.T) {
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
				if err := os.Mkdir(filepath.Join(root, "runner-temp"), 0o700); err != nil {
					t.Fatal(err)
				}
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
					"BASE_REF="+scope.base, fmt.Sprintf("BASE_REF_HASH=%x", sha256.Sum256([]byte(scope.base))),
					"GITHUB_REPOSITORY="+scope.repository, "GITHUB_EVENT_NAME="+scope.event, "HEAD_REPOSITORY="+scope.head,
					"GITHUB_ACTOR=workspace-fixture", "GH_TOKEN=fixture-only-token", "GITHUB_WORKSPACE="+filepath.Join(root, "workspace"),
					"RUNNER_TEMP="+filepath.Join(root, "runner-temp"),
					"FAIL_STAGE="+test.failure, "CALLS="+filepath.Join(root, "calls"))
				out, err := cmd.CombinedOutput()
				if (err != nil) != test.wantError {
					t.Fatalf("error=%v wantError=%v output=%s", err, test.wantError, out)
				}
				calls, readErr := os.ReadFile(filepath.Join(root, "calls"))
				if readErr != nil && !os.IsNotExist(readErr) {
					t.Fatal(readErr)
				}
				if test.hit == "true" && (strings.Contains(string(calls), "build") || strings.Contains(string(calls), "save") || strings.Contains(string(calls), "login") || strings.Contains(string(calls), "pull")) {
					t.Fatalf("exact hit rebuilt/fell back: %s", calls)
				}
				if scope.base == workspaceECRBase && strings.Contains(string(calls), "login") {
					t.Fatal("fork/default ECR branch attempted package authentication")
				}
				if config, err := os.ReadFile(filepath.Join(root, "calls.credentials")); err == nil {
					if _, err := os.Stat(strings.TrimSpace(string(config))); !os.IsNotExist(err) {
						t.Fatalf("ephemeral Docker credentials survived success/failure: %v", err)
					}
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
					build := "build -t swarm-workspace:latest --build-arg WORKSPACE_GOLANG_BASE=" + scope.base + " -f Dockerfile.workspace .\n"
					want = build + "image inspect --format {{.Id}} swarm-workspace:latest\nsave swarm-workspace:latest\nimage inspect --format {{.Id}} swarm-workspace:latest\n"
					if test.wantBuilds > 1 {
						want = strings.Repeat(build, test.wantBuilds-1) + want
					}
					if scope.base == workspaceGHCRBase {
						want = "login ghcr.io -u workspace-fixture --password-stdin\n" + want
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
}

const workspaceECRBase = "public.ecr.aws/docker/library/golang:1.25-bookworm@sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437"
const workspaceGHCRBase = "ghcr.io/division-sh/swarm-workspace-golang@sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437"

func assertWorkspaceBaseSelector(t *testing.T, steps []workspaceCacheStep) {
	t.Helper()
	var script string
	for _, step := range steps {
		if step.Name == "Select workspace base" && step.ID == "workspace-base" {
			script = step.Run
		}
	}
	if script == "" {
		t.Fatal("missing actual fixed-reference selector")
	}
	for _, row := range []struct{ name, repository, event, head, want string }{
		{"upstream-push", "division-sh/swarm", "push", "", workspaceGHCRBase},
		{"upstream-schedule", "division-sh/swarm", "schedule", "", workspaceGHCRBase},
		{"upstream-manual", "division-sh/swarm", "workflow_dispatch", "", workspaceGHCRBase},
		{"upstream-PR", "division-sh/swarm", "pull_request", "division-sh/swarm", workspaceGHCRBase},
		{"fork-PR-into-upstream", "division-sh/swarm", "pull_request", "outside/swarm", workspaceECRBase},
		{"missing-PR-head", "division-sh/swarm", "pull_request", "", workspaceECRBase},
		{"malformed-PR-head", "division-sh/swarm", "pull_request", "division-sh/swarm/extra", workspaceECRBase},
		{"fork-push", "outside/swarm", "push", "", workspaceECRBase},
		{"fork-manual", "outside/swarm", "workflow_dispatch", "", workspaceECRBase},
		{"missing-event", "division-sh/swarm", "", "division-sh/swarm", workspaceECRBase},
		{"untrusted-target-event", "division-sh/swarm", "pull_request_target", "division-sh/swarm", workspaceECRBase},
	} {
		t.Run("selector/"+row.name, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "output")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "bash", "-c", script)
			cmd.Env = append(os.Environ(), "GITHUB_OUTPUT="+output, "GITHUB_REPOSITORY="+row.repository,
				"EVENT_NAME="+row.event, "HEAD_REPOSITORY="+row.head)
			if raw, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("selector failed: %v %s", err, raw)
			}
			raw, err := os.ReadFile(output)
			want := fmt.Sprintf("ref=%s\nhash=%x\n", row.want, sha256.Sum256([]byte(row.want)))
			if err != nil || string(raw) != want {
				t.Fatalf("selected %q, want %q: %v", raw, want, err)
			}
		})
	}
}

const workspaceCacheDockerStub = `#!/bin/bash
set -euo pipefail
printf '%s\n' "$*" >> "$CALLS"
base_error() {
  ref=$BASE_REF
  endpoint='https://public.ecr.aws/v2/docker/library/golang/manifests/sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437'
  if [[ "$BASE_REF" == ghcr.io/* ]]; then
    endpoint='https://ghcr.io/v2/division-sh/swarm-workspace-golang/manifests/sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437'
  fi
  if [ "$FAIL_STAGE" = foreign_registry ]; then
    ref=foreign.invalid/golang
    endpoint=https://foreign.invalid/manifests/not-our-image
  fi
  printf 'ERROR: failed to build: failed to solve: %s: failed to resolve source metadata for %s: failed to copy: httpReadSeeker: failed open: unexpected status code %s: %s\n' "$ref" "$ref" "$endpoint" "$1" >&2
}
case "$*" in
  'login ghcr.io -u workspace-fixture --password-stdin')
    test "$(cat)" = fixture-only-token
    test -d "$DOCKER_CONFIG"
    printf '%s\n' "$DOCKER_CONFIG" > "$CALLS.credentials"
    printf '%s' fixture-only-token > "$DOCKER_CONFIG/config.json"
    test "$FAIL_STAGE" != login ;;
  'build -t swarm-workspace:latest --build-arg WORKSPACE_GOLANG_BASE='*' -f Dockerfile.workspace .')
    test "$*" = "build -t swarm-workspace:latest --build-arg WORKSPACE_GOLANG_BASE=$BASE_REF -f Dockerfile.workspace ."
    attempt=$(grep -c '^build ' "$CALLS")
    if [ "$FAIL_STAGE" = foreign_registry ]; then
      base_error '429 Too Many Requests'
      exit 1
    fi
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
		bases, defaults := 0, 0
		for _, line := range strings.Split(strings.ReplaceAll(string(raw), "\\\n", " "), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			switch strings.ToUpper(strings.Fields(line)[0]) {
			case "FROM":
				if line != "FROM ${WORKSPACE_GOLANG_BASE}" || defaults != 1 {
					return fmt.Errorf("workspace base is not the keyed argument with ECR default")
				}
				bases++
			case "ARG":
				if strings.HasPrefix(line, "ARG WORKSPACE_GOLANG_BASE") {
					if line != "ARG WORKSPACE_GOLANG_BASE="+workspaceECRBase || bases != 0 {
						return fmt.Errorf("local/server2/fork default no longer uses exact ECR pin")
					}
					defaults++
				}
			case "RUN", "USER", "WORKDIR":
			default:
				return fmt.Errorf("new Dockerfile dependency requires cache-key review: %s", line)
			}
		}
		if bases != 1 || defaults != 1 {
			return fmt.Errorf("workspace base count %d", bases)
		}
		return nil
	}
	if err := validate(raw); err != nil {
		t.Fatal(err)
	}
	for _, extra := range []string{"COPY . /workspace", "ADD https://example.invalid/source /workspace", "RUN --mount=type=bind,target=/input true", "FROM golang:1.25-bookworm", "# syntax=docker/dockerfile:1", "#syntax = docker/dockerfile:1", "# escape=`", "ARG WORKSPACE_GOLANG_BASE=" + workspaceGHCRBase} {
		if validate(append(append([]byte(nil), raw...), []byte("\n"+extra+"\n")...)) == nil {
			t.Fatalf("unkeyed input accepted: %s", extra)
		}
	}
	wrongDefault := strings.Replace(string(raw), workspaceECRBase, workspaceGHCRBase, 1)
	if validate([]byte(wrongDefault)) == nil {
		t.Fatal("private registry became the ambient local/fork default")
	}
	link, err := os.ReadFile(filepath.Join("../..", "Dockerfile.workspace-base-link"))
	if err != nil || string(link) != "FROM scratch\nLABEL org.opencontainers.image.source=\"https://github.com/division-sh/swarm\"\n" {
		t.Fatalf("association-only image gained layers/executable or wrong repository label: %v", err)
	}
	assertWorkspaceMirrorVerifier(t)
}

func assertWorkspaceMirrorVerifier(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "python3", "-c", workspaceMirrorVerifierControls)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("fixed mirror verifier refused its controls: %v\n%s", err, output)
	}
}

const workspaceMirrorVerifierControls = `
import hashlib, io, json, os, pathlib, urllib.error, urllib.request
from unittest import mock

path = pathlib.Path("../../.github/scripts/verify-workspace-base.py")
scope = {"__name__": "workspace_mirror_owned_control"}
exec(compile(path.read_bytes(), str(path), "exec"), scope)
digest = lambda value: "sha256:" + hashlib.sha256(value).hexdigest()
encode = lambda value: json.dumps(value, sort_keys=True).encode()
description = lambda value: {"digest": digest(value), "size": len(value)}
config, layer, auxiliary = b"{}", b"fixed base filesystem", b"fixed auxiliary attestation"
leaf = encode({"schemaVersion": 2, "config": description(config), "layers": [description(layer)]})
aux = encode({"schemaVersion": 2, "config": description(config), "layers": [description(auxiliary)]})
index = encode({"schemaVersion": 2, "manifests": [
    {**description(leaf), "platform": {"os": "linux", "architecture": "amd64"}},
    {**description(aux), "platform": {"os": "unknown", "architecture": "unknown"}},
]})
manifests = {digest(index): index, digest(leaf): leaf, digest(aux): aux}
blobs = {digest(config): config, digest(layer): layer, digest(auxiliary): auxiliary}
assert scope["DIGEST"] == "sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437"
# Test data changes the isolated function globals, never a production override.
scope["DIGEST"] = digest(index)
os.environ.update(GH_TOKEN="owned-fake-token", GITHUB_ACTOR="owned-actor", GITHUB_SHA="a" * 40, GITHUB_REPOSITORY_ID="23")

class FixedResponses:
    def __init__(self, fault):
        self.fault, self.read_blobs = fault, set()
    def open(self, request, timeout):
        assert timeout == 60
        url, auth = request.full_url, request.get_header("Authorization")
        if url == "https://api.github.com/orgs/division-sh/packages/container/swarm-workspace-golang":
            assert auth == "Bearer owned-fake-token"
            if self.fault in ("absent", "forbidden"):
                raise urllib.error.HTTPError(url, 404 if self.fault == "absent" else 403, "refused", {}, None)
            package = {"id": 17, "name": "swarm-workspace-golang", "package_type": "container", "visibility": "private", "repository": {"id": 23, "full_name": "division-sh/swarm"}}
            if self.fault == "public": package["visibility"] = "public"
            if self.fault == "foreign-repository": package["repository"]["full_name"] = "foreign/swarm"
            if self.fault == "crossed-repository-id": package["repository"]["id"] = 99
            return io.BytesIO(encode(package))
        if url.startswith("https://public.ecr.aws/token/"):
            assert auth == ""
            return io.BytesIO(encode({"token": "ecr-read"}))
        if url.startswith("https://ghcr.io/token?"):
            assert auth.startswith("Basic ")
            return io.BytesIO(encode({"token": "mirror-read"}))
        source, mirror = scope["SOURCE"], scope["MIRROR"]
        if url.startswith(source + "/manifests/") or url.startswith(mirror + "/manifests/"):
            is_mirror = url.startswith(mirror)
            assert auth == ("Bearer mirror-read" if is_mirror else "Bearer ecr-read")
            key = url.rsplit("/", 1)[1]
            if is_mirror and self.fault == "missing-auxiliary" and key == digest(aux):
                raise urllib.error.HTTPError(url, 404, "absent", {}, None)
            raw = manifests[key]
            if is_mirror and self.fault == "manifest-mismatch": raw += b" "
            return io.BytesIO(raw)
        if url.startswith(mirror + "/blobs/"):
            assert auth == "Bearer mirror-read"
            key = url.rsplit("/", 1)[1]
            self.read_blobs.add(key)
            if self.fault == "missing-layer" and key == digest(layer):
                raise urllib.error.HTTPError(url, 404, "absent", {}, None)
            raw = blobs[key]
            if self.fault == "config-mismatch" and key == digest(config): raw = b"[]"
            if self.fault == "layer-mismatch" and key == digest(layer): raw = b"changed"
            if self.fault == "auxiliary-mismatch" and key == digest(auxiliary): raw = b"changed"
            return io.BytesIO(raw)
        raise AssertionError("unexpected network target: " + url)

for fault in [None, "absent", "forbidden", "public", "foreign-repository", "crossed-repository-id", "manifest-mismatch", "missing-auxiliary", "missing-layer", "config-mismatch", "layer-mismatch", "auxiliary-mismatch"]:
    responses = FixedResponses(fault)
    with mock.patch.object(scope["urllib"].request, "build_opener", return_value=responses):
        try:
            result = scope["verify"]()
        except (ValueError, urllib.error.HTTPError) as error:
            assert fault is not None, fault
            if fault in ("absent", "missing-auxiliary", "missing-layer", "forbidden"):
                assert isinstance(error, urllib.error.HTTPError) and error.code == (403 if fault == "forbidden" else 404)
            elif fault in ("public", "foreign-repository", "crossed-repository-id"):
                assert str(error) == "private package/repository association refused; user policy action required"
            elif fault == "manifest-mismatch":
                assert str(error) == "manifest bytes differ from immutable digest"
            else:
                assert str(error) == "blob bytes differ from immutable descriptor"
        else:
            assert fault is None, fault
            assert result["qualification"] is False and result["digest"] == digest(index)
            assert result["manifests"] == {key: len(value) for key, value in manifests.items()}
            assert result["verified_blobs"] == {key: len(value) for key, value in blobs.items()}
            assert responses.read_blobs == set(blobs)

for fault in [None, "absent", "forbidden", "public", "foreign-repository", "crossed-repository-id"]:
    for allow_absent in [False, True]:
        try:
            package = scope["linked_package"](FixedResponses(fault), "owned-fake-token", allow_absent)
        except (ValueError, urllib.error.HTTPError):
            assert fault is not None and not (fault == "absent" and allow_absent)
        else:
            assert fault is None or (fault == "absent" and allow_absent)
            assert (package is None) == (fault == "absent")

request = urllib.request.Request(scope["MIRROR"] + "/blobs/" + digest(layer), headers={"Authorization": "Bearer mirror-read"})
redirect = scope["BlobRedirect"]().redirect_request(request, None, 302, "found", {}, "https://blob-storage.invalid/owned-object")
assert redirect.get_header("Authorization") is None
try:
    scope["BlobRedirect"]().redirect_request(request, None, 302, "found", {}, "http://blob-storage.invalid/owned-object")
except ValueError:
    pass
else:
    raise AssertionError("insecure blob redirect admitted")
`
