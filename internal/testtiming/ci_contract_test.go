package testtiming

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
	"gopkg.in/yaml.v3"
)

func TestCIJobCollectionWaitsOnlyForTerminalEvidence(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(testTimingRepoRoot(t), ".github/workflows/ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]ciWorkflowJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	checkout := workflow.Jobs["timing-budget"].Steps[0]
	if checkout.Uses != "actions/checkout@v4" || checkout.With["fetch-depth"] != 0 || checkout.With["ref"] != "${{ needs.ci-plan.outputs.execution_sha }}" {
		t.Fatal("cadence observation requires complete history at the exact execution source")
	}
	step := findWorkflowStep(workflow.Jobs["timing-budget"].Steps, "Evaluate complete plan-bound evidence")
	if step == nil {
		t.Fatal("missing timing evaluation")
	}
	const start = "if jq -e --slurpfile plan test-results/plan/proof-plan.json '\n"
	const end = "' test-results/jobs.json >/dev/null; then"
	from := strings.Index(step.Run, start)
	if from < 0 {
		t.Fatal("missing planned-job terminal predicate")
	}
	from += len(start)
	to := strings.Index(step.Run[from:], end)
	if to < 0 {
		t.Fatal("missing terminal predicate boundary")
	}
	predicate := step.Run[from : from+to]
	assertAt := strings.Index(step.Run, "-assert-execution-sha")
	fetchAt := strings.Index(step.Run, "gh api --paginate --slurp")
	evaluateAt := strings.Index(step.Run, "-evaluate-budget")
	if assertAt < 0 || fetchAt <= assertAt || evaluateAt <= fetchAt ||
		!strings.Contains(step.Run, `-event "$GITHUB_EVENT_NAME"`) ||
		!strings.Contains(step.Run, "attempts/$GITHUB_RUN_ATTEMPT/jobs?per_page=100") ||
		!strings.Contains(step.Run, "deadline=$((SECONDS + 600))") ||
		!strings.Contains(step.Run, "if [ \"$ready\" != true ]; then") ||
		!strings.Contains(step.Run, "exit 1") ||
		!strings.Contains(step.Run, "remaining < 5 ? remaining : 5") ||
		strings.Count(step.Run, "-evaluate-budget") != 1 {
		t.Fatal("job collection must be bounded, attempt-scoped, and precede one fail-closed evaluation")
	}

	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.json")
	jobsPath := filepath.Join(dir, "jobs.json")
	for _, tc := range []struct {
		name, plan, jobs string
		ready            bool
	}{
		{"complete", `{"batches":[{"id":"one"},{"id":"two"}]}`, `[{"jobs":[{"name":"Go proof one","status":"completed"},{"name":"Go proof two","status":"completed"}]}]`, true},
		{"stale successful job", `{"batches":[{"id":"one"},{"id":"two"}]}`, `[{"jobs":[{"name":"Go proof one","status":"completed"},{"name":"Go proof two","status":"in_progress"}]}]`, false},
		{"missing job", `{"batches":[{"id":"one"},{"id":"two"}]}`, `[{"jobs":[{"name":"Go proof one","status":"completed"}]}]`, false},
		{"duplicate job", `{"batches":[{"id":"one"},{"id":"two"}]}`, `[{"jobs":[{"name":"Go proof one","status":"completed"},{"name":"Go proof two","status":"completed"},{"name":"Go proof two","status":"completed"}]}]`, false},
		{"terminal failure reaches evaluator", `{"batches":[{"id":"one"},{"id":"two"}]}`, `[{"jobs":[{"name":"Go proof one","status":"completed"},{"name":"Go proof two","status":"completed","conclusion":"failure"}]}]`, true},
		{"empty plan", `{"batches":[]}`, `[{"jobs":[]}]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(planPath, []byte(tc.plan), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(jobsPath, []byte(tc.jobs), 0600); err != nil {
				t.Fatal(err)
			}
			output, err := exec.Command("jq", "-e", "--slurpfile", "plan", planPath, predicate, jobsPath).CombinedOutput()
			if got := err == nil; got != tc.ready {
				t.Fatalf("ready = %t, want %t; jq output: %s; error: %v", got, tc.ready, output, err)
			}
		})
	}
}

type ciWorkflowStep struct {
	ID              string            `yaml:"id"`
	Name            string            `yaml:"name"`
	If              string            `yaml:"if"`
	ContinueOnError bool              `yaml:"continue-on-error"`
	Run             string            `yaml:"run"`
	Uses            string            `yaml:"uses"`
	With            map[string]any    `yaml:"with"`
	Env             map[string]string `yaml:"env"`
}

func TestMacOSPossessionSelectsSQLiteMatrixWithoutPostgres(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(testTimingRepoRoot(t), ".github/workflows/ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]ciWorkflowJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, step := range workflow.Jobs["macos-sqlite-possession"].Steps {
		if !strings.Contains(step.Run, "go test ./internal/store/internal/startupownership") {
			continue
		}
		found = true
		// Keep both mixed roots' SQLite cases; only their PostgreSQL children
		// and the four PostgreSQL-only roots require excluded resources.
		want := "go test ./internal/store/internal/startupownership -skip '^(TestPostgresPossessionObservationUsesIndependentExactSessionAndKey|TestPostgresGenerationMonitorFencesStalledNativePhase|TestPostgresGenerationMonitorHealthyQueueAndCancellation|TestPostgresGenerationMonitorWithdrawsCapabilityBeforeNativeJoin)$|^(TestPossessionObservationExternalProcessReleaseAndCrashBothStores|TestRuntimeLogoutWithdrawalRefusesInvalidTemporalEvidenceBothStores)$/^postgres$' -count=1"
		if strings.TrimSpace(step.Run) != want {
			t.Fatalf("SQLite-only qualification no longer selects every SQLite package root: %s", step.Run)
		}
	}
	if !found {
		t.Fatal("missing macOS possession qualification")
	}
	raw, err = os.ReadFile(filepath.Join(testTimingRepoRoot(t), "internal/store/internal/startupownership/possession_observation_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []string{
		`testPossessionObservationExternalProcessReleaseAndCrash(t, []string{"sqlite", "postgres"})`,
		`testPossessionObservationExternalProcessReleaseAndCrash(t, []string{"sqlite"})`,
	} {
		if !strings.Contains(string(raw), call) {
			t.Fatalf("SQLite and both-store entrypoints must retain the same complete process assertion owner: %s", call)
		}
	}
}

func TestCICachesSeparateModulesAndNeverRestoreAnotherProofUnit(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(testTimingRepoRoot(t), ".github/workflows/ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]ciWorkflowJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	plan, proof := workflow.Jobs["ci-plan"], workflow.Jobs["proof-unit"]
	modules := findWorkflowStep(plan.Steps, "Restore shared Go modules")
	consumer := findWorkflowStep(proof.Steps, "Restore shared Go modules")
	if modules == nil || consumer == nil || modules.With["path"] != "~/go/pkg/mod" || consumer.With["path"] != modules.With["path"] || consumer.With["key"] != modules.With["key"] {
		t.Fatal("planner and proofs must share one exact module-only cache")
	}
	for _, identity := range []string{"runner.os", "runner.arch", "steps.go.outputs.go-version", "hashFiles('go.sum')"} {
		if !strings.Contains(modules.With["key"].(string), identity) {
			t.Fatalf("module cache omits %s", identity)
		}
	}
	populate := findWorkflowStep(plan.Steps, "Populate shared Go modules")
	saveModules := findWorkflowStep(plan.Steps, "Save shared Go modules")
	if populate == nil || populate.Run != "python3 .github/scripts/prepare-go-modules.py" || saveModules == nil || saveModules.With["path"] != modules.With["path"] || saveModules.With["key"] != "${{ steps.modules.outputs.cache-primary-key }}" {
		t.Fatal("the planner must populate the complete module graph before saving it")
	}
	build := findWorkflowStep(proof.Steps, "Restore measured Go cache")
	saveBuild := findWorkflowStep(proof.Steps, "Save measured Go cache")
	if build == nil || saveBuild == nil || build.With["path"] != "~/.cache/go-build" || saveBuild.With["path"] != build.With["path"] || saveBuild.With["key"] != "${{ steps.go-cache-restore.outputs.cache-primary-key }}" {
		t.Fatal("build cache must not duplicate modules or use a different save identity")
	}
	seed := findWorkflowStep(plan.Steps, "Restore exact-head production Go cache")
	compile := findWorkflowStep(plan.Steps, "Compile production dependencies once")
	saveSeed := findWorkflowStep(plan.Steps, "Save exact-head production Go cache")
	if seed == nil || compile == nil || saveSeed == nil || seed.With["path"] != build.With["path"] || seed.With["restore-keys"] != nil || !strings.HasSuffix(seed.With["key"].(string), "-${{ github.sha }}") || saveSeed.With["key"] != "${{ steps.production-cache.outputs.cache-primary-key }}" || saveSeed.With["path"] != seed.With["path"] {
		t.Fatal("production seed must be an exact-head Go cache, not another proof or stale seed")
	}
	for _, command := range []string{`test "$(git rev-parse HEAD)" = "$SOURCE_SHA"`, "go build ./...", `go build -race -o "$RUNNER_TEMP/swarm-ci-race" ./cmd/swarm`} {
		if !strings.Contains(compile.Run, command) {
			t.Fatalf("production seed omits exact-head compilation %q", command)
		}
	}
	if strings.Contains(compile.Run, "go test") {
		t.Fatal("cache preparation must not execute or replace a proof")
	}
	seedConsumed := false
	for _, key := range strings.Split(build.With["key"].(string)+"\n"+build.With["restore-keys"].(string), "\n") {
		if key == seed.With["key"] {
			seedConsumed = true
			continue
		}
		if strings.TrimSpace(key) != "" && (!strings.Contains(key, "matrix.unit") || !strings.Contains(key, "steps.go.outputs.go-version")) {
			t.Fatalf("build fallback can select another proof/toolchain: %s", key)
		}
	}
	if !seedConsumed {
		t.Fatal("workers do not consume the exact production seed")
	}
	for _, step := range proof.Steps {
		if step.Name == "Save shared Go modules" || (step.Name == "Run exact planned proof unit" && step.If != "") {
			t.Fatal("proofs must execute on cache hit/miss and not republish module archives")
		}
	}
}

type ciWorkflowJob struct {
	Name           string            `yaml:"name"`
	If             string            `yaml:"if"`
	Needs          []string          `yaml:"needs"`
	Environment    string            `yaml:"environment"`
	Env            map[string]string `yaml:"env"`
	Permissions    map[string]string `yaml:"permissions"`
	Steps          []ciWorkflowStep  `yaml:"steps"`
	TimeoutMinutes any               `yaml:"timeout-minutes"`
	RunsOn         string            `yaml:"runs-on"`
	Strategy       struct {
		FailFast    *any   `yaml:"fail-fast"`
		Matrix      string `yaml:"matrix"`
		MaxParallel any    `yaml:"max-parallel"`
	} `yaml:"strategy"`
}

func TestCIConsumesOnePlanAndCompletePlanBoundEvidence(t *testing.T) {
	root := testTimingRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]ciWorkflowJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatalf("parse ci.yml: %v", err)
	}
	for _, name := range []string{"ci-plan", "proof-unit", "timing-budget", "required-tests", "publish-timing-model"} {
		if _, ok := workflow.Jobs[name]; !ok {
			t.Fatalf("workflow missing canonical job %s", name)
		}
	}
	plan := findWorkflowStep(workflow.Jobs["ci-plan"].Steps, "Plan proof topology")
	for _, want := range []string{"-plan-ci", "-proof-policy .github/test-proof-plan.yaml", "-weight-model .github/test-timing-weights.json", "-plan test-results/proof-plan.json", "-matrix test-results/proof-matrix.json"} {
		if plan == nil || !strings.Contains(plan.Run, want) {
			t.Fatalf("planner step missing %q", want)
		}
	}
	producer := findWorkflowStep(workflow.Jobs["proof-unit"].Steps, "Run exact planned proof unit")
	if producer == nil || producer.Run != "bash .github/scripts/run-proof-batch.sh" || producer.Env["KEEP_GOING"] != "true" {
		t.Fatal("missing isolated batch consumer")
	}
	batchScript, err := os.ReadFile(filepath.Join(root, ".github/scripts/run-proof-batch.sh"))
	if err != nil {
		t.Fatal(err)
	}
	producer.Run = string(batchScript)
	for _, want := range []string{"-record-evidence", "-plan \"$plan\"", "-unit \"$UNIT_ID\""} {
		if producer == nil || !strings.Contains(producer.Run, want) {
			t.Fatalf("proof producer missing %q", want)
		}
	}
	aggregate := findWorkflowStep(workflow.Jobs["timing-budget"].Steps, "Evaluate complete plan-bound evidence")
	for _, want := range []string{"-evaluate-budget", "-plan test-results/plan/proof-plan.json", "-evidence-root test-results/evidence", "-workflow-head-sha \"$WORKFLOW_HEAD_SHA\""} {
		if aggregate == nil || !strings.Contains(aggregate.Run, want) {
			t.Fatalf("aggregate missing %q", want)
		}
	}
	if aggregate.Env["WORKFLOW_HEAD_SHA"] != "${{ github.event.pull_request.head.sha || github.sha }}" {
		t.Fatal("job identity must come from the triggering event, not the merge checkout or jobs evidence")
	}
	if !strings.Contains(workflow.Jobs["required-tests"].Name, "Required test summary") || !strings.Contains(workflow.Jobs["sqlite-local-dev"].Name, "SQLite local smoke") {
		t.Fatal("stable branch-protection check names drifted")
	}
	if !strings.Contains(workflow.Jobs["required-tests"].Name, "Full dispatch summary") || !strings.Contains(workflow.Jobs["sqlite-local-dev"].Name, "Full dispatch SQLite smoke") {
		t.Fatal("manual dispatch must not emit duplicate required contexts")
	}
	for _, step := range []*ciWorkflowStep{producer, aggregate} {
		if step == nil || !strings.Contains(step.Run, "-assert-execution-sha") || !strings.Contains(step.Run, "git rev-parse HEAD") {
			t.Fatal("plan-bound consumer does not assert its actual checkout SHA")
		}
	}
	if !strings.Contains(string(raw), "execution_sha=$(git rev-parse HEAD)") || !strings.Contains(string(raw), "ref: ${{ needs.ci-plan.outputs.execution_sha }}") {
		t.Fatal("workflow does not bind plan and consumer checkouts to the executed SHA")
	}
	for _, forbidden := range []string{
		"-check-confirmation",
		"-attempt confirmation",
		"confirmation_status",
		"go-test-shards.json",
		"full-conformance-packages.txt",
		"full-conformance:",
		"shard_matrix",
		"run_full_conformance",
		"test_count_flag",
		"-generate-shards",
		"-check-shards",
		"-shard-packages",
	} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("workflow retains old authority %q", forbidden)
		}
	}
}

func TestPublisherIsMasterRestrictedGeneratedOnlyAndReviewRequired(t *testing.T) {
	root := testTimingRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var workflow struct {
		Jobs map[string]ciWorkflowJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatal(err)
	}
	publisher := workflow.Jobs["publish-timing-model"]
	if publisher.Environment != "timing-model-publisher" || !strings.Contains(publisher.If, "schedule") {
		t.Fatalf("publisher environment/trigger = %q / %q", publisher.Environment, publisher.If)
	}
	text := string(raw)
	for _, want := range []string{
		"vars.TEST_MODEL_PUBLISHER_APP_ID",
		"secrets.TEST_MODEL_PUBLISHER_PRIVATE_KEY",
		"repositories: swarm",
		"permission-actions: write",
		"permission-contents: write",
		"permission-pull-requests: write",
		"automation/test-timing-model",
		"staging_branch=automation/test-timing-model-build",
		"git diff --name-only",
		"repos/division-sh/swarm/contents/.github/test-timing-weights.json",
		`-f branch="$staging_branch"`,
		`-f sha="$generated_sha"`,
		"human review and normal PR checks required",
		`gh pr list --head "$branch"`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("publisher contract missing %q", want)
		}
	}
	for _, forbidden := range []string{"gh workflow run", "gh pr merge", "git push origin master", "git commit", "GH_PAT", "PERSONAL_ACCESS_TOKEN"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("publisher retains forbidden authority %q", forbidden)
		}
	}
	if strings.Contains(text, `gh pr list --head "division-sh:$branch"`) {
		t.Fatal("publisher uses unsupported owner-qualified gh pr list head filter")
	}
}

func validateServeappPolicyPartition(policy testplanning.Policy, dir, profileName string) error {
	profile, ok := policy.Profiles[profileName]
	if !ok {
		return fmt.Errorf("missing serveapp proof profile %s", profileName)
	}
	var runs []string
	for _, id := range profile.Units {
		unit := policy.Units[id]
		if !slices.Contains(unit.Packages, policy.Module+"/internal/serveapp") {
			continue
		}
		if !slices.Equal(unit.Packages, []string{policy.Module + "/internal/serveapp"}) || unit.Run == "" || unit.Skip != "" || unit.CountMode != "count-1" || unit.BudgetClass != "full" {
			return fmt.Errorf("%s unit %s weakens the complete uncached serveapp partition: %+v", profileName, id, unit)
		}
		runs = append(runs, unit.Run)
	}
	if profileName == testplanning.ProfileLifecycle {
		// Validate the exhaustive selected-plus-deferred union. This single
		// full-only owner is not lifecycle execution credit.
		stress := policy.Units["serveapp-i-reporter-all-commit"]
		if stress.Run != testplanning.ServedReporterStressRun || !slices.Contains(policy.Profiles[testplanning.ProfileFull].Units, "serveapp-i-reporter-all-commit") || slices.Contains(profile.Units, "serveapp-i-reporter-all-commit") {
			return fmt.Errorf("lifecycle reporter deferral lost its exact full stress owner")
		}
		runs = append(runs, stress.Run)
	}
	if err := testplanning.ValidateGoProofPartition(dir, runs); err != nil {
		return fmt.Errorf("%s serveapp partition: %w", profileName, err)
	}
	return nil
}

func TestServeappPolicyPartitionRejectsLostProof(t *testing.T) {
	root := testTimingRepoRoot(t)
	for _, id := range []string{"serveapp-2564-h1", "serveapp-2564-h2", "serveapp-other-late", "serveapp-delayed-commit-preservation"} {
		for _, profileName := range []string{testplanning.ProfileLifecycle, testplanning.ProfileFull} {
			for _, change := range []struct {
				name string
				edit func(*testplanning.Policy)
			}{
				{"omitted_root", func(p *testplanning.Policy) {
					unit := p.Units[id]
					unit.Run = "^$"
					p.Units[id] = unit
				}},
				{"overlap", func(p *testplanning.Policy) {
					unit := p.Units[id]
					unit.Run = "^TestIssue2564.*$"
					p.Units[id] = unit
				}},
				{"partial_backend", func(p *testplanning.Policy) {
					unit := p.Units[id]
					unit.Run += "/sqlite"
					p.Units[id] = unit
				}},
				{"skipped_backend", func(p *testplanning.Policy) {
					unit := p.Units[id]
					unit.Skip = "postgres"
					p.Units[id] = unit
				}},
				{"cached", func(p *testplanning.Policy) {
					unit := p.Units[id]
					unit.CountMode = "cache-default"
					p.Units[id] = unit
				}},
				{"missing_profile_owner", func(p *testplanning.Policy) {
					profile := p.Profiles[profileName]
					profile.Units = slices.DeleteFunc(profile.Units, func(member string) bool { return member == id })
					p.Profiles[profileName] = profile
				}},
			} {
				t.Run(id+"/"+profileName+"/"+change.name, func(t *testing.T) {
					file, err := os.Open(filepath.Join(root, ".github", "test-proof-plan.yaml"))
					if err != nil {
						t.Fatal(err)
					}
					policy, err := testplanning.LoadPolicy(file)
					_ = file.Close()
					if err != nil {
						t.Fatal(err)
					}
					if err := validateServeappPolicyPartition(policy, filepath.Join(root, "internal", "serveapp"), profileName); err != nil {
						t.Fatalf("committed positive control: %v", err)
					}
					change.edit(&policy)
					if err := validateServeappPolicyPartition(policy, filepath.Join(root, "internal", "serveapp"), profileName); err == nil {
						t.Fatal("lost or weakened serveapp proof accepted")
					}
				})
			}
		}
	}
}

func assertGoProofPartition(t *testing.T, dir string, patterns []*regexp.Regexp) {
	t.Helper()
	runs := make([]string, len(patterns))
	for i, pattern := range patterns {
		runs[i] = pattern.String()
	}
	if err := testplanning.ValidateGoProofPartition(dir, runs); err != nil {
		t.Fatal(err)
	}
}

func collectProjectionValues(node *yaml.Node, out map[string]bool) {
	if node == nil {
		return
	}
	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			key, value := node.Content[i], node.Content[i+1]
			if key.Value == "projection" || strings.HasSuffix(key.Value, "proof_projection") {
				out[strings.TrimSpace(value.Value)] = true
			}
			collectProjectionValues(value, out)
		}
		return
	}
	for _, child := range node.Content {
		collectProjectionValues(child, out)
	}
}

func assertCoreStructuralGuardTimingInventory(t *testing.T, policy testplanning.Policy) {
	t.Helper()
	const id = "core-structural-owner-guards"
	unit, exists := policy.Units[id]
	wantPackages := []string{
		policy.Module + "/internal/releasee2e", policy.Module + "/internal/runtime",
		policy.Module + "/internal/serveapp", policy.Module + "/internal/store/internal/runtimepersistence",
	}
	if !exists || !slices.Equal(unit.Packages, wantPackages) || unit.CountMode != "count-1" ||
		unit.EnvironmentID != "" || unit.EnvironmentIDs[testplanning.VenueCI] != "ci-postgres-gateway-empty-v1" || unit.EnvironmentIDs[testplanning.VenueLocal] != "local-postgres-gateway-empty-v1" || unit.BudgetClass != "broad" ||
		unit.Skip != "" || unit.GoTimeout != "" || unit.Packable || len(unit.RequiredChildren) != 0 {
		t.Fatalf("core structural guard timing envelope changed: %+v", unit)
	}
	for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
		if slices.Contains(policy.Profiles[tier].Units, id) != (tier == testplanning.ProfileCore) {
			t.Fatalf("%s changed core-only guard scheduling", tier)
		}
	}
}

func TestCommandEvidenceJSONRejectsLegacyVersion(t *testing.T) {
	raw := []byte(`{"version":1}`)
	var evidence CommandEvidence
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	plan := testplanning.RunPlan{}
	if problems := ValidateCommandEvidence(evidence, plan); len(problems) == 0 {
		t.Fatal("legacy evidence was accepted")
	}
}

func findWorkflowStep(steps []ciWorkflowStep, name string) *ciWorkflowStep {
	for i := range steps {
		if steps[i].Name == name {
			return &steps[i]
		}
	}
	return nil
}

func testTimingRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
