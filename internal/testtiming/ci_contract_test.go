package testtiming

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
	"gopkg.in/yaml.v3"
)

type ciWorkflowStep struct {
	Name            string         `yaml:"name"`
	If              string         `yaml:"if"`
	ContinueOnError bool           `yaml:"continue-on-error"`
	Run             string         `yaml:"run"`
	Uses            string         `yaml:"uses"`
	With            map[string]any `yaml:"with"`
}

type ciWorkflowJob struct {
	Name        string           `yaml:"name"`
	If          string           `yaml:"if"`
	Needs       []string         `yaml:"needs"`
	Environment string           `yaml:"environment"`
	Steps       []ciWorkflowStep `yaml:"steps"`
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
	for _, want := range []string{"-record-evidence", "-plan \"$plan\"", "-unit \"$UNIT_ID\"", "-check-confirmation"} {
		if producer == nil || !strings.Contains(producer.Run, want) {
			t.Fatalf("proof producer missing %q", want)
		}
	}
	aggregate := findWorkflowStep(workflow.Jobs["timing-budget"].Steps, "Evaluate complete plan-bound evidence")
	for _, want := range []string{"-evaluate-budget", "-plan test-results/plan/proof-plan.json", "-evidence-root test-results/evidence"} {
		if aggregate == nil || !strings.Contains(aggregate.Run, want) {
			t.Fatalf("aggregate missing %q", want)
		}
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
		"gh workflow run ci.yml",
		"human review and normal protection required",
		`gh pr list --head "$branch"`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("publisher contract missing %q", want)
		}
	}
	for _, forbidden := range []string{"gh pr merge", "git push origin master", "git commit", "GH_PAT", "PERSONAL_ACCESS_TOKEN"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("publisher retains forbidden authority %q", forbidden)
		}
	}
	if strings.Contains(text, `gh pr list --head "division-sh:$branch"`) {
		t.Fatal("publisher uses unsupported owner-qualified gh pr list head filter")
	}
}

func TestCommittedPolicyModelAndProjectionConsumersAreCanonical(t *testing.T) {
	root := testTimingRepoRoot(t)
	policyFile, err := os.Open(filepath.Join(root, ".github", "test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	policy, err := testplanning.LoadPolicy(policyFile)
	_ = policyFile.Close()
	if err != nil {
		t.Fatalf("load proof policy: %v", err)
	}
	modelFile, err := os.Open(filepath.Join(root, ".github", "test-timing-weights.json"))
	if err != nil {
		t.Fatal(err)
	}
	model, err := testplanning.LoadWeightModel(modelFile)
	_ = modelFile.Close()
	if err != nil || len(model.Packages) == 0 {
		t.Fatalf("load weight model: %v", err)
	}
	budgetFile, err := os.Open(filepath.Join(root, ".github", "test-timing-budgets.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadBudgetPolicy(budgetFile); err != nil {
		t.Fatalf("load budget policy: %v", err)
	}
	_ = budgetFile.Close()

	const (
		runtimePackage            = "github.com/division-sh/swarm/internal/runtime"
		serveappPackage           = "github.com/division-sh/swarm/internal/serveapp"
		storePackage              = "github.com/division-sh/swarm/internal/store"
		runtimePersistencePackage = "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
		testPostgresPackage       = "github.com/division-sh/swarm/internal/testpostgres"
	)
	runtimeIsSpecial := false
	serveappIsSpecial := false
	storeIsSpecial := false
	runtimePersistenceIsSpecial := false
	testPostgresIsSpecial := false
	for _, pkg := range policy.SpecialPackages {
		if pkg == runtimePackage {
			runtimeIsSpecial = true
		}
		if pkg == serveappPackage {
			serveappIsSpecial = true
		}
		if pkg == storePackage {
			storeIsSpecial = true
		}
		if pkg == runtimePersistencePackage {
			runtimePersistenceIsSpecial = true
		}
		if pkg == testPostgresPackage {
			testPostgresIsSpecial = true
		}
	}
	if !runtimeIsSpecial {
		t.Fatal("internal/runtime must remain isolated from broad package co-scheduling")
	}
	if !serveappIsSpecial {
		t.Fatal("internal/serveapp must remain isolated from broad package co-scheduling")
	}
	if !storeIsSpecial {
		t.Fatal("internal/store must remain isolated from broad package co-scheduling")
	}
	if !runtimePersistenceIsSpecial {
		t.Fatal("internal/store/internal/runtimepersistence must remain isolated from broad package co-scheduling")
	}
	if !testPostgresIsSpecial {
		t.Fatal("internal/testpostgres must remain isolated from broad package co-scheduling")
	}
	runtimeUnit, ok := policy.Units["runtime-full"]
	if !ok || len(runtimeUnit.Packages) != 1 || runtimeUnit.Packages[0] != runtimePackage || runtimeUnit.Run != "" || runtimeUnit.CountMode != "count-1" {
		t.Fatalf("runtime-full unit = %#v, want one complete uncached internal/runtime proof", runtimeUnit)
	}
	serveappUnit, ok := policy.Units["serveapp-full"]
	if !ok || !slices.Equal(serveappUnit.Packages, []string{serveappPackage}) || serveappUnit.Run != "" || serveappUnit.CountMode != "count-1" || serveappUnit.BudgetClass != "full" {
		t.Fatalf("serveapp-full unit = %#v, want one complete uncached serveapp proof", serveappUnit)
	}
	storeUnit, ok := policy.Units["store-full"]
	if !ok || !slices.Equal(storeUnit.Packages, []string{storePackage}) || storeUnit.Run != "" || storeUnit.CountMode != "count-1" || storeUnit.BudgetClass != "broad" {
		t.Fatalf("store-full unit = %#v, want complete uncached facade proof", storeUnit)
	}
	storeRuntimeUnits := []string{"store-runtime-full-01", "store-runtime-full-02", "store-runtime-full-03", "store-runtime-full-04"}
	storeRuntimePatterns := make([]*regexp.Regexp, 0, len(storeRuntimeUnits))
	for _, unitID := range storeRuntimeUnits {
		unit, exists := policy.Units[unitID]
		if !exists || !slices.Equal(unit.Packages, []string{runtimePersistencePackage}) || unit.Run == "" || unit.CountMode != "count-1" || unit.BudgetClass != "broad" {
			t.Fatalf("%s unit = %#v, want one filtered uncached runtime-persistence proof", unitID, unit)
		}
		storeRuntimePatterns = append(storeRuntimePatterns, regexp.MustCompile(unit.Run))
	}
	assertGoProofPartition(t, filepath.Join(root, "internal", "store", "internal", "runtimepersistence"), storeRuntimePatterns)
	testPostgresUnit, ok := policy.Units["testpostgres-full"]
	if !ok || !slices.Equal(testPostgresUnit.Packages, []string{testPostgresPackage}) || testPostgresUnit.Run != "" || testPostgresUnit.CountMode != "count-1" || testPostgresUnit.BudgetClass != "broad" {
		t.Fatalf("testpostgres-full unit = %#v, want complete isolated test-manager proof", testPostgresUnit)
	}
	for _, profileName := range []string{testplanning.ProfilePRCommon, testplanning.ProfilePREscalated, testplanning.ProfileFull, testplanning.ProfileNightly} {
		foundRuntime := false
		foundServeapp := false
		foundStore := false
		foundTestPostgres := false
		foundStoreRuntime := map[string]bool{}
		for _, unit := range policy.Profiles[profileName].Units {
			if unit == "runtime-full" {
				foundRuntime = true
			}
			if unit == "serveapp-full" {
				foundServeapp = true
			}
			if unit == "store-full" {
				foundStore = true
			}
			if unit == "testpostgres-full" {
				foundTestPostgres = true
			}
			for _, storeRuntimeUnit := range storeRuntimeUnits {
				if unit == storeRuntimeUnit {
					foundStoreRuntime[storeRuntimeUnit] = true
				}
			}
		}
		if !foundRuntime {
			t.Errorf("profile %s does not include runtime-full", profileName)
		}
		if !foundServeapp {
			t.Errorf("profile %s does not include serveapp-full", profileName)
		}
		if !foundStore {
			t.Errorf("profile %s does not include store-full", profileName)
		}
		if !foundTestPostgres {
			t.Errorf("profile %s does not include testpostgres-full", profileName)
		}
		for _, storeRuntimeUnit := range storeRuntimeUnits {
			if !foundStoreRuntime[storeRuntimeUnit] {
				t.Errorf("profile %s does not include %s", profileName, storeRuntimeUnit)
			}
		}
	}

	for _, rel := range []string{".github/test-shards/go-test-shards.json", ".github/test-shards/full-conformance-packages.txt"} {
		if _, err := os.Stat(filepath.Join(root, rel)); !os.IsNotExist(err) {
			t.Fatalf("old authority survives at %s: %v", rel, err)
		}
	}

	used := map[string]bool{}
	err = filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || (!strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml")) {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(raw), "go test ") {
			t.Errorf("literal Go proof command survives in %s", path)
		}
		var document yaml.Node
		if err := yaml.Unmarshal(raw, &document); err != nil {
			return err
		}
		collectProjectionValues(&document, used)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for projection := range used {
		if _, ok := policy.Projections[projection]; !ok {
			t.Errorf("projection %s is not owned by proof policy", projection)
		}
	}
	for _, required := range []string{"required-full", "catalog-full", "selected-store-fast"} {
		if !used[required] {
			t.Errorf("canonical projection %s has no consumer", required)
		}
	}
}

func assertGoProofPartition(t *testing.T, dir string, patterns []*regexp.Regexp) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	matchedPatterns := make([]bool, len(patterns))
	proofs := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, entry.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil || function.Name == nil {
				continue
			}
			name := function.Name.Name
			if name == "TestMain" || (!strings.HasPrefix(name, "Test") && !strings.HasPrefix(name, "Example") && !strings.HasPrefix(name, "Fuzz")) {
				continue
			}
			proofs++
			matches := 0
			for index, pattern := range patterns {
				if pattern.MatchString(name) {
					matches++
					matchedPatterns[index] = true
				}
			}
			if matches != 1 {
				t.Errorf("runtime-persistence proof %s matches %d store partitions, want exactly one", name, matches)
			}
		}
	}
	if proofs == 0 {
		t.Fatal("runtime-persistence proof inventory is empty")
	}
	for index, matched := range matchedPatterns {
		if !matched {
			t.Errorf("store partition %d matches no runtime-persistence proof", index+1)
		}
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
