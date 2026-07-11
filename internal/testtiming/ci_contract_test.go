package testtiming

import (
	"encoding/json"
	"os"
	"path/filepath"
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
	if workflow.Jobs["required-tests"].Name != "Required test summary" || workflow.Jobs["sqlite-local-dev"].Name != "SQLite local smoke" {
		t.Fatal("stable branch-protection check names drifted")
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
