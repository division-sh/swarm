package testtiming

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type ciWorkflowStep struct {
	Name            string `yaml:"name"`
	If              string `yaml:"if"`
	ContinueOnError bool   `yaml:"continue-on-error"`
	Run             string `yaml:"run"`
}

type ciWorkflowJob struct {
	If    string           `yaml:"if"`
	Needs []string         `yaml:"needs"`
	Steps []ciWorkflowStep `yaml:"steps"`
}

func TestCITimingBudgetContractConsumesCompleteEvidence(t *testing.T) {
	root := testTimingRepoRoot(t)
	workflowPath := filepath.Join(root, ".github", "workflows", "ci.yml")
	raw, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatalf("read workflow: %v", err)
	}
	var workflow struct {
		Jobs map[string]ciWorkflowJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &workflow); err != nil {
		t.Fatalf("parse workflow: %v", err)
	}

	timingJob, ok := workflow.Jobs["timing-budget"]
	if !ok {
		t.Fatal("workflow missing timing-budget job")
	}
	if timingJob.If != "always()" {
		t.Fatalf("timing-budget if = %q, want always()", timingJob.If)
	}
	wantTimingNeeds := []string{"ci-plan", "full-conformance", "test-shard"}
	gotTimingNeeds := append([]string(nil), timingJob.Needs...)
	sort.Strings(gotTimingNeeds)
	if !reflect.DeepEqual(gotTimingNeeds, wantTimingNeeds) {
		t.Fatalf("timing-budget needs = %v, want %v", gotTimingNeeds, wantTimingNeeds)
	}
	for _, name := range []string{"Download shard evidence", "Download full-conformance evidence"} {
		found := findWorkflowStep(timingJob.Steps, name)
		if found == nil || !found.ContinueOnError {
			t.Fatalf("%s must continue to aggregate INCOMPLETE evidence: %+v", name, found)
		}
	}
	evaluate := findWorkflowStep(timingJob.Steps, "Evaluate complete timing evidence")
	if evaluate == nil || evaluate.If != "always()" {
		t.Fatalf("aggregate evaluation step = %+v, want if always()", evaluate)
	}
	for _, want := range []string{
		"-evaluate-budget",
		"-policy .github/test-timing-budgets.yaml",
		"-snapshot .github/test-shards/go-test-shards.json",
		"-full-packages .github/test-shards/full-conformance-packages.txt",
		"-result-json test-results/timing-budget-result.json",
	} {
		if !strings.Contains(evaluate.Run, want) {
			t.Fatalf("aggregate evaluation missing %q:\n%s", want, evaluate.Run)
		}
	}
	for _, forbidden := range []string{"semantic-smoke", "nightly-extras"} {
		if strings.Contains(evaluate.Run, forbidden) {
			t.Fatalf("hard aggregate consumes advisory surface %q", forbidden)
		}
	}

	required := workflow.Jobs["required-tests"]
	if required.If != "always()" || !containsString(required.Needs, "timing-budget") {
		t.Fatalf("required-tests does not consume timing-budget under always(): %+v", required)
	}
	for _, jobName := range []string{"test-shard", "full-conformance"} {
		job := workflow.Jobs[jobName]
		uploadName := "Upload shard timing artifact"
		if jobName == "full-conformance" {
			uploadName = "Upload full conformance artifact"
		}
		upload := findWorkflowStep(job.Steps, uploadName)
		if upload == nil || upload.If != "always()" {
			t.Fatalf("%s upload step = %+v, want if always()", jobName, upload)
		}
		runName := "Run shard tests"
		if jobName == "full-conformance" {
			runName = "Run full catalog/conformance truth"
		}
		run := findWorkflowStep(job.Steps, runName)
		for _, want := range []string{"-record-evidence", "-check-confirmation", "-count=1"} {
			if run == nil || !strings.Contains(run.Run, want) {
				t.Fatalf("%s producer missing %q", jobName, want)
			}
		}
	}

	workflowText := string(raw)
	for _, forbidden := range []string{
		"limit_seconds: 270",
		"limit_seconds: 330",
		"shard-${{ matrix.shard }}-elapsed.txt",
		"elapsed wall time",
	} {
		if strings.Contains(workflowText, forbidden) {
			t.Fatalf("workflow retains non-authoritative timing path %q", forbidden)
		}
	}
}

func TestCommittedTimingPolicyAndPackageDeclarationsAreValid(t *testing.T) {
	root := testTimingRepoRoot(t)
	policyFile, err := os.Open(filepath.Join(root, ".github", "test-timing-budgets.yaml"))
	if err != nil {
		t.Fatalf("open policy: %v", err)
	}
	defer policyFile.Close()
	policy, err := LoadBudgetPolicy(policyFile)
	if err != nil {
		t.Fatalf("LoadBudgetPolicy: %v", err)
	}
	if policy.Hard.MaxShardCommandSeconds.LimitSeconds != 270 {
		t.Fatalf("broad command limit = %v, want 270", policy.Hard.MaxShardCommandSeconds.LimitSeconds)
	}
	if policy.Hard.FullConformanceCommandSeconds.LimitSeconds != 330 {
		t.Fatalf("full command limit = %v, want 330", policy.Hard.FullConformanceCommandSeconds.LimitSeconds)
	}

	snapshotRaw, err := os.ReadFile(filepath.Join(root, ".github", "test-shards", "go-test-shards.json"))
	if err != nil {
		t.Fatalf("read shard snapshot: %v", err)
	}
	var snapshot ShardSnapshot
	if err := json.Unmarshal(snapshotRaw, &snapshot); err != nil {
		t.Fatalf("decode shard snapshot: %v", err)
	}
	if err := validateShardIDs(snapshot); err != nil {
		t.Fatalf("invalid shard snapshot: %v", err)
	}
	fullFile, err := os.Open(filepath.Join(root, ".github", "test-shards", "full-conformance-packages.txt"))
	if err != nil {
		t.Fatalf("open full packages: %v", err)
	}
	defer fullFile.Close()
	fullPackages, err := ReadPackageList(fullFile)
	if err != nil {
		t.Fatalf("read full packages: %v", err)
	}
	wantFull := []string{
		"github.com/division-sh/swarm/cmd/swarm",
		"github.com/division-sh/swarm/internal/apiv1",
		"github.com/division-sh/swarm/internal/dashboard/server",
		"github.com/division-sh/swarm/internal/runtime/bus",
		"github.com/division-sh/swarm/internal/runtime/cataloge2e",
		"github.com/division-sh/swarm/internal/runtime/conformance",
		"github.com/division-sh/swarm/internal/runtime/pipeline",
	}
	if !reflect.DeepEqual(fullPackages, wantFull) {
		t.Fatalf("full packages = %v, want unchanged supported set %v", fullPackages, wantFull)
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

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func testTimingRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return root
}
