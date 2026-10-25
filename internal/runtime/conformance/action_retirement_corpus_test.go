package conformance

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

const actionRetirementCorpusPath = "internal/runtime/conformance/testdata/action_retirement_corpus_ledger.json"

// This is a lexical source/reference census, not a count of executable actions.
// Per-file dispositions and named proof rows must not be inferred from a match.
var actionRetirementCorpusPattern = regexp.MustCompile(`\b(create_flow_instance|record_evidence|mailbox_write|artifact_repo_commit|config_from|instance_id_from|evidence_target|[A-Za-z0-9_]*(ArtifactRepo|MailboxWrite|ActionSpec|ConfigFrom|EvidenceTarget|HandlerAction|ActionRegistry|ActionRunner|ActionExecution|EmitSurfaceAction|ActionInstruction)[A-Za-z0-9_]*)\b`)

func actionRetirementCurrentCorpus(t *testing.T, root string) map[string][]byte {
	return actionRetirementCorpusFiles(t, root, false)
}

func TestActionRetirementCorpusExcludesGeneratedTestResults(t *testing.T) {
	root := t.TempDir()
	generated := filepath.Join(root, "test-results", "plan")
	if err := os.MkdirAll(generated, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(generated, "proof-plan.json"), []byte(`{"name":"ActionRunner"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "source.md"), []byte("ActionRunner"), 0o644); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(root, "nested", "foreign")
	if err := os.MkdirAll(foreign, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, ".git"), []byte("gitdir: elsewhere\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, "source.md"), []byte("ActionRunner"), 0o644); err != nil {
		t.Fatal(err)
	}
	current := actionRetirementCurrentCorpus(t, root)
	if len(current) != 1 || current["source.md"] == nil {
		t.Fatalf("generated results or foreign checkout entered the source corpus: %v", current)
	}
	if err := os.Remove(filepath.Join(root, "source.md")); err != nil {
		t.Fatal(err)
	}
	if current := actionRetirementCurrentCorpus(t, root); len(current) != 0 {
		t.Fatalf("foreign-only authored source received corpus credit: %v", current)
	}
}

func actionRetirementCorpusFiles(t *testing.T, root string, allAuthored bool) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := checkoutsource.WalkDir(root, root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "vendor" || entry.Name() == "node_modules" || entry.Name() == "test-results" {
				return filepath.SkipDir
			}
			return nil
		}
		switch filepath.Ext(path) {
		case ".go", ".yaml", ".yml", ".json", ".md":
		default:
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == actionRetirementCorpusPath {
			return nil
		}
		if allAuthored && filepath.Base(path) != "nodes.yaml" && filepath.Base(path) != "nodes.yml" && !(strings.HasPrefix(relative, "internal/runtime/testfixtures/") && strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go")) {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if allAuthored || actionRetirementCorpusPattern.Match(raw) {
			out[relative] = raw
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRetiredActionHistoricalFixtureFailsClosed(t *testing.T) {
	requireRetiredActionHistoricalFixture(t, conformanceRepoRoot(t))
}

func requireRetiredActionHistoricalFixture(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, "tests/tier4-cross-entity/test-create-entity")
	raw, err := os.ReadFile(filepath.Join(path, "tests/expected.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var expected struct {
		Conformance struct {
			Disposition string                               `yaml:"disposition"`
			Retirement  struct{ Reason, Replacement string } `yaml:"retirement"`
		} `yaml:"conformance"`
	}
	if err := yaml.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
	}
	if expected.Conformance.Disposition != "retired" || expected.Conformance.Retirement.Reason == "" || expected.Conformance.Retirement.Replacement != "tests/tier11-flow-composition/test-dynamic-flow-instance" {
		t.Fatalf("historical fixture lost exact explicit retirement: %#v", expected)
	}
	if _, err := contracts.LoadWorkflowContractBundleWithOverrides(root, path, contracts.DefaultPlatformSpecFile(root)); err == nil || !strings.Contains(err.Error(), "handler field \"action\" is not supported") {
		t.Fatalf("historical action fixture became executable: %v", err)
	}
}
