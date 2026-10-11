package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testtiming"
)

func TestCompiledQualificationRefusesCompilerMismatchBeforeLaunch(t *testing.T) {
	bin := t.TempDir()
	binaries := map[string]string{}
	for _, name := range []string{"swarm-test", "swarm-test-timing"} {
		path := filepath.Join(bin, name)
		if out, err := exec.Command("go", "build", "-o", path, "../"+name).CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", name, err, out)
		}
		binaries[name] = path
	}
	repo, _ := completionRepo(t)
	for name, data := range map[string]string{
		"go.mod":         "module proof\n\ngo 1.25.0\n",
		"source_test.go": "package proof\nimport (\"os\"; \"testing\")\nfunc TestMustNotLaunch(t *testing.T) { t.Log(\"GOVERSION_PROBE_EXECUTED\"); if os.Getenv(\"GOVERSION_PROBE_ALLOW\") != \"1\" { t.Fatal(\"deliberate versioned failure\") } }\n",
	} {
		if err := os.WriteFile(filepath.Join(repo, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	completionGit(t, repo, "add", ".")
	completionGit(t, repo, "commit", "--quiet", "-m", "test: compiler admission canary")
	head := completionGit(t, repo, "rev-parse", "HEAD")
	policy := testplanning.Policy{Version: testplanning.PolicyVersion, Module: "proof",
		Planning: testplanning.PlanningPolicy{TargetSeconds: 100, MaxShards: 1, UnknownPackageSeconds: 1},
		Profiles: map[string]testplanning.ProfilePolicy{}, Units: map[string]testplanning.UnitPolicy{}}
	for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
		policy.Profiles[tier] = testplanning.ProfilePolicy{CountMode: "count-1", EnvironmentID: "compiler-fixture"}
	}
	plan, err := testplanning.BuildPlan(policy, testplanning.WeightModel{Version: testplanning.WeightModelVersion, SourceRunID: "fixture"}, []string{"proof"}, testplanning.ProfileCore, "reviewer-bound compiler admission", head, testplanning.BuildOptions{Venue: testplanning.VenueLocal})
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := testplanning.DiscoverRootInventory(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	plan.BuildContext = inventory.BuildContext
	for i := range plan.Units {
		unit := &plan.Units[i]
		for _, name := range inventory.Packages["proof"].Roots {
			root := testplanning.TestRoot{Package: "proof", Name: name}
			unit.SelectedRoots = append(unit.SelectedRoots, root)
			unit.RequiredTests = append(unit.RequiredTests, testplanning.RequiredTest{TestRoot: root})
		}
		unit.TestBearingPackages = []string{"proof"}
	}
	writePlan := func(candidate testplanning.RunPlan) string {
		t.Helper()
		candidate.Digest = ""
		raw, err := json.Marshal(candidate)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		candidate.Digest = hex.EncodeToString(digest[:])
		raw, err = json.Marshal(candidate)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "plan.json")
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	correct := writePlan(plan)
	positive := exec.Command(binaries["swarm-test-timing"], "-assert-execution-sha", "-plan", correct, "-execution-sha", head)
	positive.Dir = repo
	if out, err := positive.CombinedOutput(); err != nil {
		t.Fatalf("equal effective compiler refused before execution: %v\n%s", err, out)
	}
	for _, cut := range []string{"missing", "foreign", "unbound"} {
		t.Run(cut, func(t *testing.T) {
			candidate := plan
			switch cut {
			case "missing":
				candidate.BuildContext.GOVERSION = ""
			case "foreign":
				candidate.BuildContext.GOVERSION += "-foreign"
			case "unbound":
				candidate.BuildContext = testplanning.BuildContext{}
			}
			path := writePlan(candidate)
			for _, name := range []string{"swarm-test", "swarm-test-timing"} {
				receipts := filepath.Join(t.TempDir(), "receipts")
				args := []string{"--planned", path, plan.Units[0].ID, "--receipts", receipts}
				if name == "swarm-test-timing" {
					args = []string{"-assert-execution-sha", "-plan", path, "-execution-sha", head}
				}
				command := exec.Command(binaries[name], args...)
				command.Dir = repo
				out, err := command.CombinedOutput()
				if err == nil || !strings.Contains(strings.ToLower(string(out)), "build context") || strings.Contains(string(out), "GOVERSION_PROBE_EXECUTED") {
					t.Fatalf("%s did not refuse compiler before launch: %v\n%s", name, err, out)
				}
				if _, err := os.Stat(receipts); !os.IsNotExist(err) {
					t.Fatalf("%s refusal published execution credit: %v", name, err)
				}
			}
		})
	}
	for _, allow := range []string{"1", "0"} {
		t.Run("executed_allow="+allow, func(t *testing.T) {
			receipts, state := t.TempDir(), t.TempDir()
			command := exec.Command(binaries["swarm-test"], "--planned", correct, plan.Units[0].ID, "--receipts", receipts)
			command.Dir = repo
			command.Env = append(os.Environ(), "GOVERSION_PROBE_ALLOW="+allow, "XDG_STATE_HOME="+state,
				"SWARM_TEST_RUN_SLOTS=1", "SWARM_TEST_POSTGRES_DSN=postgres://swarm:secret@127.0.0.1:1/postgres?sslmode=disable")
			out, executeErr := command.CombinedOutput()
			if !strings.Contains(string(out), "GOVERSION_PROBE_EXECUTED") || (executeErr == nil) != (allow == "1") {
				t.Fatalf("equal compiler did not execute/retain outcome: %v\n%s", executeErr, out)
			}
			raw, err := os.ReadFile(filepath.Join(receipts, plan.Units[0].ID+"-primary-evidence.json"))
			if err != nil {
				t.Fatal(err)
			}
			var receipt testtiming.CommandEvidence
			if err := json.Unmarshal(raw, &receipt); err != nil {
				t.Fatal(err)
			}
			var loaded testplanning.RunPlan
			raw, err = os.ReadFile(correct)
			if err != nil || json.Unmarshal(raw, &loaded) != nil {
				t.Fatal("read exact compiled worker plan", err)
			}
			if receipt.BuildContext != inventory.BuildContext || len(testtiming.ValidateCommandIdentity(receipt, loaded)) != 0 {
				t.Fatalf("worker relabeled observed compiler/outcome: %+v", receipt)
			}
			if allow == "1" && (receipt.ExitCode != 0 || len(testtiming.ValidateCommandEvidence(receipt, loaded)) != 0) {
				t.Fatal("equal compiler successful receipt refused")
			}
			if allow == "0" && (receipt.ExitCode == 0 || receipt.Report.Summary.FailedTests == 0) {
				t.Fatal("failed observed execution became successful proof")
			}
		})
	}
}
