//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
)

var expectedCheapPreflightPackages = []string{
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime",
	"github.com/division-sh/swarm/internal/runtime/effects",
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation",
	"github.com/division-sh/swarm/internal/runtime/manager",
	"github.com/division-sh/swarm/internal/testutil",
	"github.com/division-sh/swarm/scripts/rewrite-stages-2566",
	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity",
	"github.com/division-sh/swarm/cmd/swarm-test-timing",
	"github.com/division-sh/swarm/internal/testtiming",
}

func originalPreflightPhases(plan testplanning.PreflightPlan) []preflightPhase {
	phases := []preflightPhase{
		{Name: "format", Args: []string{"gofmt", "-l", "."}},
		{Name: "complexity", Args: []string{"go", "run", "./cmd/swarm-complexity", "-head", plan.HeadSHA, "-base", plan.BaseSHA}},
		{Name: "openrpc", Args: []string{"go", "run", "./cmd/swarm-openrpc-gen", "--check"}},
		{Name: "vet", Args: []string{"go", "vet", "./..."}},
	}
	for _, pkg := range plan.Packages {
		args := []string{"go", "test", "-json", "-count=1"}
		var debtBase string
		for _, root := range pkg.Required {
			if root.Name == "TestPersistenceAuthorityDebtRatchet" {
				args = append(args, "-timeout=20m")
				debtBase = plan.BaseSHA
			}
		}
		phases = append(phases, preflightPhase{Name: pkg.Package, Package: &pkg, Args: append(args, "-run", pkg.Run, pkg.Package), DebtBase: debtBase})
	}
	return phases
}

func validatePreflightSchedule(plan testplanning.PreflightPlan, phases []preflightPhase) error {
	encode := func(phases []preflightPhase) []string {
		var entries []string
		for _, phase := range phases {
			raw, _ := json.Marshal(phase)
			entries = append(entries, string(raw))
		}
		slices.Sort(entries)
		return entries
	}
	if !reflect.DeepEqual(encode(originalPreflightPhases(plan)), encode(phases)) {
		return fmt.Errorf("schedule changed command, root, child or multiplicity")
	}
	names := []string{"format", "openrpc"}
	for _, name := range expectedCheapPreflightPackages {
		for _, pkg := range plan.Packages {
			if pkg.Package == name {
				names = append(names, name)
			}
		}
	}
	names = append(names, "complexity")
	for _, pkg := range plan.Packages {
		if !slices.Contains(expectedCheapPreflightPackages, pkg.Package) {
			names = append(names, pkg.Package)
		}
	}
	names = append(names, "vet")
	var actual []string
	for _, phase := range phases {
		actual = append(actual, phase.Name)
	}
	if !reflect.DeepEqual(actual, names) {
		return fmt.Errorf("schedule order: got %v, want %v", actual, names)
	}
	return nil
}

func TestPreflightScheduleIsStableCompletePermutation(t *testing.T) {
	for _, variant := range []string{"complete", "missing-ranked", "unranked-growth", "empty"} {
		t.Run(variant, func(t *testing.T) {
			plan := testplanning.PreflightPlan{HeadSHA: strings.Repeat("a", 40), BaseSHA: strings.Repeat("b", 40)}
			packages := slices.Clone(expectedCheapPreflightPackages)
			packages = append(packages, "github.com/division-sh/swarm/future/structural")
			if variant == "missing-ranked" {
				packages = packages[1:]
			}
			if variant == "unranked-growth" {
				packages = append(packages, "github.com/division-sh/swarm/future/another")
			}
			if variant == "empty" {
				packages = nil
			}
			slices.Sort(packages)
			for _, pkg := range packages {
				plan.Packages = append(plan.Packages, testplanning.PreflightPackage{
					Package: pkg, Run: "^TestGuard$", Required: []testplanning.RequiredTest{{
						TestRoot: testplanning.TestRoot{Package: pkg, Name: "TestGuard"}, Children: []string{"hostile", "production"},
					}},
				})
			}
			before, _ := json.Marshal(plan)
			phases := preflightPhases(plan)
			if err := validatePreflightSchedule(plan, phases); err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(plan)
			if string(before) != string(after) || !reflect.DeepEqual(phases, preflightPhases(plan)) {
				t.Fatal("ordering mutated the admitted plan or was nondeterministic")
			}
			for _, mutation := range []string{"missing", "duplicate", "foreign", "reordered"} {
				t.Run(mutation, func(t *testing.T) {
					changed := slices.Clone(phases)
					switch mutation {
					case "missing":
						changed = changed[1:]
					case "duplicate":
						changed = append(changed, changed[0])
					case "foreign":
						changed[0] = preflightPhase{Name: "foreign", Args: []string{"go", "test", "foreign"}}
					case "reordered":
						changed[0], changed[1] = changed[1], changed[0]
					}
					if validatePreflightSchedule(plan, changed) == nil {
						t.Fatalf("accepted %s rank/schedule mutation", mutation)
					}
				})
			}
		})
	}
}

func preflightScheduleFixture(t *testing.T, mode string) preflightReceipt {
	t.Helper()
	repo, _ := completionRepo(t)
	program := `package main
import("fmt";"os")
const stage = %q
func main(){
 mode:=os.Getenv("SWARM_TEST_PREFLIGHT_SCHEDULE_FIXTURE")
 if stage=="openrpc" && mode=="source" {
  if err:=os.WriteFile("source_test.go",[]byte("package proof\n// changed during phase\n"),0600);err!=nil{panic(err)}
 }
 if stage=="openrpc" && mode=="build" {
  if err:=os.WriteFile(os.Getenv("GOENV"),[]byte("GOFLAGS=-tags=preflight_fixture\n"),0600);err!=nil{panic(err)}
 }
 if mode==stage {fmt.Fprintln(os.Stderr,"controlled static refusal");os.Exit(1)}
}
`
	files := map[string]string{
		"go.mod":                        "module github.com/division-sh/swarm\n\ngo 1.26.0\n",
		"cmd/swarm-openrpc-gen/main.go": fmt.Sprintf(program, "openrpc"),
		"cmd/swarm-complexity/main.go":  fmt.Sprintf(program, "complexity"),
		"internal/runtime/effects/guard_test.go": `package effects
import("os";"os/exec";"testing")
func TestGuard(t *testing.T){
 if os.Getenv("SWARM_TEST_PREFLIGHT_SCHEDULE_DESCENDANT")=="1" {
  if err:=os.WriteFile(os.Getenv("SWARM_TEST_PREFLIGHT_SCHEDULE_JOINED"),[]byte("joined\n"),0600);err!=nil{t.Fatal(err)}
  return
 }
 mode:=os.Getenv("SWARM_TEST_PREFLIGHT_SCHEDULE_FIXTURE")
 if mode=="skip" {t.Skip("controlled skipped proof")}
 if mode!="cheap" {return}
 child:=exec.Command(os.Args[0],"-test.run=^TestGuard$")
 child.Env=append(os.Environ(),"SWARM_TEST_PREFLIGHT_SCHEDULE_DESCENDANT=1")
 devnull,err:=os.OpenFile(os.DevNull,os.O_RDWR,0);if err!=nil{t.Fatal(err)};defer devnull.Close()
 child.Stdout,child.Stderr=devnull,devnull
 lifetime:=os.NewFile(3,"preflight-lifetime");defer lifetime.Close()
 child.ExtraFiles=[]*os.File{lifetime}
 if err:=child.Run();err!=nil{t.Fatal(err)}
 t.Fatal("controlled cheap guard failure")
}
`,
	}
	if mode == "format" {
		files["source_test.go"] = "package proof\nfunc unformatted( ){}\n"
	}
	if mode == "vet" {
		files["unselected/bad.go"] = "package unselected\nimport \"fmt\"\nfunc Bad(){fmt.Printf(\"%d\",\"wrong type\")}\n"
	}
	for path, source := range files {
		if err := os.MkdirAll(filepath.Join(repo, filepath.Dir(path)), 0700); err != nil {
			t.Fatal(err)
		}
		raw := []byte(source)
		if strings.HasSuffix(path, ".go") && !(mode == "format" && path == "source_test.go") {
			var err error
			raw, err = format.Source(raw)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(repo, path), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	completionGit(t, repo, "add", ".")
	completionGit(t, repo, "commit", "--quiet", "-m", "test: native preflight schedule fixture")
	head := completionGit(t, repo, "rev-parse", "HEAD")
	t.Chdir(repo)
	t.Setenv("SWARM_TEST_PREFLIGHT_SCHEDULE_FIXTURE", mode)
	t.Setenv("SWARM_TEST_PREFLIGHT_SCHEDULE_JOINED", filepath.Join(repo, "test-results", "joined"))
	if err := os.Mkdir("test-results", 0700); err != nil {
		t.Fatal(err)
	}
	if mode == "build" {
		environment := filepath.Join(t.TempDir(), "go.env")
		if err := os.WriteFile(environment, nil, 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("GOENV", environment)
	}
	build, err := testplanning.EffectiveBuildContext(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	const pkg = "github.com/division-sh/swarm/internal/runtime/effects"
	return preflightReceipt{Plan: testplanning.PreflightPlan{
		HeadSHA: head, BaseSHA: head, BuildContext: build, Packages: []testplanning.PreflightPackage{{
			Package: pkg, Run: "^TestGuard$", Required: []testplanning.RequiredTest{{TestRoot: testplanning.TestRoot{Package: pkg, Name: "TestGuard"}}},
		}},
	}}
}

func requirePreflightFailedAt(t *testing.T, receipt preflightReceipt, phase string) {
	t.Helper()
	if receipt.Complete || len(receipt.Phases) == 0 {
		t.Fatalf("failed command earned complete/empty evidence: %+v", receipt)
	}
	last := receipt.Phases[len(receipt.Phases)-1]
	if last.Name != phase || last.Passed || last.Started.IsZero() || last.Completed.Before(last.Started) {
		t.Fatalf("wrong failure or phase chronology: %+v", last)
	}
	var names []string
	for _, candidate := range preflightPhases(receipt.Plan) {
		names = append(names, candidate.Name)
	}
	position := slices.Index(names, phase)
	if !reflect.DeepEqual(receipt.Unstarted, names[position+1:]) {
		t.Fatalf("false unstarted accounting: %v, want %v", receipt.Unstarted, names[position+1:])
	}
}

func TestPreflightScheduleExecutorPreservesRefusals(t *testing.T) {
	for _, mode := range []string{"format", "openrpc", "cheap", "complexity", "vet", "source", "build", "skip", "foreign", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			receipt := preflightScheduleFixture(t, mode)
			if mode == "foreign" {
				receipt.Plan.Packages[0].Required[0].Name = "TestForeign"
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancel" {
				cancel()
			}
			err := executePreflight(ctx, t.TempDir(), &receipt)
			if err == nil {
				t.Fatal("controlled refusal became successful completion")
			}
			if mode == "cancel" {
				if receipt.Complete || len(receipt.Phases) != 0 || len(receipt.Unstarted) != len(preflightPhases(receipt.Plan)) {
					t.Fatalf("cancelled admission executed/lost phases: %+v", receipt)
				}
				return
			}
			phase := mode
			switch mode {
			case "cheap", "skip", "foreign":
				phase = receipt.Plan.Packages[0].Package
			case "source", "build":
				phase = "openrpc"
			}
			requirePreflightFailedAt(t, receipt, phase)
			if mode == "source" && !strings.Contains(err.Error(), "source_test.go") {
				t.Fatalf("lost actual post-phase source fence: %v", err)
			}
			if mode == "build" && !strings.Contains(err.Error(), "GOFLAGS") {
				t.Fatalf("lost actual post-phase build fence: %v", err)
			}
			if mode == "cheap" {
				if raw, err := os.ReadFile(os.Getenv("SWARM_TEST_PREFLIGHT_SCHEDULE_JOINED")); err != nil || string(raw) != "joined\n" {
					t.Fatalf("failed child did not join its redirected descendant: %q / %v", raw, err)
				}
			}
		})
	}
}

func TestPreflightScheduleFirstFailureMeasurement(t *testing.T) {
	for _, order := range []string{"original", "ranked"} {
		t.Run(order, func(t *testing.T) {
			receipt := preflightScheduleFixture(t, "cheap")
			phases := preflightPhases(receipt.Plan)
			if order == "original" {
				phases = originalPreflightPhases(receipt.Plan)
			}
			started := time.Now()
			err := executePreflightPhases(context.Background(), t.TempDir(), &receipt, phases)
			elapsed := time.Since(started)
			if err == nil || receipt.Complete || receipt.Phases[len(receipt.Phases)-1].Name != receipt.Plan.Packages[0].Package {
				t.Fatalf("missing actual first-failure measurement: %+v / %v", receipt, err)
			}
			raw, err := json.Marshal(struct {
				Order   string           `json:"order"`
				Seconds float64          `json:"seconds"`
				Receipt preflightReceipt `json:"receipt"`
			}{order, elapsed.Seconds(), receipt})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("first-failure-measurement %s", raw)
		})
	}
}
