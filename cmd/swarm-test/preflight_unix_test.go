//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testpostgres"
)

func TestPreflightCurrentSourceUsesAllFiniteOwners(t *testing.T) {
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(repo); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chdir(old); err != nil {
			t.Error(err)
		}
	}()
	t.Setenv("GITHUB_EVENT_PATH", "")
	metadata, _ := completionRepo(t)
	t.Setenv("GIT_DIR", filepath.Join(metadata, ".git"))
	t.Setenv("GIT_WORK_TREE", repo)
	if count := completionGit(t, repo, "rev-list", "--all", "--count"); count != "1" {
		t.Fatalf("membership fixture has more than HEAD-only history: %s", count)
	}
	if out, err := exec.Command("git", "rev-parse", "origin/master^{commit}").CombinedOutput(); err == nil {
		t.Fatalf("membership fixture unexpectedly has origin/master: %s", out)
	}
	head := completionGit(t, repo, "rev-parse", "HEAD")
	inventory, native := currentSourcePreflightNativeFixture(t, repo, head)
	debtRoots := preflightDebtFixtureRoots(native)
	if len(debtRoots) != 2 {
		t.Fatalf("native debt fixture requires both canonical owners: %+v", debtRoots)
	}
	structural, err := testplanning.StaticAuthorityGuardRoots(repo)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"docs-only", "debt-required"} {
		t.Run(mode, func(t *testing.T) {
			comparison := testplanning.PreflightComparison{OriginMasterSHA: strings.Repeat("b", 40), DiffSHA256: strings.Repeat("c", 64), DebtRequired: mode == "debt-required"}
			plan, err := testplanning.BuildPreflightPlan(repo, comparison.OriginMasterSHA, inventory, native, comparison)
			if err != nil {
				t.Fatal(err)
			}
			if err := validatePreflightStructuralCompleteness(plan, structural); err != nil {
				t.Fatal(err)
			}
			if err := validatePreflightEscapeCompleteness(plan); err != nil {
				t.Fatal(err)
			}
			assertPreflightFrozenSelectionRefusals(t, plan, structural)
			if err := validatePreflightSchedule(plan, preflightPhases(plan)); err != nil {
				t.Fatal(err)
			}
			for _, root := range debtRoots {
				var selected []testplanning.RequiredTest
				for _, pkg := range plan.Packages {
					for _, candidate := range pkg.Required {
						if candidate.TestRoot == root.TestRoot {
							selected = append(selected, candidate)
						}
					}
				}
				if comparison.DebtRequired && !reflect.DeepEqual(selected, []testplanning.RequiredTest{root}) || !comparison.DebtRequired && len(selected) != 0 {
					t.Fatalf("conditional debt root/child obligations differ: canonical=%+v selected=%+v", root, selected)
				}
			}
		})
	}
}

func currentSourcePreflightNativeFixture(t *testing.T, repo, head string) (testplanning.RootInventory, testplanning.RunPlan) {
	t.Helper()
	inventory, err := testplanning.DiscoverRootInventory(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(repo, ".github/test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	policy, err := testplanning.LoadPolicy(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(filepath.Join(repo, testplanning.GeneratedWeightModelPath))
	if err != nil {
		t.Fatal(err)
	}
	model, err := testplanning.LoadWeightModel(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := testplanning.LoadParityProofs(filepath.Join(repo, "internal/apiv1/testdata/public_surface_backend_matrix.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var packages []string
	for pkg := range inventory.Packages {
		packages = append(packages, pkg)
	}
	slices.Sort(packages)
	native, err := testplanning.BuildPlan(policy, model, packages, testplanning.ProfileFull, "current source membership fixture", head, testplanning.BuildOptions{Venue: testplanning.VenueLocal})
	if err != nil {
		t.Fatal(err)
	}
	if err := testplanning.BindExecution(&native, inventory, proofs, policy); err != nil {
		t.Fatal(err)
	}
	return inventory, native
}

func preflightDebtFixtureRoots(native testplanning.RunPlan) []testplanning.RequiredTest {
	var roots []testplanning.RequiredTest
	for _, unit := range native.Units {
		for _, root := range unit.RequiredTests {
			if root.Package == "github.com/division-sh/swarm/internal/store" && slices.Contains([]string{"TestPersistenceAuthorityDebtRatchet", "TestNativeFixtureFamiliesDoNotReceiveRawAuthority"}, root.Name) {
				roots = append(roots, root)
			}
		}
	}
	return roots
}

// Independent frozen oracle: package discovery does not admit new candidates.
var expectedPreflightReadinessCandidates = []string{
	"Test2376AdmissionSealsCompiledExecutionIdentity",
	"TestAdmissionPreservesLoaderAdmittedEffectiveSourceContext",
	"TestAdmissionRejectsIncompleteOrForeignBindings",
	"TestAdmissionRequiresPositiveSealForEmptyProjection",
	"TestAdmissionRetainsExactRunlessPromptWithoutPersistingIt",
	"TestAdmissionSealsCompleteProjectionAndResolvedAgentAuthority",
	"TestAdmissionSealsResolvedModelRevision",
	"TestSelectedAgentConstructionUsesExactFixedHeader",
	"TestSelectedConstructedActorProjectionCompleteCensus",
	"TestSelectedContractAdmissionBindsHistoricalStateKindsAndPresence",
	"TestSelectedContractHeaderMissingHistoricalEvidenceFailsClosed",
	"TestSelectedContractHeaderRejectsEveryReceiverConfigCopy",
	"TestSelectedContractRejectsContradictoryRecordedHeader",
	"TestSelectedReadinessRequiresCompleteConstruction",
}

func validatePreflightEscapeCompleteness(plan testplanning.PreflightPlan) error {
	selected := make(map[string]testplanning.RequiredTest)
	for _, pkg := range plan.Packages {
		for _, root := range pkg.Required {
			key := root.Package + "::" + root.Name
			if _, duplicate := selected[key]; duplicate || root.Package != pkg.Package {
				return fmt.Errorf("duplicate or foreign selected root: %s", key)
			}
			selected[key] = root
		}
	}
	const prefix = "github.com/division-sh/swarm/"
	for _, name := range expectedPreflightReadinessCandidates {
		if _, ok := selected[prefix+"internal/runtime/runforkreadiness::"+name]; !ok {
			return fmt.Errorf("declared readiness root missing: %s", name)
		}
	}
	for key, root := range selected {
		if root.Package == prefix+"internal/runtime/runforkreadiness" && !slices.Contains(expectedPreflightReadinessCandidates, root.Name) {
			return fmt.Errorf("undeclared readiness candidate selected: %s", key)
		}
	}
	for pkg, name := range map[string]string{
		"internal/runtime/effects":              "TestDirectPrimitiveOwnershipManifestIsTotal",
		"internal/runtime/deliverycontinuation": "TestExecutableDeliveryContinuationHasClosedProductionConsumers",
		"internal/runtime/core/worklifetime":    "TestProductionAsyncSitesAreClassified",
		"internal/runtime/runforkexecution":     "TestSelectedForkReceiverOwnershipBoundary",
	} {
		root, ok := selected[prefix+pkg+"::"+name]
		if !ok {
			return fmt.Errorf("escape root missing: %s/%s", pkg, name)
		}
		if pkg == "internal/runtime/runforkexecution" && !reflect.DeepEqual(root.Children, []string{"hostile", "production"}) {
			return fmt.Errorf("receiver children incomplete: %+v", root)
		}
	}
	for _, name := range []string{"TestExecuteSelectedContractRunForkConstructsAgentRecipientWithoutHandlerMaterializer", "TestExecuteSelectedContractRunForkMaterializesAndExecutesForkLocalAgentRuntime", "TestSelectedContractForkProviderTurnsUseCanonicalExecutionFrames", "TestExecuteSelectedContractRunForkProviderFailurePreservesEvidenceThroughCleanup"} {
		if _, ok := selected[prefix+"internal/runtime/runforkexecution::"+name]; ok {
			return fmt.Errorf("stateful root entered local preflight: %s", name)
		}
	}
	return nil
}

func validatePreflightStructuralCompleteness(plan testplanning.PreflightPlan, structural []testplanning.TestRoot) error {
	selected := map[testplanning.TestRoot]int{}
	count := 0
	for _, pkg := range plan.Packages {
		for _, root := range pkg.Required {
			selected[root.TestRoot]++
			count++
		}
	}
	want := 31 + 11 + len(structural)
	debtRequired := plan.Comparison != nil && plan.Comparison.DebtRequired
	for _, name := range []string{"TestPersistenceAuthorityDebtRatchet", "TestNativeFixtureFamiliesDoNotReceiveRawAuthority"} {
		root := testplanning.TestRoot{Package: "github.com/division-sh/swarm/internal/store", Name: name}
		expected := 0
		if debtRequired {
			expected = 1
			want++
		}
		if selected[root] != expected {
			return fmt.Errorf("conditional debt root %s has %d selected owners, want %d", name, selected[root], expected)
		}
	}
	if count != want {
		return fmt.Errorf("finite native-owner projection changed: %d roots, want %d", count, want)
	}
	for _, root := range structural {
		if selected[root] != 1 {
			return fmt.Errorf("canonical structural root %s/%s has %d selected owners", root.Package, root.Name, selected[root])
		}
	}
	return nil
}

func TestPreflightFiniteCandidatesPreserveUnselectedNativePackageGrowth(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := testplanning.DiscoverRootInventory(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	const readiness = "github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	grown := inventory.Packages[readiness]
	grown.Roots = slices.Clone(grown.Roots)
	var extras []string
	for n := range 10 {
		name := fmt.Sprintf("TestUnselectedNativeReadinessModel%d", n)
		extras = append(extras, name)
		grown.Roots = append(grown.Roots, name)
	}
	inventory.Packages[readiness] = grown
	raw, err := os.ReadFile(filepath.Join(repo, ".github/test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	policy, err := testplanning.LoadPolicy(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := testplanning.LoadParityProofs(filepath.Join(repo, "internal/apiv1/testdata/public_surface_backend_matrix.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var packages []string
	for pkg := range inventory.Packages {
		packages = append(packages, pkg)
	}
	slices.Sort(packages)
	model := testplanning.WeightModel{Version: testplanning.WeightModelVersion, SourceRunID: "preflight-growth-fixture"}
	native, err := testplanning.BuildPlan(policy, model, packages, testplanning.ProfileFull, "finite growth", strings.Repeat("a", 40), testplanning.BuildOptions{Venue: testplanning.VenueLocal})
	if err != nil {
		t.Fatal(err)
	}
	if err := testplanning.BindExecution(&native, inventory, proofs, policy); err != nil {
		t.Fatal(err)
	}
	plan, err := testplanning.BuildPreflightPlan(repo, strings.Repeat("b", 40), inventory, native, testplanning.PreflightComparison{OriginMasterSHA: strings.Repeat("b", 40), DiffSHA256: strings.Repeat("c", 64)})
	if err != nil {
		t.Fatal(err)
	}
	if err := validatePreflightEscapeCompleteness(plan); err != nil {
		t.Fatal(err)
	}
	structural, err := testplanning.StaticAuthorityGuardRoots(repo)
	if err != nil {
		t.Fatal(err)
	}
	if err := validatePreflightStructuralCompleteness(plan, structural); err != nil {
		t.Fatal(err)
	}
	debtPlan, err := testplanning.BuildPreflightPlan(repo, strings.Repeat("b", 40), inventory, native, testplanning.PreflightComparison{OriginMasterSHA: strings.Repeat("b", 40), DiffSHA256: strings.Repeat("d", 64), DebtRequired: true})
	if err != nil || debtPlan.Digest == plan.Digest {
		t.Fatalf("same-head debt plan did not bind its exact selection: %v", err)
	}
	for _, name := range []string{"TestPersistenceAuthorityDebtRatchet", "TestNativeFixtureFamiliesDoNotReceiveRawAuthority"} {
		var canonical, selected []testplanning.RequiredTest
		for _, unit := range native.Units {
			for _, root := range unit.RequiredTests {
				if root.Name == name && root.Package == "github.com/division-sh/swarm/internal/store" {
					canonical = append(canonical, root)
				}
			}
		}
		for _, pkg := range debtPlan.Packages {
			for _, root := range pkg.Required {
				if root.Name == name {
					selected = append(selected, root)
				}
			}
		}
		if len(canonical) != 1 || !reflect.DeepEqual(canonical, selected) {
			t.Fatalf("debt root/child obligations differ: %s canonical=%+v selected=%+v", name, canonical, selected)
		}
	}
	if err := validatePreflightSchedule(debtPlan, preflightPhases(debtPlan)); err != nil {
		t.Fatal(err)
	}
	for _, name := range extras {
		owners := 0
		for _, unit := range native.Units {
			for _, root := range unit.RequiredTests {
				if root.Package == readiness && root.Name == name {
					owners++
				}
			}
		}
		if owners != 1 {
			t.Fatalf("unselected native root %s has %d required owners", name, owners)
		}
	}
}

func assertPreflightFrozenSelectionRefusals(t *testing.T, plan testplanning.PreflightPlan, structural []testplanning.TestRoot) {
	t.Helper()
	const prefix = "github.com/division-sh/swarm/"
	for _, change := range []string{"wrong-readiness-name", "missing-readiness", "duplicate-readiness", "foreign-readiness", "extra-readiness", "receiver-hostile", "receiver-production", "stateful-execution", "wrong-structural-name"} {
		t.Run(change, func(t *testing.T) {
			mutant := plan
			mutant.Packages = slices.Clone(plan.Packages)
			for i := range mutant.Packages {
				pkg := &mutant.Packages[i]
				pkg.Required = slices.Clone(pkg.Required)
				if pkg.Package == prefix+"internal/runtime/runforkreadiness" {
					switch change {
					case "wrong-readiness-name":
						pkg.Required[0].Name = "TestUnselectedNativeReadinessModel"
					case "missing-readiness":
						pkg.Required = pkg.Required[1:]
					case "duplicate-readiness":
						pkg.Required = append(pkg.Required, pkg.Required[0])
					case "foreign-readiness":
						pkg.Required[0].Package = prefix + "foreign"
					case "extra-readiness":
						pkg.Required = append(pkg.Required, testplanning.RequiredTest{TestRoot: testplanning.TestRoot{Package: pkg.Package, Name: "TestUnselectedNativeReadinessModel"}})
					}
				}
				if pkg.Package == prefix+"internal/runtime/runforkexecution" {
					switch change {
					case "receiver-hostile", "receiver-production":
						pkg.Required[0].Children = []string{strings.TrimPrefix(change, "receiver-")}
					case "stateful-execution":
						pkg.Required = append(pkg.Required, testplanning.RequiredTest{TestRoot: testplanning.TestRoot{Package: pkg.Package, Name: "TestExecuteSelectedContractRunForkMaterializesAndExecutesForkLocalAgentRuntime"}})
					}
				}
				if change == "wrong-structural-name" {
					for j := range pkg.Required {
						if pkg.Required[j].TestRoot == structural[0] {
							pkg.Required[j].Name = "TestForeignStructuralGuard"
						}
					}
				}
			}
			if validatePreflightEscapeCompleteness(mutant) == nil && validatePreflightStructuralCompleteness(mutant, structural) == nil {
				t.Fatalf("accepted %s selection", change)
			}
		})
	}
	t.Run("canonical-structural-growth", func(t *testing.T) {
		grown := plan
		grown.Packages = slices.Clone(plan.Packages)
		owners := slices.Clone(structural)
		for i := range grown.Packages {
			pkg := &grown.Packages[i]
			if pkg.Package != prefix+"internal/runtime" {
				continue
			}
			pkg.Required = slices.Clone(pkg.Required)
			for n := range 4 {
				root := testplanning.TestRoot{Package: pkg.Package, Name: fmt.Sprintf("TestNewCanonicalStructuralGuard%d", n)}
				owners = append(owners, root)
				pkg.Required = append(pkg.Required, testplanning.RequiredTest{TestRoot: root})
			}
		}
		if err := validatePreflightStructuralCompleteness(grown, owners); err != nil {
			t.Fatal(err)
		}
		if err := validatePreflightEscapeCompleteness(grown); err != nil {
			t.Fatal(err)
		}
		if validatePreflightStructuralCompleteness(plan, owners) == nil {
			t.Fatal("growth omitted newly required canonical structural owners")
		}
	})
}

func TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution(t *testing.T) {
	const modeEnv = "SWARM_TEST_PREFLIGHT_PROCESS_FIXTURE"
	if mode := os.Getenv(modeEnv); mode != "" {
		if strings.HasPrefix(mode, "survivor-") {
			runPreflightSurvivorFixture(t, mode)
			return
		}
		runPreflightTerminalFixture(t, mode)
		return
	}
	t.Run("unsupported-platform-refuses-before-launch", checkPreflightUnsupportedPlatform)
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	provePreflightTerminalOutcomes(t)
	if run([]string{"preflight", "--run", "^$"}) != 2 {
		t.Fatal("local preflight admitted a filtered selection")
	}
	t.Run("static-child-cancellation", func(t *testing.T) {
		fixture := startPreflightFixture(t, context.Background(), "hold", preflightFixtureCommand())
		fixture.ready(t)
		deadline := time.Now().Add(5 * time.Second)
		fixture.cancel()
		if err := fixture.wait(time.Until(deadline)); !errors.Is(err, context.Canceled) {
			t.Fatalf("static cancellation lost its outcome: %v", err)
		}
	})
	t.Run("source-mutation-refuses-command", func(t *testing.T) {
		repo, head := completionRepo(t)
		t.Chdir(repo)
		if err := os.WriteFile("new-guard.go", []byte("package proof\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := checkPreflightSource(context.Background(), testplanning.PreflightPlan{HeadSHA: head}); err == nil {
			t.Fatal("source mutation reached guard execution")
		}
	})
	t.Run("soft-target-is-not-a-deadline", func(t *testing.T) {
		var output bytes.Buffer
		receipt := preflightReceipt{EndToEndSeconds: 301, Complete: true}
		printPreflightReceipt(&output, receipt, "retained")
		if !strings.Contains(output.String(), "soft target exceeded") || !strings.Contains(output.String(), "complete=true") || !receipt.Complete {
			t.Fatalf("soft target changed execution outcome: %s", output.String())
		}
	})
	provePreflightDoesNotAcquireNativeSlot(t)
	provePreflightSurvivingDescendants(t, repo)
	provePreflightEarlyCleanup(t, repo)
	t.Run("unresolved-lifetime-refuses-completion", func(t *testing.T) {
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := joinPreflightDescendants(ctx, reader); !errors.Is(err, context.Canceled) {
			t.Fatalf("unresolved inherited lifetime became success: %v", err)
		}
	})
}

func runPreflightTerminalFixture(t *testing.T, mode string) {
	t.Helper()
	release := os.Getenv("SWARM_TEST_PREFLIGHT_RELEASE")
	if mode == "startup-hold" {
		fmt.Fprintln(os.Stderr, "fixture entered startup before readiness")
		if err := publishSignalFixturePID(os.Getenv(signalFixturePIDEnv)+".entered", os.Getpid(), nil); err != nil {
			t.Fatal(err)
		}
		waitForPreflightFixtureRelease(t, release)
		mode = "hold"
	}
	fmt.Fprintf(os.Stderr, "fixture ready: mode=%s pid=%d\n", mode, os.Getpid())
	if release != "" || mode == "hold" {
		if err := publishSignalFixturePID(os.Getenv(signalFixturePIDEnv), os.Getpid(), nil); err != nil {
			t.Fatal(err)
		}
	}
	if mode != "hold" && release != "" {
		waitForPreflightFixtureRelease(t, release)
	}
	fmt.Fprintf(os.Stderr, "fixture selected: mode=%s\n", mode)
	if mode == "fail" {
		t.Fatal("selected guard failure")
	}
	if mode == "hold" {
		for {
			time.Sleep(time.Hour)
		}
	}
}

func checkPreflightUnsupportedPlatform(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	common, err := os.ReadFile("preflight_process.go")
	if err != nil {
		t.Fatal(err)
	}
	// Compile the actual common owner with the actual unsupported capability.
	// Remove only build-selection comments from that constant-only source file.
	fset := token.NewFileSet()
	platform, err := parser.ParseFile(fset, "preflight_process_other.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(platform.Decls) != 1 {
		t.Fatal("unsupported platform must declare only its literal capability")
	}
	decl, ok := platform.Decls[0].(*ast.GenDecl)
	if !ok || decl.Tok != token.CONST || len(decl.Specs) != 1 {
		t.Fatal("unsupported platform has executable or opaque declarations")
	}
	spec, ok := decl.Specs[0].(*ast.ValueSpec)
	if !ok || spec.Type != nil || len(spec.Names) != 1 || spec.Names[0].Name != "preflightDescendantJoinMode" || len(spec.Values) != 1 {
		t.Fatal("unsupported capability declaration changed")
	}
	literal, ok := spec.Values[0].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		t.Fatal("unsupported capability must be a literal, not executable initialization")
	}
	mode, err := strconv.Unquote(literal.Value)
	if err != nil || mode != "unsupported" {
		t.Fatal("unsupported platform must fail closed")
	}
	var capability bytes.Buffer
	if err := format.Node(&capability, fset, platform); err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{
		"go.mod":      []byte("module preflight.platform.probe\n\ngo 1.26.0\n"),
		"process.go":  common,
		"platform.go": capability.Bytes(),
		"process_test.go": []byte(`package main
import("context";"io";"os";"os/exec";"strings";"testing";"time")
const authoritySettlementTimeout = time.Second
func prepareChildProcessTree(*exec.Cmd)error{panic("unsupported platform reached process preparation")}
func signalChildProcessTree(*exec.Cmd,os.Signal)error{panic("unsupported platform reached process signaling")}
func TestUnsupported(t *testing.T){
 for _,args:=range [][]string{nil,{"must-not-launch"}}{
  timing,err:=runPreflightProcess(context.Background(),args,io.Discard,io.Discard)
  if err==nil||!strings.Contains(err.Error(),"descendant ownership is unsupported")||timing!=(preflightProcessTiming{}){t.Fatalf("unsupported execution: %+v / %v",timing,err)}
 }
}
`),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "test", "-count=1", "-run", "^TestUnsupported$", ".")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("actual unsupported owner: %v\n%s", err, output)
	}
}

func provePreflightDoesNotAcquireNativeSlot(t *testing.T) {
	t.Run("occupied-native-slot-is-untouched", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		home := t.TempDir()
		t.Setenv("XDG_STATE_HOME", home)
		t.Setenv("SWARM_TEST_PREFLIGHT_PROCESS_FIXTURE", "success")
		admission := testpostgres.NewRunAdmission(filepath.Join(home, "swarm", "test-postgres"), io.Discard)
		lease, err := admission.Acquire(context.Background(), testpostgres.RunCommand{Args: []string{"go", "test", "./fixture-owned-heavy-work"}}, 1)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := lease.Complete(context.Background(), false); err != nil {
				t.Error(err)
			}
		})
		before := preflightNativeFixtureFiles(t, home)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		fixture := startPreflightFixture(t, ctx, "success", preflightFixtureCommand())
		if err := fixture.release(); err != nil {
			t.Fatal(err)
		}
		deadline, _ := ctx.Deadline()
		err = fixture.wait(time.Until(deadline))
		raw, readErr := os.ReadFile(fixture.output.Name())
		if err := errors.Join(readErr, preflightTerminalOutcome("success", err, raw)); err != nil {
			t.Fatalf("static guard waited on an occupied native slot: %v", err)
		}
		if after := preflightNativeFixtureFiles(t, home); !reflect.DeepEqual(before, after) {
			t.Fatalf("preflight acquired/mutated native admission or service state: before=%v after=%v", before, after)
		}
	})
}

func preflightNativeFixtureFiles(t *testing.T, home string) map[string]string {
	t.Helper()
	files := map[string]string{}
	if err := filepath.WalkDir(home, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		files[path] = string(raw)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return files
}

func runPreflightSurvivorFixture(t *testing.T, mode string) {
	if mode == "survivor-child" {
		signal.Ignore(syscall.SIGTERM, syscall.SIGINT)
		if err := publishSignalFixturePID(os.Getenv(signalFixturePIDEnv), os.Getpid(), nil); err != nil {
			t.Fatal(err)
		}
		waitForPreflightFixtureRelease(t, os.Getenv("SWARM_TEST_PREFLIGHT_RELEASE"))
		return
	}
	child := exec.Command(os.Args[0], "-test.run=^TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution$")
	child.Env = replaceEnvironment(os.Environ(), "SWARM_TEST_PREFLIGHT_PROCESS_FIXTURE", "survivor-child")
	// Neither redirected stdio nor leader exit may hide the inherited descriptor.
	devnull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer devnull.Close()
	child.Stdout, child.Stderr = devnull, devnull
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	waitForSignalFixturePID(t, os.Getenv(signalFixturePIDEnv))
	if mode == "survivor-fail" {
		t.Fatal("selected guard failure with a surviving descendant")
	}
}

func provePreflightSurvivingDescendants(t *testing.T, repo string) {
	for _, mode := range []string{"survivor-success", "survivor-fail", "survivor-cancel"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{"go", "test", filepath.Join(repo, "cmd/swarm-test"), "-count=1", "-run", "^TestPreflightCommandJoinsCancellationAndRefusesIncompleteExecution$"}
			fixture := startPreflightFixture(t, context.Background(), mode, args)
			fixture.ready(t)
			if mode == "survivor-cancel" {
				fixture.cancel()
			}
			select {
			case err := <-fixture.result:
				fixture.joined, fixture.terminal = true, err
				t.Fatalf("leader exit/cancellation falsely joined a redirected survivor: %v", err)
			case <-time.After(200 * time.Millisecond):
			}
			deadline := time.Now().Add(5 * time.Second)
			if err := fixture.release(); err != nil {
				t.Fatal(err)
			}
			err := fixture.wait(time.Until(deadline))
			if (err == nil) != (mode == "survivor-success") || !fixture.joined {
				t.Fatalf("survivor result: %v / joined=%v", err, fixture.joined)
			}
			if mode == "survivor-fail" {
				var exitErr *exec.ExitError
				raw, readErr := os.ReadFile(fixture.output.Name())
				if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 || readErr != nil || !strings.Contains(string(raw), "selected guard failure with a surviving descendant") {
					t.Fatalf("survivor failed before its selected guard: %v / %v\n%s", err, readErr, raw)
				}
			}
			if mode == "survivor-cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("survivor lost cancellation: %v", err)
			}
		})
	}
}
