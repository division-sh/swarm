package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testtiming"
)

type preflightPhase struct {
	Name    string                         `json:"name"`
	Args    []string                       `json:"args"`
	Package *testplanning.PreflightPackage `json:"package,omitempty"`
}

type preflightReceipt struct {
	Plan             testplanning.PreflightPlan `json:"plan"`
	Started          time.Time                  `json:"started"`
	EndToEndSeconds  float64                    `json:"end_to_end_seconds"`
	QueueWaitSeconds float64                    `json:"queue_wait_seconds"`
	ExecutionSeconds float64                    `json:"execution_seconds"`
	CleanupSeconds   float64                    `json:"cleanup_seconds"`
	SetupSeconds     float64                    `json:"setup_seconds"`
	Complete         bool                       `json:"complete"`
	Phases           []preflightPhaseReceipt    `json:"phases"`
	Unstarted        []string                   `json:"unstarted"`
	Problem          string                     `json:"problem,omitempty"`
}

type preflightPhaseReceipt struct {
	Name             string    `json:"name"`
	Args             []string  `json:"args"`
	Started          time.Time `json:"started"`
	Completed        time.Time `json:"completed"`
	Seconds          float64   `json:"seconds"`
	SetupSeconds     float64   `json:"setup_seconds"`
	ExecutionSeconds float64   `json:"execution_seconds"`
	CleanupSeconds   float64   `json:"cleanup_seconds"`
	Passed           bool      `json:"passed"`
}

func runPreflight(ctx context.Context) int {
	started := time.Now()
	head, err := completionSource(".", "", true)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	base, err := exec.CommandContext(ctx, "git", "rev-parse", "origin/master").Output()
	if err != nil {
		fmt.Fprintln(os.Stderr, "preflight requires a pinned local origin/master:", err)
		return 1
	}
	plan, err := loadPreflightPlan(ctx, head, strings.TrimSpace(string(base)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	ledgerFile, err := os.Open(".github/flake-ledger.json")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	ledger, err := testtiming.ReadFailureLedger(ledgerFile)
	closeErr := ledgerFile.Close()
	if err = errors.Join(err, closeErr); err == nil {
		err = testtiming.ValidatePreflightEligibility(plan, ledger)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	directory := filepath.Join("test-results", "preflight", started.UTC().Format("20060102T150405.000000000"))
	if err := os.MkdirAll(directory, 0700); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	receipt := preflightReceipt{Plan: plan, Started: started.UTC(), SetupSeconds: time.Since(started).Seconds()}
	err = executePreflight(ctx, directory, &receipt)
	receipt.EndToEndSeconds = time.Since(started).Seconds()
	receipt.SetupSeconds = receipt.EndToEndSeconds - receipt.ExecutionSeconds - receipt.CleanupSeconds
	if err != nil {
		receipt.Problem = err.Error()
	}
	raw, marshalErr := json.MarshalIndent(receipt, "", "  ")
	err = errors.Join(err, marshalErr, os.WriteFile(filepath.Join(directory, "receipt.json"), append(raw, '\n'), 0600))
	receipt.Complete = receipt.Complete && err == nil
	printPreflightReceipt(os.Stderr, receipt, directory)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func printPreflightReceipt(output io.Writer, receipt preflightReceipt, directory string) {
	if receipt.EndToEndSeconds > 300 {
		fmt.Fprintln(output, "preflight soft target exceeded: review/re-rank membership; no test deadline or selector changed")
	}
	fmt.Fprintf(output, "preflight: complete=%t end-to-end=%.3fs queue=%.3fs setup/validation=%.3fs execution=%.3fs cleanup=%.3fs; source=%s go=%s selection=%s receipts=%s\n", receipt.Complete, receipt.EndToEndSeconds, receipt.QueueWaitSeconds, receipt.SetupSeconds, receipt.ExecutionSeconds, receipt.CleanupSeconds, receipt.Plan.HeadSHA, receipt.Plan.BuildContext.GOVERSION, receipt.Plan.Digest, directory)
}

func loadPreflightPlan(ctx context.Context, head, base string) (testplanning.PreflightPlan, error) {
	inventory, err := testplanning.DiscoverRootInventory(ctx, ".")
	if err != nil {
		return testplanning.PreflightPlan{}, err
	}
	raw, err := os.ReadFile(".github/test-proof-plan.yaml")
	if err != nil {
		return testplanning.PreflightPlan{}, err
	}
	policy, err := testplanning.LoadPolicy(bytes.NewReader(raw))
	if err != nil {
		return testplanning.PreflightPlan{}, err
	}
	raw, err = os.ReadFile(testplanning.GeneratedWeightModelPath)
	if err != nil {
		return testplanning.PreflightPlan{}, err
	}
	model, err := testplanning.LoadWeightModel(bytes.NewReader(raw))
	if err != nil {
		return testplanning.PreflightPlan{}, err
	}
	proofs, err := testplanning.LoadParityProofs("internal/apiv1/testdata/public_surface_backend_matrix.yaml")
	if err != nil {
		return testplanning.PreflightPlan{}, err
	}
	var packages []string
	for pkg := range inventory.Packages {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)
	native, err := testplanning.BuildPlan(policy, model, packages, testplanning.ProfileFull, "preflight ownership projection, no full execution", head, testplanning.BuildOptions{Venue: testplanning.VenueLocal})
	if err == nil {
		err = testplanning.BindExecution(&native, inventory, proofs, policy)
	}
	if err != nil {
		return testplanning.PreflightPlan{}, err
	}
	return testplanning.BuildPreflightPlan(".", base, inventory, native)
}

func preflightPhases(plan testplanning.PreflightPlan) []preflightPhase {
	phases := []preflightPhase{
		{Name: "format", Args: []string{"gofmt", "-l", "."}},
		{Name: "openrpc", Args: []string{"go", "run", "./cmd/swarm-openrpc-gen", "--check"}},
	}
	cheap := []string{
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
	for _, name := range cheap {
		for _, pkg := range plan.Packages {
			if pkg.Package == name {
				phases = append(phases, preflightPackagePhase(pkg))
			}
		}
	}
	phases = append(phases, preflightPhase{Name: "complexity", Args: []string{"go", "run", "./cmd/swarm-complexity", "-head", plan.HeadSHA, "-base", plan.BaseSHA}})
	for _, pkg := range plan.Packages {
		if !slices.Contains(cheap, pkg.Package) {
			phases = append(phases, preflightPackagePhase(pkg))
		}
	}
	return append(phases, preflightPhase{Name: "vet", Args: []string{"go", "vet", "./..."}})
}

func preflightPackagePhase(pkg testplanning.PreflightPackage) preflightPhase {
	return preflightPhase{Name: pkg.Package, Package: &pkg, Args: []string{"go", "test", "-json", "-count=1", "-run", pkg.Run, pkg.Package}}
}

func executePreflight(ctx context.Context, directory string, receipt *preflightReceipt) error {
	return executePreflightPhases(ctx, directory, receipt, preflightPhases(receipt.Plan))
}

func executePreflightPhases(ctx context.Context, directory string, receipt *preflightReceipt, phases []preflightPhase) error {
	for _, phase := range phases {
		receipt.Unstarted = append(receipt.Unstarted, phase.Name)
	}
	for _, phase := range phases {
		if err := checkPreflightSource(ctx, receipt.Plan); err != nil {
			return err
		}
		started := time.Now()
		timing, err := executePreflightPhase(ctx, directory, phase)
		err = errors.Join(err, checkPreflightSource(ctx, receipt.Plan))
		receipt.Unstarted = receipt.Unstarted[1:]
		completed := time.Now()
		seconds := completed.Sub(started).Seconds()
		receipt.Phases = append(receipt.Phases, preflightPhaseReceipt{Name: phase.Name, Args: phase.Args, Started: started.UTC(), Completed: completed.UTC(), Seconds: seconds, SetupSeconds: seconds - timing.ExecutionSeconds - timing.CleanupSeconds, ExecutionSeconds: timing.ExecutionSeconds, CleanupSeconds: timing.CleanupSeconds, Passed: err == nil})
		receipt.ExecutionSeconds += timing.ExecutionSeconds
		receipt.CleanupSeconds += timing.CleanupSeconds
		fmt.Fprintf(os.Stderr, "preflight %s: passed=%t %.3fs\n", phase.Name, err == nil, time.Since(started).Seconds())
		if err != nil {
			return fmt.Errorf("preflight %s: %w", phase.Name, err)
		}
	}
	receipt.Complete = true
	return nil
}

func checkPreflightSource(ctx context.Context, plan testplanning.PreflightPlan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := completionSource(".", plan.HeadSHA, true); err != nil {
		return err
	}
	build, err := testplanning.EffectiveBuildContext(ctx, ".")
	if err != nil {
		return err
	}
	if build != plan.BuildContext {
		return fmt.Errorf("effective preflight build selection changed")
	}
	return nil
}

func executePreflightPhase(ctx context.Context, directory string, phase preflightPhase) (timing preflightProcessTiming, err error) {
	path := filepath.Join(directory, strings.ReplaceAll(phase.Name, "/", "_")+".log")
	log, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return timing, err
	}
	defer func() {
		started := time.Now()
		err = errors.Join(err, log.Close())
		timing.CleanupSeconds += time.Since(started).Seconds()
	}()
	timing, err = runPreflightProcess(ctx, phase.Args, log, os.Stderr)
	if err != nil {
		return timing, fmt.Errorf("command failed; output retained at %s: %w", path, err)
	}
	if _, err := log.Seek(0, io.SeekStart); err != nil {
		return timing, err
	}
	if phase.Package != nil {
		report, err := testtiming.ParseReport(log)
		if err != nil {
			return timing, err
		}
		return timing, testtiming.ValidatePreflightReport(*phase.Package, report)
	}
	if phase.Name == "format" {
		raw, err := io.ReadAll(log)
		if err != nil {
			return timing, err
		}
		if len(bytes.TrimSpace(raw)) != 0 {
			return timing, fmt.Errorf("unformatted source:\n%s", raw)
		}
	}
	return timing, nil
}
