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
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testtiming"
)

type preflightPhase struct {
	Name    string                         `json:"name"`
	Args    []string                       `json:"args"`
	Package *testplanning.PreflightPackage `json:"package,omitempty"`
}

type preflightReceipt struct {
	Plan         testplanning.PreflightPlan `json:"plan"`
	Started      time.Time                  `json:"started"`
	Seconds      float64                    `json:"seconds"`
	SetupSeconds float64                    `json:"setup_seconds"`
	Complete     bool                       `json:"complete"`
	Phases       []preflightPhaseReceipt    `json:"phases"`
	Unstarted    []string                   `json:"unstarted"`
	Problem      string                     `json:"problem,omitempty"`
}

type preflightPhaseReceipt struct {
	Name    string   `json:"name"`
	Args    []string `json:"args"`
	Seconds float64  `json:"seconds"`
	Passed  bool     `json:"passed"`
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
	capacity, err := testpostgres.RunCapacityFromEnvironment()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	admission, err := testpostgres.DefaultRunAdmission(os.Stderr)
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
	err = executePreflight(ctx, directory, &receipt, admission, capacity)
	receipt.Seconds = time.Since(started).Seconds()
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
	if receipt.Seconds > 300 {
		fmt.Fprintln(output, "preflight soft target exceeded: review/re-rank membership; no test deadline or selector changed")
	}
	fmt.Fprintf(output, "preflight: complete=%t %.3fs; source=%s go=%s selection=%s receipts=%s\n", receipt.Complete, receipt.Seconds, receipt.Plan.HeadSHA, receipt.Plan.BuildContext.GOVERSION, receipt.Plan.Digest, directory)
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
		{Name: "complexity", Args: []string{"go", "run", "./cmd/swarm-complexity", "-head", plan.HeadSHA, "-base", plan.BaseSHA}},
		{Name: "openrpc", Args: []string{"go", "run", "./cmd/swarm-openrpc-gen", "--check"}},
		{Name: "vet", Args: []string{"go", "vet", "./..."}},
	}
	for _, pkg := range plan.Packages {
		phases = append(phases, preflightPhase{Name: pkg.Package, Package: &pkg, Args: []string{"go", "test", "-json", "-count=1", "-run", pkg.Run, pkg.Package}})
	}
	return phases
}

func executePreflight(ctx context.Context, directory string, receipt *preflightReceipt, admission *testpostgres.RunAdmission, capacity int) error {
	for _, phase := range preflightPhases(receipt.Plan) {
		receipt.Unstarted = append(receipt.Unstarted, phase.Name)
	}
	for _, phase := range preflightPhases(receipt.Plan) {
		if err := checkPreflightSource(ctx, receipt.Plan); err != nil {
			return err
		}
		started := time.Now()
		err := executePreflightPhase(ctx, directory, phase, admission, capacity)
		err = errors.Join(err, checkPreflightSource(ctx, receipt.Plan))
		receipt.Unstarted = receipt.Unstarted[1:]
		receipt.Phases = append(receipt.Phases, preflightPhaseReceipt{Name: phase.Name, Args: phase.Args, Seconds: time.Since(started).Seconds(), Passed: err == nil})
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

func executePreflightPhase(ctx context.Context, directory string, phase preflightPhase, admission *testpostgres.RunAdmission, capacity int) (err error) {
	path := filepath.Join(directory, strings.ReplaceAll(phase.Name, "/", "_")+".log")
	log, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, log.Close()) }()
	if phase.Package != nil {
		err = runPreflightProcess(ctx, phase.Args, log, os.Stderr, admission, capacity)
	} else {
		err = runPreflightStaticProcess(ctx, phase.Args, log, os.Stderr)
	}
	if err != nil {
		return fmt.Errorf("command failed; output retained at %s: %w", path, err)
	}
	if _, err := log.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if phase.Package != nil {
		report, err := testtiming.ParseReport(log)
		if err != nil {
			return err
		}
		return testtiming.ValidatePreflightReport(*phase.Package, report)
	}
	if phase.Name == "format" {
		raw, err := io.ReadAll(log)
		if err != nil {
			return err
		}
		if len(bytes.TrimSpace(raw)) != 0 {
			return fmt.Errorf("unformatted source:\n%s", raw)
		}
	}
	return nil
}

func runPreflightProcess(ctx context.Context, args []string, out, stderr io.Writer, admission *testpostgres.RunAdmission, capacity int) error {
	lease, err := admission.Acquire(ctx, testpostgres.RunCommand{Args: args, ModelOnlyETA: true}, capacity)
	if err != nil {
		return err
	}
	command := exec.CommandContext(ctx, args[0], args[1:]...)
	command.Stdout, command.Stderr = out, stderr
	command.Env = replaceEnvironment(os.Environ(), testpostgres.RunWrapperEnv, "1")
	if err = prepareChildProcessTree(command); err == nil {
		err = lease.InheritTo(command)
	}
	if err == nil {
		command.Cancel = func() error { return signalChildProcessTree(command, syscall.SIGTERM) }
		err = command.Run()
	}
	settlement, cancel := context.WithTimeout(context.Background(), authoritySettlementTimeout)
	defer cancel()
	joinErr := lease.Join(settlement)
	if joinErr != nil {
		return errors.Join(err, joinErr)
	}
	return errors.Join(err, ctx.Err(), lease.Complete(settlement, err == nil && ctx.Err() == nil))
}

func runPreflightStaticProcess(ctx context.Context, args []string, out, stderr io.Writer) error {
	command := exec.CommandContext(ctx, args[0], args[1:]...)
	command.Stdout, command.Stderr = out, stderr
	if err := prepareChildProcessTree(command); err != nil {
		return err
	}
	command.Cancel = func() error { return signalChildProcessTree(command, syscall.SIGTERM) }
	return errors.Join(command.Run(), ctx.Err())
}
