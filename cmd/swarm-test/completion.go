package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testtiming"
)

func runCompletion(profile string, explicit bool) int {
	head, err := completionSource(".", "", explicit)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return runCompletionWithSignals(func(ctx context.Context) int {
		return runCompletionContext(ctx, profile, explicit, head)
	})
}

func runCompletionWithSignals(execute func(context.Context) int) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var interrupted atomic.Int32
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	joined := make(chan struct{})
	stop := make(chan struct{})
	handle := func(value os.Signal) {
		if signal, ok := value.(syscall.Signal); ok {
			interrupted.CompareAndSwap(0, int32(signal))
		}
		cancel()
	}
	go func() {
		defer close(joined)
		for {
			select {
			case value := <-signals:
				handle(value)
			case <-stop:
				// Freeze only after accepted signals have affected the final result.
				for {
					select {
					case value := <-signals:
						handle(value)
					default:
						return
					}
				}
			}
		}
	}()
	code := execute(ctx)
	signal.Stop(signals)
	close(stop)
	<-joined
	if value := interrupted.Load(); value != 0 {
		return receivedSignalExitCode(value)
	}
	return code
}

func runCompletionContext(ctx context.Context, profile string, explicit bool, head string) int {
	if err := completionHostPreflight(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	inventory, err := testplanning.DiscoverRootInventory(ctx, ".")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	policyFile, err := os.Open(".github/test-proof-plan.yaml")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	policy, err := testplanning.LoadPolicy(policyFile)
	_ = policyFile.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	modelFile, err := os.Open(testplanning.GeneratedWeightModelPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	model, err := testplanning.LoadWeightModel(modelFile)
	_ = modelFile.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	proofs, err := testplanning.LoadParityProofs("internal/apiv1/testdata/public_surface_backend_matrix.yaml")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	packages := make([]string, 0, len(inventory.Packages))
	for pkg := range inventory.Packages {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)
	reason := "local developer feedback, not reviewer-bound qualification"
	if explicit {
		reason = "explicit local tier selection; reviewer compares with Local-Tier"
	}
	plan, err := testplanning.BuildPlan(policy, model, packages, profile, reason, head, testplanning.BuildOptions{Venue: testplanning.VenueLocal})
	if err == nil {
		err = testplanning.BindExecution(&plan, inventory, proofs, policy)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if _, err := completionSource(".", head, explicit); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	capacity, err := completionPlanPreflight(ctx, plan)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "swarm-test %s: %d planned units; modeled ETA is approximate\n", profile, len(plan.Units))
	fmt.Fprintf(os.Stderr, "source=%s venue=%s plan=%s tier-deferred=%d; no deferred root earns execution credit\n", plan.HeadSHA, plan.Venue, plan.Digest, len(plan.DeferredRoots))
	receipts := filepath.Join("test-results", "local", profile+"-"+time.Now().UTC().Format("20060102T150405.000000000"))
	if err := os.MkdirAll(receipts, 0700); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	raw, _ := json.MarshalIndent(plan, "", "  ")
	if err := os.WriteFile(filepath.Join(receipts, "proof-plan.json"), raw, 0600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "local proof receipts: %s (effective tier %s)\n", receipts, profile)
	cache, err := os.MkdirTemp("", "swarm-test-build-products-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(cache)
	executable, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	env := replaceEnvironment(os.Environ(), "SWARM_TEST_BUILD_CACHE", cache)
	env = replaceEnvironment(env, testpostgres.RunCapacityEnv, fmt.Sprint(capacity))
	env = replaceEnvironment(env, "GOMAXPROCS", fmt.Sprint(max(1, runtime.GOMAXPROCS(0)/capacity)))
	code := executeCompletionWorkers(ctx, plan, receipts, executable, env, explicit, capacity)
	if _, err := completionSource(".", head, explicit); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if code != 0 {
		return code
	}
	fmt.Fprintf(os.Stderr, "swarm-test %s: all %d planned units passed required execution\n", profile, len(plan.Units))
	return 0
}

func runPlanned(args []string) int {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go run ./cmd/swarm-test --planned <plan.json> <unit-id> [--receipts directory] [--feedback]")
		return 2
	}
	flags := flag.NewFlagSet("planned", flag.ContinueOnError)
	receipts := flags.String("receipts", "", "retain unit evidence in this directory")
	feedback := flags.Bool("feedback", false, "unqualified default-core developer feedback only")
	if err := flags.Parse(args[2:]); err != nil || flags.NArg() != 0 {
		return 2
	}
	raw, err := os.ReadFile(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	var plan testplanning.RunPlan
	if err := json.Unmarshal(raw, &plan); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := plan.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if *feedback && (plan.Venue != testplanning.VenueLocal || plan.Profile != testplanning.ProfileCore || plan.Reason != "local developer feedback, not reviewer-bound qualification") {
		fmt.Fprintln(os.Stderr, "feedback worker cannot execute a reviewer-bound plan")
		return 1
	}
	if _, err := completionSource(".", plan.HeadSHA, !*feedback); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if plan.BuildContext.GOOS == "" {
		fmt.Fprintln(os.Stderr, "planned unit has no bound build context")
		return 1
	}
	unit, err := plan.Unit(args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	actual, err := testplanning.EffectiveBuildContext(context.Background(), ".")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if actual != plan.BuildContext {
		fmt.Fprintln(os.Stderr, "effective Go build context differs from planned context")
		return 1
	}
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := plan.ValidateExecutionSHA(strings.TrimSpace(string(head))); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return executeCompletionUnit(plan, unit, !*feedback, *receipts)
}

func completionSource(repo, expectedHead string, requireClean bool) (string, error) {
	raw, err := exec.Command("git", "-C", repo, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("read source commit: %w", err)
	}
	head := strings.TrimSpace(string(raw))
	if requireClean {
		if expectedHead != "" && head != expectedHead {
			return head, fmt.Errorf("qualification source HEAD changed: planned %s, actual %s", expectedHead, head)
		}
		status, err := exec.Command("git", "-C", repo, "status", "--porcelain=v1", "--untracked-files=all", "--ignore-submodules=none").Output()
		if err != nil {
			return head, fmt.Errorf("inspect qualification source: %w", err)
		}
		if len(status) != 0 {
			return head, fmt.Errorf("reviewer-bound qualification requires clean source at %s:\n%s", head, status)
		}
	}
	return head, nil
}

func executeCompletionUnit(plan testplanning.RunPlan, unit testplanning.ProofUnit, reviewerBound bool, receipts ...string) int {
	if reviewerBound {
		if _, err := completionSource(".", plan.HeadSHA, true); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	var directory string
	if len(receipts) > 0 {
		directory = receipts[0]
	}
	file, err := os.CreateTemp(directory, unit.ID+"-*.json")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if directory == "" {
		defer os.Remove(file.Name())
	}
	defer file.Close()
	start := time.Now()
	code := runTestArgs(unitTestArgs(unit), io.MultiWriter(os.Stdout, file), true, unit.WorkloadProfile, unit.ExecutionTier, modeledDuration(unit))
	if reviewerBound {
		if _, err := completionSource(".", plan.HeadSHA, true); err != nil {
			fmt.Fprintln(os.Stderr, err)
			if code == 0 {
				code = 1
			}
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	report, err := testtiming.ParseReport(file)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	evidence := testtiming.CommandEvidence{
		WorkflowRunID: 1, WorkflowAttempt: 1, Version: testtiming.CommandEvidenceVersion,
		PlanDigest: plan.Digest, Profile: plan.Profile, WorkloadProfile: unit.WorkloadProfile,
		ExecutionTier: unit.ExecutionTier, BuildContext: plan.BuildContext, HeadSHA: plan.HeadSHA,
		UnitID: unit.ID, Surface: unit.ID, Attempt: testtiming.AttemptPrimary,
		ElapsedSeconds: time.Since(start).Seconds(), ExitCode: code, Packages: unit.Packages,
		EnvironmentID: unit.EnvironmentID, CountMode: unit.CountMode, Run: unit.Run,
		Skip: unit.Skip, GoTimeout: unit.GoTimeout, Report: report,
	}
	if directory != "" {
		raw, _ := json.MarshalIndent(evidence, "", "  ")
		if err := os.WriteFile(filepath.Join(directory, unit.ID+"-primary-evidence.json"), raw, 0600); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	if problems := testtiming.ValidateCommandEvidence(evidence, plan); len(problems) != 0 {
		fmt.Fprintf(os.Stderr, "swarm-test %s incomplete: %s\n", unit.ID, strings.Join(problems, "; "))
		return 1
	}
	if code != 0 {
		return code
	}
	return 0
}

func unitTestArgs(unit testplanning.ProofUnit) []string {
	args := append([]string(nil), unit.Packages...)
	if unit.Run != "" {
		args = append(args, "-run", unit.Run)
	}
	if unit.Skip != "" {
		args = append(args, "-skip", unit.Skip)
	}
	if unit.CountMode == "count-1" {
		args = append(args, "-count=1")
	}
	if unit.GoTimeout != "" {
		args = append(args, "-timeout", unit.GoTimeout)
	}
	return append(args, "-json")
}

func modeledDuration(unit testplanning.ProofUnit) time.Duration {
	if unit.WeightSeconds <= 0 {
		return 0
	}
	return time.Duration(unit.WeightSeconds * float64(time.Second))
}

func replaceEnvironment(env []string, name, value string) []string {
	prefix := name + "="
	result := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return append(result, prefix+value)
}
