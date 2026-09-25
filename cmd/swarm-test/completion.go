package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testtiming"
)

func runCompletion(profile string) int {
	inventory, err := testplanning.DiscoverRootInventory(context.Background(), ".")
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
	head, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "read source commit: %v\n", err)
		return 1
	}
	plan, err := testplanning.BuildPlan(policy, model, packages, profile, "explicit wrapper completion", strings.TrimSpace(string(head)))
	if err == nil {
		err = testplanning.BindExecution(&plan, inventory, proofs)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "swarm-test %s: %d planned units; modeled ETA is approximate\n", profile, len(plan.Units))
	for _, unit := range plan.Units {
		fmt.Fprintf(os.Stderr, "swarm-test %s: starting %s\n", profile, unit.ID)
		if code := executeCompletionUnit(plan, unit); code != 0 {
			return code
		}
	}
	fmt.Fprintf(os.Stderr, "swarm-test %s: all %d planned units passed required execution\n", profile, len(plan.Units))
	return 0
}

func runPlanned(args []string) int {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run ./cmd/swarm-test --planned <plan.json> <unit-id>")
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
	return executeCompletionUnit(plan, unit)
}

func executeCompletionUnit(plan testplanning.RunPlan, unit testplanning.ProofUnit) int {
	file, err := os.CreateTemp("", "swarm-test-unit-*.json")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.Remove(file.Name())
	defer file.Close()
	start := time.Now()
	code := runTestArgs(unitTestArgs(unit), io.MultiWriter(os.Stdout, file), true, unit.WorkloadProfile, unit.ExecutionTier, modeledDuration(unit))
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
