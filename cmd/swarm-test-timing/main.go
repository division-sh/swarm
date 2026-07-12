package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testtiming"
)

type config struct {
	inputPath       string
	markdownPath    string
	packagesPath    string
	changedPath     string
	proofPolicyPath string
	weightModelPath string
	planPath        string
	matrixPath      string
	evidencePath    string
	evidenceRoot    string
	budgetPath      string
	resultJSONPath  string
	event           string
	profile         string
	headSHA         string
	executionSHA    string
	unitID          string
	attempt         string
	sourceRunID     string
	topN            int
	exitCode        int
	elapsedSeconds  float64
	planCI          bool
	recordEvidence  bool
	checkConfirm    bool
	evaluateBudget  bool
	updateWeights   bool
	validatePublish bool
	assertExecution bool
}

func main() {
	var cfg config
	flag.StringVar(&cfg.inputPath, "input", "-", "path to go test -json output, or - for stdin")
	flag.StringVar(&cfg.markdownPath, "markdown", "-", "path to write Markdown output, or - for stdout")
	flag.StringVar(&cfg.packagesPath, "packages", "", "newline-delimited discovered Go package inventory")
	flag.StringVar(&cfg.changedPath, "changed-files", "", "newline-delimited changed paths")
	flag.StringVar(&cfg.proofPolicyPath, "proof-policy", ".github/test-proof-plan.yaml", "canonical proof policy")
	flag.StringVar(&cfg.weightModelPath, "weight-model", ".github/test-timing-weights.json", "generated historical weight model")
	flag.StringVar(&cfg.planPath, "plan", "", "run plan path")
	flag.StringVar(&cfg.matrixPath, "matrix", "", "GitHub Actions matrix output path")
	flag.StringVar(&cfg.evidencePath, "evidence", "", "typed command evidence path")
	flag.StringVar(&cfg.evidenceRoot, "evidence-root", "", "directory containing evidence JSON")
	flag.StringVar(&cfg.budgetPath, "policy", ".github/test-timing-budgets.yaml", "timing budget policy")
	flag.StringVar(&cfg.resultJSONPath, "result-json", "", "machine-readable budget result path")
	flag.StringVar(&cfg.event, "event", "", "GitHub event name")
	flag.StringVar(&cfg.profile, "profile", "", "explicit proof profile")
	flag.StringVar(&cfg.headSHA, "head-sha", "", "exact tested commit SHA")
	flag.StringVar(&cfg.executionSHA, "execution-sha", "", "actual checked-out commit SHA")
	flag.StringVar(&cfg.unitID, "unit", "", "proof unit ID")
	flag.StringVar(&cfg.attempt, "attempt", "", "primary or confirmation")
	flag.StringVar(&cfg.sourceRunID, "source-run-id", "", "successful source run for generated weights")
	flag.IntVar(&cfg.topN, "top", 20, "number of slow packages/tests in Markdown")
	flag.IntVar(&cfg.exitCode, "exit-code", -1, "recorded go test exit code")
	flag.Float64Var(&cfg.elapsedSeconds, "elapsed-seconds", -1, "recorded command elapsed seconds")
	flag.BoolVar(&cfg.planCI, "plan-ci", false, "emit a digest-bound CI run plan and matrix")
	flag.BoolVar(&cfg.recordEvidence, "record-evidence", false, "record evidence bound to a plan unit")
	flag.BoolVar(&cfg.checkConfirm, "check-confirmation", false, "print whether this exact unit needs confirmation")
	flag.BoolVar(&cfg.evaluateBudget, "evaluate-budget", false, "evaluate complete evidence against the emitted plan")
	flag.BoolVar(&cfg.updateWeights, "update-weight-model", false, "update generated weights from successful plan evidence")
	flag.BoolVar(&cfg.validatePublish, "validate-publish-diff", false, "fail unless changed-files contains only the generated model")
	flag.BoolVar(&cfg.assertExecution, "assert-execution-sha", false, "fail unless the checked-out commit matches the run plan")
	flag.Parse()

	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	modes := 0
	for _, enabled := range []bool{cfg.planCI, cfg.recordEvidence, cfg.checkConfirm, cfg.evaluateBudget, cfg.updateWeights, cfg.validatePublish, cfg.assertExecution} {
		if enabled {
			modes++
		}
	}
	if modes > 1 {
		return fmt.Errorf("exactly one command mode may be selected")
	}
	switch {
	case cfg.planCI:
		return planCI(cfg)
	case cfg.recordEvidence:
		return recordEvidence(cfg)
	case cfg.checkConfirm:
		return checkConfirmation(cfg)
	case cfg.evaluateBudget:
		return evaluateBudget(cfg)
	case cfg.updateWeights:
		return updateWeightModel(cfg)
	case cfg.validatePublish:
		paths, err := readLines(cfg.changedPath)
		if err != nil {
			return err
		}
		return testplanning.ValidatePublicationDiff(paths)
	case cfg.assertExecution:
		if cfg.planPath == "" || cfg.executionSHA == "" {
			return fmt.Errorf("-plan and -execution-sha are required with -assert-execution-sha")
		}
		plan, err := readPlan(cfg.planPath)
		if err != nil {
			return err
		}
		return plan.ValidateExecutionSHA(cfg.executionSHA)
	default:
		return writeTimingReport(cfg)
	}
}

func planCI(cfg config) error {
	if cfg.planPath == "" || cfg.matrixPath == "" || cfg.packagesPath == "" || cfg.event == "" || cfg.headSHA == "" {
		return fmt.Errorf("-plan, -matrix, -packages, -event, and -head-sha are required with -plan-ci")
	}
	policy, err := readProofPolicy(cfg.proofPolicyPath)
	if err != nil {
		return err
	}
	model, err := readWeightModel(cfg.weightModelPath)
	if err != nil {
		return err
	}
	packages, err := readLines(cfg.packagesPath)
	if err != nil {
		return err
	}
	changed, err := readOptionalLines(cfg.changedPath)
	if err != nil {
		return err
	}
	profile, reason, err := policy.ResolveProfile(cfg.event, changed, cfg.profile)
	if err != nil {
		return err
	}
	plan, err := testplanning.BuildPlan(policy, model, packages, profile, reason, cfg.headSHA)
	if err != nil {
		return err
	}
	if err := writeJSON(cfg.planPath, plan); err != nil {
		return err
	}
	matrix, err := testplanning.MatrixJSON(plan)
	if err != nil {
		return err
	}
	if err := os.WriteFile(cfg.matrixPath, append(matrix, '\n'), 0o644); err != nil {
		return fmt.Errorf("write matrix: %w", err)
	}
	return writePlanMarkdown(cfg.markdownPath, plan)
}

func recordEvidence(cfg config) error {
	if cfg.planPath == "" || cfg.unitID == "" || cfg.evidencePath == "" || cfg.elapsedSeconds < 0 || cfg.exitCode < 0 {
		return fmt.Errorf("-plan, -unit, -evidence, non-negative -elapsed-seconds, and non-negative -exit-code are required with -record-evidence")
	}
	plan, err := readPlan(cfg.planPath)
	if err != nil {
		return err
	}
	unit, err := plan.Unit(cfg.unitID)
	if err != nil {
		return err
	}
	input, closeInput, err := openInput(cfg.inputPath)
	if err != nil {
		return err
	}
	defer closeInput()
	report, err := testtiming.ParseReport(input)
	if err != nil {
		return err
	}
	countMode := unit.CountMode
	if cfg.attempt == testtiming.AttemptConfirmation {
		countMode = testtiming.CountModeOne
	}
	evidence := testtiming.CommandEvidence{
		Version:        testtiming.CommandEvidenceVersion,
		PlanDigest:     plan.Digest,
		Profile:        plan.Profile,
		HeadSHA:        plan.HeadSHA,
		UnitID:         unit.ID,
		Surface:        unit.ID,
		Attempt:        cfg.attempt,
		ElapsedSeconds: cfg.elapsedSeconds,
		ExitCode:       cfg.exitCode,
		Packages:       append([]string(nil), unit.Packages...),
		EnvironmentID:  unit.EnvironmentID,
		CountMode:      countMode,
		Report:         report,
	}
	return writeJSON(cfg.evidencePath, evidence)
}

func checkConfirmation(cfg config) error {
	plan, err := readPlan(cfg.planPath)
	if err != nil {
		return err
	}
	policy, err := readBudgetPolicy(cfg.budgetPath)
	if err != nil {
		return err
	}
	evidence, err := readEvidence(cfg.evidencePath)
	if err != nil {
		return err
	}
	required, err := testtiming.ConfirmationRequired(policy, plan, evidence)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, required)
	return err
}

func evaluateBudget(cfg config) error {
	if cfg.resultJSONPath == "" || cfg.markdownPath == "" || cfg.evidenceRoot == "" {
		return fmt.Errorf("-result-json, -markdown, and -evidence-root are required with -evaluate-budget")
	}
	plan, err := readPlan(cfg.planPath)
	if err != nil {
		return err
	}
	policy, err := readBudgetPolicy(cfg.budgetPath)
	if err != nil {
		return err
	}
	model, err := readWeightModel(cfg.weightModelPath)
	if err != nil {
		return err
	}
	evidence, problems := readEvidenceTree(cfg.evidenceRoot)
	result := testtiming.EvaluateBudget(policy, testtiming.EvaluationOptions{
		Plan:              plan,
		HistoricalWeights: model.Packages,
		LoadProblems:      problems,
	}, evidence)
	if err := writeJSON(cfg.resultJSONPath, result); err != nil {
		return err
	}
	out, closeOutput, err := openOutput(cfg.markdownPath)
	if err != nil {
		return err
	}
	defer closeOutput()
	if err := testtiming.WriteBudgetMarkdown(out, result); err != nil {
		return err
	}
	if result.ExitCode() != 0 {
		return fmt.Errorf("timing budget status is %s", result.Status)
	}
	return nil
}

func updateWeightModel(cfg config) error {
	if cfg.planPath == "" || cfg.evidenceRoot == "" || cfg.sourceRunID == "" {
		return fmt.Errorf("-plan, -evidence-root, and -source-run-id are required with -update-weight-model")
	}
	plan, err := readPlan(cfg.planPath)
	if err != nil {
		return err
	}
	current, err := readWeightModel(cfg.weightModelPath)
	if err != nil {
		return err
	}
	evidence, problems := readEvidenceTree(cfg.evidenceRoot)
	if len(problems) > 0 {
		return fmt.Errorf("cannot update weights from incomplete evidence: %s", strings.Join(problems, "; "))
	}
	observed := map[string]float64{}
	seenUnits := map[string]bool{}
	for _, item := range evidence {
		if item.Attempt != testtiming.AttemptPrimary {
			continue
		}
		if problems := testtiming.ValidateCommandEvidence(item, plan); len(problems) > 0 {
			return fmt.Errorf("invalid evidence for %s: %s", item.UnitID, strings.Join(problems, "; "))
		}
		if item.ExitCode != 0 {
			return fmt.Errorf("unit %s failed; refusing weight update", item.UnitID)
		}
		seenUnits[item.UnitID] = true
		for _, timing := range item.Report.Packages {
			observed[timing.Package] = timing.Elapsed
		}
	}
	for _, unit := range plan.Units {
		if !seenUnits[unit.ID] {
			return fmt.Errorf("unit %s has no primary evidence", unit.ID)
		}
	}
	next := testplanning.WeightModel{Version: testplanning.WeightModelVersion, SourceRunID: current.SourceRunID, Packages: map[string]float64{}}
	changed := false
	for pkg, value := range observed {
		old, ok := current.Packages[pkg]
		if ok && math.Abs(value-old) <= math.Max(1, old*0.10) {
			next.Packages[pkg] = old
			continue
		}
		next.Packages[pkg] = value
		changed = true
	}
	if len(current.Packages) != len(next.Packages) {
		changed = true
	}
	if !changed {
		_, err := fmt.Fprintln(os.Stdout, "unchanged")
		return err
	}
	next.SourceRunID = cfg.sourceRunID
	file, err := os.Create(cfg.weightModelPath)
	if err != nil {
		return fmt.Errorf("create weight model: %w", err)
	}
	defer file.Close()
	if err := testplanning.WriteWeightModel(file, next); err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, "changed")
	return err
}

func writeTimingReport(cfg config) error {
	input, closeInput, err := openInput(cfg.inputPath)
	if err != nil {
		return err
	}
	defer closeInput()
	report, err := testtiming.ParseReport(input)
	if err != nil {
		return err
	}
	out, closeOutput, err := openOutput(cfg.markdownPath)
	if err != nil {
		return err
	}
	defer closeOutput()
	return testtiming.WriteMarkdown(out, report, testtiming.MarkdownOptions{TopN: cfg.topN})
}

func writePlanMarkdown(path string, plan testplanning.RunPlan) error {
	out, closeOutput, err := openOutput(path)
	if err != nil {
		return err
	}
	defer closeOutput()
	if _, err := fmt.Fprintf(out, "### CI proof plan\n\n- profile: `%s`\n- reason: %s\n- head: `%s`\n- digest: `%s`\n- units: `%d`\n- package granularity ceiling: `%.3fs`\n\n", plan.Profile, plan.Reason, plan.HeadSHA, plan.Digest, len(plan.Units), plan.GranularityMax); err != nil {
		return err
	}
	for _, unit := range plan.Units {
		if _, err := fmt.Fprintf(out, "- `%s`: %.3fs, %d packages, %s/%s\n", unit.ID, unit.WeightSeconds, len(unit.Packages), unit.CountMode, unit.EnvironmentID); err != nil {
			return err
		}
	}
	return nil
}

func readProofPolicy(path string) (testplanning.Policy, error) {
	file, err := os.Open(path)
	if err != nil {
		return testplanning.Policy{}, fmt.Errorf("open proof policy: %w", err)
	}
	defer file.Close()
	return testplanning.LoadPolicy(file)
}

func readWeightModel(path string) (testplanning.WeightModel, error) {
	file, err := os.Open(path)
	if err != nil {
		return testplanning.WeightModel{}, fmt.Errorf("open weight model: %w", err)
	}
	defer file.Close()
	return testplanning.LoadWeightModel(file)
}

func readPlan(path string) (testplanning.RunPlan, error) {
	var plan testplanning.RunPlan
	if err := readJSON(path, &plan); err != nil {
		return plan, err
	}
	if err := plan.Validate(); err != nil {
		return plan, err
	}
	return plan, nil
}

func readBudgetPolicy(path string) (testtiming.BudgetPolicy, error) {
	file, err := os.Open(path)
	if err != nil {
		return testtiming.BudgetPolicy{}, fmt.Errorf("open budget policy: %w", err)
	}
	defer file.Close()
	return testtiming.LoadBudgetPolicy(file)
}

func readEvidence(path string) (testtiming.CommandEvidence, error) {
	var evidence testtiming.CommandEvidence
	return evidence, readJSON(path, &evidence)
}

func readEvidenceTree(root string) ([]testtiming.CommandEvidence, []string) {
	var evidence []testtiming.CommandEvidence
	var problems []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "-evidence.json") {
			return nil
		}
		item, err := readEvidence(path)
		if err != nil {
			problems = append(problems, err.Error())
			return nil
		}
		evidence = append(evidence, item)
		return nil
	})
	if err != nil {
		problems = append(problems, fmt.Sprintf("walk evidence root: %v", err))
	}
	sort.Strings(problems)
	return evidence, problems
}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()
	var values []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if value := strings.TrimSpace(scanner.Text()); value != "" {
			values = append(values, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return values, nil
}

func readOptionalLines(path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	return readLines(path)
}

func readJSON(path string, value any) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode %s: trailing JSON", path)
		}
		return fmt.Errorf("decode %s trailing data: %w", path, err)
	}
	return nil
}

func writeJSON(path string, value any) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func openInput(path string) (io.Reader, func(), error) {
	if path == "" || path == "-" {
		return os.Stdin, func() {}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open input: %w", err)
	}
	return file, func() { _ = file.Close() }, nil
}

func openOutput(path string) (io.Writer, func(), error) {
	if path == "" || path == "-" {
		return os.Stdout, func() {}, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, nil, fmt.Errorf("create output: %w", err)
	}
	return file, func() { _ = file.Close() }, nil
}
