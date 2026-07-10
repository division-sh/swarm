package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/testtiming"
)

func main() {
	var inputPath string
	var markdownPath string
	var packagesPath string
	var weightsPath string
	var snapshotPath string
	var sourceLabel string
	var shardMatrixPath string
	var evidencePath string
	var evidenceRoot string
	var policyPath string
	var surface string
	var attempt string
	var environmentID string
	var countMode string
	var resultJSONPath string
	var fullPackagesPath string
	var topN int
	var shardCount int
	var shardPackages int
	var exitCode int
	var maxImbalance float64
	var elapsedSeconds float64
	var generateShards bool
	var checkShards bool
	var recordEvidence bool
	var checkConfirmation bool
	var evaluateBudget bool
	var expectFull bool
	flag.StringVar(&inputPath, "input", "-", "path to go test -json output, or - for stdin")
	flag.StringVar(&markdownPath, "markdown", "-", "path to write Markdown report, or - for stdout")
	flag.StringVar(&packagesPath, "packages", "", "path to newline-delimited Go package list for shard snapshot operations")
	flag.StringVar(&weightsPath, "weights", "", "optional go test -json timing input for shard generation")
	flag.StringVar(&snapshotPath, "snapshot", "", "path to shard snapshot for shard operations")
	flag.StringVar(&sourceLabel, "source-label", "", "optional source label recorded in generated shard snapshots")
	flag.StringVar(&shardMatrixPath, "shard-matrix", "", "path to write GitHub Actions shard matrix JSON, or - for stdout")
	flag.StringVar(&evidencePath, "evidence", "", "path to read or write typed command evidence")
	flag.StringVar(&evidenceRoot, "evidence-root", "", "directory containing *-evidence.json files for budget evaluation")
	flag.StringVar(&policyPath, "policy", "", "path to the committed timing budget policy")
	flag.StringVar(&surface, "surface", "", "command surface identity (shard-N or full-conformance)")
	flag.StringVar(&attempt, "attempt", "", "command attempt (primary or confirmation)")
	flag.StringVar(&environmentID, "environment-id", "", "declared command environment identity")
	flag.StringVar(&countMode, "count-mode", "", "test count mode (cache-default or count-1)")
	flag.StringVar(&resultJSONPath, "result-json", "", "path to write machine-readable budget result")
	flag.StringVar(&fullPackagesPath, "full-packages", "", "path to the canonical full-conformance package list")
	flag.IntVar(&topN, "top", 20, "number of slow packages and tests to include")
	flag.IntVar(&shardCount, "shards", 4, "number of shards to generate")
	flag.IntVar(&shardPackages, "shard-packages", 0, "print packages assigned to the given shard ID")
	flag.IntVar(&exitCode, "exit-code", -1, "recorded go test command exit code")
	flag.Float64Var(&maxImbalance, "max-imbalance", 0.25, "warning threshold for shard weight imbalance")
	flag.Float64Var(&elapsedSeconds, "elapsed-seconds", -1, "recorded go test command elapsed seconds")
	flag.BoolVar(&generateShards, "generate-shards", false, "generate a shard snapshot from package list and optional timing weights")
	flag.BoolVar(&checkShards, "check-shards", false, "validate a shard snapshot against a package list")
	flag.BoolVar(&recordEvidence, "record-evidence", false, "record typed command evidence from go test JSON")
	flag.BoolVar(&checkConfirmation, "check-confirmation", false, "print whether a primary evidence file requires confirmation")
	flag.BoolVar(&evaluateBudget, "evaluate-budget", false, "evaluate complete shard/full command evidence")
	flag.BoolVar(&expectFull, "expect-full", false, "require full-conformance evidence during budget evaluation")
	flag.Parse()

	if err := run(runConfig{
		inputPath:         inputPath,
		markdownPath:      markdownPath,
		packagesPath:      packagesPath,
		weightsPath:       weightsPath,
		snapshotPath:      snapshotPath,
		sourceLabel:       sourceLabel,
		shardMatrixPath:   shardMatrixPath,
		evidencePath:      evidencePath,
		evidenceRoot:      evidenceRoot,
		policyPath:        policyPath,
		surface:           surface,
		attempt:           attempt,
		environmentID:     environmentID,
		countMode:         countMode,
		resultJSONPath:    resultJSONPath,
		fullPackagesPath:  fullPackagesPath,
		topN:              topN,
		shardCount:        shardCount,
		shardPackages:     shardPackages,
		exitCode:          exitCode,
		maxImbalance:      maxImbalance,
		elapsedSeconds:    elapsedSeconds,
		generateShards:    generateShards,
		checkShards:       checkShards,
		recordEvidence:    recordEvidence,
		checkConfirmation: checkConfirmation,
		evaluateBudget:    evaluateBudget,
		expectFull:        expectFull,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type runConfig struct {
	inputPath         string
	markdownPath      string
	packagesPath      string
	weightsPath       string
	snapshotPath      string
	sourceLabel       string
	shardMatrixPath   string
	evidencePath      string
	evidenceRoot      string
	policyPath        string
	surface           string
	attempt           string
	environmentID     string
	countMode         string
	resultJSONPath    string
	fullPackagesPath  string
	topN              int
	shardCount        int
	shardPackages     int
	exitCode          int
	maxImbalance      float64
	elapsedSeconds    float64
	generateShards    bool
	checkShards       bool
	recordEvidence    bool
	checkConfirmation bool
	evaluateBudget    bool
	expectFull        bool
}

func run(cfg runConfig) error {
	switch {
	case cfg.recordEvidence:
		return recordCommandEvidence(cfg)
	case cfg.checkConfirmation:
		return checkCommandConfirmation(cfg)
	case cfg.evaluateBudget:
		return evaluateTimingBudget(cfg)
	case cfg.generateShards:
		return generateShardSnapshot(cfg)
	case cfg.checkShards:
		return checkShardSnapshot(cfg)
	case cfg.shardPackages > 0:
		return printShardPackages(cfg)
	case cfg.shardMatrixPath != "":
		return writeShardMatrix(cfg)
	default:
		return writeTimingReport(cfg)
	}
}

func recordCommandEvidence(cfg runConfig) error {
	if strings.TrimSpace(cfg.evidencePath) == "" {
		return fmt.Errorf("-evidence is required with -record-evidence")
	}
	if cfg.elapsedSeconds < 0 {
		return fmt.Errorf("-elapsed-seconds must be non-negative with -record-evidence")
	}
	if cfg.exitCode < 0 {
		return fmt.Errorf("-exit-code must be non-negative with -record-evidence")
	}
	packages, err := readPackagesFile(cfg.packagesPath)
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
	evidence := testtiming.CommandEvidence{
		Version:        testtiming.CommandEvidenceVersion,
		Surface:        strings.TrimSpace(cfg.surface),
		Attempt:        strings.TrimSpace(cfg.attempt),
		ElapsedSeconds: cfg.elapsedSeconds,
		ExitCode:       cfg.exitCode,
		Packages:       packages,
		EnvironmentID:  strings.TrimSpace(cfg.environmentID),
		CountMode:      strings.TrimSpace(cfg.countMode),
		Report:         report,
	}
	return writeJSONFile(cfg.evidencePath, evidence)
}

func checkCommandConfirmation(cfg runConfig) error {
	policy, err := readBudgetPolicy(cfg.policyPath)
	if err != nil {
		return err
	}
	evidence, err := readCommandEvidence(cfg.evidencePath)
	if err != nil {
		return err
	}
	required, err := testtiming.ConfirmationRequired(policy, evidence)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, required)
	return err
}

func evaluateTimingBudget(cfg runConfig) error {
	if strings.TrimSpace(cfg.resultJSONPath) == "" {
		return fmt.Errorf("-result-json is required with -evaluate-budget")
	}
	policy, err := readBudgetPolicy(cfg.policyPath)
	if err != nil {
		return err
	}
	snapshot, err := readShardSnapshot(cfg.snapshotPath)
	if err != nil {
		return err
	}
	fullPackages, err := readPackagesFile(cfg.fullPackagesPath)
	if err != nil {
		return fmt.Errorf("read full-conformance packages: %w", err)
	}
	evidence, loadProblems := readEvidenceTree(cfg.evidenceRoot)
	result := testtiming.EvaluateBudget(policy, testtiming.EvaluationOptions{
		Snapshot:     snapshot,
		FullPackages: fullPackages,
		ExpectFull:   cfg.expectFull,
		LoadProblems: loadProblems,
	}, evidence)
	jsonOutput, closeJSON, err := openOutput(cfg.resultJSONPath)
	if err != nil {
		return err
	}
	if err := testtiming.WriteBudgetJSON(jsonOutput, result); err != nil {
		closeJSON()
		return err
	}
	closeJSON()
	markdownOutput, closeMarkdown, err := openOutput(cfg.markdownPath)
	if err != nil {
		return err
	}
	if err := testtiming.WriteBudgetMarkdown(markdownOutput, result); err != nil {
		closeMarkdown()
		return err
	}
	closeMarkdown()
	if result.ExitCode() != 0 {
		return fmt.Errorf("timing budget status: %s", result.Status)
	}
	return nil
}

func writeTimingReport(cfg runConfig) error {
	input, closeInput, err := openInput(cfg.inputPath)
	if err != nil {
		return err
	}
	defer closeInput()

	report, err := testtiming.ParseReport(input)
	if err != nil {
		return err
	}

	output, closeOutput, err := openOutput(cfg.markdownPath)
	if err != nil {
		return err
	}
	defer closeOutput()
	return testtiming.WriteMarkdown(output, report, testtiming.MarkdownOptions{TopN: cfg.topN})
}

func generateShardSnapshot(cfg runConfig) error {
	if strings.TrimSpace(cfg.snapshotPath) == "" {
		return fmt.Errorf("-snapshot is required with -generate-shards")
	}
	packages, err := readPackagesFile(cfg.packagesPath)
	if err != nil {
		return err
	}
	weights := map[string]float64{}
	source := "package-list"
	if strings.TrimSpace(cfg.weightsPath) != "" {
		input, closeInput, err := openInput(cfg.weightsPath)
		if err != nil {
			return err
		}
		defer closeInput()
		weights, err = testtiming.ParsePackageWeights(input)
		if err != nil {
			return err
		}
		source = cfg.weightsPath
	}
	if strings.TrimSpace(cfg.sourceLabel) != "" {
		source = strings.TrimSpace(cfg.sourceLabel)
	}
	snapshot, err := testtiming.BuildShardSnapshot(packages, weights, cfg.shardCount, source, cfg.maxImbalance)
	if err != nil {
		return err
	}
	return writeJSONFile(cfg.snapshotPath, snapshot)
}

func checkShardSnapshot(cfg runConfig) error {
	snapshot, err := readShardSnapshot(cfg.snapshotPath)
	if err != nil {
		return err
	}
	packages, err := readPackagesFile(cfg.packagesPath)
	if err != nil {
		return err
	}
	validation, err := testtiming.ValidateShardSnapshot(snapshot, packages)
	if err != nil {
		return formatShardValidationError(validation, err)
	}
	if snapshot.MaxImbalance > 0 && validation.ImbalanceRatio > snapshot.MaxImbalance {
		fmt.Fprintf(os.Stderr, "warning: shard imbalance %.1f%% exceeds configured %.1f%% (min %.3fs, max %.3fs)\n",
			validation.ImbalanceRatio*100, snapshot.MaxImbalance*100, validation.MinWeight, validation.MaxWeight)
	}
	if cfg.shardMatrixPath != "" {
		return writeShardMatrixFromSnapshot(cfg.shardMatrixPath, snapshot)
	}
	return nil
}

func printShardPackages(cfg runConfig) error {
	snapshot, err := readShardSnapshot(cfg.snapshotPath)
	if err != nil {
		return err
	}
	return writeShardPackages(os.Stdout, snapshot, cfg.shardPackages)
}

func writeShardPackages(w io.Writer, snapshot testtiming.ShardSnapshot, shardID int) error {
	packages, err := testtiming.PackagesForShard(snapshot, shardID)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, strings.Join(packages, "\n"))
	return err
}

func writeShardMatrix(cfg runConfig) error {
	snapshot, err := readShardSnapshot(cfg.snapshotPath)
	if err != nil {
		return err
	}
	return writeShardMatrixFromSnapshot(cfg.shardMatrixPath, snapshot)
}

func writeShardMatrixFromSnapshot(path string, snapshot testtiming.ShardSnapshot) error {
	raw, err := testtiming.ShardMatrixJSON(snapshot)
	if err != nil {
		return err
	}
	output, closeOutput, err := openOutput(path)
	if err != nil {
		return err
	}
	defer closeOutput()
	_, err = output.Write(raw)
	return err
}

func readPackagesFile(path string) ([]string, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("-packages is required for shard snapshot operations")
	}
	input, closeInput, err := openInput(path)
	if err != nil {
		return nil, err
	}
	defer closeInput()
	return testtiming.ReadPackageList(input)
}

func readShardSnapshot(path string) (testtiming.ShardSnapshot, error) {
	if strings.TrimSpace(path) == "" {
		return testtiming.ShardSnapshot{}, fmt.Errorf("-snapshot is required for shard operations")
	}
	input, closeInput, err := openInput(path)
	if err != nil {
		return testtiming.ShardSnapshot{}, err
	}
	defer closeInput()
	var snapshot testtiming.ShardSnapshot
	if err := json.NewDecoder(input).Decode(&snapshot); err != nil {
		return testtiming.ShardSnapshot{}, fmt.Errorf("decode shard snapshot %s: %w", path, err)
	}
	return snapshot, nil
}

func readBudgetPolicy(path string) (testtiming.BudgetPolicy, error) {
	if strings.TrimSpace(path) == "" {
		return testtiming.BudgetPolicy{}, fmt.Errorf("-policy is required")
	}
	input, closeInput, err := openInput(path)
	if err != nil {
		return testtiming.BudgetPolicy{}, err
	}
	defer closeInput()
	policy, err := testtiming.LoadBudgetPolicy(input)
	if err != nil {
		return testtiming.BudgetPolicy{}, fmt.Errorf("load budget policy %s: %w", path, err)
	}
	return policy, nil
}

func readCommandEvidence(path string) (testtiming.CommandEvidence, error) {
	if strings.TrimSpace(path) == "" {
		return testtiming.CommandEvidence{}, fmt.Errorf("-evidence is required")
	}
	input, closeInput, err := openInput(path)
	if err != nil {
		return testtiming.CommandEvidence{}, err
	}
	defer closeInput()
	var evidence testtiming.CommandEvidence
	decoder := json.NewDecoder(input)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&evidence); err != nil {
		return testtiming.CommandEvidence{}, fmt.Errorf("decode command evidence %s: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return testtiming.CommandEvidence{}, fmt.Errorf("decode command evidence %s: multiple JSON values", path)
		}
		return testtiming.CommandEvidence{}, fmt.Errorf("decode command evidence %s trailing data: %w", path, err)
	}
	return evidence, nil
}

func readEvidenceTree(root string) ([]testtiming.CommandEvidence, []string) {
	if strings.TrimSpace(root) == "" {
		return nil, []string{"-evidence-root is required"}
	}
	var evidence []testtiming.CommandEvidence
	var problems []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			problems = append(problems, fmt.Sprintf("read evidence path %s: %v", path, walkErr))
			return nil
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "-evidence.json") {
			return nil
		}
		item, err := readCommandEvidence(path)
		if err != nil {
			problems = append(problems, err.Error())
			return nil
		}
		evidence = append(evidence, item)
		return nil
	})
	if err != nil {
		problems = append(problems, fmt.Sprintf("walk evidence root %s: %v", root, err))
	}
	return evidence, problems
}

func writeJSONFile(path string, value any) error {
	output, closeOutput, err := openOutput(path)
	if err != nil {
		return err
	}
	defer closeOutput()
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func formatShardValidationError(validation testtiming.ShardValidation, err error) error {
	var parts []string
	if len(validation.Missing) > 0 {
		parts = append(parts, "missing packages: "+strings.Join(validation.Missing, ", "))
	}
	if len(validation.Extra) > 0 {
		parts = append(parts, "extra packages: "+strings.Join(validation.Extra, ", "))
	}
	if len(validation.Duplicates) > 0 {
		parts = append(parts, "duplicate packages: "+strings.Join(validation.Duplicates, ", "))
	}
	if len(parts) == 0 {
		return err
	}
	return fmt.Errorf("%w: %s", err, strings.Join(parts, "; "))
}

func openInput(path string) (io.Reader, func(), error) {
	if path == "" || path == "-" {
		return os.Stdin, func() {}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, func() {}, fmt.Errorf("open input %s: %w", path, err)
	}
	return file, func() { _ = file.Close() }, nil
}

func openOutput(path string) (io.Writer, func(), error) {
	if path == "" || path == "-" {
		return os.Stdout, func() {}, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, func() {}, fmt.Errorf("create output %s: %w", path, err)
	}
	return file, func() { _ = file.Close() }, nil
}
