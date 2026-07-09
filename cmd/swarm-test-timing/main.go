package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
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
	var topN int
	var shardCount int
	var shardPackages int
	var maxImbalance float64
	var generateShards bool
	var checkShards bool
	flag.StringVar(&inputPath, "input", "-", "path to go test -json output, or - for stdin")
	flag.StringVar(&markdownPath, "markdown", "-", "path to write Markdown report, or - for stdout")
	flag.StringVar(&packagesPath, "packages", "", "path to newline-delimited Go package list for shard snapshot operations")
	flag.StringVar(&weightsPath, "weights", "", "optional go test -json timing input for shard generation")
	flag.StringVar(&snapshotPath, "snapshot", "", "path to shard snapshot for shard operations")
	flag.StringVar(&sourceLabel, "source-label", "", "optional source label recorded in generated shard snapshots")
	flag.StringVar(&shardMatrixPath, "shard-matrix", "", "path to write GitHub Actions shard matrix JSON, or - for stdout")
	flag.IntVar(&topN, "top", 20, "number of slow packages and tests to include")
	flag.IntVar(&shardCount, "shards", 4, "number of shards to generate")
	flag.IntVar(&shardPackages, "shard-packages", 0, "print packages assigned to the given shard ID")
	flag.Float64Var(&maxImbalance, "max-imbalance", 0.25, "warning threshold for shard weight imbalance")
	flag.BoolVar(&generateShards, "generate-shards", false, "generate a shard snapshot from package list and optional timing weights")
	flag.BoolVar(&checkShards, "check-shards", false, "validate a shard snapshot against a package list")
	flag.Parse()

	if err := run(runConfig{
		inputPath:       inputPath,
		markdownPath:    markdownPath,
		packagesPath:    packagesPath,
		weightsPath:     weightsPath,
		snapshotPath:    snapshotPath,
		sourceLabel:     sourceLabel,
		shardMatrixPath: shardMatrixPath,
		topN:            topN,
		shardCount:      shardCount,
		shardPackages:   shardPackages,
		maxImbalance:    maxImbalance,
		generateShards:  generateShards,
		checkShards:     checkShards,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type runConfig struct {
	inputPath       string
	markdownPath    string
	packagesPath    string
	weightsPath     string
	snapshotPath    string
	sourceLabel     string
	shardMatrixPath string
	topN            int
	shardCount      int
	shardPackages   int
	maxImbalance    float64
	generateShards  bool
	checkShards     bool
}

func run(cfg runConfig) error {
	switch {
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

func writeTimingReport(cfg runConfig) error {
	input, closeInput, err := openInput(cfg.inputPath)
	if err != nil {
		return err
	}
	defer closeInput()

	report, err := testtiming.ParseReport(input, testtiming.Options{TopN: cfg.topN})
	if err != nil {
		return err
	}

	output, closeOutput, err := openOutput(cfg.markdownPath)
	if err != nil {
		return err
	}
	defer closeOutput()
	return testtiming.WriteMarkdown(output, report)
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
	packages, err := testtiming.PackagesForShard(snapshot, cfg.shardPackages)
	if err != nil {
		return err
	}
	fmt.Println(strings.Join(packages, " "))
	return nil
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
		return nil, func() {}, fmt.Errorf("create markdown report %s: %w", path, err)
	}
	return file, func() { _ = file.Close() }, nil
}
