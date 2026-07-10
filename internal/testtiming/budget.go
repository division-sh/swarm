package testtiming

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	BudgetPolicyVersion    = 1
	CommandEvidenceVersion = 1
	BudgetResultVersion    = 1

	SurfaceFullConformance = "full-conformance"
	AttemptPrimary         = "primary"
	AttemptConfirmation    = "confirmation"
	CountModeCacheDefault  = "cache-default"
	CountModeOne           = "count-1"
)

type BudgetPolicy struct {
	Version                 int                `yaml:"version"`
	Hard                    HardBudgets        `yaml:"hard"`
	PackageReferenceSeconds map[string]float64 `yaml:"package_reference_seconds"`
}

type HardBudgets struct {
	MaxShardCommandSeconds        CommandBudget `yaml:"max_shard_command_seconds"`
	FullConformanceCommandSeconds CommandBudget `yaml:"full_conformance_command_seconds"`
}

type CommandBudget struct {
	LimitSeconds  float64 `yaml:"limit_seconds"`
	Justification string  `yaml:"justification"`
}

type CommandEvidence struct {
	Version        int      `json:"version"`
	Surface        string   `json:"surface"`
	Attempt        string   `json:"attempt"`
	ElapsedSeconds float64  `json:"elapsed_seconds"`
	ExitCode       int      `json:"exit_code"`
	Packages       []string `json:"packages"`
	EnvironmentID  string   `json:"environment_id"`
	CountMode      string   `json:"count_mode"`
	Report         Report   `json:"report"`
}

type BudgetStatus string

const (
	BudgetPass       BudgetStatus = "PASS"
	BudgetWarn       BudgetStatus = "WARN"
	BudgetFail       BudgetStatus = "FAIL"
	BudgetIncomplete BudgetStatus = "INCOMPLETE"
)

type SurfaceResult struct {
	Surface                  string       `json:"surface"`
	Status                   BudgetStatus `json:"status"`
	LimitSeconds             float64      `json:"limit_seconds"`
	PrimarySeconds           *float64     `json:"primary_seconds,omitempty"`
	ConfirmationSeconds      *float64     `json:"confirmation_seconds,omitempty"`
	PrimaryPackageElapsedSec *float64     `json:"primary_package_elapsed_seconds,omitempty"`
	Problems                 []string     `json:"problems,omitempty"`
}

type PackageDiagnostic struct {
	Kind             string  `json:"kind"`
	Package          string  `json:"package"`
	ActualSeconds    float64 `json:"actual_seconds,omitempty"`
	ReferenceSeconds float64 `json:"reference_seconds,omitempty"`
	Message          string  `json:"message"`
}

type BudgetResult struct {
	Version            int                 `json:"version"`
	Status             BudgetStatus        `json:"status"`
	Surfaces           []SurfaceResult     `json:"surfaces"`
	PackageDiagnostics []PackageDiagnostic `json:"package_diagnostics,omitempty"`
	Problems           []string            `json:"problems,omitempty"`
}

type EvaluationOptions struct {
	Snapshot     ShardSnapshot
	FullPackages []string
	ExpectFull   bool
	LoadProblems []string
}

type evidenceAttempts struct {
	primary      *CommandEvidence
	confirmation *CommandEvidence
}

func ShardSurface(id int) string {
	return fmt.Sprintf("shard-%d", id)
}

func LoadBudgetPolicy(r io.Reader) (BudgetPolicy, error) {
	if r == nil {
		return BudgetPolicy{}, fmt.Errorf("budget policy reader is nil")
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		return BudgetPolicy{}, fmt.Errorf("read budget policy: %w", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	var policy BudgetPolicy
	if err := decoder.Decode(&policy); err != nil {
		return BudgetPolicy{}, fmt.Errorf("decode budget policy: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return BudgetPolicy{}, fmt.Errorf("decode budget policy: multiple YAML documents")
		}
		return BudgetPolicy{}, fmt.Errorf("decode budget policy trailing data: %w", err)
	}
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return BudgetPolicy{}, fmt.Errorf("inspect budget policy: %w", err)
	}
	if err := validateBudgetPolicy(policy, &document); err != nil {
		return BudgetPolicy{}, err
	}
	return policy, nil
}

func validateBudgetPolicy(policy BudgetPolicy, document *yaml.Node) error {
	if policy.Version != BudgetPolicyVersion {
		return fmt.Errorf("budget policy version = %d, want %d", policy.Version, BudgetPolicyVersion)
	}
	budgets := []struct {
		name   string
		budget CommandBudget
	}{
		{"hard.max_shard_command_seconds", policy.Hard.MaxShardCommandSeconds},
		{"hard.full_conformance_command_seconds", policy.Hard.FullConformanceCommandSeconds},
	}
	for _, item := range budgets {
		if !finitePositive(item.budget.LimitSeconds) {
			return fmt.Errorf("%s.limit_seconds must be a finite positive number", item.name)
		}
		justification := strings.TrimSpace(item.budget.Justification)
		if justification == "" {
			return fmt.Errorf("%s.justification must be non-empty", item.name)
		}
		if strings.ContainsAny(justification, "\r\n") {
			return fmt.Errorf("%s.justification must be one line", item.name)
		}
		path := strings.Split(item.name+".justification", ".")
		node := mappingPath(document, path...)
		if node == nil || node.Style == yaml.LiteralStyle || node.Style == yaml.FoldedStyle {
			return fmt.Errorf("%s.justification must be a plain one-line scalar", item.name)
		}
	}
	for pkg, seconds := range policy.PackageReferenceSeconds {
		if strings.TrimSpace(pkg) == "" {
			return fmt.Errorf("package_reference_seconds contains an empty package")
		}
		if !finiteNonNegative(seconds) {
			return fmt.Errorf("package_reference_seconds[%s] must be finite and non-negative", pkg)
		}
	}
	return nil
}

func mappingPath(document *yaml.Node, path ...string) *yaml.Node {
	if document == nil || len(document.Content) == 0 {
		return nil
	}
	node := document.Content[0]
	for _, part := range path {
		if node.Kind != yaml.MappingNode {
			return nil
		}
		var next *yaml.Node
		for i := 0; i+1 < len(node.Content); i += 2 {
			if node.Content[i].Value == part {
				next = node.Content[i+1]
				break
			}
		}
		if next == nil {
			return nil
		}
		node = next
	}
	return node
}

func ConfirmationRequired(policy BudgetPolicy, evidence CommandEvidence) (bool, error) {
	problems := ValidateCommandEvidence(evidence)
	if len(problems) > 0 {
		return false, fmt.Errorf("primary evidence is incomplete: %s", strings.Join(problems, "; "))
	}
	if evidence.Attempt != AttemptPrimary {
		return false, fmt.Errorf("confirmation check requires a primary attempt")
	}
	if evidence.ExitCode != 0 || evidence.Report.Summary.FailedPackages > 0 || evidence.Report.Summary.FailedTests > 0 {
		return false, fmt.Errorf("failed primary evidence is not confirmation-eligible")
	}
	budget, err := policy.budgetForSurface(evidence.Surface)
	if err != nil {
		return false, err
	}
	return evidence.ElapsedSeconds > budget.LimitSeconds, nil
}

func ValidateCommandEvidence(evidence CommandEvidence) []string {
	var problems []string
	if evidence.Version != CommandEvidenceVersion {
		problems = append(problems, fmt.Sprintf("version %d is unsupported", evidence.Version))
	}
	if _, _, err := parseSurface(evidence.Surface); err != nil {
		problems = append(problems, err.Error())
	}
	if evidence.Attempt != AttemptPrimary && evidence.Attempt != AttemptConfirmation {
		problems = append(problems, fmt.Sprintf("attempt %q is unsupported", evidence.Attempt))
	}
	if !finiteNonNegative(evidence.ElapsedSeconds) {
		problems = append(problems, "elapsed_seconds must be finite and non-negative")
	}
	if evidence.ExitCode < 0 {
		problems = append(problems, "exit_code must be non-negative")
	}
	if strings.TrimSpace(evidence.EnvironmentID) == "" {
		problems = append(problems, "environment_id must be non-empty")
	}
	if evidence.CountMode != CountModeCacheDefault && evidence.CountMode != CountModeOne {
		problems = append(problems, fmt.Sprintf("count_mode %q is unsupported", evidence.CountMode))
	}
	if evidence.Attempt == AttemptConfirmation && evidence.CountMode != CountModeOne {
		problems = append(problems, "confirmation count_mode must be count-1")
	}
	declared, packageProblems := canonicalPackageList(evidence.Packages)
	problems = append(problems, packageProblems...)
	if evidence.Report.Summary.MalformedLines != 0 {
		problems = append(problems, fmt.Sprintf("report has %d malformed lines", evidence.Report.Summary.MalformedLines))
	}
	if evidence.Report.Summary.DuplicatePackageEvents != 0 {
		problems = append(problems, fmt.Sprintf("report has %d duplicate terminal package events", evidence.Report.Summary.DuplicatePackageEvents))
	}
	if evidence.Report.Summary.DuplicateTestEvents != 0 {
		problems = append(problems, fmt.Sprintf("report has %d duplicate terminal test events", evidence.Report.Summary.DuplicateTestEvents))
	}
	reportPackages, reportProblems := reportPackageList(evidence.Report)
	problems = append(problems, reportProblems...)
	problems = append(problems, reportTestProblems(evidence.Report)...)
	if !equalStrings(declared, reportPackages) {
		problems = append(problems, fmt.Sprintf("declared packages %v do not match report packages %v", declared, reportPackages))
	}
	return problems
}

func reportTestProblems(report Report) []string {
	var problems []string
	seen := map[string]bool{}
	failed := 0
	skipped := 0
	for _, timing := range report.Tests {
		pkg := strings.TrimSpace(timing.Package)
		test := strings.TrimSpace(timing.Test)
		if pkg == "" || test == "" {
			problems = append(problems, "report contains a test with empty package or name")
			continue
		}
		key := pkg + "\x00" + test
		if seen[key] {
			problems = append(problems, fmt.Sprintf("report contains duplicate test %s.%s", pkg, test))
			continue
		}
		seen[key] = true
		if !isTerminalAction(timing.Result) {
			problems = append(problems, fmt.Sprintf("test %s.%s has non-terminal result %q", pkg, test, timing.Result))
		}
		if !finiteNonNegative(timing.Elapsed) {
			problems = append(problems, fmt.Sprintf("test %s.%s elapsed must be finite and non-negative", pkg, test))
		}
		switch timing.Result {
		case "fail":
			failed++
		case "skip":
			skipped++
		}
	}
	if report.Summary.Tests != len(report.Tests) {
		problems = append(problems, fmt.Sprintf("summary test count %d does not match %d test rows", report.Summary.Tests, len(report.Tests)))
	}
	if report.Summary.FailedTests != failed || report.Summary.SkippedTests != skipped {
		problems = append(problems, "summary test results do not match test rows")
	}
	return problems
}

func reportPackageList(report Report) ([]string, []string) {
	var problems []string
	packages := make([]string, 0, len(report.Packages))
	seen := map[string]bool{}
	failed := 0
	skipped := 0
	elapsed := 0.0
	for _, timing := range report.Packages {
		pkg := strings.TrimSpace(timing.Package)
		if pkg == "" {
			problems = append(problems, "report contains an empty package")
			continue
		}
		if seen[pkg] {
			problems = append(problems, fmt.Sprintf("report contains duplicate package %s", pkg))
			continue
		}
		seen[pkg] = true
		packages = append(packages, pkg)
		if !isTerminalAction(timing.Result) {
			problems = append(problems, fmt.Sprintf("package %s has non-terminal result %q", pkg, timing.Result))
		}
		if !finiteNonNegative(timing.Elapsed) {
			problems = append(problems, fmt.Sprintf("package %s elapsed must be finite and non-negative", pkg))
		}
		elapsed += timing.Elapsed
		switch timing.Result {
		case "fail":
			failed++
		case "skip":
			skipped++
		}
	}
	sort.Strings(packages)
	if report.Summary.Packages != len(report.Packages) {
		problems = append(problems, fmt.Sprintf("summary package count %d does not match %d package rows", report.Summary.Packages, len(report.Packages)))
	}
	if report.Summary.FailedPackages != failed || report.Summary.SkippedPackages != skipped {
		problems = append(problems, "summary package results do not match package rows")
	}
	if math.Abs(report.Summary.PackageElapsedSec-elapsed) > 0.000001 {
		problems = append(problems, "summary package elapsed does not match package rows")
	}
	return packages, problems
}

func EvaluateBudget(policy BudgetPolicy, opts EvaluationOptions, evidence []CommandEvidence) BudgetResult {
	result := BudgetResult{Version: BudgetResultVersion, Status: BudgetPass}
	result.Problems = append(result.Problems, opts.LoadProblems...)
	if err := validateShardIDs(opts.Snapshot); err != nil {
		result.Problems = append(result.Problems, fmt.Sprintf("invalid shard snapshot: %v", err))
	}
	result.Problems = append(result.Problems, validateSnapshotPackageDeclarations(opts.Snapshot)...)

	expected := make(map[string][]string, opts.Snapshot.ShardCount+1)
	for _, shard := range opts.Snapshot.Shards {
		expected[ShardSurface(shard.ID)] = append([]string(nil), shard.Packages...)
	}
	if opts.ExpectFull {
		expected[SurfaceFullConformance] = append([]string(nil), opts.FullPackages...)
	}

	grouped := map[string]*evidenceAttempts{}
	for i := range evidence {
		item := &evidence[i]
		if _, ok := expected[item.Surface]; !ok {
			result.Problems = append(result.Problems, fmt.Sprintf("unexpected evidence surface %s", item.Surface))
			continue
		}
		group := grouped[item.Surface]
		if group == nil {
			group = &evidenceAttempts{}
			grouped[item.Surface] = group
		}
		switch item.Attempt {
		case AttemptPrimary:
			if group.primary != nil {
				result.Problems = append(result.Problems, fmt.Sprintf("duplicate primary evidence for %s", item.Surface))
			} else {
				group.primary = item
			}
		case AttemptConfirmation:
			if group.confirmation != nil {
				result.Problems = append(result.Problems, fmt.Sprintf("duplicate confirmation evidence for %s", item.Surface))
			} else {
				group.confirmation = item
			}
		default:
			result.Problems = append(result.Problems, fmt.Sprintf("unsupported attempt %q for %s", item.Attempt, item.Surface))
		}
	}

	surfaces := make([]string, 0, len(expected))
	for surface := range expected {
		surfaces = append(surfaces, surface)
	}
	sort.Slice(surfaces, func(i, j int) bool { return surfaceLess(surfaces[i], surfaces[j]) })
	for _, surface := range surfaces {
		budget, err := policy.budgetForSurface(surface)
		if err != nil {
			result.Problems = append(result.Problems, err.Error())
			continue
		}
		surfaceResult := evaluateSurface(surface, budget, expected[surface], grouped[surface])
		result.Surfaces = append(result.Surfaces, surfaceResult)
		result.Status = mergeStatus(result.Status, surfaceResult.Status)
	}
	if len(result.Problems) > 0 {
		result.Status = BudgetIncomplete
	}
	if result.Status != BudgetIncomplete {
		result.PackageDiagnostics = packageDiagnostics(policy, opts, grouped)
	}
	return result
}

func validateSnapshotPackageDeclarations(snapshot ShardSnapshot) []string {
	var problems []string
	seen := map[string]int{}
	for _, shard := range snapshot.Shards {
		packages, packageProblems := canonicalPackageList(shard.Packages)
		for _, problem := range packageProblems {
			problems = append(problems, fmt.Sprintf("shard %d: %s", shard.ID, problem))
		}
		for _, pkg := range packages {
			if previous, exists := seen[pkg]; exists {
				problems = append(problems, fmt.Sprintf("package %s appears in shards %d and %d", pkg, previous, shard.ID))
				continue
			}
			seen[pkg] = shard.ID
		}
	}
	return problems
}

func evaluateSurface(surface string, budget CommandBudget, expectedPackages []string, group *evidenceAttempts) SurfaceResult {
	result := SurfaceResult{Surface: surface, Status: BudgetPass, LimitSeconds: budget.LimitSeconds}
	if group == nil || group.primary == nil {
		result.Status = BudgetIncomplete
		result.Problems = append(result.Problems, "primary evidence is missing")
		return result
	}
	primary := group.primary
	result.PrimarySeconds = floatPointer(primary.ElapsedSeconds)
	result.PrimaryPackageElapsedSec = floatPointer(primary.Report.Summary.PackageElapsedSec)
	result.Problems = append(result.Problems, ValidateCommandEvidence(*primary)...)
	wantPackages, packageProblems := canonicalPackageList(expectedPackages)
	result.Problems = append(result.Problems, packageProblems...)
	gotPackages, _ := canonicalPackageList(primary.Packages)
	if !equalStrings(wantPackages, gotPackages) {
		result.Problems = append(result.Problems, fmt.Sprintf("packages do not match canonical surface declaration: got %v want %v", gotPackages, wantPackages))
	}
	if primary.ExitCode != 0 || primary.Report.Summary.FailedPackages > 0 || primary.Report.Summary.FailedTests > 0 {
		result.Problems = append(result.Problems, fmt.Sprintf("primary command failed with exit code %d", primary.ExitCode))
	}
	if len(result.Problems) > 0 {
		result.Status = BudgetIncomplete
		return result
	}

	over := primary.ElapsedSeconds > budget.LimitSeconds
	if !over {
		if group.confirmation != nil {
			result.Status = BudgetIncomplete
			result.Problems = append(result.Problems, "confirmation exists for a primary command within budget")
		}
		return result
	}
	if group.confirmation == nil {
		result.Status = BudgetIncomplete
		result.Problems = append(result.Problems, "over-budget primary is missing its one required confirmation")
		return result
	}
	confirmation := group.confirmation
	result.ConfirmationSeconds = floatPointer(confirmation.ElapsedSeconds)
	result.Problems = append(result.Problems, ValidateCommandEvidence(*confirmation)...)
	if confirmation.Surface != primary.Surface {
		result.Problems = append(result.Problems, "confirmation surface differs from primary")
	}
	if !equalStrings(primary.Packages, confirmation.Packages) {
		result.Problems = append(result.Problems, "confirmation packages differ from primary")
	}
	if confirmation.EnvironmentID != primary.EnvironmentID {
		result.Problems = append(result.Problems, "confirmation environment differs from primary")
	}
	if confirmation.CountMode != CountModeOne {
		result.Problems = append(result.Problems, "confirmation must use count-1")
	}
	if confirmation.ExitCode != 0 || confirmation.Report.Summary.FailedPackages > 0 || confirmation.Report.Summary.FailedTests > 0 {
		result.Problems = append(result.Problems, fmt.Sprintf("confirmation command failed with exit code %d", confirmation.ExitCode))
	}
	if len(result.Problems) > 0 {
		result.Status = BudgetIncomplete
		return result
	}
	if confirmation.ElapsedSeconds > budget.LimitSeconds {
		result.Status = BudgetFail
	} else {
		result.Status = BudgetWarn
	}
	return result
}

func packageDiagnostics(policy BudgetPolicy, opts EvaluationOptions, grouped map[string]*evidenceAttempts) []PackageDiagnostic {
	observed := map[string]PackageTiming{}
	declared := map[string]bool{}
	for _, shard := range opts.Snapshot.Shards {
		for _, pkg := range shard.Packages {
			declared[pkg] = true
		}
		group := grouped[ShardSurface(shard.ID)]
		if group == nil || group.primary == nil {
			continue
		}
		for _, timing := range group.primary.Report.Packages {
			observed[timing.Package] = timing
		}
	}
	for _, pkg := range opts.FullPackages {
		declared[pkg] = true
	}
	if opts.ExpectFull {
		group := grouped[SurfaceFullConformance]
		if group != nil && group.primary != nil {
			for _, timing := range group.primary.Report.Packages {
				if _, broad := observed[timing.Package]; !broad {
					observed[timing.Package] = timing
				}
			}
		}
	}

	var diagnostics []PackageDiagnostic
	for pkg, timing := range observed {
		reference, ok := policy.PackageReferenceSeconds[pkg]
		if !ok {
			diagnostics = append(diagnostics, PackageDiagnostic{
				Kind:          "new",
				Package:       pkg,
				ActualSeconds: timing.Elapsed,
				Message:       fmt.Sprintf("new package %s has no committed timing reference", pkg),
			})
			continue
		}
		if timing.Elapsed > reference*1.25 && timing.Elapsed-reference > 5 {
			diagnostics = append(diagnostics, PackageDiagnostic{
				Kind:             "regression",
				Package:          pkg,
				ActualSeconds:    timing.Elapsed,
				ReferenceSeconds: reference,
				Message:          fmt.Sprintf("package %s increased from %.3fs to %.3fs (>25%% and >5s)", pkg, reference, timing.Elapsed),
			})
		}
	}
	for pkg, reference := range policy.PackageReferenceSeconds {
		if !declared[pkg] {
			diagnostics = append(diagnostics, PackageDiagnostic{
				Kind:             "stale",
				Package:          pkg,
				ReferenceSeconds: reference,
				Message:          fmt.Sprintf("stale timing reference %s is absent from broad and full package declarations", pkg),
			})
		}
	}
	sort.Slice(diagnostics, func(i, j int) bool {
		if diagnostics[i].Kind != diagnostics[j].Kind {
			return diagnostics[i].Kind < diagnostics[j].Kind
		}
		return diagnostics[i].Package < diagnostics[j].Package
	})
	return diagnostics
}

func WriteBudgetJSON(w io.Writer, result BudgetResult) error {
	if w == nil {
		return fmt.Errorf("budget JSON writer is nil")
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func WriteBudgetMarkdown(w io.Writer, result BudgetResult) error {
	if w == nil {
		return fmt.Errorf("budget Markdown writer is nil")
	}
	if _, err := fmt.Fprintln(w, "# CI Test Timing Budget"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "\n**Status: %s**\n\n", result.Status); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "Hard latency is command-level `go test` elapsed. Package elapsed is concurrent work telemetry only; GitHub job wall time is non-authoritative."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "\n| Surface | Primary | Confirmation | Limit | Package work | Status |\n| --- | ---: | ---: | ---: | ---: | --- |"); err != nil {
		return err
	}
	for _, surface := range result.Surfaces {
		if _, err := fmt.Fprintf(w, "| `%s` | %s | %s | %.0fs | %s | %s |\n",
			surface.Surface,
			formatOptionalSeconds(surface.PrimarySeconds),
			formatOptionalSeconds(surface.ConfirmationSeconds),
			surface.LimitSeconds,
			formatOptionalSeconds(surface.PrimaryPackageElapsedSec),
			surface.Status,
		); err != nil {
			return err
		}
	}
	if len(result.PackageDiagnostics) > 0 {
		if _, err := fmt.Fprintln(w, "\n## Advisory Package Diagnostics"); err != nil {
			return err
		}
		for _, diagnostic := range result.PackageDiagnostics {
			if _, err := fmt.Fprintf(w, "- `%s`: %s\n", diagnostic.Kind, diagnostic.Message); err != nil {
				return err
			}
		}
	}
	var problems []string
	problems = append(problems, result.Problems...)
	for _, surface := range result.Surfaces {
		for _, problem := range surface.Problems {
			problems = append(problems, surface.Surface+": "+problem)
		}
	}
	if len(problems) > 0 {
		if _, err := fmt.Fprintln(w, "\n## Blocking Problems"); err != nil {
			return err
		}
		for _, problem := range problems {
			if _, err := fmt.Fprintf(w, "- %s\n", problem); err != nil {
				return err
			}
		}
	}
	if result.Status == BudgetWarn || result.Status == BudgetFail || result.Status == BudgetIncomplete {
		if _, err := fmt.Fprintln(w, "\n## Remediation"); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "1. Regenerate/rebalance the committed shard snapshot with `go run ./cmd/swarm-test-timing -generate-shards -packages <go-packages-required.txt> -weights <go-test.json> -snapshot .github/test-shards/go-test-shards.json -shards 6`."); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "2. Retier an appropriately heavy proof family, or optimize the regressed package without weakening coverage."); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "3. Raise a committed budget only through review with a new one-line justification."); err != nil {
			return err
		}
	}
	return nil
}

func (result BudgetResult) ExitCode() int {
	if result.Status == BudgetPass || result.Status == BudgetWarn {
		return 0
	}
	return 1
}

func (policy BudgetPolicy) budgetForSurface(surface string) (CommandBudget, error) {
	kind, _, err := parseSurface(surface)
	if err != nil {
		return CommandBudget{}, err
	}
	if kind == "shard" {
		return policy.Hard.MaxShardCommandSeconds, nil
	}
	return policy.Hard.FullConformanceCommandSeconds, nil
}

func parseSurface(surface string) (string, int, error) {
	if surface == SurfaceFullConformance {
		return "full", 0, nil
	}
	if !strings.HasPrefix(surface, "shard-") {
		return "", 0, fmt.Errorf("surface %q is unsupported", surface)
	}
	id, err := strconv.Atoi(strings.TrimPrefix(surface, "shard-"))
	if err != nil || id <= 0 {
		return "", 0, fmt.Errorf("surface %q has an invalid shard id", surface)
	}
	return "shard", id, nil
}

func canonicalPackageList(packages []string) ([]string, []string) {
	var problems []string
	out := make([]string, 0, len(packages))
	seen := map[string]bool{}
	for _, raw := range packages {
		pkg := strings.TrimSpace(raw)
		if pkg == "" {
			problems = append(problems, "package list contains an empty package")
			continue
		}
		if seen[pkg] {
			problems = append(problems, fmt.Sprintf("package list contains duplicate %s", pkg))
			continue
		}
		seen[pkg] = true
		out = append(out, pkg)
	}
	sort.Strings(out)
	if len(out) == 0 {
		problems = append(problems, "package list is empty")
	}
	return out, problems
}

func surfaceLess(left, right string) bool {
	leftKind, leftID, _ := parseSurface(left)
	rightKind, rightID, _ := parseSurface(right)
	if leftKind != rightKind {
		return leftKind == "shard"
	}
	return leftID < rightID
}

func mergeStatus(current, next BudgetStatus) BudgetStatus {
	severity := map[BudgetStatus]int{BudgetPass: 0, BudgetWarn: 1, BudgetFail: 2, BudgetIncomplete: 3}
	if severity[next] > severity[current] {
		return next
	}
	return current
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	for i := range leftCopy {
		if leftCopy[i] != rightCopy[i] {
			return false
		}
	}
	return true
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func finiteNonNegative(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func floatPointer(value float64) *float64 {
	return &value
}

func formatOptionalSeconds(value *float64) string {
	if value == nil {
		return "-"
	}
	return fmt.Sprintf("%.3fs", *value)
}
