package testtiming

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestLoadBudgetPolicyValidatesHardOwner(t *testing.T) {
	valid := `version: 1
hard:
  max_shard_command_seconds:
    limit_seconds: 270
    justification: Broad ceiling from pinned command observations.
  full_conformance_command_seconds:
    limit_seconds: 330
    justification: Full ceiling from pinned command observations.
package_reference_seconds:
  github.com/division-sh/swarm/a: 10
`
	policy, err := LoadBudgetPolicy(strings.NewReader(valid))
	if err != nil {
		t.Fatalf("LoadBudgetPolicy: %v", err)
	}
	if policy.Hard.MaxShardCommandSeconds.LimitSeconds != 270 || policy.Hard.FullConformanceCommandSeconds.LimitSeconds != 330 {
		t.Fatalf("hard budgets = %+v", policy.Hard)
	}

	tests := []struct {
		name string
		edit func(string) string
		want string
	}{
		{
			name: "unknown field",
			edit: func(input string) string { return input + "unknown: true\n" },
			want: "field unknown not found",
		},
		{
			name: "missing justification",
			edit: func(input string) string {
				return strings.Replace(input, "    justification: Broad ceiling from pinned command observations.\n", "", 1)
			},
			want: "justification must be non-empty",
		},
		{
			name: "blank justification",
			edit: func(input string) string {
				return strings.Replace(input, "justification: Broad ceiling from pinned command observations.", `justification: ""`, 1)
			},
			want: "justification must be non-empty",
		},
		{
			name: "literal multiline justification",
			edit: func(input string) string {
				return strings.Replace(input, "justification: Broad ceiling from pinned command observations.", "justification: |\n      first line\n      second line", 1)
			},
			want: "justification must be one line",
		},
		{
			name: "folded multiline justification",
			edit: func(input string) string {
				return strings.Replace(input, "justification: Broad ceiling from pinned command observations.", "justification: >\n      first line\n      second line", 1)
			},
			want: "plain one-line scalar",
		},
		{
			name: "non-positive limit",
			edit: func(input string) string { return strings.Replace(input, "limit_seconds: 270", "limit_seconds: 0", 1) },
			want: "finite positive",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadBudgetPolicy(strings.NewReader(tt.edit(valid)))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("LoadBudgetPolicy error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestConfirmationRequiredUsesCommandLatencyOnly(t *testing.T) {
	policy := testBudgetPolicy()
	evidence := testEvidence(ShardSurface(1), AttemptPrimary, 270, []PackageTiming{{Package: "a", Result: "pass", Elapsed: 900}})

	required, err := ConfirmationRequired(policy, evidence)
	if err != nil || required {
		t.Fatalf("at limit required=%v err=%v, want false", required, err)
	}
	evidence.ElapsedSeconds = 271
	required, err = ConfirmationRequired(policy, evidence)
	if err != nil || !required {
		t.Fatalf("over limit required=%v err=%v, want true", required, err)
	}
	evidence.ExitCode = 1
	if _, err := ConfirmationRequired(policy, evidence); err == nil || !strings.Contains(err.Error(), "not confirmation-eligible") {
		t.Fatalf("failed confirmation check error = %v", err)
	}
}

func TestEvaluateBudgetAppliesBoundedConfirmationStateMachine(t *testing.T) {
	policy := testBudgetPolicy()
	snapshot := testSnapshot([]string{"a"})
	opts := EvaluationOptions{Snapshot: snapshot}

	tests := []struct {
		name     string
		evidence []CommandEvidence
		want     BudgetStatus
	}{
		{
			name:     "at limit passes",
			evidence: []CommandEvidence{testEvidence(ShardSurface(1), AttemptPrimary, 270, passTimings("a"))},
			want:     BudgetPass,
		},
		{
			name: "one breach then pass warns",
			evidence: []CommandEvidence{
				testEvidence(ShardSurface(1), AttemptPrimary, 271, passTimings("a")),
				testEvidence(ShardSurface(1), AttemptConfirmation, 269, passTimings("a")),
			},
			want: BudgetWarn,
		},
		{
			name: "two breaches fail",
			evidence: []CommandEvidence{
				testEvidence(ShardSurface(1), AttemptPrimary, 271, passTimings("a")),
				testEvidence(ShardSurface(1), AttemptConfirmation, 271, passTimings("a")),
			},
			want: BudgetFail,
		},
		{
			name:     "missing confirmation incomplete",
			evidence: []CommandEvidence{testEvidence(ShardSurface(1), AttemptPrimary, 271, passTimings("a"))},
			want:     BudgetIncomplete,
		},
		{
			name: "unneeded confirmation incomplete",
			evidence: []CommandEvidence{
				testEvidence(ShardSurface(1), AttemptPrimary, 269, passTimings("a")),
				testEvidence(ShardSurface(1), AttemptConfirmation, 269, passTimings("a")),
			},
			want: BudgetIncomplete,
		},
		{
			name: "failed primary incomplete",
			evidence: func() []CommandEvidence {
				failed := testEvidence(ShardSurface(1), AttemptPrimary, 10, []PackageTiming{{Package: "a", Result: "fail", Elapsed: 10}})
				failed.ExitCode = 1
				return []CommandEvidence{failed}
			}(),
			want: BudgetIncomplete,
		},
		{
			name: "changed confirmation environment incomplete",
			evidence: func() []CommandEvidence {
				primary := testEvidence(ShardSurface(1), AttemptPrimary, 271, passTimings("a"))
				confirmation := testEvidence(ShardSurface(1), AttemptConfirmation, 269, passTimings("a"))
				confirmation.EnvironmentID = "different"
				return []CommandEvidence{primary, confirmation}
			}(),
			want: BudgetIncomplete,
		},
		{
			name: "changed confirmation packages incomplete",
			evidence: []CommandEvidence{
				testEvidence(ShardSurface(1), AttemptPrimary, 271, passTimings("a")),
				testEvidence(ShardSurface(1), AttemptConfirmation, 269, passTimings("b")),
			},
			want: BudgetIncomplete,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EvaluateBudget(policy, opts, tt.evidence)
			if result.Status != tt.want {
				t.Fatalf("status = %s, want %s; result=%+v", result.Status, tt.want, result)
			}
		})
	}
}

func TestEvaluateBudgetAppliesFullCommandBoundary(t *testing.T) {
	policy := testBudgetPolicy()
	opts := EvaluationOptions{
		Snapshot:     testSnapshot([]string{"a"}),
		FullPackages: []string{"catalog"},
		ExpectFull:   true,
	}
	shard := testEvidence(ShardSurface(1), AttemptPrimary, 10, passTimings("a"))
	tests := []struct {
		name     string
		evidence []CommandEvidence
		want     BudgetStatus
	}{
		{
			name:     "at limit passes",
			evidence: []CommandEvidence{testEvidence(SurfaceFullConformance, AttemptPrimary, 330, passTimings("catalog"))},
			want:     BudgetPass,
		},
		{
			name: "passing confirmation warns",
			evidence: []CommandEvidence{
				testEvidence(SurfaceFullConformance, AttemptPrimary, 331, passTimings("catalog")),
				testEvidence(SurfaceFullConformance, AttemptConfirmation, 329, passTimings("catalog")),
			},
			want: BudgetWarn,
		},
		{
			name: "repeated breach fails",
			evidence: []CommandEvidence{
				testEvidence(SurfaceFullConformance, AttemptPrimary, 331, passTimings("catalog")),
				testEvidence(SurfaceFullConformance, AttemptConfirmation, 331, passTimings("catalog")),
			},
			want: BudgetFail,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evidence := append([]CommandEvidence{shard}, tt.evidence...)
			result := EvaluateBudget(policy, opts, evidence)
			if result.Status != tt.want {
				t.Fatalf("status = %s, want %s: %+v", result.Status, tt.want, result)
			}
		})
	}
}

func TestEvaluateBudgetFailsClosedOnIncompleteEvidenceSets(t *testing.T) {
	policy := testBudgetPolicy()
	snapshot := ShardSnapshot{
		Version:    ShardSnapshotVersion,
		ShardCount: 2,
		Shards: []PackageShard{
			{ID: 1, Packages: []string{"a"}},
			{ID: 2, Packages: []string{"b"}},
		},
	}
	complete := []CommandEvidence{
		testEvidence(ShardSurface(1), AttemptPrimary, 10, passTimings("a")),
		testEvidence(ShardSurface(2), AttemptPrimary, 10, passTimings("b")),
	}
	if got := EvaluateBudget(policy, EvaluationOptions{Snapshot: snapshot}, complete).Status; got != BudgetPass {
		t.Fatalf("complete status = %s, want PASS", got)
	}

	tests := []struct {
		name string
		edit func([]CommandEvidence) ([]CommandEvidence, EvaluationOptions)
	}{
		{
			name: "missing shard",
			edit: func(input []CommandEvidence) ([]CommandEvidence, EvaluationOptions) {
				return input[:1], EvaluationOptions{Snapshot: snapshot}
			},
		},
		{
			name: "duplicate shard",
			edit: func(input []CommandEvidence) ([]CommandEvidence, EvaluationOptions) {
				return append(input, input[0]), EvaluationOptions{Snapshot: snapshot}
			},
		},
		{
			name: "malformed report",
			edit: func(input []CommandEvidence) ([]CommandEvidence, EvaluationOptions) {
				input[0].Report.Summary.MalformedLines = 1
				return input, EvaluationOptions{Snapshot: snapshot}
			},
		},
		{
			name: "partial report",
			edit: func(input []CommandEvidence) ([]CommandEvidence, EvaluationOptions) {
				input[0].Report.Packages = nil
				input[0].Report.Summary.Packages = 0
				input[0].Report.Summary.PackageElapsedSec = 0
				return input, EvaluationOptions{Snapshot: snapshot}
			},
		},
		{
			name: "load problem",
			edit: func(input []CommandEvidence) ([]CommandEvidence, EvaluationOptions) {
				return input, EvaluationOptions{Snapshot: snapshot, LoadProblems: []string{"malformed evidence file"}}
			},
		},
		{
			name: "unexpected full evidence",
			edit: func(input []CommandEvidence) ([]CommandEvidence, EvaluationOptions) {
				return append(input, testEvidence(SurfaceFullConformance, AttemptPrimary, 10, passTimings("catalog"))), EvaluationOptions{Snapshot: snapshot, FullPackages: []string{"catalog"}}
			},
		},
		{
			name: "expected full evidence missing",
			edit: func(input []CommandEvidence) ([]CommandEvidence, EvaluationOptions) {
				return input, EvaluationOptions{Snapshot: snapshot, FullPackages: []string{"catalog"}, ExpectFull: true}
			},
		},
		{
			name: "duplicate package across shard declaration",
			edit: func(input []CommandEvidence) ([]CommandEvidence, EvaluationOptions) {
				badSnapshot := snapshot
				badSnapshot.Shards = append([]PackageShard(nil), snapshot.Shards...)
				badSnapshot.Shards[1].Packages = []string{"a"}
				input[1] = testEvidence(ShardSurface(2), AttemptPrimary, 10, passTimings("a"))
				return input, EvaluationOptions{Snapshot: badSnapshot}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := append([]CommandEvidence(nil), complete...)
			evidence, opts := tt.edit(input)
			if got := EvaluateBudget(policy, opts, evidence).Status; got != BudgetIncomplete {
				t.Fatalf("status = %s, want INCOMPLETE", got)
			}
		})
	}
}

func TestPackageDiagnosticsUseCompleteTruthOutsideMarkdownTopN(t *testing.T) {
	policy := testBudgetPolicy()
	policy.PackageReferenceSeconds = map[string]float64{"a": 1, "b": 30, "c": 10}
	timings := []PackageTiming{
		{Package: "b", Result: "pass", Elapsed: 30},
		{Package: "c", Result: "pass", Elapsed: 20},
		{Package: "a", Result: "pass", Elapsed: 1},
	}
	evidence := testEvidence(ShardSurface(1), AttemptPrimary, 10, timings)
	result := EvaluateBudget(policy, EvaluationOptions{Snapshot: testSnapshot([]string{"a", "b", "c"})}, []CommandEvidence{evidence})
	if result.Status != BudgetPass {
		t.Fatalf("status = %s, want PASS", result.Status)
	}
	if len(result.PackageDiagnostics) != 1 || result.PackageDiagnostics[0].Package != "c" || result.PackageDiagnostics[0].Kind != "regression" {
		t.Fatalf("diagnostics = %+v, want outside-top-N regression for c", result.PackageDiagnostics)
	}
	var markdown strings.Builder
	if err := WriteMarkdown(&markdown, evidence.Report, MarkdownOptions{TopN: 1}); err != nil {
		t.Fatalf("WriteMarkdown: %v", err)
	}
	if strings.Contains(markdown.String(), "`c`") {
		t.Fatalf("c unexpectedly appears in top-1 Markdown:\n%s", markdown.String())
	}
}

func TestPackageRegressionRequiresBothAdvisoryThresholds(t *testing.T) {
	tests := []struct {
		name      string
		reference float64
		actual    float64
		want      int
	}{
		{name: "both exceeded", reference: 10, actual: 20, want: 1},
		{name: "percentage only", reference: 1, actual: 2, want: 0},
		{name: "seconds only", reference: 100, actual: 106, want: 0},
		{name: "exact five seconds", reference: 10, actual: 15, want: 0},
		{name: "exact twenty five percent", reference: 40, actual: 50, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := testBudgetPolicy()
			policy.PackageReferenceSeconds = map[string]float64{"a": tt.reference}
			evidence := testEvidence(ShardSurface(1), AttemptPrimary, 10, []PackageTiming{{Package: "a", Result: "pass", Elapsed: tt.actual}})
			result := EvaluateBudget(policy, EvaluationOptions{Snapshot: testSnapshot([]string{"a"})}, []CommandEvidence{evidence})
			if len(result.PackageDiagnostics) != tt.want {
				t.Fatalf("diagnostics = %+v, want count %d", result.PackageDiagnostics, tt.want)
			}
		})
	}
}

func TestPackageDiagnosticsUseBroadOwnerAndFullOnlyTail(t *testing.T) {
	policy := testBudgetPolicy()
	policy.PackageReferenceSeconds = map[string]float64{"overlap": 10, "catalog": 10}
	shard := testEvidence(ShardSurface(1), AttemptPrimary, 10, []PackageTiming{{Package: "overlap", Result: "pass", Elapsed: 10}})
	full := testEvidence(SurfaceFullConformance, AttemptPrimary, 10, []PackageTiming{
		{Package: "overlap", Result: "pass", Elapsed: 100},
		{Package: "catalog", Result: "pass", Elapsed: 20},
	})
	result := EvaluateBudget(policy, EvaluationOptions{
		Snapshot:     testSnapshot([]string{"overlap"}),
		FullPackages: []string{"overlap", "catalog"},
		ExpectFull:   true,
	}, []CommandEvidence{shard, full})
	if result.Status != BudgetPass {
		t.Fatalf("status = %s, want PASS", result.Status)
	}
	if len(result.PackageDiagnostics) != 1 || result.PackageDiagnostics[0].Package != "catalog" {
		t.Fatalf("diagnostics = %+v, want only full-only catalog regression", result.PackageDiagnostics)
	}
}

func TestPackageDiagnosticsReportNewAndStaleReferences(t *testing.T) {
	policy := testBudgetPolicy()
	policy.PackageReferenceSeconds = map[string]float64{"a": 1, "stale": 2}
	evidence := testEvidence(ShardSurface(1), AttemptPrimary, 10, []PackageTiming{
		{Package: "a", Result: "pass", Elapsed: 1},
		{Package: "new", Result: "pass", Elapsed: 2},
	})
	result := EvaluateBudget(policy, EvaluationOptions{Snapshot: testSnapshot([]string{"a", "new"})}, []CommandEvidence{evidence})
	if len(result.PackageDiagnostics) != 2 {
		t.Fatalf("diagnostics = %+v, want new and stale", result.PackageDiagnostics)
	}
	if result.PackageDiagnostics[0].Kind != "new" || result.PackageDiagnostics[1].Kind != "stale" {
		t.Fatalf("diagnostic order/kinds = %+v", result.PackageDiagnostics)
	}
}

func TestBudgetResultProjectionsShareOneStatus(t *testing.T) {
	result := BudgetResult{
		Version: BudgetResultVersion,
		Status:  BudgetFail,
		Surfaces: []SurfaceResult{{
			Surface:        ShardSurface(1),
			Status:         BudgetFail,
			LimitSeconds:   270,
			PrimarySeconds: floatPointer(271),
		}},
	}
	var machine bytes.Buffer
	if err := WriteBudgetJSON(&machine, result); err != nil {
		t.Fatalf("WriteBudgetJSON: %v", err)
	}
	var decoded BudgetResult
	if err := json.Unmarshal(machine.Bytes(), &decoded); err != nil {
		t.Fatalf("decode machine result: %v", err)
	}
	var markdown strings.Builder
	if err := WriteBudgetMarkdown(&markdown, result); err != nil {
		t.Fatalf("WriteBudgetMarkdown: %v", err)
	}
	if decoded.Status != BudgetFail || !strings.Contains(markdown.String(), "**Status: FAIL**") || result.ExitCode() == 0 {
		t.Fatalf("projections disagree: decoded=%s exit=%d markdown=%s", decoded.Status, result.ExitCode(), markdown.String())
	}
	if !strings.Contains(markdown.String(), "go run ./cmd/swarm-test-timing -generate-shards") {
		t.Fatalf("markdown lacks real remediation command:\n%s", markdown.String())
	}
}

func testBudgetPolicy() BudgetPolicy {
	return BudgetPolicy{
		Version: BudgetPolicyVersion,
		Hard: HardBudgets{
			MaxShardCommandSeconds:        CommandBudget{LimitSeconds: 270, Justification: "test"},
			FullConformanceCommandSeconds: CommandBudget{LimitSeconds: 330, Justification: "test"},
		},
		PackageReferenceSeconds: map[string]float64{},
	}
}

func testSnapshot(packages []string) ShardSnapshot {
	return ShardSnapshot{
		Version:    ShardSnapshotVersion,
		ShardCount: 1,
		Shards:     []PackageShard{{ID: 1, Packages: append([]string(nil), packages...)}},
	}
}

func passTimings(packages ...string) []PackageTiming {
	out := make([]PackageTiming, 0, len(packages))
	for _, pkg := range packages {
		out = append(out, PackageTiming{Package: pkg, Result: "pass", Elapsed: 1})
	}
	return out
}

func testEvidence(surface, attempt string, elapsed float64, timings []PackageTiming) CommandEvidence {
	packages := make([]string, 0, len(timings))
	report := Report{Packages: append([]PackageTiming(nil), timings...)}
	for _, timing := range timings {
		packages = append(packages, timing.Package)
		report.Summary.PackageElapsedSec += timing.Elapsed
		switch timing.Result {
		case "fail":
			report.Summary.FailedPackages++
		case "skip":
			report.Summary.SkippedPackages++
		}
	}
	report.Summary.Events = len(timings)
	report.Summary.Packages = len(timings)
	countMode := CountModeCacheDefault
	if attempt == AttemptConfirmation || surface == SurfaceFullConformance {
		countMode = CountModeOne
	}
	return CommandEvidence{
		Version:        CommandEvidenceVersion,
		Surface:        surface,
		Attempt:        attempt,
		ElapsedSeconds: elapsed,
		Packages:       packages,
		EnvironmentID:  "ci-postgres-v1",
		CountMode:      countMode,
		Report:         report,
	}
}
