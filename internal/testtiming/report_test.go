package testtiming

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseReportSummarizesPackageAndTestTimings(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"Time":"2026-06-01T00:00:00Z","Action":"run","Package":"github.com/division-sh/swarm/slow","Test":"TestSlow"}`,
		`{"Time":"2026-06-01T00:00:10Z","Action":"pass","Package":"github.com/division-sh/swarm/fast","Elapsed":1.25}`,
		`{"Time":"2026-06-01T00:00:20Z","Action":"fail","Package":"github.com/division-sh/swarm/slow","Test":"TestSlow","Elapsed":12.5}`,
		`{"Time":"2026-06-01T00:00:21Z","Action":"pass","Package":"github.com/division-sh/swarm/slow","Test":"TestMedium","Elapsed":3.125}`,
		`{"Time":"2026-06-01T00:00:22Z","Action":"fail","Package":"github.com/division-sh/swarm/slow","Elapsed":14}`,
		`{"Time":"2026-06-01T00:00:23Z","Action":"skip","Package":"github.com/division-sh/swarm/skipped","Test":"TestSkipped","Elapsed":0}`,
		`{"Time":"2026-06-01T00:00:24Z","Action":"skip","Package":"github.com/division-sh/swarm/skipped","Elapsed":0}`,
		`not-json`,
	}, "\n"))

	report, err := ParseReport(input, Options{TopN: 10})
	if err != nil {
		t.Fatalf("ParseReport: %v", err)
	}
	if report.Summary.Events != 7 {
		t.Fatalf("events = %d, want 7", report.Summary.Events)
	}
	if report.Summary.MalformedLines != 1 {
		t.Fatalf("malformed lines = %d, want 1", report.Summary.MalformedLines)
	}
	if report.Summary.Packages != 3 || report.Summary.FailedPackages != 1 || report.Summary.SkippedPackages != 1 {
		t.Fatalf("package summary = %+v, want 3 packages / 1 failed / 1 skipped", report.Summary)
	}
	if report.Summary.Tests != 3 || report.Summary.FailedTests != 1 || report.Summary.SkippedTests != 1 {
		t.Fatalf("test summary = %+v, want 3 tests / 1 failed / 1 skipped", report.Summary)
	}
	if got, want := report.Summary.PackageElapsedSec, 15.25; got != want {
		t.Fatalf("package elapsed sum = %v, want %v", got, want)
	}
	if got := report.SlowPackages[0]; got.Package != "github.com/division-sh/swarm/slow" || got.Result != "fail" || got.Elapsed != 14 {
		t.Fatalf("slowest package = %+v, want swarm/slow fail 14s", got)
	}
	if got := report.SlowTests[0]; got.Package != "github.com/division-sh/swarm/slow" || got.Test != "TestSlow" || got.Result != "fail" || got.Elapsed != 12.5 {
		t.Fatalf("slowest test = %+v, want swarm/slow TestSlow fail 12.5s", got)
	}
}

func TestParseReportAppliesTopLimit(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"Action":"pass","Package":"github.com/division-sh/swarm/a","Test":"TestA","Elapsed":1}`,
		`{"Action":"pass","Package":"github.com/division-sh/swarm/a","Elapsed":1}`,
		`{"Action":"pass","Package":"github.com/division-sh/swarm/b","Test":"TestB","Elapsed":2}`,
		`{"Action":"pass","Package":"github.com/division-sh/swarm/b","Elapsed":2}`,
	}, "\n"))

	report, err := ParseReport(input, Options{TopN: 1})
	if err != nil {
		t.Fatalf("ParseReport: %v", err)
	}
	if len(report.SlowPackages) != 1 || report.SlowPackages[0].Package != "github.com/division-sh/swarm/b" {
		t.Fatalf("slow packages = %+v, want only swarm/b", report.SlowPackages)
	}
	if len(report.SlowTests) != 1 || report.SlowTests[0].Test != "TestB" {
		t.Fatalf("slow tests = %+v, want only TestB", report.SlowTests)
	}
	if report.Summary.Packages != 2 || report.Summary.Tests != 2 {
		t.Fatalf("summary after trim = %+v, want untrimmed counts", report.Summary)
	}
}

func TestWriteMarkdownIncludesSummaryAndTables(t *testing.T) {
	report := Report{
		Summary: Summary{
			Events:            4,
			Packages:          1,
			Tests:             1,
			PackageElapsedSec: 3.5,
		},
		SlowPackages: []PackageTiming{{Package: "github.com/division-sh/swarm/pkg", Result: "pass", Elapsed: 3.5}},
		SlowTests:    []TestTiming{{Package: "github.com/division-sh/swarm/pkg", Test: "TestThing", Result: "pass", Elapsed: 2.25}},
	}
	var out strings.Builder
	if err := WriteMarkdown(&out, report); err != nil {
		t.Fatalf("WriteMarkdown: %v", err)
	}
	text := out.String()
	for _, want := range []string{
		"# Go Test Timing Report",
		"| Parsed events | 4 |",
		"| Package elapsed sum | 3.500s |",
		"| `github.com/division-sh/swarm/pkg` | pass | 3.500s |",
		"| `github.com/division-sh/swarm/pkg` | `TestThing` | pass | 2.250s |",
		"observability-only",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("markdown missing %q:\n%s", want, text)
		}
	}
}

func TestBuildShardSnapshotBalancesMeasuredWeightsDeterministically(t *testing.T) {
	packages := []string{
		"github.com/division-sh/swarm/a",
		"github.com/division-sh/swarm/b",
		"github.com/division-sh/swarm/c",
		"github.com/division-sh/swarm/d",
	}
	weights := map[string]float64{
		"github.com/division-sh/swarm/a": 8,
		"github.com/division-sh/swarm/b": 4,
		"github.com/division-sh/swarm/c": 2,
		"github.com/division-sh/swarm/d": 2,
	}

	snapshot, err := BuildShardSnapshot(packages, weights, 2, "fixture", 0.25)
	if err != nil {
		t.Fatalf("BuildShardSnapshot: %v", err)
	}
	if snapshot.Version != ShardSnapshotVersion || snapshot.ShardCount != 2 {
		t.Fatalf("snapshot metadata = %+v, want version %d / 2 shards", snapshot, ShardSnapshotVersion)
	}
	if got := strings.Join(snapshot.Shards[0].Packages, " "); got != "github.com/division-sh/swarm/a" {
		t.Fatalf("shard 1 packages = %q, want a", got)
	}
	if got := strings.Join(snapshot.Shards[1].Packages, " "); got != "github.com/division-sh/swarm/b github.com/division-sh/swarm/c github.com/division-sh/swarm/d" {
		t.Fatalf("shard 2 packages = %q, want b c d", got)
	}
	validation, err := ValidateShardSnapshot(snapshot, packages)
	if err != nil {
		t.Fatalf("ValidateShardSnapshot: %v", err)
	}
	if validation.ImbalanceRatio != 0 {
		t.Fatalf("imbalance ratio = %v, want 0", validation.ImbalanceRatio)
	}
}

func TestValidateShardSnapshotRejectsMissingExtraAndDuplicatePackages(t *testing.T) {
	snapshot := ShardSnapshot{
		Version:    ShardSnapshotVersion,
		ShardCount: 2,
		Shards: []PackageShard{
			{ID: 1, Weight: 1, Packages: []string{"github.com/division-sh/swarm/a", "github.com/division-sh/swarm/a"}},
			{ID: 2, Weight: 1, Packages: []string{"github.com/division-sh/swarm/extra"}},
		},
	}

	validation, err := ValidateShardSnapshot(snapshot, []string{
		"github.com/division-sh/swarm/a",
		"github.com/division-sh/swarm/b",
	})
	if err == nil {
		t.Fatal("ValidateShardSnapshot succeeded, want error")
	}
	if got := strings.Join(validation.Missing, ","); got != "github.com/division-sh/swarm/b" {
		t.Fatalf("missing = %q, want b", got)
	}
	if got := strings.Join(validation.Extra, ","); got != "github.com/division-sh/swarm/extra" {
		t.Fatalf("extra = %q, want extra", got)
	}
	if got := strings.Join(validation.Duplicates, ","); got != "github.com/division-sh/swarm/a" {
		t.Fatalf("duplicates = %q, want a", got)
	}
}

func TestShardMatrixJSONAndPackagesForShard(t *testing.T) {
	snapshot := ShardSnapshot{
		Version:    ShardSnapshotVersion,
		ShardCount: 2,
		Shards: []PackageShard{
			{ID: 1, Weight: 1, Packages: []string{"github.com/division-sh/swarm/a"}},
			{ID: 2, Weight: 1, Packages: []string{"github.com/division-sh/swarm/b"}},
		},
	}
	raw, err := ShardMatrixJSON(snapshot)
	if err != nil {
		t.Fatalf("ShardMatrixJSON: %v", err)
	}
	var decoded struct {
		Include []struct {
			Shard int `json:"shard"`
		} `json:"include"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v raw=%s", err, raw)
	}
	if len(decoded.Include) != 2 || decoded.Include[0].Shard != 1 || decoded.Include[1].Shard != 2 {
		t.Fatalf("matrix = %+v, want shards 1 and 2", decoded.Include)
	}
	packages, err := PackagesForShard(snapshot, 2)
	if err != nil {
		t.Fatalf("PackagesForShard: %v", err)
	}
	if got := strings.Join(packages, " "); got != "github.com/division-sh/swarm/b" {
		t.Fatalf("packages = %q, want shard 2 package", got)
	}
}

func TestParsePackageWeightsUsesTerminalPackageEventsOnly(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"Action":"pass","Package":"github.com/division-sh/swarm/a","Test":"TestA","Elapsed":99}`,
		`{"Action":"pass","Package":"github.com/division-sh/swarm/a","Elapsed":3}`,
		`{"Action":"skip","Package":"github.com/division-sh/swarm/b","Elapsed":0}`,
		`{"Action":"output","Package":"github.com/division-sh/swarm/c","Elapsed":8}`,
		`not-json`,
	}, "\n"))

	weights, err := ParsePackageWeights(input)
	if err != nil {
		t.Fatalf("ParsePackageWeights: %v", err)
	}
	if got := weights["github.com/division-sh/swarm/a"]; got != 3 {
		t.Fatalf("weight a = %v, want package elapsed 3", got)
	}
	if got := weights["github.com/division-sh/swarm/b"]; got != minPackageWeight {
		t.Fatalf("weight b = %v, want min package weight", got)
	}
	if _, ok := weights["github.com/division-sh/swarm/c"]; ok {
		t.Fatalf("output-only package got weight: %+v", weights)
	}
}
