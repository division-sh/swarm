package testtiming

import (
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

	report, err := ParseReport(input)
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
	if got := report.Packages[0]; got.Package != "github.com/division-sh/swarm/slow" || got.Result != "fail" || got.Elapsed != 14 {
		t.Fatalf("slowest package = %+v, want swarm/slow fail 14s", got)
	}
	if got := report.Tests[0]; got.Package != "github.com/division-sh/swarm/slow" || got.Test != "TestSlow" || got.Result != "fail" || got.Elapsed != 12.5 {
		t.Fatalf("slowest test = %+v, want swarm/slow TestSlow fail 12.5s", got)
	}
}

func TestParseReportKeepsCompleteTruthAndMarkdownAppliesTopLimit(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`{"Action":"pass","Package":"github.com/division-sh/swarm/a","Test":"TestA","Elapsed":1}`,
		`{"Action":"pass","Package":"github.com/division-sh/swarm/a","Elapsed":1}`,
		`{"Action":"pass","Package":"github.com/division-sh/swarm/b","Test":"TestB","Elapsed":2}`,
		`{"Action":"pass","Package":"github.com/division-sh/swarm/b","Elapsed":2}`,
	}, "\n"))

	report, err := ParseReport(input)
	if err != nil {
		t.Fatalf("ParseReport: %v", err)
	}
	if len(report.Packages) != 2 || report.Packages[0].Package != "github.com/division-sh/swarm/b" {
		t.Fatalf("packages = %+v, want complete truth ordered with swarm/b first", report.Packages)
	}
	if len(report.Tests) != 2 || report.Tests[0].Test != "TestB" {
		t.Fatalf("tests = %+v, want complete truth ordered with TestB first", report.Tests)
	}
	if report.Summary.Packages != 2 || report.Summary.Tests != 2 {
		t.Fatalf("summary = %+v, want complete counts", report.Summary)
	}
	var out strings.Builder
	if err := WriteMarkdown(&out, report, MarkdownOptions{TopN: 1}); err != nil {
		t.Fatalf("WriteMarkdown: %v", err)
	}
	if strings.Contains(out.String(), "github.com/division-sh/swarm/a") {
		t.Fatalf("markdown includes package/test outside top N:\n%s", out.String())
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
		Packages: []PackageTiming{{Package: "github.com/division-sh/swarm/pkg", Result: "pass", Elapsed: 3.5}},
		Tests:    []TestTiming{{Package: "github.com/division-sh/swarm/pkg", Test: "TestThing", Result: "pass", Elapsed: 2.25}},
	}
	var out strings.Builder
	if err := WriteMarkdown(&out, report, MarkdownOptions{TopN: 20}); err != nil {
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
