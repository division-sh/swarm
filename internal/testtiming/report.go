package testtiming

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

const defaultTopN = 20

type Options struct {
	TopN int
}

type Summary struct {
	Events            int
	MalformedLines    int
	Packages          int
	FailedPackages    int
	SkippedPackages   int
	Tests             int
	FailedTests       int
	SkippedTests      int
	PackageElapsedSec float64
}

type PackageTiming struct {
	Package string
	Result  string
	Elapsed float64
}

type TestTiming struct {
	Package string
	Test    string
	Result  string
	Elapsed float64
}

type Report struct {
	Summary      Summary
	SlowPackages []PackageTiming
	SlowTests    []TestTiming
}

type event struct {
	Action  string  `json:"Action"`
	Package string  `json:"Package"`
	Test    string  `json:"Test"`
	Elapsed float64 `json:"Elapsed"`
}

func ParseReport(r io.Reader, opts Options) (Report, error) {
	if r == nil {
		return Report{}, fmt.Errorf("input reader is nil")
	}
	topN := opts.TopN
	if topN <= 0 {
		topN = defaultTopN
	}
	report := Report{}
	packages := map[string]PackageTiming{}
	tests := map[string]TestTiming{}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var evt event
		if err := json.Unmarshal([]byte(line), &evt); err != nil {
			report.Summary.MalformedLines++
			continue
		}
		report.Summary.Events++
		action := strings.TrimSpace(evt.Action)
		pkg := strings.TrimSpace(evt.Package)
		test := strings.TrimSpace(evt.Test)
		if !isTerminalAction(action) || pkg == "" {
			continue
		}
		if test == "" {
			packages[pkg] = PackageTiming{Package: pkg, Result: action, Elapsed: evt.Elapsed}
			continue
		}
		key := pkg + "\x00" + test
		tests[key] = TestTiming{Package: pkg, Test: test, Result: action, Elapsed: evt.Elapsed}
	}
	if err := scanner.Err(); err != nil {
		return Report{}, fmt.Errorf("read test JSON: %w", err)
	}
	report.SlowPackages = packageTimings(packages)
	report.SlowTests = testTimings(tests)
	report.Summary.Packages = len(report.SlowPackages)
	report.Summary.Tests = len(report.SlowTests)
	for _, pkg := range report.SlowPackages {
		report.Summary.PackageElapsedSec += pkg.Elapsed
		switch pkg.Result {
		case "fail":
			report.Summary.FailedPackages++
		case "skip":
			report.Summary.SkippedPackages++
		}
	}
	for _, test := range report.SlowTests {
		switch test.Result {
		case "fail":
			report.Summary.FailedTests++
		case "skip":
			report.Summary.SkippedTests++
		}
	}
	report.SlowPackages = trimPackageTimings(report.SlowPackages, topN)
	report.SlowTests = trimTestTimings(report.SlowTests, topN)
	return report, nil
}

func WriteMarkdown(w io.Writer, report Report) error {
	if w == nil {
		return fmt.Errorf("output writer is nil")
	}
	if _, err := fmt.Fprintln(w, "# Go Test Timing Report"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "Generated from `go test -json`. Timing is observability-only; this report does not enforce budgets."); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "## Summary"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "| Metric | Value |"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "| --- | ---: |"); err != nil {
		return err
	}
	rows := []struct {
		name  string
		value string
	}{
		{"Parsed events", fmt.Sprintf("%d", report.Summary.Events)},
		{"Malformed lines ignored", fmt.Sprintf("%d", report.Summary.MalformedLines)},
		{"Packages", fmt.Sprintf("%d", report.Summary.Packages)},
		{"Failed packages", fmt.Sprintf("%d", report.Summary.FailedPackages)},
		{"Skipped packages", fmt.Sprintf("%d", report.Summary.SkippedPackages)},
		{"Tests", fmt.Sprintf("%d", report.Summary.Tests)},
		{"Failed tests", fmt.Sprintf("%d", report.Summary.FailedTests)},
		{"Skipped tests", fmt.Sprintf("%d", report.Summary.SkippedTests)},
		{"Package elapsed sum", formatSeconds(report.Summary.PackageElapsedSec)},
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(w, "| %s | %s |\n", row.name, row.value); err != nil {
			return err
		}
	}
	if err := writePackages(w, report.SlowPackages); err != nil {
		return err
	}
	return writeTests(w, report.SlowTests)
}

func isTerminalAction(action string) bool {
	return action == "pass" || action == "fail" || action == "skip"
}

func packageTimings(values map[string]PackageTiming) []PackageTiming {
	out := make([]PackageTiming, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Elapsed != out[j].Elapsed {
			return out[i].Elapsed > out[j].Elapsed
		}
		return out[i].Package < out[j].Package
	})
	return out
}

func testTimings(values map[string]TestTiming) []TestTiming {
	out := make([]TestTiming, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Elapsed != out[j].Elapsed {
			return out[i].Elapsed > out[j].Elapsed
		}
		if out[i].Package != out[j].Package {
			return out[i].Package < out[j].Package
		}
		return out[i].Test < out[j].Test
	})
	return out
}

func trimPackageTimings(values []PackageTiming, limit int) []PackageTiming {
	if limit <= 0 || len(values) <= limit {
		return values
	}
	return values[:limit]
}

func trimTestTimings(values []TestTiming, limit int) []TestTiming {
	if limit <= 0 || len(values) <= limit {
		return values
	}
	return values[:limit]
}

func writePackages(w io.Writer, values []PackageTiming) error {
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "## Slowest Packages"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if len(values) == 0 {
		_, err := fmt.Fprintln(w, "No package timing events found.")
		return err
	}
	if _, err := fmt.Fprintln(w, "| Package | Result | Elapsed |"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "| --- | --- | ---: |"); err != nil {
		return err
	}
	for _, value := range values {
		if _, err := fmt.Fprintf(w, "| `%s` | %s | %s |\n", value.Package, value.Result, formatSeconds(value.Elapsed)); err != nil {
			return err
		}
	}
	return nil
}

func writeTests(w io.Writer, values []TestTiming) error {
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "## Slowest Tests"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if len(values) == 0 {
		_, err := fmt.Fprintln(w, "No test timing events found.")
		return err
	}
	if _, err := fmt.Fprintln(w, "| Package | Test | Result | Elapsed |"); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "| --- | --- | --- | ---: |"); err != nil {
		return err
	}
	for _, value := range values {
		if _, err := fmt.Fprintf(w, "| `%s` | `%s` | %s | %s |\n", value.Package, value.Test, value.Result, formatSeconds(value.Elapsed)); err != nil {
			return err
		}
	}
	return nil
}

func formatSeconds(seconds float64) string {
	return fmt.Sprintf("%.3fs", seconds)
}
