package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testtiming"
)

type failureOptions struct {
	run        int64
	last       int
	population string
	ledger     string
	output     string
}

type failureRunMetadata struct {
	testplanning.QualifiedRun
	Event string `json:"event"`
}

type failurePopulationReport struct {
	Runs    []testtiming.FailureRun      `json:"runs"`
	Matches []testtiming.FailureMatch    `json:"matches"`
	Summary testtiming.FailurePopulation `json:"summary"`
}

func runFailures(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("failures", flag.ContinueOnError)
	var options failureOptions
	flags.Int64Var(&options.run, "run", 0, "inspect every attempt of one Actions run")
	flags.IntVar(&options.last, "last", 0, "observe the last N PR ci.yml runs (1..150)")
	flags.StringVar(&options.population, "population", "", "offline JSON array of exact observed runs/attempts")
	flags.StringVar(&options.ledger, "ledger", ".github/flake-ledger.json", "reviewed manifestation ledger")
	flags.StringVar(&options.output, "json", "", "retain exact observations and population result")
	if err := flags.Parse(args); err != nil {
		return err
	}
	modes := 0
	for _, selected := range []bool{options.run > 0, options.last > 0, options.population != ""} {
		if selected {
			modes++
		}
	}
	if modes != 1 || options.run < 0 || options.last < 0 || options.last > 150 || flags.NArg() != 0 {
		return fmt.Errorf("usage: swarm-test-timing failures (-run ID | -last N | -population FILE) [-json FILE]")
	}
	file, err := os.Open(options.ledger)
	if err != nil {
		return err
	}
	ledger, err := testtiming.ReadFailureLedger(file)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runs, err := readFailurePopulation(ctx, options)
	if err != nil {
		return err
	}
	summary, err := ledger.Summarize(runs)
	if err != nil {
		return err
	}
	var matches []testtiming.FailureMatch
	for _, run := range runs {
		for _, attempt := range run.Attempts {
			for _, problem := range attempt.Problems {
				fmt.Fprintf(output, "run %d/%d: incomplete observation: %s\n", run.ID, attempt.Number, problem)
			}
			for _, failure := range attempt.Failures {
				match, err := ledger.Match(failure)
				if err != nil {
					return err
				}
				matches = append(matches, match)
				printFailureMatch(output, match)
			}
		}
	}
	fmt.Fprintf(output, "PR population=%d; runs with RED attempts=%d; PR-owned-only=%d; reviewed non-PR-only=%d; unknown-only=%d; mixed=%d; in-flight=%d; cancelled=%d; incompletely observed=%d; earlier RED now green=%d\n", summary.PRRuns, summary.RedRuns, summary.PRAttributable, summary.NonPROnly, summary.UnknownOnly, summary.Mixed, summary.InFlight, summary.Cancelled, summary.Incomplete, summary.EarlierRedNowGreen)
	if summary.RedRuns == 0 {
		fmt.Fprintln(output, "reviewed non-PR RED share: unmeasured (zero RED-run denominator)")
	} else {
		fmt.Fprintf(output, "reviewed non-PR-only RED share lower bound: %d/%d (%.2f%%); unknown/mixed observations are not exonerated\n", summary.NonPROnly, summary.RedRuns, 100*float64(summary.NonPROnly)/float64(summary.RedRuns))
	}
	var kinds []string
	for kind := range summary.ByPhenomenon {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		count := summary.ByPhenomenon[kind]
		fmt.Fprintf(output, "  %s: RED runs=%d; runs with reviewed non-PR evidence=%d; unknown observations=%d (overlapping classes, not additive)\n", kind, count.RedRuns, count.ReviewedNonPRRuns, count.UnknownObservations)
	}
	return finishFailurePopulation(options.output, failurePopulationReport{runs, matches, summary})
}

func finishFailurePopulation(path string, report failurePopulationReport) error {
	if path != "" {
		if err := writeJSON(path, report); err != nil {
			return err
		}
	}
	if report.Summary.Incomplete != 0 {
		return fmt.Errorf("failure population incompletely observed: %d runs; inspect the retained report for missing history or evidence", report.Summary.Incomplete)
	}
	return nil
}

func printFailureMatch(output io.Writer, match testtiming.FailureMatch) {
	id := match.Record
	if id == "" {
		id = "not in active ledger"
	}
	fmt.Fprintf(output, "%d/%d job=%d %s %s/%s: %s; outcome=%s; phenomenon=%s; attribution=%s; source=%s\n", match.RunID, match.Attempt, match.JobID, match.Job, match.Package, match.Test, id, match.Conclusion, match.Phenomenon, match.Attribution, match.Source)
	if match.Problem != "" {
		fmt.Fprintf(output, "  incomplete evidence: %s\n", match.Problem)
	}
	if match.Record != "" {
		fmt.Fprintf(output, "  tracking=%s; status=%s; repair=%s\n", match.Tracking, match.Status, match.Repair)
	}
	if match.Rate != nil {
		fmt.Fprintf(output, "  historical scoped rate=%d/%d; %s; %s\n", match.Rate.Failures, match.Rate.Attempts, match.Rate.Context, match.Rate.Evidence)
	}
}

func readFailurePopulation(ctx context.Context, options failureOptions) ([]testtiming.FailureRun, error) {
	if options.population != "" {
		var runs []testtiming.FailureRun
		err := readJSON(options.population, &runs)
		return runs, err
	}
	var metadata []failureRunMetadata
	if options.run != 0 {
		var run failureRunMetadata
		if err := failureAPI(ctx, "repos/division-sh/swarm/actions/runs/"+strconv.FormatInt(options.run, 10), &run); err != nil {
			return nil, err
		}
		metadata = append(metadata, run)
	} else {
		for page := 1; len(metadata) < options.last; page++ {
			var batch struct {
				Runs []failureRunMetadata `json:"workflow_runs"`
			}
			path := fmt.Sprintf("repos/division-sh/swarm/actions/workflows/ci.yml/runs?event=pull_request&per_page=100&page=%d", page)
			if err := failureAPI(ctx, path, &batch); err != nil {
				return nil, err
			}
			if len(batch.Runs) == 0 {
				break
			}
			metadata = append(metadata, batch.Runs...)
		}
		if len(metadata) > options.last {
			metadata = metadata[:options.last]
		}
	}
	var runs []testtiming.FailureRun
	for _, run := range metadata {
		observed := testtiming.FailureRun{ID: run.ID, RunAttempt: run.RunAttempt, Event: run.Event, HeadSHA: run.HeadSHA, Status: run.Status, Conclusion: run.Conclusion}
		for number := 1; number <= run.RunAttempt; number++ {
			attempt := observeFailureAttempt(ctx, run, number)
			observed.Attempts = append(observed.Attempts, attempt)
		}
		runs = append(runs, observed)
	}
	return runs, nil
}

func failureAPI(ctx context.Context, path string, target any) error {
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	raw, err := ghRead(readCtx, path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}
