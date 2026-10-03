package testtiming

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/testplanning"
)

// CadenceAttempt is observed execution, never a qualification override.
type CadenceAttempt struct {
	Plan         testplanning.RunPlan
	RunID        int64
	Attempt      int
	Evidence     []CommandEvidence
	LoadProblems []string
}

type CadenceAttribution struct {
	Package             string `json:"package"`
	Root                string `json:"root"`
	Kind                string `json:"kind"`
	Review              string `json:"review"`
	IntroducingCommit   string `json:"introducing_commit,omitempty"`
	FirstDetectionRunID int64  `json:"first_detection_run_id,omitempty"`
}

type CadenceFinding struct {
	testplanning.TestRoot
	Classification      string `json:"classification"`
	Review              string `json:"review,omitempty"`
	ConfirmedCoreEscape bool   `json:"confirmed_core_escape"`
	FirstParentLag      *int   `json:"first_parent_lag,omitempty"`
}

type CadenceObservation struct {
	Version                       int              `json:"version"`
	RunID                         int64            `json:"workflow_run_id"`
	Attempt                       int              `json:"workflow_attempt"`
	HeadSHA                       string           `json:"head_sha"`
	PlanDigest                    string           `json:"plan_digest"`
	Venue                         string           `json:"venue"`
	ObservedUnits                 int              `json:"observed_units"`
	PlannedUnits                  int              `json:"planned_units"`
	Problems                      []string         `json:"problems"`
	Findings                      []CadenceFinding `json:"findings"`
	EligibleClassifiedRegressions int              `json:"eligible_classified_regressions"`
	ConfirmedCoreEscapes          int              `json:"confirmed_core_escapes"`
	EscapeRate                    *float64         `json:"escape_rate,omitempty"`
}

type CadenceCoreProof struct {
	CadenceAttempt
	SuccessfulRun bool
}

func ObserveFullCadence(full CadenceAttempt, core *CadenceCoreProof, attributions []CadenceAttribution, firstParentLineage []string) (CadenceObservation, error) {
	result := CadenceObservation{Version: 1, RunID: full.RunID, Attempt: full.Attempt, HeadSHA: full.Plan.HeadSHA, PlanDigest: full.Plan.Digest, Venue: full.Plan.Venue, PlannedUnits: len(full.Plan.Units), Problems: []string{}, Findings: []CadenceFinding{}}
	if err := full.Plan.Validate(); err != nil {
		return result, err
	}
	if full.Plan.Profile != testplanning.ProfileFull || full.Plan.BuildContext.GOOS == "" || full.RunID < 1 || full.Attempt < 1 {
		return result, fmt.Errorf("cadence observation requires bound full execution and exact run/attempt")
	}
	result.Problems = append(result.Problems, full.LoadProblems...)
	findings := map[testplanning.TestRoot]bool{}
	seen := map[string]bool{}
	for _, evidence := range full.Evidence {
		if evidence.WorkflowRunID != full.RunID || evidence.WorkflowAttempt != full.Attempt || seen[evidence.UnitID] {
			result.Problems = append(result.Problems, "foreign/duplicate full command: "+evidence.UnitID)
			continue
		}
		seen[evidence.UnitID] = true
		if problems := ValidateCommandIdentity(evidence, full.Plan); len(problems) != 0 {
			result.Problems = append(result.Problems, problems...)
			continue
		}
		result.ObservedUnits++
		result.Problems = append(result.Problems, ValidateCommandEvidence(evidence, full.Plan)...)
		collectCadenceFailures(full.Plan, evidence, findings)
	}
	for _, unit := range full.Plan.Units {
		if !seen[unit.ID] {
			result.Problems = append(result.Problems, "missing full command: "+unit.ID)
		}
	}
	classified, err := cadenceAttributionMap(attributions, findings)
	if err != nil {
		return result, err
	}
	for root := range findings {
		finding := CadenceFinding{TestRoot: root, Classification: "unclassified_candidate"}
		if attribution, ok := classified[root]; ok {
			finding.Classification, finding.Review = attribution.Kind, attribution.Review
			if attribution.Kind == "regression" {
				lag, err := cadenceRegressionLag(full, core, attribution, firstParentLineage)
				if err != nil {
					result.Problems = append(result.Problems, root.Name+": "+err.Error())
				} else {
					finding.FirstParentLag = &lag
					finding.ConfirmedCoreEscape = true
					result.EligibleClassifiedRegressions++
					result.ConfirmedCoreEscapes++
				}
			}
		}
		result.Findings = append(result.Findings, finding)
	}
	sort.Slice(result.Findings, func(i, j int) bool {
		a, b := result.Findings[i], result.Findings[j]
		return a.Package+"\x00"+a.Name < b.Package+"\x00"+b.Name
	})
	if result.EligibleClassifiedRegressions > 0 {
		rate := float64(result.ConfirmedCoreEscapes) / float64(result.EligibleClassifiedRegressions)
		result.EscapeRate = &rate
	}
	return result, nil
}

func collectCadenceFailures(plan testplanning.RunPlan, evidence CommandEvidence, findings map[testplanning.TestRoot]bool) {
	_, packageProblems := reportPackageList(evidence.Report)
	if len(packageProblems) != 0 || len(reportTestProblems(evidence.Report)) != 0 || evidence.Report.Summary.MalformedLines != 0 || evidence.Report.Summary.DuplicatePackageEvents != 0 || evidence.Report.Summary.DuplicateTestEvents != 0 {
		return
	}
	unit, _ := plan.Unit(evidence.UnitID)
	selected := map[testplanning.TestRoot]bool{}
	for _, root := range unit.SelectedRoots {
		selected[root] = true
	}
	for _, test := range evidence.Report.Tests {
		root := testplanning.TestRoot{Package: test.Package, Name: strings.SplitN(test.Test, "/", 2)[0]}
		if test.Result == "fail" && selected[root] {
			findings[root] = true
		}
	}
}

func cadenceAttributionMap(attributions []CadenceAttribution, findings map[testplanning.TestRoot]bool) (map[testplanning.TestRoot]CadenceAttribution, error) {
	result := map[testplanning.TestRoot]CadenceAttribution{}
	for _, item := range attributions {
		root := testplanning.TestRoot{Package: item.Package, Name: item.Root}
		if _, duplicate := result[root]; duplicate || !findings[root] || !strings.HasPrefix(item.Review, "https://github.com/division-sh/swarm/") {
			return nil, fmt.Errorf("duplicate/foreign/unreviewed cadence attribution for %s.%s", item.Package, item.Root)
		}
		switch item.Kind {
		case "regression", "infra", "harness", "budget", "non_regression":
		default:
			return nil, fmt.Errorf("unsupported cadence classification %q", item.Kind)
		}
		result[root] = item
	}
	return result, nil
}

func cadenceRegressionLag(full CadenceAttempt, core *CadenceCoreProof, item CadenceAttribution, lineage []string) (int, error) {
	if len(lineage) == 0 || lineage[0] != full.Plan.HeadSHA || item.FirstDetectionRunID != full.RunID || item.IntroducingCommit == "" {
		return 0, fmt.Errorf("first detection/master introducing lineage is unknown")
	}
	positions := map[string]int{}
	for position, sha := range lineage {
		positions[sha] = position
	}
	introduced, ok := positions[item.IntroducingCommit]
	if !ok {
		return 0, fmt.Errorf("introducer is not on detected first-parent master lineage")
	}
	if core == nil || !core.SuccessfulRun || core.Plan.Profile != testplanning.ProfileCore || core.Plan.Venue != testplanning.VenueCI || core.Plan.BuildContext.GOOS == "" || core.RunID >= full.RunID || core.RunID < 1 || core.Attempt < 1 || len(core.LoadProblems) != 0 {
		return 0, fmt.Errorf("preceding actual successful core proof is unavailable")
	}
	position, ok := positions[core.Plan.HeadSHA]
	if !ok || position > introduced {
		return 0, fmt.Errorf("core proof does not cover the introducing master lineage")
	}
	if err := core.Plan.Validate(); err != nil {
		return 0, err
	}
	for _, unit := range core.Plan.Units {
		for _, root := range unit.SelectedRoots {
			if root.Package == item.Package && root.Name == item.Root {
				return 0, fmt.Errorf("failure is not a full-only root relative to preceding core proof")
			}
		}
	}
	if err := cadenceCoreCompleteness(core.CadenceAttempt); err != nil {
		return 0, err
	}
	return introduced, nil
}

func cadenceCoreCompleteness(core CadenceAttempt) error {
	seen := map[string]bool{}
	for _, evidence := range core.Evidence {
		if evidence.WorkflowRunID != core.RunID || evidence.WorkflowAttempt != core.Attempt || seen[evidence.UnitID] || evidence.ExitCode != 0 || evidence.Report.Summary.FailedPackages != 0 || evidence.Report.Summary.FailedTests != 0 || len(ValidateCommandEvidence(evidence, core.Plan)) != 0 {
			return fmt.Errorf("preceding core command is foreign/duplicate/failed/incomplete")
		}
		seen[evidence.UnitID] = true
	}
	if len(seen) != len(core.Plan.Units) {
		return fmt.Errorf("preceding core proof has missing units")
	}
	return nil
}

func WriteCadenceMarkdown(out io.Writer, observation CadenceObservation) error {
	rate := "N/A"
	if observation.EscapeRate != nil {
		rate = fmt.Sprintf("%.1f%%", *observation.EscapeRate*100)
	}
	_, err := fmt.Fprintf(out, "## Full cadence observation\n\nRun %d attempt %d; source `%s`; plan `%s`; venue `%s`. Observed %d/%d units.\n\nConfirmed core escapes: %d / %d eligible classified regressions; rate **%s**. This observation grants no qualification or defect closure. Unknown provenance/lag remains unknown.\n\n", observation.RunID, observation.Attempt, observation.HeadSHA, observation.PlanDigest, observation.Venue, observation.ObservedUnits, observation.PlannedUnits, observation.ConfirmedCoreEscapes, observation.EligibleClassifiedRegressions, rate)
	if err != nil {
		return err
	}
	for _, finding := range observation.Findings {
		lag := "unknown"
		if finding.FirstParentLag != nil {
			lag = fmt.Sprint(*finding.FirstParentLag)
		}
		if _, err := fmt.Fprintf(out, "- `%s.%s`: %s; confirmed core escape=%t; first-parent landing lag=%s.\n", finding.Package, finding.Name, finding.Classification, finding.ConfirmedCoreEscape, lag); err != nil {
			return err
		}
	}
	for _, problem := range observation.Problems {
		if _, err := fmt.Fprintf(out, "- Evidence problem: %s\n", problem); err != nil {
			return err
		}
	}
	return nil
}
