package testtiming

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/testplanning"
)

const (
	TestTimeReferenceRunID  = int64(37937174260)
	TestTimeReferenceHead   = "a4c1027c2204063b04565102a271a90db0134029"
	TestTimeReferenceSource = "04cd8aa0eb6e2ec77c5295b551164bf63bd4b011"
	TestTimeApproval        = "https://github.com/division-sh/swarm/issues/2535#issuecomment-6082942278"
	TestTimeReferencePolicy = "c5179845ce6eb6798523126f196b04e960136a19d199008fe458b318d6fa123b"
	// Changing the anchor requires a reviewed versioned policy adjustment, not
	// a Test-Time body line, automatic publisher, or each new merge base.
	TestTimeReferenceDigest = "02affac9993e2d9f767b861fd746cfecf3e318b562e5fd6e0d20691618079591"
)

type TimingCell struct {
	Package     string `json:"package"`
	Root        string `json:"root"`
	Backend     string `json:"backend,omitempty"`
	Environment string `json:"environment"`
	Count       string `json:"count"`
}

type RootTime struct {
	TimingCell
	Seconds float64 `json:"seconds"`
}

type ReferenceRootTime struct {
	RootTime
	Tiers []string `json:"tiers"`
}

type TestTimeReference struct {
	Version      int                       `json:"version"`
	Approval     string                    `json:"approval"`
	WorkflowHead string                    `json:"workflow_head"`
	RunID        int64                     `json:"run_id"`
	Attempt      int                       `json:"attempt"`
	Source       string                    `json:"source"`
	PlanDigest   string                    `json:"plan_digest"`
	PolicySHA256 string                    `json:"policy_sha256"`
	BuildContext testplanning.BuildContext `json:"build_context"`
	Roots        []ReferenceRootTime       `json:"roots"`
}

type TestTimeTier struct {
	Tier      string     `json:"tier"`
	Baseline  float64    `json:"baseline_seconds"`
	Candidate float64    `json:"candidate_seconds"`
	Growth    float64    `json:"growth_seconds"`
	Allowance float64    `json:"allowance_seconds"`
	Retained  int        `json:"retained"`
	Added     []RootTime `json:"added,omitempty"`
	Removed   []RootTime `json:"removed_or_renamed,omitempty"`
}

type TestTimeResult struct {
	ReferenceRunID  int64          `json:"reference_run_id"`
	ReferenceSource string         `json:"reference_source"`
	Status          BudgetStatus   `json:"status"`
	Tiers           []TestTimeTier `json:"tiers"`
	Problems        []string       `json:"problems,omitempty"`
}

func WriteTestTimeReference(w io.Writer, reference TestTimeReference) error {
	encoder := json.NewEncoder(w)
	return encoder.Encode(reference)
}

func LoadTestTimeReference(r io.Reader) (TestTimeReference, error) {
	raw, err := io.ReadAll(io.LimitReader(r, 8<<20))
	if err != nil {
		return TestTimeReference{}, err
	}
	var reference TestTimeReference
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reference); err != nil {
		return reference, err
	}
	var canonical bytes.Buffer
	if err := WriteTestTimeReference(&canonical, reference); err != nil {
		return reference, err
	}
	digest := sha256.Sum256(raw)
	if !bytes.Equal(raw, canonical.Bytes()) || hex.EncodeToString(digest[:]) != TestTimeReferenceDigest {
		return reference, fmt.Errorf("test-time anchor bytes differ from the independently approved pinned reference")
	}
	if reference.Version != 2 || reference.Approval != TestTimeApproval || reference.RunID != TestTimeReferenceRunID || reference.Attempt != 1 || reference.WorkflowHead != TestTimeReferenceHead || reference.Source != TestTimeReferenceSource || reference.PolicySHA256 != TestTimeReferencePolicy || len(reference.Roots) == 0 {
		return reference, fmt.Errorf("test-time reference identity is not the approved full run")
	}
	return reference, nil
}

// CaptureTestTimeReference has no automatic publication path. It consumes the
// complete admitted full receipts once, retaining execution/backend identity
// and membership selected by the exact policy at that execution source.
func CaptureTestTimeReference(plan testplanning.RunPlan, evidence []CommandEvidence, policyBytes []byte) (TestTimeReference, error) {
	if plan.Profile != testplanning.ProfileFull || plan.Venue != testplanning.VenueCI || plan.HeadSHA != TestTimeReferenceSource {
		return TestTimeReference{}, fmt.Errorf("reference must be the approved hosted full execution")
	}
	policyDigest := sha256.Sum256(policyBytes)
	if hex.EncodeToString(policyDigest[:]) != TestTimeReferencePolicy {
		return TestTimeReference{}, fmt.Errorf("reference capture requires the exact policy from the approved execution source")
	}
	policy, err := testplanning.LoadPolicy(bytes.NewReader(policyBytes))
	if err != nil {
		return TestTimeReference{}, err
	}
	roots, err := observedRootTimes(plan, TestTimeReferenceRunID, 1, evidence)
	if err != nil {
		return TestTimeReference{}, err
	}
	frozen, err := referenceTimingRoots(roots, policy)
	if err != nil {
		return TestTimeReference{}, err
	}
	return TestTimeReference{Version: 2, Approval: TestTimeApproval, WorkflowHead: TestTimeReferenceHead, RunID: TestTimeReferenceRunID, Attempt: 1, Source: plan.HeadSHA, PlanDigest: plan.Digest, PolicySHA256: TestTimeReferencePolicy, BuildContext: plan.BuildContext, Roots: frozen}, nil
}

func referenceTimingRoots(roots []RootTime, policy testplanning.Policy) ([]ReferenceRootTime, error) {
	var frozen []ReferenceRootTime
	for _, root := range roots {
		item := ReferenceRootTime{RootTime: root}
		for _, tier := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
			include, err := policy.SelectTimingCell(tier, testplanning.TestRoot{Package: root.Package, Name: root.Root}, root.Backend)
			if err != nil {
				return nil, err
			}
			if include {
				item.Tiers = append(item.Tiers, tier)
			}
		}
		if !slices.Contains(item.Tiers, testplanning.ProfileFull) {
			return nil, fmt.Errorf("reference cell %s.%s/%s lacks a reference-era full owner", root.Package, root.Root, root.Backend)
		}
		frozen = append(frozen, item)
	}
	return frozen, nil
}

func observedRootTimes(plan testplanning.RunPlan, runID int64, attempt int, evidence []CommandEvidence) ([]RootTime, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	seenUnits := map[string]bool{}
	seenCells := map[TimingCell]bool{}
	var roots []RootTime
	for _, item := range evidence {
		if item.WorkflowRunID != runID || item.WorkflowAttempt != attempt || seenUnits[item.UnitID] {
			return nil, fmt.Errorf("foreign or duplicate test-time receipt for %s", item.UnitID)
		}
		if problems := ValidateCommandEvidence(item, plan); len(problems) != 0 {
			return nil, fmt.Errorf("invalid test-time receipt for %s: %s", item.UnitID, strings.Join(problems, "; "))
		}
		if item.ExitCode != 0 || item.Report.Summary.FailedPackages != 0 || item.Report.Summary.FailedTests != 0 {
			return nil, fmt.Errorf("failed test-time proof for %s", item.UnitID)
		}
		seenUnits[item.UnitID] = true
		unit, _ := plan.Unit(item.UnitID)
		unitRoots, err := unitRootTimes(unit, item)
		if err != nil {
			return nil, err
		}
		for _, root := range unitRoots {
			if seenCells[root.TimingCell] {
				return nil, fmt.Errorf("duplicate test-time execution cell %s.%s/%s", root.Package, root.Root, root.Backend)
			}
			seenCells[root.TimingCell] = true
			roots = append(roots, root)
		}
	}
	if len(seenUnits) != len(plan.Units) {
		return nil, fmt.Errorf("test-time proof is incomplete: %d/%d units", len(seenUnits), len(plan.Units))
	}
	sort.Slice(roots, func(i, j int) bool { return timingCellKey(roots[i].TimingCell) < timingCellKey(roots[j].TimingCell) })
	return roots, nil
}

func unitRootTimes(unit testplanning.ProofUnit, item CommandEvidence) ([]RootTime, error) {
	backend, _ := testplanning.SoakBackend(unit.Run)
	// Parent elapsed includes descendants; direct children are a conservative
	// fallback, never an additive second charge.
	times := map[testplanning.TestRoot]float64{}
	parents := map[testplanning.TestRoot]bool{}
	for _, row := range item.Report.Tests {
		name, child, _ := strings.Cut(row.Test, "/")
		root := testplanning.TestRoot{Package: row.Package, Name: name}
		if child == "" {
			parents[root] = true
		}
		if !strings.Contains(child, "/") {
			times[root] = math.Max(times[root], row.Elapsed)
		}
	}
	var roots []RootTime
	for _, root := range unit.SelectedRoots {
		if !parents[root] {
			return nil, fmt.Errorf("missing required test-time root %s.%s", root.Package, root.Name)
		}
		cell := TimingCell{Package: root.Package, Root: root.Name, Backend: backend, Environment: item.EnvironmentID, Count: item.CountMode}
		roots = append(roots, RootTime{TimingCell: cell, Seconds: times[root]})
	}
	return roots, nil
}

func timingCellKey(cell TimingCell) string {
	return cell.Package + "\x00" + cell.Root + "\x00" + cell.Backend + "\x00" + cell.Environment + "\x00" + cell.Count
}

func EvaluateTestTime(reference TestTimeReference, policy testplanning.Policy, plan testplanning.RunPlan, runID int64, attempt int, evidence []CommandEvidence) TestTimeResult {
	result := TestTimeResult{ReferenceRunID: reference.RunID, ReferenceSource: reference.Source, Status: BudgetPass}
	roots, err := observedRootTimes(plan, runID, attempt, evidence)
	if err != nil || plan.Venue != testplanning.VenueCI || plan.BuildContext != reference.BuildContext {
		result.Status = BudgetIncomplete
		result.Problems = append(result.Problems, fmt.Sprintf("test-time proof scope mismatch or incomplete: %v", err))
		return result
	}
	scopes := map[string]bool{}
	for _, root := range reference.Roots {
		scopes[root.Environment+"\x00"+root.Count] = true
	}
	for _, root := range roots {
		if !scopes[root.Environment+"\x00"+root.Count] {
			result.Status = BudgetIncomplete
			result.Problems = append(result.Problems, "test-time execution environment/count differs from the hosted reference")
			return result
		}
	}
	for _, tier := range testTimeComparisonTiers(plan) {
		row, problems := compareTimingTier(reference.Roots, roots, policy, tier)
		result.Tiers = append(result.Tiers, row)
		result.Problems = append(result.Problems, problems...)
	}
	if len(result.Problems) != 0 {
		result.Status = BudgetFail
	}
	return result
}

func testTimeComparisonTiers(plan testplanning.RunPlan) []string {
	tiers := []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull}
	selected := slices.Clone(tiers[:testplanning.TierRank(plan.Profile)])
	if len(plan.ExtraUnits) != 0 && plan.Profile != testplanning.ProfileFull {
		selected = append(selected, testplanning.ProfileFull)
	}
	return selected
}

func compareTimingTier(reference []ReferenceRootTime, candidate []RootTime, policy testplanning.Policy, tier string) (TestTimeTier, []string) {
	row := TestTimeTier{Tier: tier}
	baseline, problems := projectReferenceTimingRoots(reference, tier)
	candidate, candidateProblems := projectTimingRoots(candidate, policy, tier)
	problems = append(problems, candidateProblems...)
	base := map[TimingCell]RootTime{}
	for _, root := range baseline {
		base[root.TimingCell] = root
		row.Baseline += root.Seconds
	}
	for _, root := range candidate {
		row.Candidate += root.Seconds
		if _, exists := base[root.TimingCell]; exists {
			row.Retained++
			delete(base, root.TimingCell)
		} else {
			row.Added = append(row.Added, root)
			if root.Seconds > 30 {
				problems = append(problems, fmt.Sprintf("%s: new or promoted root %s.%s/%s is %.3fs (>30s): Test-Time rationale AND independent placement approval require a reviewed versioned policy adjustment", tier, root.Package, root.Root, root.Backend, root.Seconds))
			}
		}
	}
	// Removed/renamed roots are reported separately and cannot subsidize new
	// work. Required-root absence in the candidate already fails admission.
	for _, root := range base {
		row.Removed = append(row.Removed, root)
		row.Candidate += root.Seconds
	}
	sort.Slice(row.Removed, func(i, j int) bool {
		return timingCellKey(row.Removed[i].TimingCell) < timingCellKey(row.Removed[j].TimingCell)
	})
	row.Growth = row.Candidate - row.Baseline
	row.Allowance = math.Min(row.Baseline*0.05, 600)
	if row.Baseline == 0 || row.Growth > row.Allowance+1e-9 {
		problems = append(problems, fmt.Sprintf("%s: unreviewed test-time growth %.3fs exceeds min(5%% of %.3fs, 600s) = %.3fs", tier, row.Growth, row.Baseline, row.Allowance))
	}
	return row, problems
}

func projectReferenceTimingRoots(roots []ReferenceRootTime, tier string) ([]RootTime, []string) {
	var selected []RootTime
	var problems []string
	for _, root := range roots {
		var canonical []string
		for _, known := range []string{testplanning.ProfileCore, testplanning.ProfileLifecycle, testplanning.ProfileFull} {
			if slices.Contains(root.Tiers, known) {
				canonical = append(canonical, known)
			}
		}
		if !slices.Equal(root.Tiers, canonical) || !slices.Contains(root.Tiers, testplanning.ProfileFull) {
			problems = append(problems, fmt.Sprintf("invalid pinned tier membership for %s.%s/%s", root.Package, root.Root, root.Backend))
			continue
		}
		if slices.Contains(root.Tiers, tier) {
			selected = append(selected, root.RootTime)
		}
	}
	return selected, problems
}

func projectTimingRoots(roots []RootTime, policy testplanning.Policy, tier string) ([]RootTime, []string) {
	var selected []RootTime
	var problems []string
	for _, root := range roots {
		include, err := policy.SelectTimingCell(tier, testplanning.TestRoot{Package: root.Package, Name: root.Root}, root.Backend)
		if err != nil {
			problems = append(problems, err.Error())
		}
		if include {
			selected = append(selected, root)
		}
	}
	return selected, problems
}

func WriteTestTimeMarkdown(w io.Writer, result TestTimeResult) error {
	if _, err := fmt.Fprintf(w, "\n## Pinned Test-Time Ratchet\n\n%s against full run %d (`%s`). Parent/direct-child elapsed is counted once; separate soak cells count separately. Removed/renamed roots grant no timing credit. A Test-Time line alone never approves an exception.\n\n| Tier | Baseline | Comparable + new (removed neutral) | Growth | min(5%%, 600s) | Retained | Added | Removed/renamed |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n", result.Status, result.ReferenceRunID, result.ReferenceSource); err != nil {
		return err
	}
	for _, tier := range result.Tiers {
		if _, err := fmt.Fprintf(w, "| %s | %.3fs | %.3fs | %.3fs | %.3fs | %d | %d | %d |\n", tier.Tier, tier.Baseline, tier.Candidate, tier.Growth, tier.Allowance, tier.Retained, len(tier.Added), len(tier.Removed)); err != nil {
			return err
		}
	}
	for _, problem := range result.Problems {
		if _, err := fmt.Fprintf(w, "- %s\n", problem); err != nil {
			return err
		}
	}
	return nil
}
