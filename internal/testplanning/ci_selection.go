package testplanning

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

type CISelection struct {
	Version    int      `json:"version"`
	Tier       string   `json:"tier"`
	ExtraUnits []string `json:"extra_units"`
}

type CISelectionReport struct {
	CISelection
	SelectionDigest string `json:"selection_digest"`
	PlanDigest      string `json:"plan_digest"`
	ExecutionSHA    string `json:"execution_sha"`
	WorkflowRunID   int64  `json:"workflow_run_id"`
	WorkflowAttempt int    `json:"workflow_attempt"`
	CheckName       string `json:"check_name"`
}

func (p RunPlan) CISelectionReport(runID int64, attempt int) (CISelectionReport, error) {
	if err := p.Validate(); err != nil {
		return CISelectionReport{}, err
	}
	if p.Venue != VenueCI || runID <= 0 || attempt <= 0 {
		return CISelectionReport{}, fmt.Errorf("CI selection requires a hosted plan and exact run/attempt")
	}
	selection, err := p.CISelection()
	if err != nil {
		return CISelectionReport{}, err
	}
	return CISelectionReport{
		CISelection: selection, SelectionDigest: selection.Digest(), PlanDigest: p.Digest,
		ExecutionSHA: p.HeadSHA, WorkflowRunID: runID, WorkflowAttempt: attempt,
		CheckName: selection.CheckName(runID, attempt),
	}, nil
}

func (s CISelection) CheckName(runID int64, attempt int) string {
	return fmt.Sprintf("CI tier: %s; selection: %s; run: %d; attempt: %d", s.Tier, s.Digest(), runID, attempt)
}

func CIUnits(body string) ([]string, error) {
	var declarations []string
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.HasPrefix(strings.TrimSpace(line), "CI-Units") {
			declarations = append(declarations, line)
		}
	}
	if len(declarations) == 0 {
		return []string{}, nil
	}
	if len(declarations) != 1 || !strings.HasPrefix(declarations[0], "CI-Units: ") {
		return nil, fmt.Errorf("CI-Units requires one canonical declaration")
	}
	return canonicalExtraUnits(strings.Split(strings.TrimPrefix(declarations[0], "CI-Units: "), ", "))
}

func canonicalExtraUnits(ids []string) ([]string, error) {
	ids = slices.Clone(ids)
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" || strings.ContainsAny(id, " ,\t\r\n") || seen[id] {
			return nil, fmt.Errorf("invalid or duplicate CI-Units identifier %q", id)
		}
		seen[id] = true
	}
	slices.Sort(ids)
	if ids == nil {
		ids = []string{}
	}
	return ids, nil
}

func (p Policy) selectExtraUnits(tier string, ids []string) ([]string, error) {
	ids, err := canonicalExtraUnits(ids)
	if err != nil {
		return nil, err
	}
	base, ok := p.Profiles[tier]
	if !ok {
		return nil, fmt.Errorf("unknown CI-Units base tier %q", tier)
	}
	for _, id := range ids {
		if !slices.Contains(p.Profiles[ProfileFull].Units, id) {
			return nil, fmt.Errorf("CI-Units %q is not an admitted hosted full unit", id)
		}
		if slices.Contains(base.Units, id) {
			return nil, fmt.Errorf("CI-Units %q already belongs to base tier %s", id, tier)
		}
		unit, ok := p.Units[id]
		if !ok {
			return nil, fmt.Errorf("unknown CI-Units %q", id)
		}
		if _, err := environmentForVenue(unit.EnvironmentID, unit.EnvironmentIDs, VenueCI); err != nil {
			return nil, fmt.Errorf("CI-Units %s: %w", id, err)
		}
	}
	return ids, nil
}

func (p RunPlan) CISelection() (CISelection, error) {
	ids, err := canonicalExtraUnits(p.ExtraUnits)
	if err != nil || TierRank(p.Profile) == 0 {
		return CISelection{}, fmt.Errorf("invalid CI selection: %v", err)
	}
	return CISelection{Version: 1, Tier: p.Profile, ExtraUnits: ids}, nil
}

func (s CISelection) Digest() string {
	raw, _ := json.Marshal(s)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func CheckCurrentCISelection(plan RunPlan, body string) error {
	tier, _ := CITier(body)
	ids, err := CIUnits(body)
	if err != nil {
		return err
	}
	if tier != plan.Profile || !slices.Equal(ids, plan.ExtraUnits) {
		return fmt.Errorf("current CI tier/units differ from the qualifying selection; new execution required")
	}
	return nil
}

func (p RunPlan) validateExtraUnits() error {
	ids, err := canonicalExtraUnits(p.ExtraUnits)
	if err != nil || !slices.Equal(ids, p.ExtraUnits) {
		return fmt.Errorf("plan extra_units are not canonical: %v", err)
	}
	if len(ids) != 0 && (p.Venue != VenueCI || p.Profile == ProfileFull) {
		return fmt.Errorf("extra_units require a hosted lower-tier plan")
	}
	for _, id := range ids {
		if _, err := p.Unit(id); err != nil {
			return fmt.Errorf("requested CI-Units missing from plan: %w", err)
		}
	}
	return nil
}
