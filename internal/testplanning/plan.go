package testplanning

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
)

const RunPlanVersion = 1

type BuildOptions struct {
	IncludeSoak       bool
	IncludeParityFull bool
}

type RunPlan struct {
	Version        int          `json:"version"`
	PolicyVersion  int          `json:"policy_version"`
	Profile        string       `json:"profile"`
	Reason         string       `json:"reason"`
	HeadSHA        string       `json:"head_sha"`
	Digest         string       `json:"digest"`
	BuildContext   BuildContext `json:"build_context"`
	TargetSeconds  float64      `json:"target_seconds"`
	GranularityMax float64      `json:"granularity_max_seconds"`
	Packages       []string     `json:"packages"`
	Units          []ProofUnit  `json:"units"`
}

type ProofUnit struct {
	ID                  string              `json:"id"`
	Packages            []string            `json:"packages"`
	WorkloadProfile     string              `json:"workload_profile"`
	ExecutionTier       string              `json:"execution_tier"`
	Run                 string              `json:"run,omitempty"`
	Skip                string              `json:"skip,omitempty"`
	GoTimeout           string              `json:"go_timeout,omitempty"`
	CountMode           string              `json:"count_mode"`
	EnvironmentID       string              `json:"environment_id"`
	BudgetClass         string              `json:"budget_class"`
	WeightSeconds       float64             `json:"weight_seconds"`
	SelectedRoots       []TestRoot          `json:"selected_roots,omitempty"`
	RequiredTests       []RequiredTest      `json:"required_tests,omitempty"`
	TestBearingPackages []string            `json:"test_bearing_packages,omitempty"`
	RequiredChildren    map[string][]string `json:"required_children,omitempty"`
}

type weightedPackage struct {
	name   string
	weight float64
}

func BuildPlan(policy Policy, model WeightModel, packages []string, profile, reason, headSHA string, options ...BuildOptions) (RunPlan, error) {
	if len(options) > 1 {
		return RunPlan{}, fmt.Errorf("build plan accepts at most one option set")
	}
	var option BuildOptions
	if len(options) == 1 {
		option = options[0]
	}
	if err := policy.Validate(); err != nil {
		return RunPlan{}, err
	}
	if err := model.Validate(); err != nil {
		return RunPlan{}, err
	}
	profilePolicy, ok := policy.Profiles[profile]
	if !ok {
		return RunPlan{}, fmt.Errorf("unknown profile %q", profile)
	}
	packages, err := canonicalStrings(packages)
	if err != nil {
		return RunPlan{}, fmt.Errorf("package inventory: %w", err)
	}
	if len(packages) == 0 {
		return RunPlan{}, fmt.Errorf("package inventory is empty")
	}
	discovered := make(map[string]bool, len(packages))
	for _, pkg := range packages {
		discovered[pkg] = true
	}
	special := map[string]bool{}
	for _, pkg := range policy.SpecialPackages {
		if !discovered[pkg] {
			return RunPlan{}, fmt.Errorf("special package %s is absent from discovered inventory", pkg)
		}
		special[pkg] = true
	}
	selectedPackages := append([]string(nil), packages...)
	if profile == ProfileLocal {
		selected := map[string]bool{}
		for _, pkg := range packages {
			if !special[pkg] {
				selected[pkg] = true
			}
		}
		for _, id := range profilePolicy.Units {
			for _, pkg := range policy.Units[id].Packages {
				selected[pkg] = true
			}
		}
		selectedPackages = selectedPackages[:0]
		for pkg := range selected {
			selectedPackages = append(selectedPackages, pkg)
		}
		sort.Strings(selectedPackages)
	}
	var broad []weightedPackage
	var total float64
	var granularityMax float64
	for _, pkg := range packages {
		if special[pkg] {
			continue
		}
		weight, ok := model.Packages[pkg]
		if !ok {
			weight = policy.Planning.UnknownPackageSeconds
		} else if weight <= 0 {
			weight = 0.1
		}
		broad = append(broad, weightedPackage{name: pkg, weight: weight})
		total += weight
		if weight > granularityMax {
			granularityMax = weight
		}
	}
	shardCount := int(math.Ceil(total / policy.Planning.TargetSeconds))
	if shardCount < 1 {
		shardCount = 1
	}
	if shardCount > policy.Planning.MaxShards {
		shardCount = policy.Planning.MaxShards
	}
	if profile == ProfileLocal {
		shardCount = 1
	}
	if shardCount > len(broad) {
		shardCount = len(broad)
	}
	shards := make([]ProofUnit, shardCount)
	for i := range shards {
		shards[i] = ProofUnit{
			ID:              fmt.Sprintf("broad-%02d", i+1),
			WorkloadProfile: profile,
			ExecutionTier:   executionTier(profile, "broad"),
			CountMode:       profilePolicy.CountMode,
			EnvironmentID:   profilePolicy.EnvironmentID,
			BudgetClass:     "broad",
		}
	}
	sort.Slice(broad, func(i, j int) bool {
		if broad[i].weight != broad[j].weight {
			return broad[i].weight > broad[j].weight
		}
		return broad[i].name < broad[j].name
	})
	for _, item := range broad {
		target := lightestUnit(shards)
		shards[target].Packages = append(shards[target].Packages, item.name)
		shards[target].WeightSeconds += item.weight
	}
	for i := range shards {
		sort.Strings(shards[i].Packages)
	}
	units := append([]ProofUnit(nil), shards...)
	unitIDs := append([]string(nil), profilePolicy.Units...)
	if option.IncludeSoak {
		if profile != ProfilePRCommon && profile != ProfilePREscalated {
			return RunPlan{}, fmt.Errorf("affected soak is only an optional PR addition")
		}
		unitIDs = append(unitIDs, "conformance-soak-sqlite", "conformance-soak-postgres")
	}
	if option.IncludeParityFull {
		if profile != ProfilePRCommon && profile != ProfilePREscalated {
			return RunPlan{}, fmt.Errorf("parity supplements are only an optional PR addition")
		}
		unitIDs = append(unitIDs, "parity-served-source-artifact", "parity-connected-channel-onboarding-surface", "parity-destructive-reset-crash", "parity-golden-forced-restart")
	}
	for _, id := range unitIDs {
		specialUnit := policy.Units[id]
		for _, pkg := range specialUnit.Packages {
			if !discovered[pkg] {
				return RunPlan{}, fmt.Errorf("unit %s package %s is absent from discovered inventory", id, pkg)
			}
		}
		unitPackages, err := canonicalStrings(specialUnit.Packages)
		if err != nil {
			return RunPlan{}, fmt.Errorf("unit %s: %w", id, err)
		}
		unit := ProofUnit{
			ID:               id,
			WorkloadProfile:  profile,
			ExecutionTier:    executionTier(profile, specialUnit.BudgetClass),
			Packages:         unitPackages,
			Run:              specialUnit.Run,
			Skip:             specialUnit.Skip,
			GoTimeout:        specialUnit.GoTimeout,
			CountMode:        specialUnit.CountMode,
			EnvironmentID:    specialUnit.EnvironmentID,
			BudgetClass:      specialUnit.BudgetClass,
			RequiredChildren: specialUnit.RequiredChildren,
		}
		weightProfile := profile
		if strings.HasPrefix(id, "parity-") {
			unit.WorkloadProfile = ProfileFull
			weightProfile = ProfileFull
		}
		weight, measured := model.UnitSeconds(weightProfile, unit)
		if !measured {
			weight = policy.Planning.UnknownPackageSeconds
		}
		unit.WeightSeconds = weight
		units = append(units, unit)
	}
	plan := RunPlan{
		Version:        RunPlanVersion,
		PolicyVersion:  policy.Version,
		Profile:        profile,
		Reason:         reason,
		HeadSHA:        strings.TrimSpace(headSHA),
		TargetSeconds:  policy.Planning.TargetSeconds,
		GranularityMax: granularityMax,
		Packages:       selectedPackages,
		Units:          units,
	}
	if plan.HeadSHA == "" {
		return RunPlan{}, fmt.Errorf("head SHA must be non-empty")
	}
	digest, err := planDigest(plan)
	if err != nil {
		return RunPlan{}, err
	}
	plan.Digest = digest
	if err := plan.Validate(); err != nil {
		return RunPlan{}, err
	}
	return plan, nil
}

func (p RunPlan) Validate() error {
	if p.Version != RunPlanVersion {
		return fmt.Errorf("run plan version = %d, want %d", p.Version, RunPlanVersion)
	}
	switch p.Profile {
	case ProfileLocal, ProfilePRCommon, ProfilePREscalated, ProfileFull, ProfileNightly:
	default:
		return fmt.Errorf("run plan has unsupported profile %q", p.Profile)
	}
	if p.Profile == "" || p.HeadSHA == "" || p.Digest == "" {
		return fmt.Errorf("run plan profile, head_sha, and digest must be non-empty")
	}
	if len(p.Units) == 0 || len(p.Packages) == 0 {
		return fmt.Errorf("run plan has no units or package inventory")
	}
	seenUnits := map[string]bool{}
	seenPackages := map[string][]ProofUnit{}
	soakBackends := map[string]bool{}
	for _, unit := range p.Units {
		if unit.ID == "" || seenUnits[unit.ID] {
			return fmt.Errorf("run plan has empty or duplicate unit id %q", unit.ID)
		}
		seenUnits[unit.ID] = true
		if len(unit.Packages) == 0 {
			return fmt.Errorf("unit %s has no packages", unit.ID)
		}
		if !validCountMode(unit.CountMode) || unit.EnvironmentID == "" {
			return fmt.Errorf("unit %s has invalid count/environment identity", unit.ID)
		}
		if unit.ExecutionTier != executionTier(p.Profile, unit.BudgetClass) {
			return fmt.Errorf("unit %s has invalid execution tier %q", unit.ID, unit.ExecutionTier)
		}
		if unit.WorkloadProfile != p.Profile && !(strings.HasPrefix(unit.ID, "parity-") && unit.WorkloadProfile == ProfileFull && (p.Profile == ProfilePRCommon || p.Profile == ProfilePREscalated)) {
			return fmt.Errorf("unit %s has invalid workload profile %q", unit.ID, unit.WorkloadProfile)
		}
		if unit.BudgetClass != "broad" && unit.BudgetClass != "full" && unit.BudgetClass != "soak" {
			return fmt.Errorf("unit %s has unsupported budget class %q", unit.ID, unit.BudgetClass)
		}
		if err := validateSoakSelection(unit.Packages, unit.Run, unit.Skip, unit.GoTimeout, unit.CountMode, unit.BudgetClass); err != nil {
			return fmt.Errorf("unit %s: %w", unit.ID, err)
		}
		if unit.BudgetClass == "soak" {
			backend, _ := SoakBackend(unit.Run)
			if soakBackends[backend] {
				return fmt.Errorf("duplicate soak backend %s", backend)
			}
			soakBackends[backend] = true
		}
		if p.BuildContext.GOOS != "" {
			if p.BuildContext.GOARCH == "" || p.BuildContext.CGOEnabled == "" {
				return fmt.Errorf("run plan has incomplete Go build context")
			}
			selected := map[string]bool{}
			for _, root := range unit.SelectedRoots {
				selected[root.Package+"\x00"+root.Name] = true
			}
			for _, required := range unit.RequiredTests {
				if !selected[required.Package+"\x00"+required.Name] {
					return fmt.Errorf("unit %s requires unselected root %s.%s", unit.ID, required.Package, required.Name)
				}
			}
		}
		for _, pkg := range unit.Packages {
			for _, previous := range seenPackages[pkg] {
				if previous.Run == "" || unit.Run == "" {
					return fmt.Errorf("package %s is duplicated across unfiltered units %s and %s", pkg, previous.ID, unit.ID)
				}
			}
			seenPackages[pkg] = append(seenPackages[pkg], unit)
		}
	}
	if len(soakBackends) != 0 && (!soakBackends["sqlite"] || !soakBackends["postgres"]) {
		return fmt.Errorf("soak plan requires both backend cells")
	}
	wantPackages, err := canonicalStrings(p.Packages)
	if err != nil {
		return fmt.Errorf("run plan package inventory: %w", err)
	}
	gotPackages := make([]string, 0, len(seenPackages))
	for pkg := range seenPackages {
		gotPackages = append(gotPackages, pkg)
	}
	sort.Strings(gotPackages)
	if strings.Join(gotPackages, "\x00") != strings.Join(wantPackages, "\x00") {
		return fmt.Errorf("run plan units cover %v, want discovered inventory %v", gotPackages, wantPackages)
	}
	want, err := planDigest(p)
	if err != nil {
		return err
	}
	if p.Digest != want {
		return fmt.Errorf("run plan digest %s does not match canonical digest %s", p.Digest, want)
	}
	return nil
}

func executionTier(profile, budget string) string {
	if budget == "soak" {
		return "soak"
	}
	if profile == ProfileLocal {
		return "local"
	}
	return "full"
}

func (p RunPlan) Unit(id string) (ProofUnit, error) {
	for _, unit := range p.Units {
		if unit.ID == id {
			return unit, nil
		}
	}
	return ProofUnit{}, fmt.Errorf("plan unit %q not found", id)
}

func (p RunPlan) ValidateExecutionSHA(actual string) error {
	if err := p.Validate(); err != nil {
		return err
	}
	actual = strings.TrimSpace(actual)
	if actual == "" {
		return fmt.Errorf("execution SHA must be non-empty")
	}
	if actual != p.HeadSHA {
		return fmt.Errorf("execution SHA %s does not match planned SHA %s", actual, p.HeadSHA)
	}
	return nil
}

func MatrixJSON(plan RunPlan) ([]byte, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	type entry struct {
		Unit string `json:"unit"`
	}
	matrix := struct {
		Include []entry `json:"include"`
	}{Include: make([]entry, 0, len(plan.Units))}
	// Offer expensive units first without changing the digest-bound plan. Actions
	// controls admission; this ordering does not promise a runner start order.
	ordered := append([]ProofUnit(nil), plan.Units...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].WeightSeconds == ordered[j].WeightSeconds {
			return ordered[i].ID < ordered[j].ID
		}
		return ordered[i].WeightSeconds > ordered[j].WeightSeconds
	})
	for _, unit := range ordered {
		matrix.Include = append(matrix.Include, entry{Unit: unit.ID})
	}
	return json.Marshal(matrix)
}

func planDigest(plan RunPlan) (string, error) {
	plan.Digest = ""
	raw, err := json.Marshal(plan)
	if err != nil {
		return "", fmt.Errorf("marshal run plan digest: %w", err)
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func lightestUnit(units []ProofUnit) int {
	target := 0
	for i := 1; i < len(units); i++ {
		if units[i].WeightSeconds < units[target].WeightSeconds {
			target = i
		}
	}
	return target
}

func canonicalStrings(values []string) ([]string, error) {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if seen[value] {
			return nil, fmt.Errorf("duplicate value %q", value)
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out, nil
}
