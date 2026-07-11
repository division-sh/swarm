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

type RunPlan struct {
	Version        int         `json:"version"`
	PolicyVersion  int         `json:"policy_version"`
	Profile        string      `json:"profile"`
	Reason         string      `json:"reason"`
	HeadSHA        string      `json:"head_sha"`
	Digest         string      `json:"digest"`
	TargetSeconds  float64     `json:"target_seconds"`
	GranularityMax float64     `json:"granularity_max_seconds"`
	Packages       []string    `json:"packages"`
	Units          []ProofUnit `json:"units"`
}

type ProofUnit struct {
	ID            string   `json:"id"`
	Packages      []string `json:"packages"`
	Run           string   `json:"run,omitempty"`
	CountMode     string   `json:"count_mode"`
	EnvironmentID string   `json:"environment_id"`
	BudgetClass   string   `json:"budget_class"`
	WeightSeconds float64  `json:"weight_seconds"`
}

type weightedPackage struct {
	name   string
	weight float64
}

func BuildPlan(policy Policy, model WeightModel, packages []string, profile, reason, headSHA string) (RunPlan, error) {
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
	if shardCount > len(broad) {
		shardCount = len(broad)
	}
	shards := make([]ProofUnit, shardCount)
	for i := range shards {
		shards[i] = ProofUnit{
			ID:            fmt.Sprintf("broad-%02d", i+1),
			CountMode:     profilePolicy.CountMode,
			EnvironmentID: profilePolicy.EnvironmentID,
			BudgetClass:   "broad",
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
	for _, id := range profilePolicy.Units {
		specialUnit := policy.Units[id]
		weight := 0.0
		for _, pkg := range specialUnit.Packages {
			if !discovered[pkg] {
				return RunPlan{}, fmt.Errorf("unit %s package %s is absent from discovered inventory", id, pkg)
			}
			packageWeight, ok := model.Packages[pkg]
			if !ok {
				packageWeight = policy.Planning.UnknownPackageSeconds
			} else if packageWeight <= 0 {
				packageWeight = 0.1
			}
			weight += packageWeight
		}
		unitPackages, err := canonicalStrings(specialUnit.Packages)
		if err != nil {
			return RunPlan{}, fmt.Errorf("unit %s: %w", id, err)
		}
		units = append(units, ProofUnit{
			ID:            id,
			Packages:      unitPackages,
			Run:           specialUnit.Run,
			CountMode:     specialUnit.CountMode,
			EnvironmentID: specialUnit.EnvironmentID,
			BudgetClass:   specialUnit.BudgetClass,
			WeightSeconds: weight,
		})
	}
	plan := RunPlan{
		Version:        RunPlanVersion,
		PolicyVersion:  policy.Version,
		Profile:        profile,
		Reason:         reason,
		HeadSHA:        strings.TrimSpace(headSHA),
		TargetSeconds:  policy.Planning.TargetSeconds,
		GranularityMax: granularityMax,
		Packages:       append([]string(nil), packages...),
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
	if p.Profile == "" || p.HeadSHA == "" || p.Digest == "" {
		return fmt.Errorf("run plan profile, head_sha, and digest must be non-empty")
	}
	if len(p.Units) == 0 || len(p.Packages) == 0 {
		return fmt.Errorf("run plan has no units or package inventory")
	}
	seenUnits := map[string]bool{}
	seenPackages := map[string]string{}
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
		if unit.BudgetClass != "broad" && unit.BudgetClass != "full" {
			return fmt.Errorf("unit %s has unsupported budget class %q", unit.ID, unit.BudgetClass)
		}
		for _, pkg := range unit.Packages {
			if owner, ok := seenPackages[pkg]; ok {
				return fmt.Errorf("package %s is duplicated across units %s and %s", pkg, owner, unit.ID)
			}
			seenPackages[pkg] = unit.ID
		}
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

func (p RunPlan) Unit(id string) (ProofUnit, error) {
	for _, unit := range p.Units {
		if unit.ID == id {
			return unit, nil
		}
	}
	return ProofUnit{}, fmt.Errorf("plan unit %q not found", id)
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
	for _, unit := range plan.Units {
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
