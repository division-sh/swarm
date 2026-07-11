package testplanning

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const PolicyVersion = 1

const (
	ProfilePRCommon    = "pr-common"
	ProfilePREscalated = "pr-escalated"
	ProfileFull        = "full"
	ProfileNightly     = "nightly"
)

type Policy struct {
	Version         int                         `yaml:"version"`
	Module          string                      `yaml:"module"`
	Planning        PlanningPolicy              `yaml:"planning"`
	EscalationPaths []string                    `yaml:"escalation_paths"`
	SpecialPackages []string                    `yaml:"special_packages"`
	Profiles        map[string]ProfilePolicy    `yaml:"profiles"`
	Units           map[string]UnitPolicy       `yaml:"units"`
	Projections     map[string]ProjectionPolicy `yaml:"projections"`
}

type PlanningPolicy struct {
	TargetSeconds         float64 `yaml:"target_seconds"`
	MaxShards             int     `yaml:"max_shards"`
	UnknownPackageSeconds float64 `yaml:"unknown_package_seconds"`
	MaxImbalance          float64 `yaml:"max_imbalance"`
}

type ProfilePolicy struct {
	CountMode     string   `yaml:"count_mode"`
	EnvironmentID string   `yaml:"environment_id"`
	Units         []string `yaml:"units"`
}

type UnitPolicy struct {
	Packages      []string `yaml:"packages"`
	Run           string   `yaml:"run,omitempty"`
	CountMode     string   `yaml:"count_mode"`
	EnvironmentID string   `yaml:"environment_id"`
	BudgetClass   string   `yaml:"budget_class"`
}

type ProjectionPolicy struct {
	Profile string `yaml:"profile,omitempty"`
	Unit    string `yaml:"unit,omitempty"`
}

func LoadPolicy(r io.Reader) (Policy, error) {
	if r == nil {
		return Policy{}, fmt.Errorf("proof policy reader is nil")
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		return Policy{}, fmt.Errorf("read proof policy: %w", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	var policy Policy
	if err := decoder.Decode(&policy); err != nil {
		return Policy{}, fmt.Errorf("decode proof policy: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Policy{}, fmt.Errorf("decode proof policy: multiple YAML documents")
		}
		return Policy{}, fmt.Errorf("decode proof policy trailing data: %w", err)
	}
	if err := policy.Validate(); err != nil {
		return Policy{}, err
	}
	return policy, nil
}

func (p Policy) Validate() error {
	var problems []string
	if p.Version != PolicyVersion {
		problems = append(problems, fmt.Sprintf("version = %d, want %d", p.Version, PolicyVersion))
	}
	if strings.TrimSpace(p.Module) == "" {
		problems = append(problems, "module must be non-empty")
	}
	if p.Planning.TargetSeconds <= 0 {
		problems = append(problems, "planning.target_seconds must be positive")
	}
	if p.Planning.MaxShards <= 0 {
		problems = append(problems, "planning.max_shards must be positive")
	}
	if p.Planning.UnknownPackageSeconds <= 0 {
		problems = append(problems, "planning.unknown_package_seconds must be positive")
	}
	if p.Planning.MaxImbalance < 0 || p.Planning.MaxImbalance >= 1 {
		problems = append(problems, "planning.max_imbalance must be in [0,1)")
	}
	for _, pattern := range p.EscalationPaths {
		if _, err := regexp.Compile(pattern); err != nil {
			problems = append(problems, fmt.Sprintf("escalation path %q: %v", pattern, err))
		}
	}
	if duplicate := duplicateStrings(p.SpecialPackages); duplicate != "" {
		problems = append(problems, fmt.Sprintf("special_packages duplicates %q", duplicate))
	}
	requiredProfiles := []string{ProfilePRCommon, ProfilePREscalated, ProfileFull, ProfileNightly}
	for _, name := range requiredProfiles {
		profile, ok := p.Profiles[name]
		if !ok {
			problems = append(problems, fmt.Sprintf("profiles missing %s", name))
			continue
		}
		if !validCountMode(profile.CountMode) {
			problems = append(problems, fmt.Sprintf("profiles.%s.count_mode %q is unsupported", name, profile.CountMode))
		}
		if strings.TrimSpace(profile.EnvironmentID) == "" {
			problems = append(problems, fmt.Sprintf("profiles.%s.environment_id must be non-empty", name))
		}
		if duplicate := duplicateStrings(profile.Units); duplicate != "" {
			problems = append(problems, fmt.Sprintf("profiles.%s.units duplicates %q", name, duplicate))
		}
		for _, unit := range profile.Units {
			if _, ok := p.Units[unit]; !ok {
				problems = append(problems, fmt.Sprintf("profiles.%s references unknown unit %s", name, unit))
			}
		}
	}
	for name, unit := range p.Units {
		if strings.TrimSpace(name) == "" {
			problems = append(problems, "units contains an empty id")
		}
		if len(unit.Packages) == 0 {
			problems = append(problems, fmt.Sprintf("units.%s.packages must be non-empty", name))
		}
		if duplicate := duplicateStrings(unit.Packages); duplicate != "" {
			problems = append(problems, fmt.Sprintf("units.%s.packages duplicates %q", name, duplicate))
		}
		if !validCountMode(unit.CountMode) {
			problems = append(problems, fmt.Sprintf("units.%s.count_mode %q is unsupported", name, unit.CountMode))
		}
		if strings.TrimSpace(unit.EnvironmentID) == "" {
			problems = append(problems, fmt.Sprintf("units.%s.environment_id must be non-empty", name))
		}
		if unit.BudgetClass != "broad" && unit.BudgetClass != "full" {
			problems = append(problems, fmt.Sprintf("units.%s.budget_class %q is unsupported", name, unit.BudgetClass))
		}
		if unit.Run != "" {
			if _, err := regexp.Compile(unit.Run); err != nil {
				problems = append(problems, fmt.Sprintf("units.%s.run %q: %v", name, unit.Run, err))
			}
		}
	}
	for name, projection := range p.Projections {
		if (projection.Profile == "") == (projection.Unit == "") {
			problems = append(problems, fmt.Sprintf("projections.%s must set exactly one of profile or unit", name))
		}
		if projection.Profile != "" {
			if _, ok := p.Profiles[projection.Profile]; !ok {
				problems = append(problems, fmt.Sprintf("projections.%s references unknown profile %s", name, projection.Profile))
			}
		}
		if projection.Unit != "" {
			if _, ok := p.Units[projection.Unit]; !ok {
				problems = append(problems, fmt.Sprintf("projections.%s references unknown unit %s", name, projection.Unit))
			}
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("invalid proof policy: %s", strings.Join(problems, "; "))
	}
	return nil
}

func (p Policy) ResolveProfile(event string, changedFiles []string, forced string) (string, string, error) {
	if forced != "" {
		if _, ok := p.Profiles[forced]; !ok {
			return "", "", fmt.Errorf("unknown forced profile %q", forced)
		}
		return forced, "explicit profile", nil
	}
	switch event {
	case "pull_request":
		for _, path := range changedFiles {
			for _, pattern := range p.EscalationPaths {
				matched, err := regexp.MatchString(pattern, path)
				if err != nil {
					return "", "", err
				}
				if matched {
					return ProfilePREscalated, "changed path: " + path, nil
				}
			}
		}
		return ProfilePRCommon, "no escalation path changed", nil
	case "schedule":
		return ProfileNightly, "scheduled full truth", nil
	case "push", "workflow_dispatch":
		return ProfileFull, "full-truth event: " + event, nil
	default:
		return "", "", fmt.Errorf("unsupported CI event %q", event)
	}
}

func validCountMode(value string) bool {
	return value == "cache-default" || value == "count-1"
}

func duplicateStrings(values []string) string {
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return ""
		}
		if seen[value] {
			return value
		}
		seen[value] = true
	}
	return ""
}
