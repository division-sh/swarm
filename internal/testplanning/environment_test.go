package testplanning

import (
	"maps"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
)

var localEnvironmentUnits = []string{
	"local-release-golden-smoke", "local-catalog-smoke", "local-generated-fanout-fixture",
	"local-fanout-handoff-ack-loss", "local-api-matrix-registry", "local-api-routing-canaries",
	"local-routing-reporter", "local-delivery-continuation", "local-release-golden-restart",
	"local-release-lifecycle-smoke", "local-serveapp-canaries", "local-runtime-bus-full",
}

func managedEnvironmentIDs() map[string]string {
	return map[string]string{VenueCI: "ci-postgres-gateway-empty-v1", VenueLocal: "local-postgres-gateway-empty-v1"}
}

func environmentPlanInputs(t *testing.T) (Policy, WeightModel, []string) {
	t.Helper()
	file, err := os.Open("../../.github/test-proof-plan.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	policy, err := LoadPolicy(file)
	if err != nil {
		t.Fatal(err)
	}
	packages := []string{policy.Module + "/example-broad"}
	for _, unit := range policy.Units {
		packages = append(packages, unit.Packages...)
	}
	sort.Strings(packages)
	return policy, WeightModel{Version: WeightModelVersion, SourceRunID: "environment-control"}, slices.Compact(packages)
}

func TestProofPlanEnvironmentMatchesVenueBothDirections(t *testing.T) {
	policy, model, packages := environmentPlanInputs(t)
	for _, venue := range []string{VenueCI, VenueLocal} {
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			t.Run(venue+"/"+tier, func(t *testing.T) {
				plan, err := BuildPlan(policy, model, packages, tier, "environment control", "head", BuildOptions{Venue: venue})
				if err != nil {
					t.Fatal(err)
				}
				want := venue + "-postgres-gateway-empty-v1"
				for _, unit := range plan.Units {
					if unit.EnvironmentID != want {
						t.Errorf("%s has environment %q, want %q", unit.ID, unit.EnvironmentID, want)
					}
				}
			})
		}
	}
}

func TestAllTwelveLocalEnvironmentDeclarationsSelectHostedRecipe(t *testing.T) {
	policy, model, packages := environmentPlanInputs(t)
	for _, id := range localEnvironmentUnits {
		for _, venue := range []string{VenueCI, VenueLocal} {
			t.Run(id+"/"+venue, func(t *testing.T) {
				selected := policy
				selected.Profiles = map[string]ProfilePolicy{}
				for tier, profile := range policy.Profiles {
					profile.Units = []string{id}
					selected.Profiles[tier] = profile
				}
				plan, err := BuildPlan(selected, model, packages, ProfileCore, "isolated declaration", "head", BuildOptions{Venue: venue})
				if err != nil {
					t.Fatal(err)
				}
				unit, err := plan.Unit(id)
				if err != nil || unit.EnvironmentID != venue+"-postgres-gateway-empty-v1" {
					t.Fatalf("unit=%+v error=%v", unit, err)
				}
			})
		}
	}
}

func TestVenueEnvironmentDeclarationsFailClosed(t *testing.T) {
	for _, test := range []struct {
		name  string
		fixed string
		ids   map[string]string
		venue string
	}{
		{"absent", "", nil, VenueCI},
		{"ambiguous", "env", managedEnvironmentIDs(), VenueCI},
		{"unknown venue", "", map[string]string{"other": "env"}, VenueCI},
		{"empty recipe", "", map[string]string{VenueCI: ""}, VenueCI},
		{"noncanonical recipe", "", map[string]string{VenueCI: " env "}, VenueCI},
		{"noncanonical fixed recipe", " env ", nil, VenueCI},
		{"missing hosted recipe", "", map[string]string{VenueLocal: "local"}, VenueCI},
		{"missing local recipe", "", map[string]string{VenueCI: "ci"}, VenueLocal},
		{"unsupported selected venue", "env", nil, "other"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := environmentForVenue(test.fixed, test.ids, test.venue); err == nil {
				t.Fatal("invalid or missing venue declaration admitted")
			}
		})
	}
	policy, model, packages := environmentPlanInputs(t)
	for _, owner := range []string{"profile", "unit"} {
		t.Run(owner+" missing venue", func(t *testing.T) {
			selected := policy
			selected.Profiles = maps.Clone(policy.Profiles)
			selected.Units = maps.Clone(policy.Units)
			if owner == "profile" {
				profile := selected.Profiles[ProfileCore]
				profile.EnvironmentIDs = map[string]string{VenueLocal: "local"}
				selected.Profiles[ProfileCore] = profile
			} else {
				unit := selected.Units["local-release-golden-smoke"]
				unit.EnvironmentIDs = map[string]string{VenueLocal: "local"}
				selected.Units["local-release-golden-smoke"] = unit
			}
			if _, err := BuildPlan(selected, model, packages, ProfileCore, "missing venue", "head"); err == nil || !strings.Contains(err.Error(), "no recipe for venue") {
				t.Fatalf("missing selected recipe was not refused: %v", err)
			}
		})
	}
	selected := policy
	selected.Profiles = maps.Clone(policy.Profiles)
	profile := selected.Profiles[ProfileCore]
	profile.Units = []string{"selected-store-fast"}
	selected.Profiles[ProfileCore] = profile
	if _, err := BuildPlan(selected, model, packages, ProfileCore, "local-only projection", "head"); err == nil {
		t.Fatal("local-only explicit-PostgreSQL projection admitted to hosted execution")
	}
}

func TestCommittedEnvironmentDeclarationCensus(t *testing.T) {
	policy, _, _ := environmentPlanInputs(t)
	if len(localEnvironmentUnits) != 12 {
		t.Fatal("original twelve-declaration census changed")
	}
	for name, profile := range policy.Profiles {
		if profile.EnvironmentID != "" || !maps.Equal(profile.EnvironmentIDs, managedEnvironmentIDs()) {
			t.Fatalf("profile %s bypasses explicit venue declarations", name)
		}
	}
	for id, unit := range policy.Units {
		want := managedEnvironmentIDs()
		if id == "selected-store-fast" {
			want = map[string]string{VenueLocal: "local-postgres-explicit-v1"}
		}
		if unit.EnvironmentID != "" || !maps.Equal(unit.EnvironmentIDs, want) {
			t.Fatalf("unit %s bypasses its exact venue recipe: %+v", id, unit)
		}
	}
}
