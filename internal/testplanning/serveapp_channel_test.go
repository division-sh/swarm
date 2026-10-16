package testplanning

import (
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestServeappChannelCompositionPreservesWholeWorkload(t *testing.T) {
	f := loadCatalogTierFixture(t)
	full := f.bind(t, ProfileFull)
	channel, _ := full.Unit(channelUnit)
	for _, catalog := range [][]string{nil, {"catalog-runtime"}, {"catalog-replay-1"}, {"catalog-runtime", "catalog-replay-1", "catalog-replay-2-3"}} {
		t.Run(strings.Join(catalog, "+"), func(t *testing.T) {
			base := f.bind(t, ProfileCore, catalog...)
			extras := append(slices.Clone(catalog), channelUnit)
			plan := f.bind(t, ProfileCore, extras...)
			slices.Reverse(extras)
			reordered := f.bind(t, ProfileCore, extras...)
			if !reflect.DeepEqual(plan, reordered) {
				t.Fatal("supplement ordering changed the bound plan")
			}
			want, got := channelRequiredUnion(base, channel), channelRequiredUnion(plan)
			if !reflect.DeepEqual(want, got) {
				t.Fatal("core + canonical channel root/child union changed")
			}
			actual, _ := plan.Unit(channelUnit)
			actual.WeightSeconds = channel.WeightSeconds
			if !reflect.DeepEqual(actual, channel) {
				t.Fatal("complete channel roots, deferrals or execution recipe changed")
			}
			complement, err := plan.Unit(channelComplement)
			if err != nil || len(complement.SelectedRoots) != 12 {
				t.Fatalf("private twelve-root complement: %d roots, %v", len(complement.SelectedRoots), err)
			}
			if _, measured := f.model.UnitSeconds(ProfileCore, complement); measured || complement.WeightSeconds != f.policy.Planning.UnknownPackageSeconds {
				t.Fatal("complement borrowed the thirteen-root owner's timing history")
			}
			for _, retired := range []string{channelParityUnit, serveappCanaryUnit} {
				if _, err := plan.Unit(retired); err == nil {
					t.Fatalf("covered owner %s still executes", retired)
				}
			}
			if err := CheckCurrentCISelection(plan, "CI-Tier: core\nCI-Units: "+strings.Join(plan.ExtraUnits, ", ")); err != nil {
				t.Fatal(err)
			}
			if err := CheckCurrentCISelection(plan, "CI-Tier: core"); err == nil {
				t.Fatal("changed post-receipt selection accepted")
			}
			var completionRoots int
			for _, root := range actual.SelectedRoots {
				if strings.HasPrefix(root.Name, "TestClaudeCLICompletion2270") {
					completionRoots++
				}
			}
			if completionRoots == 0 {
				t.Fatal("canonical completion2270 controls absent")
			}
			t.Logf("complete channel: %d roots, %d completion2270 roots; complement: twelve; exact child union retained", len(actual.SelectedRoots), completionRoots)
		})
	}
}

func channelRequiredUnion(plan RunPlan, extra ...ProofUnit) map[TestRoot][]string {
	union := map[TestRoot][]string{}
	for _, unit := range append(slices.Clone(plan.Units), extra...) {
		for _, required := range unit.RequiredTests {
			children := append(slices.Clone(union[required.TestRoot]), required.Children...)
			slices.Sort(children)
			union[required.TestRoot] = slices.Compact(children)
		}
	}
	return union
}

func TestServeappChannelCompositionBaselinePlans(t *testing.T) {
	f := loadCatalogTierFixture(t)
	for _, venue := range []string{VenueCI, VenueLocal} {
		for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
			t.Run(venue+"/"+tier, func(t *testing.T) {
				plan, err := BuildPlan(f.policy, f.model, f.packages, tier, "unchanged channel baseline", "same-head", BuildOptions{Venue: venue})
				if err == nil {
					err = BindExecution(&plan, f.inventory, f.proofs, f.policy)
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err := plan.Unit(channelComplement); err == nil {
					t.Fatal("private complement changed a baseline tier")
				}
				t.Logf("baseline-plan %s/%s %s", venue, tier, plan.Digest)
			})
		}
	}
}

func TestServeappChannelCompositionRefusesPolicyDrift(t *testing.T) {
	f := loadCatalogTierFixture(t)
	for _, tc := range []struct {
		name string
		edit func(*Policy)
	}{
		{"missing complement", func(p *Policy) { delete(p.Units, channelComplement) }},
		{"missing complement root", func(p *Policy) {
			u := p.Units[channelComplement]
			u.Run = strings.ReplaceAll(u.Run, "TestMockAgentSupportedSurfaceSQLitePostgres|", "")
			u.RequiredChildren = maps.Clone(u.RequiredChildren)
			delete(u.RequiredChildren, "TestMockAgentSupportedSurfaceSQLitePostgres")
			p.Units[channelComplement] = u
		}},
		{"extra complement root", func(p *Policy) {
			u := p.Units[channelComplement]
			u.Run = "(?:" + u.Run + ")|^" + channelReceiver + "$"
			p.Units[channelComplement] = u
		}},
		{"wrong parity transfer", func(p *Policy) {
			u := p.Units[channelParityUnit]
			u.Run = "^TestClaudeCLICompletion2270ProtocolFixture$"
			p.Units[channelParityUnit] = u
		}},
		{"new overlapping canary", func(p *Policy) {
			u := p.Units[serveappCanaryUnit]
			u.Run = "(?:" + u.Run + ")|^TestClaudeCLICompletion2270ProtocolFixture$"
			p.Units[serveappCanaryUnit] = u
		}},
		{"new nonoverlapping canary", func(p *Policy) {
			u := p.Units[serveappCanaryUnit]
			u.Run = "(?:" + u.Run + ")|^TestProviderAliasPhysicalTupleProofPreservesScopeNullsAndMultiplicity$"
			p.Units[serveappCanaryUnit] = u
		}},
		{"missing receiver backend", func(p *Policy) {
			u := p.Units[serveappCanaryUnit]
			u.RequiredChildren = maps.Clone(u.RequiredChildren)
			u.RequiredChildren[channelReceiver] = []string{"sqlite"}
			p.Units[serveappCanaryUnit] = u
		}},
		{"missing complement backend", func(p *Policy) {
			u := p.Units[channelComplement]
			u.RequiredChildren = maps.Clone(u.RequiredChildren)
			u.RequiredChildren["TestMockAgentSupportedSurfaceSQLitePostgres"] = []string{"sqlite"}
			p.Units[channelComplement] = u
		}},
		{"skip", func(p *Policy) {
			u := p.Units[channelComplement]
			u.Skip = "^TestMockAgentSupportedSurfaceSQLitePostgres$"
			p.Units[channelComplement] = u
		}},
		{"count", func(p *Policy) {
			u := p.Units[channelComplement]
			u.CountMode = "cache-default"
			p.Units[channelComplement] = u
		}},
		{"environment", func(p *Policy) {
			u := p.Units[channelComplement]
			u.EnvironmentIDs = map[string]string{VenueCI: "foreign", VenueLocal: "foreign"}
			p.Units[channelComplement] = u
		}},
		{"timeout", func(p *Policy) { u := p.Units[channelComplement]; u.GoTimeout = "20m"; p.Units[channelComplement] = u }},
		{"budget", func(p *Policy) {
			u := p.Units[channelComplement]
			u.BudgetClass = "broad"
			p.Units[channelComplement] = u
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := f.policy
			policy.Units = maps.Clone(policy.Units)
			tc.edit(&policy)
			plan, err := BuildPlan(policy, f.model, f.packages, ProfileCore, "hostile channel policy", "same-head", BuildOptions{ExtraUnits: []string{channelUnit}})
			if err == nil {
				err = BindExecution(&plan, f.inventory, f.proofs, policy)
			}
			if err == nil {
				t.Fatal("unreviewed ownership/recipe change admitted")
			}
			t.Log(err)
		})
	}
	for _, ids := range [][]string{{channelComplement}, {channelUnit, channelUnit}, {channelUnit, "foreign"}, {channelUnit, "serveapp-channel-neutral"}, {channelUnit, "serveapp-other-late"}} {
		t.Run(strings.Join(ids, "+"), func(t *testing.T) {
			plan, err := BuildPlan(f.policy, f.model, f.packages, ProfileCore, "invalid supplement", "same-head", BuildOptions{ExtraUnits: ids})
			if err == nil {
				err = BindExecution(&plan, f.inventory, f.proofs, f.policy)
			}
			if err == nil {
				t.Fatal("private/invalid/unrelated overlapping supplement admitted")
			}
		})
	}
}

func TestServeappChannelCompositionRefusesBoundRecipeDrift(t *testing.T) {
	f := loadCatalogTierFixture(t)
	full := f.bind(t, ProfileFull)
	original := f.bind(t, ProfileCore, channelUnit)
	for _, tc := range []struct {
		name string
		edit func(*ProofUnit)
	}{
		{"missing root", func(u *ProofUnit) { u.SelectedRoots = u.SelectedRoots[1:] }},
		{"missing required proof", func(u *ProofUnit) { u.RequiredTests = u.RequiredTests[1:] }},
		{"missing required child", func(u *ProofUnit) {
			u.RequiredTests = slices.Clone(u.RequiredTests)
			for i := range u.RequiredTests {
				if u.RequiredTests[i].Name == channelReceiver {
					u.RequiredTests[i].Children = []string{"sqlite"}
				}
			}
		}},
		{"missing parity child", func(u *ProofUnit) {
			u.RequiredTests = slices.Clone(u.RequiredTests)
			for i := range u.RequiredTests {
				if u.RequiredTests[i].Name == channelOnboarding {
					u.RequiredTests[i].Children = []string{"default_sqlite"}
				}
			}
		}},
		{"extra required child", func(u *ProofUnit) {
			u.RequiredChildren = maps.Clone(u.RequiredChildren)
			u.RequiredChildren[channelReceiver] = []string{"sqlite", "postgres", "foreign"}
		}},
		{"run", func(u *ProofUnit) { u.Run = "^TestClaudeCLICompletion2270ProtocolFixture$" }},
		{"skip", func(u *ProofUnit) { u.Skip = "^TestClaudeCLICompletion2270ProtocolFixture$" }},
		{"count", func(u *ProofUnit) { u.CountMode = "cache-default" }},
		{"environment", func(u *ProofUnit) { u.EnvironmentID = "foreign" }},
		{"timeout", func(u *ProofUnit) { u.GoTimeout = "20m" }},
		{"budget", func(u *ProofUnit) { u.BudgetClass = "broad" }},
		{"workload", func(u *ProofUnit) { u.WorkloadProfile = ProfileCore }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := original
			plan.Units = slices.Clone(original.Units)
			for i := range plan.Units {
				if plan.Units[i].ID == channelUnit {
					tc.edit(&plan.Units[i])
				}
			}
			if err := bindServeappChannelComposition(&plan, full, f.inventory, f.proofs, f.policy); err == nil {
				t.Fatal("bound channel evidence/recipe drift admitted")
			}
		})
	}
	proofs := slices.Clone(f.proofs)
	for i := range proofs {
		if proofs[i].Name == channelOnboarding {
			proofs[i].Children = []string{"default_sqlite"}
		}
	}
	plan, err := BuildPlan(f.policy, f.model, f.packages, ProfileCore, "missing parity child", "same-head", BuildOptions{ExtraUnits: []string{channelUnit}})
	if err == nil {
		err = BindExecution(&plan, f.inventory, proofs, f.policy)
	}
	if err == nil {
		t.Fatal("missing Telegram PostgreSQL parity obligation admitted")
	}
}
