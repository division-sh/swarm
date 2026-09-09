package testcatalog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testplanning"
	"gopkg.in/yaml.v3"
)

func TestCatalogExternalProofPartitionsThroughInventory(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*testing.T, string, *testplanning.Policy)
		want   string
	}{
		{"complete", nil, ""},
		{"new test omission", func(t *testing.T, root string, _ *testplanning.Policy) {
			writeCatalogTestFile(t, filepath.Join(root, "internal/executor/new_test.go"), "package executor\nfunc TestNew(t any) {}\n")
		}, "TestNew matches 0"},
		{"omission", func(_ *testing.T, _ string, p *testplanning.Policy) {
			u := p.Units["second"]
			u.Run = "^TestAbsent$"
			p.Units["second"] = u
		}, "TestSecond matches 0"},
		{"overlap", func(_ *testing.T, _ string, p *testplanning.Policy) {
			u := p.Units["second"]
			u.Run = "^Test.*$"
			p.Units["second"] = u
		}, "matches 2"},
		{"dead unit", func(_ *testing.T, _ string, p *testplanning.Policy) {
			u := p.Units["second"]
			u.Run = "^TestAbsent$"
			p.Units["dead"] = u
			for id, profile := range p.Profiles {
				profile.Units = append(profile.Units, "dead")
				p.Profiles[id] = profile
			}
		}, "matches no proof"},
		{"partial backend selection", func(_ *testing.T, _ string, p *testplanning.Policy) {
			u := p.Units["second"]
			u.Run = "^TestSecond$/sqlite"
			p.Units["second"] = u
		}, "partial-subtest"},
		{"cached unit", func(_ *testing.T, _ string, p *testplanning.Policy) {
			u := p.Units["second"]
			u.CountMode = "cache-default"
			p.Units["second"] = u
		}, "requires count-1"},
		{"profile missing unit", func(_ *testing.T, _ string, p *testplanning.Policy) {
			profile := p.Profiles[testplanning.ProfileNightly]
			profile.Units = []string{"external-proof"}
			p.Profiles[testplanning.ProfileNightly] = profile
		}, "TestSecond matches 0"},
		{"profile equivalent but different units", func(_ *testing.T, _ string, p *testplanning.Policy) {
			p.Units["other"] = p.Units["second"]
			profile := p.Profiles[testplanning.ProfileNightly]
			profile.Units = []string{"external-proof", "other"}
			p.Profiles[testplanning.ProfileNightly] = profile
		}, "changes CI owners"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := writeExternalProofInventory(t, externalProofSpec("examples/external", "github.com/division-sh/swarm/internal/executor", []string{"claim.external"}), "")
			writeCatalogTestFile(t, filepath.Join(root, "internal/executor/second_test.go"), "package executor\nfunc TestSecond(t any) {}\n")
			policy, err := testplanning.LoadPolicy(strings.NewReader(externalProofPolicy("github.com/division-sh/swarm/internal/executor")))
			if err != nil {
				t.Fatal(err)
			}
			u := policy.Units["external-proof"]
			u.Run = "^TestProof$"
			policy.Units["external-proof"] = u
			u.Run = "^TestSecond$"
			policy.Units["second"] = u
			for id, profile := range policy.Profiles {
				profile.Units = append(profile.Units, "second")
				policy.Profiles[id] = profile
			}
			if tt.mutate != nil {
				tt.mutate(t, root, &policy)
			}
			raw, err := yaml.Marshal(policy)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, ".github/test-proof-plan.yaml"), raw, 0o600); err != nil {
				t.Fatal(err)
			}
			_, err = Load(root)
			if tt.want == "" && err != nil || tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)) {
				t.Fatalf("Load = %v, want %q", err, tt.want)
			}
		})
	}
}
