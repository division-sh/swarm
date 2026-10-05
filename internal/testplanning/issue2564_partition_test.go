package testplanning

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func loadIssue2564Policy(t *testing.T) Policy {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "..", ".github", "test-proof-plan.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	p, err := LoadPolicy(f)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func validateIssue2564WorkloadPartition(p Policy) error {
	for id, run := range map[string]string{
		"serveapp-2564-h1": "^TestIssue2564(H1EquivalentCorpus|ServedH1ReconstructedEquivalentBothStores)$",
		"serveapp-2564-h2": "^TestIssue2564(ReconstructedEquivalentH2BothStores|H2.*)$",
	} {
		u := p.Units[id]
		if !reflect.DeepEqual(u.Packages, []string{p.Module + "/internal/serveapp"}) || u.Run != run ||
			u.Skip != "" || u.GoTimeout != "" || u.CountMode != "count-1" ||
			u.EnvironmentID != "ci-postgres-gateway-empty-v1" || u.BudgetClass != "full" {
			return fmt.Errorf("%s changed complete workload envelope: %+v", id, u)
		}
		want := map[string][]string{"TestIssue2564ServedH1ReconstructedEquivalentBothStores": {"sqlite/overlap", "sqlite/no_overlap", "postgres/overlap", "postgres/no_overlap"}}
		if id == "serveapp-2564-h2" {
			want = map[string][]string{"TestIssue2564ReconstructedEquivalentH2BothStores": {"sqlite/dense", "sqlite/paced", "postgres/dense", "postgres/paced"}}
		}
		if !reflect.DeepEqual(u.RequiredChildren, want) {
			return fmt.Errorf("%s lost a required workload/backend: %+v", id, u.RequiredChildren)
		}
		for _, profile := range []string{ProfileLifecycle, ProfileFull} {
			members := 0
			for _, unit := range p.Profiles[profile].Units {
				if unit == id {
					members++
				}
			}
			if members != 1 {
				return fmt.Errorf("%s has %d mandatory %s owners", id, members, profile)
			}
		}
	}
	for _, profile := range []string{ProfileLifecycle, ProfileFull} {
		var runs []string
		for _, id := range p.Profiles[profile].Units {
			u := p.Units[id]
			for _, pkg := range u.Packages {
				if pkg == p.Module+"/internal/serveapp" {
					if u.Skip != "" {
						return fmt.Errorf("%s excludes served proof roots", id)
					}
					runs = append(runs, u.Run)
				}
			}
		}
		if err := ValidateGoProofPartition(filepath.Join("..", "serveapp"), runs); err != nil {
			return fmt.Errorf("%s: %w", profile, err)
		}
	}
	return nil
}

func TestIssue2564WorkloadsRemainRequiredCompleteRoots(t *testing.T) {
	if err := validateIssue2564WorkloadPartition(loadIssue2564Policy(t)); err != nil {
		t.Fatal(err)
	}
}

func TestIssue2564WorkloadPartitionRejectsLostProof(t *testing.T) {
	for _, id := range []string{"serveapp-2564-h1", "serveapp-2564-h2"} {
		for _, change := range []struct {
			name string
			edit func(*UnitPolicy)
		}{
			{"omitted", func(u *UnitPolicy) { u.Run = "^$" }},
			{"overlap", func(u *UnitPolicy) { u.Run = "^TestIssue2564.*$" }},
			{"partial_backend", func(u *UnitPolicy) { u.Run += "/sqlite" }},
			{"skipped_backend", func(u *UnitPolicy) { u.Skip = "postgres" }},
			{"cached", func(u *UnitPolicy) { u.CountMode = "cache-default" }},
			{"deadline_increase", func(u *UnitPolicy) { u.GoTimeout = "20m" }},
			{"missing_children", func(u *UnitPolicy) { u.RequiredChildren = nil }},
		} {
			t.Run(id+"/"+change.name, func(t *testing.T) {
				p := loadIssue2564Policy(t)
				u := p.Units[id]
				change.edit(&u)
				p.Units[id] = u
				if validateIssue2564WorkloadPartition(p) == nil {
					t.Fatal("lost or weakened workload proof accepted")
				}
			})
		}
		for _, profile := range []string{ProfileLifecycle, ProfileFull} {
			t.Run(id+"/missing_"+profile, func(t *testing.T) {
				p := loadIssue2564Policy(t)
				row := p.Profiles[profile]
				row.Units = nil
				for _, member := range loadIssue2564Policy(t).Profiles[profile].Units {
					if member != id {
						row.Units = append(row.Units, member)
					}
				}
				p.Profiles[profile] = row
				if validateIssue2564WorkloadPartition(p) == nil {
					t.Fatal("optional workload proof accepted")
				}
			})
		}
	}
}
