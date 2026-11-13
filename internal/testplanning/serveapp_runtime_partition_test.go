package testplanning

import (
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"testing"
)

const originalServeappRuntimeRun = `^TestR($|[^e].*|e($|[^c].*|c($|[^e].*|e($|[^i].*|i($|[^v].*|v($|[^e].*|e($|[^r].*)))))))$`
const sessionRecoveryRun = `^TestRunServe(SessionRecovery(Learned|Registered)BothStores|TerminalAccountDoesNotStarveRecoveryBothStores)$`

var sessionRecoveryRoots = []string{
	"TestRunServeSessionRecoveryLearnedBothStores",
	"TestRunServeSessionRecoveryRegisteredBothStores",
	"TestRunServeTerminalAccountDoesNotStarveRecoveryBothStores",
}

func TestServeappRuntimeSplitPreservesBoundPlanRootUnion(t *testing.T) {
	f := loadCatalogTierFixture(t)
	old := regexp.MustCompile(originalServeappRuntimeRun)
	ids := []string{"serveapp-runtime", "serveapp-session-recovery"}
	const pkg = "github.com/division-sh/swarm/internal/serveapp"
	for _, id := range ids {
		u := f.policy.Units[id]
		if !slices.Equal(u.Packages, []string{pkg}) || u.CountMode != "count-1" || u.BudgetClass != "full" || u.GoTimeout != "" || u.Skip != "" || !reflect.DeepEqual(u.EnvironmentIDs, managedEnvironmentIDs()) {
			t.Fatalf("%s changed its whole-root/count/environment/default-timeout envelope: %+v", id, u)
		}
		assertServeappLateProfileMembership(t, f.policy, id)
	}
	if f.policy.Units[ids[1]].Run != sessionRecoveryRun {
		t.Fatal("recovery owner is not the three exact roots")
	}
	for _, profile := range []string{ProfileLifecycle, ProfileFull} {
		plan := f.bind(t, profile)
		owners := map[string]int{}
		for _, id := range ids {
			u, err := plan.Unit(id)
			if err != nil {
				t.Fatal(err)
			}
			for _, root := range u.SelectedRoots {
				if root.Package != pkg {
					t.Fatal("split acquired a foreign package", root)
				}
				owners[root.Name]++
			}
			if id == ids[1] && len(u.SelectedRoots) != len(sessionRecoveryRoots) {
				t.Fatal("recovery owner lost or added a root", u.SelectedRoots)
			}
			t.Logf("%s %s digest=%s roots=%d", profile, id, plan.Digest, len(u.SelectedRoots))
		}
		for _, name := range f.inventory.Packages[pkg].Roots {
			want := 0
			if old.MatchString(name) {
				want = 1
			}
			if owners[name] != want {
				t.Errorf("%s %s owners=%d want=%d", profile, name, owners[name], want)
			}
		}
	}
}

func TestServeappRuntimeSplitRefusesOmissionOverlapAndNeighborLoss(t *testing.T) {
	policy := loadPersistenceDebtPolicy(t)
	old := regexp.MustCompile(originalServeappRuntimeRun)
	ordinary := regexp.MustCompile(policy.Units["serveapp-runtime"].Run)
	recovery := regexp.MustCompile(policy.Units["serveapp-session-recovery"].Run)
	for _, name := range []string{"TestR", "TestReceiver", "TestReceiverFuture", "TestRunServeFuture"} {
		if ordinary.MatchString(name) != old.MatchString(name) || recovery.MatchString(name) {
			t.Fatal("original namespace boundary changed", name)
		}
	}
	for _, root := range sessionRecoveryRoots {
		if ordinary.MatchString(root) || !recovery.MatchString(root) {
			t.Fatal("exact recovery root has the wrong owner", root)
		}
		for i := len("TestR"); i < len(root); i++ {
			for _, name := range []string{root[:i], root[:i] + "X" + root[i+1:], root[:i] + "_" + root[i:]} {
				if ordinary.MatchString(name) != old.MatchString(name) || recovery.MatchString(name) {
					t.Fatal("future prefix/neighbor lost its original owner", name)
				}
			}
		}
		for _, name := range []string{root + "Future", root + "_2"} {
			if !ordinary.MatchString(name) || recovery.MatchString(name) {
				t.Fatal("future sibling was silently moved", name)
			}
		}
	}
	for _, runs := range [][]string{
		{ordinary.String()},
		{originalServeappRuntimeRun, recovery.String()},
		{ordinary.String(), "^" + sessionRecoveryRoots[0] + "$"},
		{ordinary.String(), "^Test.*$"},
	} {
		matchers := []func(string) bool{func(name string) bool { return !old.MatchString(name) }}
		for _, run := range runs {
			matchers = append(matchers, regexp.MustCompile(run).MatchString)
		}
		if validateGoProofMatchers(filepath.Join("..", "..", "internal", "serveapp"), matchers) == nil {
			t.Fatal("omission, overlap, dropped root or broadened owner accepted", runs)
		}
	}
}
