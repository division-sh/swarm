package testplanning

import (
	"fmt"
	"sort"
)

// Retained ownership is derived from the same policy and active full census.
// It is not a second list of package names or a change-based selector.
func bindTierDeferrals(plan *RunPlan, full RunPlan, policy Policy) error {
	selected := map[TestRoot]bool{}
	for _, unit := range plan.Units {
		for _, root := range unit.SelectedRoots {
			selected[root] = true
		}
	}
	owners := map[TestRoot][]string{}
	for _, unit := range full.Units {
		for _, root := range unit.SelectedRoots {
			owners[root] = append(owners[root], unit.ID)
		}
	}
	plan.DeferredRoots = nil
	for root, fullOwners := range owners {
		if selected[root] {
			continue
		}
		minimum, err := minimumRootTier(policy, root)
		if err != nil {
			return err
		}
		if TierRank(minimum) <= TierRank(plan.Profile) {
			return fmt.Errorf("%s omits required %s root %s.%s", plan.Profile, minimum, root.Package, root.Name)
		}
		sort.Strings(fullOwners)
		plan.DeferredRoots = append(plan.DeferredRoots, TierDeferral{TestRoot: root, MinimumTier: minimum, FullOwners: fullOwners})
	}
	sort.Slice(plan.DeferredRoots, func(i, j int) bool {
		a, b := plan.DeferredRoots[i], plan.DeferredRoots[j]
		if a.Package != b.Package {
			return a.Package < b.Package
		}
		return a.Name < b.Name
	})
	return nil
}

func minimumRootTier(policy Policy, root TestRoot) (string, error) {
	for _, tier := range []string{ProfileCore, ProfileLifecycle, ProfileFull} {
		for _, id := range policy.Profiles[tier].Units {
			unit := policy.Units[id]
			for _, pkg := range unit.Packages {
				if pkg != root.Package {
					continue
				}
				matches, err := selectedByUnit(ProofUnit{Run: unit.Run, Skip: unit.Skip}, root.Name)
				if err != nil {
					return "", err
				}
				if matches {
					return tier, nil
				}
			}
		}
	}
	return "", fmt.Errorf("root %s.%s has no declared full owner", root.Package, root.Name)
}
