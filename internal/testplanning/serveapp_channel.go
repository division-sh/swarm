package testplanning

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
)

const (
	channelUnit        = "serveapp-channel"
	channelParityUnit  = "parity-connected-channel-onboarding-surface"
	serveappCanaryUnit = "local-serveapp-canaries"
	channelComplement  = "local-serveapp-canaries-without-channel"
	channelOnboarding  = "TestChannelConnectTelegramFirstUserJourney"
	channelReceiver    = "TestReceiverCompositionRestartBothStores"
)

func composeServeappChannel(profile, venue string, extras []string) bool {
	return profile == ProfileCore && venue == VenueCI && slices.Contains(extras, channelUnit)
}

// Only these two core owners have reviewed replacements in the complete channel
// workload. Other canary/supplement overlaps still fail the active-root census.
func (p Policy) composeServeappChannelCanaries(ids []string) ([]string, error) {
	if !slices.Contains(ids, channelParityUnit) || !slices.Contains(ids, serveappCanaryUnit) {
		return nil, fmt.Errorf("channel composition requires its original core owners")
	}
	if _, ok := p.Units[channelComplement]; !ok {
		return nil, fmt.Errorf("channel composition requires declared unit %s", channelComplement)
	}
	composed := make([]string, 0, len(ids)-1)
	for _, id := range ids {
		switch id {
		case channelParityUnit:
			continue
		case serveappCanaryUnit:
			id = channelComplement
		}
		composed = append(composed, id)
	}
	return composed, nil
}

func bindServeappChannelComposition(plan *RunPlan, full RunPlan, inventory RootInventory, parity []ParityProof, policy Policy) error {
	if !composeServeappChannel(plan.Profile, plan.Venue, plan.ExtraUnits) {
		return nil
	}
	// Reuse binding to retain every base obligation, including other explicitly
	// requested supplements and the existing finite catalog compositions.
	extras := slices.DeleteFunc(slices.Clone(plan.ExtraUnits), func(id string) bool { return id == channelUnit })
	model := WeightModel{Version: WeightModelVersion, SourceRunID: "channel-composition-census"}
	packages := make([]string, 0, len(inventory.Packages))
	for pkg := range inventory.Packages {
		packages = append(packages, pkg)
	}
	base, err := BuildPlan(policy, model, packages, ProfileCore, "channel composition base", plan.HeadSHA, BuildOptions{Venue: VenueCI, ExtraUnits: extras})
	if err == nil {
		err = BindExecution(&base, inventory, parity, policy)
	}
	if err != nil {
		return fmt.Errorf("channel composition base: %w", err)
	}
	channel, err := full.Unit(channelUnit)
	if err != nil {
		return err
	}
	if err := validateChannelTransfers(base, channel); err != nil {
		return err
	}
	expectedRequired, expectedDeferred := channelProofUnion(append(slices.Clone(base.Units), channel))
	actualRequired, actualDeferred := channelProofUnion(plan.Units)
	if !reflect.DeepEqual(expectedRequired, actualRequired) || !reflect.DeepEqual(expectedDeferred, actualDeferred) {
		return fmt.Errorf("channel composition changed the core/channel proof obligation union")
	}
	wanted := make(map[string]ProofUnit, len(base.Units))
	for _, unit := range base.Units {
		// The reference has no placement weights, so compare broad obligations
		// above as a union rather than imposing its synthetic shard layout.
		if strings.HasPrefix(unit.ID, "broad-") {
			continue
		}
		switch unit.ID {
		case channelParityUnit:
			continue
		case serveappCanaryUnit:
			unit.ID = channelComplement
			unit.Run = policy.Units[channelComplement].Run
			unit.SelectedRoots = slices.DeleteFunc(slices.Clone(unit.SelectedRoots), func(root TestRoot) bool { return root.Name == channelReceiver })
			unit.RequiredTests = slices.DeleteFunc(slices.Clone(unit.RequiredTests), func(root RequiredTest) bool { return root.Name == channelReceiver })
			unit.RequiredChildren = maps.Clone(unit.RequiredChildren)
			delete(unit.RequiredChildren, channelReceiver)
		}
		wanted[unit.ID] = unit
	}
	wanted[channelUnit] = channel
	var compared int
	for _, actual := range plan.Units {
		if strings.HasPrefix(actual.ID, "broad-") {
			continue
		}
		expected, ok := wanted[actual.ID]
		// The reference census intentionally has no timing model. Actual weights
		// remain selected by BuildPlan, using the complement's own recipe identity.
		expected.WeightSeconds, actual.WeightSeconds = 0, 0
		if !ok || !reflect.DeepEqual(expected, actual) {
			return fmt.Errorf("channel composition changed roots, children or recipe of %s", actual.ID)
		}
		compared++
	}
	if compared != len(wanted) {
		return fmt.Errorf("channel composition changed unrelated unit ownership")
	}
	return nil
}

func channelProofUnion(units []ProofUnit) (map[TestRoot]RequiredTest, map[TestRoot]DeferredTest) {
	required := map[TestRoot]RequiredTest{}
	deferred := map[TestRoot]DeferredTest{}
	for _, unit := range units {
		for _, proof := range unit.RequiredTests {
			children := append(slices.Clone(required[proof.TestRoot].Children), proof.Children...)
			slices.Sort(children)
			proof.Children = slices.Compact(children)
			required[proof.TestRoot] = proof
		}
		for _, proof := range unit.DeferredTests {
			deferred[proof.TestRoot] = proof
		}
	}
	return required, deferred
}

func validateChannelTransfers(base RunPlan, channel ProofUnit) error {
	approved := map[string]struct {
		owner    string
		children []string
	}{
		channelOnboarding: {channelParityUnit, []string{"default_sqlite", "explicit_postgres"}},
		channelReceiver:   {serveappCanaryUnit, []string{"postgres", "sqlite"}},
	}
	channelRoots := make(map[TestRoot]RequiredTest, len(channel.RequiredTests))
	for _, required := range channel.RequiredTests {
		channelRoots[required.TestRoot] = required
	}
	transferred := map[string]bool{}
	for _, unit := range base.Units {
		for _, root := range unit.SelectedRoots {
			required, overlaps := channelRoots[root]
			if !overlaps {
				continue
			}
			transfer, ok := approved[root.Name]
			if !ok || root.Package != "github.com/division-sh/swarm/internal/serveapp" || unit.ID != transfer.owner || transferred[root.Name] {
				return fmt.Errorf("channel composition has unapproved overlap %s.%s in %s", root.Package, root.Name, unit.ID)
			}
			var original RequiredTest
			for _, candidate := range unit.RequiredTests {
				if candidate.TestRoot == root {
					original = candidate
				}
			}
			if !slices.Equal(original.Children, transfer.children) || !slices.Equal(required.Children, transfer.children) {
				return fmt.Errorf("channel composition loses required backend children for %s", root.Name)
			}
			transferred[root.Name] = true
		}
	}
	if len(transferred) != len(approved) {
		return fmt.Errorf("channel composition does not transfer exactly its two approved core proofs")
	}
	return nil
}
