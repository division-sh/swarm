package runfork

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
)

// EqualSelectedContractRecipientPlanning compares canonical recipient sets and exact model metadata.
func EqualSelectedContractRecipientPlanning(left, right RunForkSelectedContractRecipientPlanning) (bool, error) {
	var keys [2][][]forkrecipient.Key
	for side, planning := range []RunForkSelectedContractRecipientPlanning{left, right} {
		for index, event := range planning.RecipientPlanEvents {
			group, err := selectedContractModelRecipientKeys(event.Recipients)
			if err != nil {
				return false, fmt.Errorf("recipient planning side %d event %d: %w", side, index, err)
			}
			keys[side] = append(keys[side], group)
		}
	}
	if !equalSelectedContractModelRecipientKeyGroups(keys[0], keys[1]) {
		return false, nil
	}
	left.RecipientPlanEvents = slices.Clone(left.RecipientPlanEvents)
	right.RecipientPlanEvents = slices.Clone(right.RecipientPlanEvents)
	for _, planning := range []*RunForkSelectedContractRecipientPlanning{&left, &right} {
		for index := range planning.RecipientPlanEvents {
			planning.RecipientPlanEvents[index].Recipients = nil
		}
	}
	return reflect.DeepEqual(left, right), nil
}

// EqualSelectedContractRouteTopology compares static and dynamic recipient sets and exact model metadata.
func EqualSelectedContractRouteTopology(left, right RunForkSelectedContractRouteTopology) (bool, error) {
	var keys [2][][]forkrecipient.Key
	for side, topology := range []RunForkSelectedContractRouteTopology{left, right} {
		for index, event := range topology.StaticRouteEvents {
			group, err := selectedContractModelRecipientKeys(event.DerivedRecipients)
			if err != nil {
				return false, fmt.Errorf("route topology side %d static event %d: %w", side, index, err)
			}
			keys[side] = append(keys[side], group)
		}
		for index, proof := range topology.DynamicTopologyProofs {
			group, err := selectedContractModelRecipientKeys(proof.DerivedRecipients)
			if err != nil {
				return false, fmt.Errorf("route topology side %d dynamic proof %d: %w", side, index, err)
			}
			keys[side] = append(keys[side], group)
		}
	}
	if !equalSelectedContractModelRecipientKeyGroups(keys[0], keys[1]) {
		return false, nil
	}
	left.StaticRouteEvents = slices.Clone(left.StaticRouteEvents)
	right.StaticRouteEvents = slices.Clone(right.StaticRouteEvents)
	left.DynamicTopologyProofs = slices.Clone(left.DynamicTopologyProofs)
	right.DynamicTopologyProofs = slices.Clone(right.DynamicTopologyProofs)
	for _, topology := range []*RunForkSelectedContractRouteTopology{&left, &right} {
		for index := range topology.StaticRouteEvents {
			topology.StaticRouteEvents[index].DerivedRecipients = nil
		}
		for index := range topology.DynamicTopologyProofs {
			topology.DynamicTopologyProofs[index].DerivedRecipients = nil
		}
	}
	return reflect.DeepEqual(left, right), nil
}

// EqualSelectedContractFrontierEvents compares canonical recipient sets and exact event metadata.
func EqualSelectedContractFrontierEvents(left, right []RunForkSelectedContractFrontierEvent) (bool, error) {
	var keys [2][][]forkrecipient.Key
	for side, events := range [][]RunForkSelectedContractFrontierEvent{left, right} {
		for index, event := range events {
			group, err := selectedContractModelRecipientKeys(event.DerivedRecipients)
			if err != nil {
				return false, fmt.Errorf("frontier side %d event %d: %w", side, index, err)
			}
			keys[side] = append(keys[side], group)
		}
	}
	if !equalSelectedContractModelRecipientKeyGroups(keys[0], keys[1]) {
		return false, nil
	}
	left, right = slices.Clone(left), slices.Clone(right)
	for _, events := range [][]RunForkSelectedContractFrontierEvent{left, right} {
		for index := range events {
			events[index].DerivedRecipients = nil
		}
	}
	return reflect.DeepEqual(left, right), nil
}

// Only validated canonical recipient keys participate in evidence equality.
// Container metadata remains exact, including historical routes and dispositions.
func selectedContractModelRecipientKeys(evidence []forkrecipient.Evidence) ([]forkrecipient.Key, error) {
	canonical, err := forkrecipient.CanonicalSet(evidence)
	if err != nil {
		return nil, err
	}
	keys := make([]forkrecipient.Key, 0, len(canonical))
	for _, recipient := range canonical {
		key, err := recipient.Key()
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func equalSelectedContractModelRecipientKeyGroups(left, right [][]forkrecipient.Key) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !slices.Equal(left[index], right[index]) {
			return false
		}
	}
	return true
}
