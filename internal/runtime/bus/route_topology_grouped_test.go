package bus

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

// This is the pre-grouping per-owner scan, retained as an independent oracle.
func materializedRoutesByScan(rt *RouteTable, identity runtimeflowidentity.RunScopedFlowInstance) []FlowInstanceRouteRecord {
	identity, err := normalizeFlowInstanceRouteIdentity(identity)
	if err != nil {
		return nil
	}
	owner, exists, err := rt.matchFlowInstanceRouteOwnerLocked(identity)
	if err != nil || !exists || !flowInstanceRouteIdentityEqual(owner, identity) {
		return nil
	}
	type routeKey struct {
		instancePath, eventPattern string
		recipient                  events.DeliveryRecipient
	}
	seen := make(map[routeKey]struct{})
	out := make([]FlowInstanceRouteRecord, 0, 8)
	for _, pattern := range rt.patterns {
		if pattern.RunID != identity.RunID || strings.Trim(strings.TrimSpace(pattern.InstancePath), "/") != identity.Route.InstancePath {
			continue
		}
		record := FlowInstanceRouteRecord{
			Identity: identity, EventPattern: strings.TrimSpace(pattern.EventPattern),
			SubscriberType: pattern.Subscriber.Recipient.Code(),
			SubscriberID:   pattern.Subscriber.Recipient.ID(),
			SourceFlow:     identity.Route.ScopeKey,
		}
		key := routeKey{record.Identity.Route.InstancePath, record.EventPattern, pattern.Subscriber.Recipient}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventPattern != out[j].EventPattern {
			return out[i].EventPattern < out[j].EventPattern
		}
		if out[i].SubscriberType != out[j].SubscriberType {
			return out[i].SubscriberType < out[j].SubscriberType
		}
		return out[i].SubscriberID < out[j].SubscriberID
	})
	return out
}

func TestGroupedMaterializedRouteSetsMatchPerOwnerScan(t *testing.T) {
	rt := newRouteTable(nil)
	firstRun := busInternalTestRunID
	secondRun := "00000000-0000-4000-8000-000000000002"
	var identities []runtimeflowidentity.RunScopedFlowInstance
	for _, runID := range []string{firstRun, secondRun} {
		for _, instanceID := range []string{"one", "two"} {
			identity, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, runtimeflowidentity.DeriveRoute("workers", instanceID))
			if err != nil {
				t.Fatal(err)
			}
			rt.instanceOwners[identity] = identity
			identities = append(identities, identity)
			for _, recipientID := range []string{"alpha", "beta", "alpha"} {
				rt.patterns = append(rt.patterns, routePattern{
					RunID: runID, InstancePath: identity.Route.InstancePath,
					SourceInstancePath: "sources/source-one",
					EventPattern:       identity.Route.InstancePath + "/work.ready",
					Subscriber:         Subscriber{Recipient: events.MustAgentDeliveryRecipient(recipientID)},
				})
			}
		}
	}
	identities = append(identities, identities[0], runtimeflowidentity.RunScopedFlowInstance{})
	got := rt.materializedRouteRecordSets(identities)
	if len(got) != len(identities) {
		t.Fatalf("record set count = %d, want %d", len(got), len(identities))
	}
	for i, identity := range identities {
		want := materializedRoutesByScan(rt, identity)
		if got[i].Identity != identity || !reflect.DeepEqual(got[i].Routes, want) {
			t.Fatalf("owner %d grouped routes = %#v, want %#v", i, got[i], want)
		}
	}
}
