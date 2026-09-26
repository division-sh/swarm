package manager

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestRecoverRejectsMalformedCanonicalRecipientEvidence(t *testing.T) {
	for _, carrier := range []string{"static_route_events", "dynamic_topology_proofs", "recipient_plan_events"} {
		for _, tc := range []struct {
			name   string
			mutate func(map[string]any)
			want   string
		}{
			{"missing plan", func(e map[string]any) { delete(e, "agent_plan") }, "requires agent_plan"},
			{"wrong recipient", func(e map[string]any) { e["subscriber_id"] = "agent-other" }, "does not match its recipient"},
			{"missing handler", func(e map[string]any) { delete(e, "handler_event") }, "exact handler event"},
			{"missing authority", func(e map[string]any) { delete(e, "authority") }, "authority"},
			{"unknown recipient", func(e map[string]any) { e["subscriber_type"] = "subscription" }, "kind"},
			{"subscription evidence", func(e map[string]any) { e["subscription_recipients"] = []string{"agent-a"} }, "unknown field"},
			{"live run instead of plan", func(e map[string]any) {
				e["agent_plan"].(map[string]any)["run_id"] = "00000000-0000-0000-0000-000000000501"
			}, "unknown field"},
			{"incomplete connect", func(e map[string]any) { e["authority"] = map[string]any{"kind": "connect"} }, "connect plan"},
		} {
			t.Run(carrier+"/"+tc.name, func(t *testing.T) {
				record := selectedContractRouteRecoveryRecord(t, "00000000-0000-0000-0000-000000000604")
				mutateSelectedContractRecoveryRecipient(t, &record, carrier, tc.mutate)
				bus := &recoveryTestBus{selectedRouteRecoveries: []SelectedContractRouteRecoveryRecord{record}}
				am := newTestAgentManager(t, bus, nil, &recoveryTestStore{})
				err := am.restoreSelectedContractRouteRecoveries(context.Background())
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("restore error = %v, want %q after valid record fingerprint", err, tc.want)
				}
				if len(am.SelectedContractRouteRecoverySnapshot()) != 0 || len(bus.restored) != 0 {
					t.Fatal("invalid evidence installed recovery truth or live routes")
				}
			})
		}
	}
}

func TestRecoverCanonicalRecipientReadbackPreservesSelectedEvidence(t *testing.T) {
	for _, carrier := range []string{"static_route_events", "dynamic_topology_proofs", "recipient_plan_events"} {
		t.Run(carrier, func(t *testing.T) {
			record := selectedContractRouteRecoveryRecord(t, "00000000-0000-0000-0000-000000000605")
			mutateSelectedContractRecoveryRecipient(t, &record, carrier, func(e map[string]any) {
				e["path"] = "review/inst-other"
				e["route_source"] = "diagnostic-only:changed"
				e["handler_event"] = "work.changed"
				e["agent_plan"].(map[string]any)["name"].(map[string]any)["owner"] = "test://recovery/other-owner"
			})
			bus := &recoveryTestBus{selectedRouteRecoveries: []SelectedContractRouteRecoveryRecord{record}}
			am := newTestAgentManager(t, bus, nil, &recoveryTestStore{})
			if err := am.restoreSelectedContractRouteRecoveries(context.Background()); err != nil {
				t.Fatal(err)
			}
			truth := am.SelectedContractRouteRecoverySnapshot()[record.ForkRunID]
			got := truth.RecipientPlanning.RecipientPlanEvents[0].Recipients[0]
			switch carrier {
			case "static_route_events":
				got = truth.RouteTopology.StaticRouteEvents[0].DerivedRecipients[0]
			case "dynamic_topology_proofs":
				got = truth.RouteTopology.DynamicTopologyProofs[0].DerivedRecipients[0]
			}
			if got.Path != "review/inst-other" || got.RouteSourceCode() != "diagnostic-only:changed" ||
				got.HandlerEvent() != "work.changed" || got.AgentPlan.Name.Owner != "test://recovery/other-owner" {
				t.Fatalf("recovery reduced selected evidence: %#v", got)
			}
			if len(bus.restored) != 0 {
				t.Fatal("evidence readback installed live routes")
			}
		})
	}
}

func TestRecoverDeploymentRevisionRouteHasNoSyntheticEvent(t *testing.T) {
	record := selectedContractRouteRecoveryRecord(t, "00000000-0000-0000-0000-000000000606")
	record.ForkPoint = runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 4}
	record.ForkEventID = ""
	bus := &recoveryTestBus{selectedRouteRecoveries: []SelectedContractRouteRecoveryRecord{record}}
	manager := newTestAgentManager(t, bus, nil, &recoveryTestStore{})
	if err := manager.restoreSelectedContractRouteRecoveries(context.Background()); err != nil {
		t.Fatalf("eventless recovery rejected: %v", err)
	}
	truth := manager.SelectedContractRouteRecoverySnapshot()[record.ForkRunID]
	if truth.Record.ForkPoint != record.ForkPoint || truth.Record.ForkEventID != "" {
		t.Fatalf("recovery invented or changed fork event: %+v", truth.Record)
	}
	record.ForkEventID = "00000000-0000-0000-0000-000000000701"
	if err := validateSelectedContractRouteRecoveryRecord(record); err == nil {
		t.Fatal("deployment revision accepted event-shaped route identity")
	}
}

func mutateSelectedContractRecoveryRecipient(t *testing.T, record *SelectedContractRouteRecoveryRecord, carrier string, mutate func(map[string]any)) {
	t.Helper()
	raw, fingerprint := &record.RouteTopology, &record.RouteTopologyFingerprint
	field := "derived_recipients"
	if carrier == "recipient_plan_events" {
		raw, fingerprint = &record.RecipientPlanning, &record.RecipientPlanningFingerprint
		field = "recipients"
	}
	var body map[string]any
	if err := json.Unmarshal(*raw, &body); err != nil {
		t.Fatal(err)
	}
	item := body[carrier].([]any)[0].(map[string]any)
	mutate(item[field].([]any)[0].(map[string]any))
	*raw = mustRecoveryJSON(t, body)
	*fingerprint = recoveryJSONFingerprint(*raw)
}
