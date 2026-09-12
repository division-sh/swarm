package runtimepersistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

type recipientAuthorityRecoveryStore interface {
	selectedRouteRecoveryStore
	runtimemanager.ManagerPersistence
	agentfixture.Store
	ListSelectedContractRouteRecoveryRecords(context.Context) ([]runtimemanager.SelectedContractRouteRecoveryRecord, error)
}

type recipientAuthorityRecoveryEventStore struct {
	runtimebus.InMemoryEventStore
	selected recipientAuthorityRecoveryStore
}

func (s *recipientAuthorityRecoveryEventStore) ListSelectedContractRouteRecoveryRecords(ctx context.Context) ([]runtimemanager.SelectedContractRouteRecoveryRecord, error) {
	return s.selected.ListSelectedContractRouteRecoveryRecords(ctx)
}

func TestRunForkRecipientAuthorityRecoveryRoundTripBothStores(t *testing.T) {
	want := recipientAuthorityRecoveryEvidence(t)
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(recipientAuthorityRecoveryStore)
			ctx := managedExecutionStoreTestContext(t, testAuthorActivityContext())
			req := recipientAuthorityRecoveryRequest(t, ctx, selected, want)
			recorded, err := selected.RecordRunForkSelectedContractRouteRecovery(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			loaded := requireRecipientAuthorityRecoveryLoad(t, ctx, selected, req.ForkRunID)
			if loaded.RouteTopologyFingerprint != recorded.RouteTopologyFingerprint ||
				loaded.RecipientPlanningFingerprint != recorded.RecipientPlanningFingerprint ||
				loaded.StaticRouteEventCount != 1 || loaded.RecipientPlanEventCount != 1 {
				t.Fatalf("record/load lost fingerprints or counts: %#v", loaded)
			}
			manager := recipientAuthorityRecoveryManager(t, ctx, selected)
			if err := manager.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			truth, ok := manager.SelectedContractRouteRecoverySnapshot()[req.ForkRunID]
			if !ok || truth.Record.SourceRunID != req.SourceRunID || truth.Record.ForkEventID != req.ForkEventID {
				t.Fatalf("manager lost record ownership: %#v", truth.Record)
			}
			if len(truth.RouteTopology.StaticRouteEvents) != 1 || len(truth.RecipientPlanning.RecipientPlanEvents) != 1 {
				t.Fatal("manager lost event carriers")
			}
			for name, got := range map[string][]forkrecipient.Evidence{
				"topology": truth.RouteTopology.StaticRouteEvents[0].DerivedRecipients,
				"planning": truth.RecipientPlanning.RecipientPlanEvents[0].Recipients,
			} {
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s lost full ordered node/handler/connect/agent evidence:\ngot=%#v\nwant=%#v", name, got, want)
				}
				requireRecipientAuthorityRecoveryLaws(t, req, got)
			}

			// Record hashes protect serialized diagnostics too; semantic identity does not.
			relabelled := make([]forkrecipient.Evidence, len(want))
			for i, evidence := range want {
				relabelled[i] = recipientAuthorityRelabel(t, evidence, "untrusted-label:not-a-route")
			}
			req.RouteTopology.StaticRouteEvents[0].DerivedRecipients = relabelled
			req.RecipientPlanning.RecipientPlanEvents[0].Recipients = relabelled
			if _, err := selected.RecordRunForkSelectedContractRouteRecovery(ctx, req); err != nil {
				t.Fatal(err)
			}
			changed := requireRecipientAuthorityRecoveryLoad(t, ctx, selected, req.ForkRunID)
			if changed.RouteTopologyFingerprint == loaded.RouteTopologyFingerprint || changed.RecipientPlanningFingerprint == loaded.RecipientPlanningFingerprint {
				t.Fatal("record byte-integrity fingerprint erased changed diagnostics")
			}
			if err := manager.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			after := manager.SelectedContractRouteRecoverySnapshot()[req.ForkRunID].RecipientPlanning.RecipientPlanEvents[0].Recipients
			if !reflect.DeepEqual(after, relabelled) {
				t.Fatal("manager did not reload the exact relabelled evidence")
			}
			for i := range want {
				beforeHash, err := want[i].Fingerprint()
				if err != nil {
					t.Fatal(err)
				}
				afterHash, err := after[i].Fingerprint()
				if err != nil || beforeHash != afterHash {
					t.Fatalf("recipient %d semantic fingerprint changed with label: %v", i, err)
				}
				if equal, err := forkrecipient.Equal(want[i], after[i]); err != nil || !equal {
					t.Fatalf("recipient %d semantic identity changed with label: %v", i, err)
				}
			}
		})
	}
}

func TestRunForkRecipientAuthorityRecoveryRejectsReducedWireBothStores(t *testing.T) {
	evidence := recipientAuthorityRecoveryEvidence(t)
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(recipientAuthorityRecoveryStore)
			ctx := managedExecutionStoreTestContext(t, testAuthorActivityContext())
			req := recipientAuthorityRecoveryRequest(t, ctx, selected, evidence)
			for _, carrier := range []string{"route_topology", "recipient_planning"} {
				for _, tc := range []struct {
					name   string
					kind   string
					mutate func(map[string]any)
					want   string
				}{
					{"old node DTO", "node", func(e map[string]any) { delete(e, "handler_node"); delete(e, "handler_event"); delete(e, "authority") }, "requires handler_node"},
					{"old agent DTO", "agent", func(e map[string]any) { delete(e, "agent_plan"); delete(e, "handler_event"); delete(e, "authority") }, "requires agent_plan"},
					{"missing handler event", "node", func(e map[string]any) { delete(e, "handler_event") }, "exact handler event"},
					{"different handler owner", "node", func(e map[string]any) { e["subscriber_id"] = mustPersistenceRootNode("not-the-handler").Key() }, "exact handler owner"},
					{"missing agent plan", "agent", func(e map[string]any) { delete(e, "agent_plan") }, "requires agent_plan"},
					{"wrong plan agent", "agent", func(e map[string]any) {
						e["agent_plan"].(map[string]any)["name"].(map[string]any)["agent_id"] = "other-agent"
					}, "does not match its recipient"},
					{"live identity in plan", "agent", func(e map[string]any) { e["agent_plan"].(map[string]any)["run_id"] = req.SourceRunID }, "unknown field"},
					{"missing authority", "node", func(e map[string]any) { delete(e, "authority") }, "authority"},
					{"missing connect pin", "connect", func(e map[string]any) { delete(e["authority"].(map[string]any), "receiver_pin") }, "receiver pin"},
					{"missing connect edge", "connect", func(e map[string]any) { delete(e["authority"].(map[string]any), "plan_id") }, "connect plan"},
					{"local with connect evidence", "connect", func(e map[string]any) { e["authority"].(map[string]any)["kind"] = "local" }, "local selected recipient cannot carry connect"},
					{"unknown executable evidence", "agent", func(e map[string]any) { e["worker_token"] = "not-authority" }, "unknown field"},
				} {
					t.Run(carrier+"/"+tc.name, func(t *testing.T) {
						if _, err := selected.RecordRunForkSelectedContractRouteRecovery(ctx, req); err != nil {
							t.Fatal(err)
						}
						record := requireRecipientAuthorityRecoveryLoad(t, ctx, selected, req.ForkRunID)
						raw := record.RouteTopology
						collection, recipients := "static_route_events", "derived_recipients"
						if carrier == "recipient_planning" {
							raw, collection, recipients = record.RecipientPlanning, "recipient_plan_events", "recipients"
						}
						var body map[string]any
						if err := json.Unmarshal(raw, &body); err != nil {
							t.Fatal(err)
						}
						members := body[collection].([]any)[0].(map[string]any)[recipients].([]any)
						found := false
						for _, member := range members {
							e := member.(map[string]any)
							if (tc.kind == "connect" && e["authority"].(map[string]any)["kind"] == "connect") || e["subscriber_type"] == tc.kind {
								tc.mutate(e)
								found = true
								break
							}
						}
						if !found {
							t.Fatalf("missing %s fixture", tc.kind)
						}
						corrupt, err := json.Marshal(body)
						if err != nil {
							t.Fatal(err)
						}
						hash := recipientAuthorityRecordHash(t, corrupt)
						query := "UPDATE run_fork_selected_contract_route_recoveries SET " + carrier + " = $1, " + carrier + "_fingerprint = $2 WHERE fork_run_id = $3"
						if _, err := fixture.db.ExecContext(ctx, query, string(corrupt), hash, req.ForkRunID); err != nil {
							t.Fatal(err)
						}
						// The store returns opaque JSON; the manager is the strict typed consumer.
						loaded := requireRecipientAuthorityRecoveryLoad(t, ctx, selected, req.ForkRunID)
						gotRaw, gotHash := loaded.RouteTopology, loaded.RouteTopologyFingerprint
						if carrier == "recipient_planning" {
							gotRaw, gotHash = loaded.RecipientPlanning, loaded.RecipientPlanningFingerprint
						}
						if gotHash != hash || recipientAuthorityRecordHash(t, gotRaw) != hash {
							t.Fatal("hostility did not reach typed decode with a valid record hash")
						}
						manager := recipientAuthorityRecoveryManager(t, ctx, selected)
						err = manager.Recover(ctx)
						if err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "fingerprint mismatch") {
							t.Fatalf("Recover error = %v, want typed rejection %q", err, tc.want)
						}
						if len(manager.SelectedContractRouteRecoverySnapshot()) != 0 {
							t.Fatal("invalid recovered recipients became manager truth")
						}
					})
				}
			}
		})
	}
}

func recipientAuthorityRecoveryRequest(t *testing.T, ctx context.Context, store recipientAuthorityRecoveryStore, evidence []forkrecipient.Evidence) runfork.RunForkSelectedContractRouteRecoveryRequest {
	t.Helper()
	source, child, eventID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	now := time.Now().UTC()
	requireRunningRunForTest(t, ctx, store, source, now)
	requireRunningRunForTest(t, ctx, store, child, now)
	event := eventtest.ExistingRunRootIngress(eventID, "item.received", "route-recovery", "", []byte(`{}`), 0, source, events.EventEnvelope{}, now)
	if err := commitSemanticEventFixture(ctx, store, event); err != nil {
		t.Fatal(err)
	}
	selection, topology, planning := testSelectedRouteRecoveryEvidence(eventID)
	topology.StaticRouteEvents[0].DerivedRecipients = evidence
	planning.RecipientPlanEvents[0].Recipients = evidence
	return runfork.RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID: child, SourceRunID: source, ForkEventID: eventID,
		ContractSelection: selection, RouteTopology: topology, RecipientPlanning: planning,
	}
}

func requireRecipientAuthorityRecoveryLoad(t *testing.T, ctx context.Context, store selectedRouteRecoveryStore, fork string) runfork.RunForkSelectedContractRouteRecovery {
	t.Helper()
	record, found, err := store.LoadRunForkSelectedContractRouteRecovery(ctx, fork)
	if err != nil || !found {
		t.Fatalf("load recovery found=%v err=%v", found, err)
	}
	return record
}

func recipientAuthorityRecoveryManager(t *testing.T, ctx context.Context, store recipientAuthorityRecoveryStore) *runtimemanager.AgentManager {
	t.Helper()
	bus := &selectedRouteRecoveryPostgresBus{store: &recipientAuthorityRecoveryEventStore{selected: store}}
	manager := runtimemanager.NewAgentManager(bus, nil, store)
	prepareSelectedRouteRecoveryStartup(t, ctx, manager, agentfixture.Lifecycle(t, store))
	return manager
}

// This is a persistence/codec matrix, not execution admission for mutated plans.
// Connect identities and the starting node/agent come from the canonical fixture.
func recipientAuthorityRecoveryEvidence(t *testing.T) []forkrecipient.Evidence {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyReceiverMixedAgent(t), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	graph := pinrouting.CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("compile receiver graph: %#v", issues)
	}
	table, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	var out []forkrecipient.Evidence
	var agentBase forkrecipient.Input
	connectCount := 0
	for _, plan := range graph.Plans() {
		endpoint := plan.ReceiverEndpoint().Readback()
		if endpoint.ResolvedEvent != "sink/work.completed" && endpoint.ResolvedEvent != "sink/child.seeded" {
			continue
		}
		planID, err := pinrouting.ConnectPlanIdentity(plan)
		if err != nil {
			t.Fatal(err)
		}
		for _, subscriber := range table.Resolve(endpoint.ResolvedEvent) {
			in := forkrecipient.Input{
				Recipient: subscriber.Recipient, Path: "sink", HandlerEvent: plan.ReceiverLocalEvent(),
				RouteSource: "selected-source-fixture", AgentPlan: subscriber.AgentPlan,
			}
			if in.Recipient.IsNode() {
				in.HandlerNode, _ = in.Recipient.Node()
			} else if in.Recipient.IsAgent() {
				agentBase = in
			} else {
				continue
			}
			connected, err := forkrecipient.NewConnect(in, planID, plan.ReceiverPinIdentity().EvidenceIdentity())
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, connected)
			connectCount++
			local, err := forkrecipient.NewLocal(in)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, local)
		}
	}
	if connectCount != 3 || agentBase.AgentPlan.IsZero() {
		t.Fatalf("fixture must supply two node pins and one full agent plan: connects=%d", connectCount)
	}
	for _, axis := range []struct {
		name   string
		mutate func(*forkrecipient.Input)
	}{
		{"agent ID", func(in *forkrecipient.Input) {
			in.AgentPlan.Name.AgentID = "other-agent"
			in.Recipient = events.MustAgentDeliveryRecipient("other-agent")
		}},
		{"owner", func(in *forkrecipient.Input) { in.AgentPlan.Name.Owner += "/other-owner" }},
		{"source", func(in *forkrecipient.Input) { in.AgentPlan.Name.Source = agentidentity.NameSourceRuntimeCreated }},
		{"presence", func(in *forkrecipient.Input) { in.AgentPlan.Route = agentidentity.RootRoute() }},
		{"scope", func(in *forkrecipient.Input) { in.AgentPlan.Route.ScopeKey = "different-scope" }},
		{"instance", func(in *forkrecipient.Input) { in.AgentPlan.Route.InstanceID = "different-instance" }},
		{"route path", func(in *forkrecipient.Input) { in.AgentPlan.Route.InstancePath = "sink/other-instance" }},
		{"recipient path", func(in *forkrecipient.Input) { in.Path = "sink/other-path" }},
		{"handler event", func(in *forkrecipient.Input) { in.HandlerEvent = "work.other" }},
	} {
		in := agentBase
		axis.mutate(&in)
		evidence, err := forkrecipient.NewLocal(in)
		if err != nil {
			t.Fatalf("%s variant: %v", axis.name, err)
		}
		out = append(out, evidence)
	}
	return out
}

func requireRecipientAuthorityRecoveryLaws(t *testing.T, req runfork.RunForkSelectedContractRouteRecoveryRequest, evidence []forkrecipient.Evidence) {
	t.Helper()
	keys, hashes := map[forkrecipient.Key]bool{}, map[string]bool{}
	agentPlans := map[string]map[agentidentity.Plan]bool{}
	for i, e := range evidence {
		key, err := e.Key()
		if err != nil || keys[key] {
			t.Fatalf("recipient %d invalid or collapsed exact key: %v", i, err)
		}
		keys[key] = true
		hash, err := e.Fingerprint()
		if err != nil || hashes[hash] {
			t.Fatalf("recipient %d invalid or collapsed semantic fingerprint: %v", i, err)
		}
		hashes[hash] = true
		if e.Recipient.IsAgent() {
			if agentPlans[e.Recipient.ID()] == nil {
				agentPlans[e.Recipient.ID()] = map[agentidentity.Plan]bool{}
			}
			agentPlans[e.Recipient.ID()][e.AgentPlan] = true
			for _, run := range []string{req.ForkRunID, req.SourceRunID, uuid.NewString()} {
				live, err := e.AgentPlan.Live(run)
				if err != nil {
					t.Fatal(err)
				}
				if err := e.Satisfies(req.ForkRunID, e, live); (err == nil) != (run == req.ForkRunID) {
					t.Fatalf("recovered plan satisfaction for %s: %v", run, err)
				}
			}
		}
	}
	if len(agentPlans) != 2 || len(agentPlans["observer"]) != 7 || len(agentPlans["other-agent"]) != 1 {
		t.Fatalf("full plans retained=%#v, want seven observer plans and one coherent different-ID plan", agentPlans)
	}
	canonical, err := forkrecipient.CanonicalSet(evidence)
	if err != nil || len(canonical) != len(evidence) {
		t.Fatalf("recovered exact evidence coalesced: %d/%d err=%v", len(canonical), len(evidence), err)
	}
}

func recipientAuthorityRelabel(t *testing.T, e forkrecipient.Evidence, label string) forkrecipient.Evidence {
	t.Helper()
	in := forkrecipient.Input{Recipient: e.Recipient, Path: e.Path, AgentPlan: e.AgentPlan, HandlerNode: e.HandlerNode(), HandlerEvent: e.HandlerEvent(), RouteSource: label}
	var got forkrecipient.Evidence
	var err error
	if plan, pin, connected := e.Connect(); connected {
		got, err = forkrecipient.NewConnect(in, plan, pin)
	} else {
		got, err = forkrecipient.NewLocal(in)
	}
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func recipientAuthorityRecordHash(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:])
}
