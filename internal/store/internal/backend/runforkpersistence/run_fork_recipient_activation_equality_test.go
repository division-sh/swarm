package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

func TestSelectedContractRouteRecoveryActivationEqualityBothStores(t *testing.T) {
	evidence := activationEqualitySourceRecipients(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := activationEqualityDatabase(t, backend)
			for _, carrier := range []string{"static", "dynamic", "planning"} {
				for _, change := range []string{"unchanged", "diagnostic", "set_order_duplicates", "plan_owner", "handler_node", "handler_event", "connect_edge", "receiver_pin", "local_authority", "metadata", "malformed", "unknown_aggregate", "unknown_event", "corrupt_payload"} {
					t.Run(carrier+"/"+change, func(t *testing.T) {
						expected := activationEqualityRecord(t, evidence)
						actual := expected
						topology, planning, err := decodeRunForkSelectedContractRouteRecoveryModels(expected)
						if err != nil {
							t.Fatal(err)
						}
						var recipients *[]forkrecipient.Evidence
						var metadata *string
						switch carrier {
						case "static":
							recipients, metadata = &topology.StaticRouteEvents[0].DerivedRecipients, &topology.StaticRouteEvents[0].EventName
						case "dynamic":
							recipients, metadata = &topology.DynamicTopologyProofs[0].DerivedRecipients, &topology.DynamicTopologyProofs[0].FlowInstance
						case "planning":
							recipients, metadata = &planning.RecipientPlanEvents[0].Recipients, &planning.RecipientPlanEvents[0].EventName
						}
						switch change {
						case "diagnostic", "corrupt_payload":
							for i, recipient := range *recipients {
								(*recipients)[i] = activationEqualityRecipientVariant(t, recipient, evidence, "diagnostic")
							}
						case "set_order_duplicates":
							slices.Reverse(*recipients)
							*recipients = append(*recipients, (*recipients)[0])
						case "metadata":
							*metadata += "/different"
						case "unchanged", "malformed", "unknown_aggregate", "unknown_event":
						default:
							index := 0
							if change == "plan_owner" {
								index = slices.IndexFunc(*recipients, func(e forkrecipient.Evidence) bool { return e.Recipient.IsAgent() })
							} else {
								index = slices.IndexFunc(*recipients, func(e forkrecipient.Evidence) bool { return e.Recipient.IsNode() })
							}
							if index < 0 {
								t.Fatal("fixture missing required recipient kind")
							}
							(*recipients)[index] = activationEqualityRecipientVariant(t, (*recipients)[index], evidence, change)
						}
						actual.RouteTopology, actual.RouteTopologyFingerprint, err = runForkSelectedContractRecoveryJSONFingerprint(topology)
						if err != nil {
							t.Fatal(err)
						}
						actual.RecipientPlanning, actual.RecipientPlanningFingerprint, err = runForkSelectedContractRecoveryJSONFingerprint(planning)
						if err != nil {
							t.Fatal(err)
						}
						if change == "diagnostic" && actual.RouteTopologyFingerprint == expected.RouteTopologyFingerprint && actual.RecipientPlanningFingerprint == expected.RecipientPlanningFingerprint {
							t.Fatal("diagnostic mutation did not change payload fingerprint")
						}
						if change == "malformed" || change == "unknown_aggregate" || change == "unknown_event" {
							activationEqualityMalformedPayload(t, &actual, carrier, change)
						}
						if change == "corrupt_payload" {
							actual.RouteTopologyFingerprint = expected.RouteTopologyFingerprint
							actual.RecipientPlanningFingerprint = expected.RecipientPlanningFingerprint
						}
						ctx := context.Background()
						tx, err := db.BeginTx(ctx, nil)
						if err != nil {
							t.Fatal(err)
						}
						defer tx.Rollback()
						if err := insertRunForkSelectedContractRouteRecovery(ctx, tx, actual); err != nil {
							t.Fatal(err)
						}
						loaded, err := loadRunForkSelectedContractRouteRecovery(ctx, tx, `WHERE fork_run_id = $1`, actual.ForkRunID)
						if err != nil || loaded.RouteTopologyFingerprint != actual.RouteTopologyFingerprint || loaded.RecipientPlanningFingerprint != actual.RecipientPlanningFingerprint {
							t.Fatalf("actual stored record readback: %v", err)
						}
						// The exact validator called by activation and existing-materialization reuse.
						err = validateRunForkSelectedContractRouteRecoveryAtActivation(ctx, tx, expected)
						wantOK := change == "unchanged" || change == "diagnostic" || change == "set_order_duplicates"
						if (err == nil) != wantOK {
							t.Fatalf("activation route validation: %v, want acceptance %v", err, wantOK)
						}
						if change == "corrupt_payload" && !strings.Contains(err.Error(), "fingerprint mismatch") {
							t.Fatalf("corrupt payload missed integrity boundary: %v", err)
						}
						if change == "malformed" && !strings.Contains(err.Error(), "decode persisted") {
							t.Fatalf("self-consistent hash hid malformed evidence: %v", err)
						}
						if (change == "unknown_aggregate" || change == "unknown_event") && !strings.Contains(err.Error(), `unknown field "unexpected_authority"`) {
							t.Fatalf("self-consistent hash hid unknown aggregate field: %v", err)
						}
					})
				}
			}
		})
	}
}

func activationEqualityDatabase(t *testing.T, backend string) *sql.DB {
	t.Helper()
	var db *sql.DB
	jsonType, timeType := "TEXT", "TEXT"
	if backend == "postgres" {
		_, db, _ = testutil.StartEmptyPostgres(t)
		jsonType, timeType = "JSONB", "TIMESTAMPTZ"
	} else {
		var err error
		db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "activation.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}
	db.SetMaxOpenConns(1)
	// SQL-backed activation-boundary fixture, not a full run/lineage materialization.
	// Preserve real JSONB normalization while excluding unrelated foreign-key setup.
	_, err := db.Exec(fmt.Sprintf(`CREATE TEMP TABLE run_fork_selected_contract_route_recoveries (
		fork_run_id TEXT PRIMARY KEY, source_run_id TEXT NOT NULL, fork_event_id TEXT NOT NULL,
		owner TEXT NOT NULL, runtime_recovery_owner TEXT NOT NULL, mode TEXT NOT NULL, bundle_hash TEXT,
		route_topology_owner TEXT NOT NULL, dynamic_topology_owner TEXT, recipient_planning_owner TEXT NOT NULL,
		frontier_evidence_fingerprint TEXT NOT NULL, route_topology_fingerprint TEXT NOT NULL, recipient_planning_fingerprint TEXT NOT NULL,
		static_route_event_count INTEGER NOT NULL, dynamic_topology_proof_count INTEGER NOT NULL, recipient_plan_event_count INTEGER NOT NULL,
		route_topology %s NOT NULL, recipient_planning %s NOT NULL, created_at %s NOT NULL
	)`, jsonType, jsonType, timeType))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func activationEqualityRecord(t *testing.T, evidence []forkrecipient.Evidence) runfork.RunForkSelectedContractRouteRecovery {
	t.Helper()
	eventID := uuid.NewString()
	selection := runfork.RunForkContractSelection{Mode: "selected_contracts"}
	topology := runfork.RunForkSelectedContractRouteTopology{
		Owner: runfork.RunForkSelectedContractRouteTopologyOwner, NonMutating: true,
		ContractSelection: selection, FrontierEvidenceFingerprint: "fixed-frontier",
		StaticRouteEvents:     []runfork.RunForkSelectedContractRouteEvent{{SourceEventID: eventID, EventName: "sink/work.completed", DerivedRecipients: slices.Clone(evidence)}},
		DynamicTopologyProofs: []runfork.RunForkSelectedContractDynamicTopologyProof{{FlowInstance: "sink", SourceEventIDs: []string{eventID}, EventNames: []string{"sink/work.completed"}, DerivedRecipients: slices.Clone(evidence)}},
	}
	planning := runfork.RunForkSelectedContractRecipientPlanning{
		Owner: runfork.RunForkSelectedContractRecipientPlanningOwner, NonMutating: true, RecipientPlanningSupported: true,
		RouteTopologyOwner: topology.Owner, ContractSelection: selection, FrontierEvidenceFingerprint: topology.FrontierEvidenceFingerprint,
		RecipientPlanEvents: []runfork.RunForkSelectedContractRecipientPlanEvent{{SourceEventID: eventID, EventName: "sink/work.completed", Recipients: slices.Clone(evidence)}},
	}
	record, err := normalizeRunForkSelectedContractRouteRecovery(runfork.RunForkSelectedContractRouteRecoveryRequest{
		ForkRunID: uuid.NewString(), SourceRunID: uuid.NewString(), ForkEventID: eventID,
		ContractSelection: selection, RouteTopology: topology, RecipientPlanning: planning,
	}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func activationEqualitySourceRecipients(t *testing.T) []forkrecipient.Evidence {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyReceiverMixedAgent(t), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	graph := pinrouting.CompileConnectGraph(source)
	if issues := graph.Issues(); len(issues) != 0 {
		t.Fatalf("fixture connect graph: %#v", issues)
	}
	table, err := runtimebus.DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	var out []forkrecipient.Evidence
	for _, plan := range graph.Plans() {
		event := plan.ReceiverEndpoint().Readback().ResolvedEvent
		if event != "sink/work.completed" && event != "sink/child.seeded" {
			continue
		}
		planID, err := pinrouting.ConnectPlanIdentity(plan)
		if err != nil {
			t.Fatal(err)
		}
		for _, subscriber := range table.Resolve(event) {
			in := forkrecipient.Input{Recipient: subscriber.Recipient, Path: "sink", HandlerEvent: plan.ReceiverLocalEvent(), AgentPlan: subscriber.AgentPlan, RouteSource: "selected-source"}
			if in.Recipient.IsNode() {
				in.HandlerNode, _ = in.Recipient.Node()
			}
			e, err := forkrecipient.NewConnect(in, planID, plan.ReceiverPinIdentity().EvidenceIdentity())
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, e)
		}
	}
	if len(out) != 3 {
		t.Fatalf("fixture requires two node event/pins and one full agent plan; got %d", len(out))
	}
	for _, subscriber := range table.Resolve("work.requested") {
		if !subscriber.Recipient.IsNode() {
			continue
		}
		node, _ := subscriber.Recipient.Node()
		e, err := forkrecipient.NewLocal(forkrecipient.Input{Recipient: subscriber.Recipient, HandlerNode: node, HandlerEvent: "work.requested", RouteSource: "selected-source"})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	if len(out) != 4 {
		t.Fatal("fixture requires its distinct root controller handler")
	}
	return out
}

func activationEqualityRecipientVariant(t *testing.T, e forkrecipient.Evidence, all []forkrecipient.Evidence, change string) forkrecipient.Evidence {
	t.Helper()
	in := forkrecipient.Input{Recipient: e.Recipient, Path: e.Path, AgentPlan: e.AgentPlan, HandlerNode: e.HandlerNode(), HandlerEvent: e.HandlerEvent(), RouteSource: e.RouteSourceCode()}
	edge, pin, connected := e.Connect()
	switch change {
	case "diagnostic":
		in.RouteSource = "independently recovered diagnostic"
	case "plan_owner":
		in.AgentPlan.Name.Owner += "/other"
	case "handler_event":
		in.HandlerEvent = "different.event"
	case "handler_node", "connect_edge", "receiver_pin":
		found := false
		for _, other := range all {
			otherEdge, otherPin, otherConnected := other.Connect()
			matches := change == "handler_node" && other.HandlerNode() != e.HandlerNode() ||
				change == "connect_edge" && otherConnected && otherEdge != edge ||
				change == "receiver_pin" && otherConnected && otherPin != pin
			if other.Recipient.IsNode() && matches {
				switch change {
				case "handler_node":
					in.Recipient, in.HandlerNode = other.Recipient, other.HandlerNode()
				case "connect_edge":
					edge = otherEdge
				case "receiver_pin":
					pin = otherPin
				}
				found = true
				break
			}
		}
		if !found {
			t.Fatal("fixture lacks distinct declared sibling handler/edge/pin")
		}
	}
	var out forkrecipient.Evidence
	var err error
	if change == "local_authority" || !connected {
		out, err = forkrecipient.NewLocal(in)
	} else {
		out, err = forkrecipient.NewConnect(in, edge, pin)
	}
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func activationEqualityMalformedPayload(t *testing.T, record *runfork.RunForkSelectedContractRouteRecovery, carrier, change string) {
	t.Helper()
	raw, fingerprint := &record.RouteTopology, &record.RouteTopologyFingerprint
	array, field := "static_route_events", "derived_recipients"
	if carrier == "dynamic" {
		array = "dynamic_topology_proofs"
	} else if carrier == "planning" {
		raw, fingerprint = &record.RecipientPlanning, &record.RecipientPlanningFingerprint
		array, field = "recipient_plan_events", "recipients"
	}
	var payload map[string]any
	if err := json.Unmarshal(*raw, &payload); err != nil {
		t.Fatal(err)
	}
	switch change {
	case "unknown_aggregate":
		payload["unexpected_authority"] = "must not be erased"
	case "unknown_event":
		payload[array].([]any)[0].(map[string]any)["unexpected_authority"] = "must not be erased"
	default:
		payload[array].([]any)[0].(map[string]any)[field] = []any{map[string]any{}}
	}
	var err error
	*raw, *fingerprint, err = runForkSelectedContractRecoveryJSONFingerprint(payload)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSelectedContractRouteRecoveryDecodeRejectsTrailingJSON(t *testing.T) {
	evidence := activationEqualitySourceRecipients(t)
	for _, carrier := range []string{"topology", "planning"} {
		t.Run(carrier, func(t *testing.T) {
			record := activationEqualityRecord(t, evidence)
			if carrier == "topology" {
				record.RouteTopology = append(slices.Clone(record.RouteTopology), []byte(` {"extra":true}`)...)
			} else {
				record.RecipientPlanning = append(slices.Clone(record.RecipientPlanning), []byte(` {"extra":true}`)...)
			}
			if _, _, err := decodeRunForkSelectedContractRouteRecoveryModels(record); err == nil || !strings.Contains(err.Error(), "unexpected trailing JSON") {
				t.Fatalf("trailing document admitted: %v", err)
			}
		})
	}
}
