package pipeline_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	swarmruntime "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// M13 exercises the real payload-schema owner, selected-store publication,
// delivery claims, engine and persisted hydration. Reconstruction replaces the
// store adapter, bus and coordinator; it is not a process-kill, public-HTTP,
// driver-COMMIT fault, fork or eager nested-construction proof.
func TestA2AccumulatorSchemaAdmittedCanonicalEvidenceOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			for _, scenario := range []struct {
				name     string
				payload  string
				equal    string
				optional bool
			}{
				{"absent_optional", `{"id":"absent","value":3,"amount":7,"nested":{"numbers":[1,2],"fraction":1.0}}`,
					`{"nested":{"fraction":1.0,"numbers":[1,2]},"amount":7,"value":3,"id":"absent"}`, false},
				{"null_optional_is_omission", `{"id":"null","value":3,"amount":7,"optional":null,"nested":{"numbers":[1,2],"fraction":1.0,"note":null}}`,
					`{"nested":{"fraction":1.0,"numbers":[1,2]},"value":3,"id":"null","amount":7}`, false},
				{"present_optional_record", `{"id":"present","value":3,"amount":7,"optional":"kept","nested":{"numbers":[1,2],"fraction":1.0,"note":"kept"}}`,
					`{"optional":"kept","nested":{"note":"kept","fraction":1.0,"numbers":[1,2]},"value":3,"amount":7,"id":"present"}`, true},
				{"equivalent_numeric_spelling", `{"id":"numeric","value":3,"amount":7,"nested":{"numbers":[1,2],"fraction":1.0}}`,
					`{"id":"numeric","value":3,"amount":7.0,"nested":{"numbers":[1,2],"fraction":1}}`, false},
				{"fractional_numeric", `{"id":"fraction","value":3,"amount":7.5,"nested":{"numbers":[1,2],"fraction":0.25}}`,
					`{"nested":{"fraction":2.5e-1,"numbers":[1,2]},"amount":7.5e0,"value":3,"id":"fraction"}`, false},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					proof := newA2CanonicalAccumulatorProof(t, selected)
					initial := a2CanonicalAccumulatorEvidence(t, proof)
					original := proof.publish(t, "numeric.requested", scenario.payload)
					admission, ok := original.event.PayloadAdmission()
					if !ok || admission.Binding().SchemaClass() != events.PayloadSchemaAuthored || admission.Binding().SchemaDigest() == "" || admission.Binding().EventKey() != "numeric.requested" {
						t.Fatalf("publication lacks authored schema admission: %#v", admission)
					}
					expected := a2CanonicalAccumulatorObject(t, original.event.Payload())
					if _, present := expected["optional"]; present != scenario.optional {
						t.Fatalf("optional admission changed presence: %#v", expected)
					}
					if _, present := expected["nested"].(map[string]any)["note"]; present != scenario.optional {
						t.Fatalf("named-record admission changed optional presence: %#v", expected)
					}
					proof.execute(t, original, "")
					originalDelivery := requireA2CanonicalAccumulatorClaim(t, proof, original)
					before := a2CanonicalAccumulatorEvidence(t, proof)
					if before.Publications != initial.Publications+1 || before.Mutations <= initial.Mutations {
						t.Fatalf("new arrival lacks its real business effect/history: before=%#v after=%#v", initial, before)
					}
					restartA2CanonicalAccumulatorProof(t, proof)
					requireA2CanonicalAccumulatorItem(t, proof, expected)
					duplicate := proof.publish(t, "numeric.requested", scenario.equal)
					duplicateAdmission, ok := duplicate.event.PayloadAdmission()
					if !ok || !duplicateAdmission.Binding().Equal(admission.Binding()) || duplicate.event.ID() == original.event.ID() {
						t.Fatal("duplicate did not independently traverse the same schema owner")
					}
					proof.execute(t, duplicate, "")
					if requireA2CanonicalAccumulatorClaim(t, proof, duplicate) == originalDelivery {
						t.Fatal("new publication reused the original delivery identity")
					}
					want := before
					want.Revision++ // The new delivery settles without another business effect.
					if got := a2CanonicalAccumulatorEvidence(t, proof); got != want {
						rows, err := proof.selected.db.QueryContext(proof.ctx, `SELECT domain,path,CAST(old_value AS TEXT),CAST(new_value AS TEXT) FROM entity_mutations WHERE caused_by_event=$1`, duplicate.event.ID())
						if err != nil {
							t.Fatal(err)
						}
						for rows.Next() {
							var domain, path, oldValue, newValue string
							if err := rows.Scan(&domain, &path, &oldValue, &newValue); err != nil {
								t.Fatal(err)
							}
							t.Logf("duplicate mutation: %s %s old=%s new=%s", domain, path, oldValue, newValue)
						}
						if err := rows.Close(); err != nil {
							t.Fatal(err)
						}
						t.Errorf("canonical duplicate changed business/history/output: got=%#v want=%#v", got, want)
					}
					restartA2CanonicalAccumulatorProof(t, proof)
					requireA2CanonicalAccumulatorItem(t, proof, expected)
					before = a2CanonicalAccumulatorEvidence(t, proof)
					for _, change := range []string{"numeric_value", "record_numeric", "record_order", "optional_presence", "record_optional_presence"} {
						t.Run(change, func(t *testing.T) {
							changed := a2CanonicalAccumulatorObject(t, original.event.Payload())
							record := changed["nested"].(map[string]any)
							switch change {
							case "numeric_value":
								changed["amount"] = float64(9.25)
							case "record_numeric":
								record["fraction"] = float64(9.25)
							case "record_order":
								record["numbers"] = []any{int64(2), int64(1)}
							case "optional_presence":
								if scenario.optional {
									delete(changed, "optional")
								} else {
									changed["optional"] = "null"
								}
							case "record_optional_presence":
								if scenario.optional {
									delete(record, "note")
								} else {
									record["note"] = "null"
								}
							}
							raw, err := canonicaljson.MarshalPreservingNumberKinds(changed)
							if err != nil {
								t.Fatal(err)
							}
							restartA2CanonicalAccumulatorProof(t, proof)
							publication := proof.publish(t, "numeric.requested", string(raw))
							proof.execute(t, publication, failures.ClassConflictingDuplicate)
							if got := a2CanonicalAccumulatorEvidence(t, proof); got != before {
								t.Fatalf("conflict mutated original evidence/effects: got=%#v before=%#v", got, before)
							}
							restartA2CanonicalAccumulatorProof(t, proof)
							requireA2CanonicalAccumulatorItem(t, proof, expected)
						})
					}
					// Exact durable replay is not another business-key publication.
					handoff, err := proof.selected.events.ProveHandoff(proof.ctx, original.event.ID(), original.route)
					if err != nil {
						t.Fatal(err)
					}
					if err := proof.bus.AcceptCommittedDeliveryHandoffs([]deliverylifecycle.DurableHandoffProof{handoff}); err != nil {
						t.Fatal(err)
					}
					proof.execute(t, original, "")
					if got := a2CanonicalAccumulatorEvidence(t, proof); got != before {
						t.Fatalf("settled delivery replay changed evidence: got=%#v before=%#v", got, before)
					}
				})
			}
			t.Run("schema_refusal", func(t *testing.T) {
				proof := newA2CanonicalAccumulatorProof(t, selected)
				for _, malformed := range []string{
					`{"id":"bad","value":3,"amount":"7","nested":{"numbers":[1,2],"fraction":1.0}}`,
					`{"id":"bad","value":3,"amount":null,"nested":{"numbers":[1,2],"fraction":1.0}}`,
					`{"id":"bad","value":3,"amount":7,"nested":null}`,
					`{"id":"bad","value":3,"amount":7,"nested":{"numbers":[1,2]}}`,
					`{"id":"bad","value":3,"amount":7,"nested":{"numbers":[1,null],"fraction":1.0}}`,
					`{"id":"bad","value":3,"amount":7,"nested":{"numbers":[1,2],"fraction":1.0,"note":false}}`,
				} {
					before := a2CanonicalAccumulatorEvidence(t, proof)
					event := eventtest.ExistingRunRootIngress(uuid.NewString(), "numeric.requested", "transport-operator", uuid.NewString(), []byte(malformed), 0, proof.runID, events.EventEnvelope{}, time.Now().UTC())
					if err := proof.bus.Publish(proof.ctx, event); err == nil {
						t.Fatalf("schema admitted malformed numeric/named record: %s", malformed)
					}
					var publications, deliveries int
					if err := proof.selected.db.QueryRowContext(proof.ctx, `SELECT COUNT(*) FROM events WHERE event_id=$1`, event.ID()).Scan(&publications); err != nil {
						t.Fatal(err)
					}
					if err := proof.selected.db.QueryRowContext(proof.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1`, event.ID()).Scan(&deliveries); err != nil {
						t.Fatal(err)
					}
					if publications != 0 || deliveries != 0 || a2CanonicalAccumulatorEvidence(t, proof) != before {
						t.Fatalf("schema refusal persisted publication/delivery/business changes: %d/%d", publications, deliveries)
					}
				}
			})
		})
	}
}

func TestA2AccumulatorPersistedDuplicatedKeyEvidenceRefusesOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			proof := newA2CanonicalAccumulatorProof(t, backend.open(t))
			proof.execute(t, proof.publish(t, "numeric.requested",
				`{"id":"original","value":3,"amount":7,"nested":{"numbers":[1,2],"fraction":1.0}}`), "")
			var original []byte
			if err := proof.selected.db.QueryRowContext(proof.ctx,
				`SELECT accumulator FROM entity_state WHERE run_id=$1 AND entity_id=$1`, proof.runID).Scan(&original); err != nil {
				t.Fatal(err)
			}
			var buckets map[string]any
			if err := canonicaljson.DecodePreservingNumberLexemes(original, &buckets); err != nil {
				t.Fatal(err)
			}
			node := proof.module.nodes[1].Node
			handlers := buckets[node.Key()].(map[string]any)["handler_accumulators"].(map[string]any)
			stored := handlers[timeridentity.NewAccumulatorBucketRef(node, "numeric.requested").Key()].(map[string]any)
			items := stored["items"].([]any)
			if len(items) != 1 {
				t.Fatalf("canonical writer produced %d items, want one", len(items))
			}
			stored["items"] = append(items, items[0])
			receipts := stored["received"].(map[string]any)
			receipts["orphan"] = receipts["original"]
			hostile, err := canonicaljson.MarshalPreservingNumberKinds(buckets)
			if err != nil {
				t.Fatal(err)
			}
			// SQL installs corruption only while this component driver is idle;
			// all positive state, publications, claims and outcomes use real owners.
			write := func(raw []byte) {
				t.Helper()
				result, err := proof.selected.db.ExecContext(proof.ctx,
					`UPDATE entity_state SET accumulator=$1 WHERE run_id=$2 AND entity_id=$2`, string(raw), proof.runID)
				if err != nil {
					t.Fatal(err)
				}
				if rows, err := result.RowsAffected(); err != nil || rows != 1 {
					t.Fatalf("hostile fixture changed %d receivers: %v", rows, err)
				}
			}
			write(hostile)
			t.Cleanup(func() { write(original) })
			restartA2CanonicalAccumulatorProof(t, proof)
			before := a2CanonicalAccumulatorEvidence(t, proof)
			payload := `{"id":"next","value":4,"amount":8,"nested":{"numbers":[3,4],"fraction":0.5}}`
			refused := proof.publish(t, "numeric.requested", payload)
			proof.execute(t, refused, failures.ClassSchemaInvalid)
			if after := a2CanonicalAccumulatorEvidence(t, proof); after != before {
				t.Fatalf("corrupt keyed evidence changed business state/history/output: before=%#v after=%#v", before, after)
			}
			requireRefusal := func() {
				t.Helper()
				var status string
				var attempts, settled, open int
				if err := proof.selected.db.QueryRowContext(proof.ctx, `SELECT d.status,
 (SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id AND a.claim_token IS NOT NULL),
 (SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id AND a.closure_kind='settled' AND a.open_marker=FALSE),
 (SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id AND a.open_marker=TRUE)
 FROM event_deliveries d WHERE d.event_id=$1 AND d.subscriber_type='node'`, refused.event.ID()).Scan(&status, &attempts, &settled, &open); err != nil {
					t.Fatal(err)
				}
				if status != "dead_letter" || attempts != 1 || settled != 1 || open != 0 {
					t.Fatalf("corruption refusal did not settle its exact claim: status=%s attempts=%d settled=%d open=%d", status, attempts, settled, open)
				}
			}
			requireRefusal()
			write(original)
			restartA2CanonicalAccumulatorProof(t, proof)
			lawful := proof.publish(t, "numeric.requested", payload)
			proof.execute(t, lawful, "")
			requireA2CanonicalAccumulatorClaim(t, proof, lawful)
			requireRefusal()
		})
	}
}

func newA2CanonicalAccumulatorProof(t *testing.T, selected gateRecoveryStoreCase) *a2AccumulatorPersistenceProof {
	t.Helper()
	root := canonicalrouting.CopySemanticNumericIngress(t)
	// Extend the canonical numeric fixture's schema and per-arrival handler;
	// its original arithmetic, output schema and entity declaration remain live.
	for _, name := range []string{"events.yaml", "types.yaml", "nodes.yaml"} {
		path := filepath.Join(root, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := yaml.Unmarshal(raw, &document); err != nil {
			t.Fatal(err)
		}
		switch name {
		case "events.yaml":
			fields := document["numeric.requested"].(map[string]any)
			fields["id"], fields["amount"], fields["optional"] = "text", "numeric", "text?"
		case "types.yaml":
			document["types"].(map[string]any)["NumericInput"].(map[string]any)["note"] = "text?"
		case "nodes.yaml":
			handler := document["numeric"].(map[string]any)["event_handlers"].(map[string]any)["numeric.requested"].(map[string]any)
			handler["accumulate"] = map[string]any{"into": "items", "from": "payload", "key": "payload.id"}
			handler["data_accumulation"] = map[string]any{"writes": []any{map[string]any{"target_field": "score", "value": "payload.value"}}}
		}
		updated, err := yaml.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, updated, 0600); err != nil {
			t.Fatal(err)
		}
	}
	repo := pipeline.WorkflowRepoRoot()
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	runID := uuid.NewString()
	insertGateRecoveryRun(t, selected, runID)
	ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
	proof := &a2AccumulatorPersistenceProof{selected: selected, ctx: ctx, runID: runID, module: proposedEffectProofModule{
		source: source, nodes: []pipeline.WorkflowNode{
			{Node: externalPipelineSourceNode(t, source, ".", "collector"), Subscriptions: []events.EventType{"numeric.completed"}, ExecutionType: contracts.SystemNodeExecutionType},
			{Node: externalPipelineSourceNode(t, source, ".", "numeric"), Subscriptions: []events.EventType{"numeric.requested"}, ExecutionType: contracts.SystemNodeExecutionType},
		},
	}}
	restartA2CanonicalAccumulatorProof(t, proof)
	if _, err := proof.pc.MaterializeInitialEntry(ctx, testRunScopedWorkflowInstanceForRun(runID, runID), pipeline.WorkflowInstance{
		InstanceID: runID, StorageRef: runID, EntityID: runID, EntityType: "widget", WorkflowName: source.WorkflowName(), WorkflowVersion: source.WorkflowVersion(),
		CurrentState: "waiting", Fields: map[string]any{"score": int64(0)},
	}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return proof
}

func restartA2CanonicalAccumulatorProof(t *testing.T, proof *a2AccumulatorPersistenceProof) {
	t.Helper()
	if proof.pc != nil {
		reconstructed, ok := proof.selected.trace.(gateRecoverySelectedStore)
		if !ok {
			t.Fatal("canonical evidence proof needs an independently reconstructed store adapter")
		}
		previous, ok := proof.selected.events.(gateRecoveryTraceStore)
		if !ok {
			t.Fatal("canonical evidence proof cannot retain its previous store adapter")
		}
		proof.selected.events, proof.selected.trace = reconstructed, previous
	}
	reconstructed := proof.selected.events
	admitter := swarmruntime.NewRuntimePayloadAdmitter(nil, proof.module.source, authorActivityTestSourceArtifactFact)
	reconstructed.(swarmruntime.EventPayloadAdmissionBinder).SetEventPayloadAdmitter(admitter)
	work, ok := worklifetime.OccurrenceFromContext(proof.ctx)
	if !ok {
		t.Fatal("canonical evidence proof lost its actual runtime lifetime")
	}
	bus, err := newScopedTestEventBus(t, reconstructed, runtimebus.EventBusOptions{ContractBundle: proof.module.source, PayloadAdmitter: admitter, WorkOwner: work})
	if err != nil {
		t.Fatal(err)
	}
	proof.bus = bus
	proof.persistence = &a2AccumulatorPersistenceObserver{WorkflowPersistenceOwner: reconstructed.(pipeline.WorkflowPersistenceOwner)}
	proof.selected.persistence = pipeline.NewWorkflowPersistence(proof.persistence)
	proof.pc = newGateRecoveryCoordinator(proof.bus, proof.selected, pipeline.PipelineCoordinatorOptions{Module: proof.module, WorkOwner: work})
}

func a2CanonicalAccumulatorObject(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var object map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &object); err != nil {
		t.Fatal(err)
	}
	projected, err := workflowexpr.ProjectCELValue(object)
	if err != nil {
		t.Fatal(err)
	}
	return projected.(map[string]any)
}

func requireA2CanonicalAccumulatorItem(t *testing.T, proof *a2AccumulatorPersistenceProof, expected map[string]any) {
	t.Helper()
	state := proof.accumulator(t, "numeric.requested")
	matches := 0
	for _, item := range state.Items {
		if item["id"] != expected["id"] {
			continue
		}
		matches++
		projected, err := workflowexpr.ProjectCELValue(item)
		if err != nil || !reflect.DeepEqual(projected, expected) {
			t.Fatalf("hydration changed original optional/numeric/record execution evidence: got=%#v want=%#v err=%v", projected, expected, err)
		}
		hash, err := canonicaljson.Hash(expected)
		if err != nil || state.Received[expected["id"].(string)] != hash {
			t.Fatalf("hydrated item lost canonical business receipt: %#v err=%v", state.Received, err)
		}
	}
	if matches != 1 || len(state.Items) != len(state.Received) || len(state.Deliveries) != 0 {
		t.Fatalf("canonical keyed item/receipt multiplicity changed: matches=%d state=%#v expected=%#v", matches, state, expected)
	}
}

func a2CanonicalAccumulatorEvidence(t *testing.T, proof *a2AccumulatorPersistenceProof) a2AccumulatorPersistedEvidence {
	t.Helper()
	evidence := proof.evidence(t)
	if err := proof.selected.db.QueryRowContext(proof.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='numeric.completed'`, proof.runID).Scan(&evidence.Publications); err != nil {
		t.Fatal(err)
	}
	return evidence
}

func requireA2CanonicalAccumulatorClaim(t *testing.T, proof *a2AccumulatorPersistenceProof, publication a2AccumulatorPublication) string {
	t.Helper()
	var deliveryID, status string
	var attempts, settled int
	if err := proof.selected.db.QueryRowContext(proof.ctx, `SELECT d.delivery_id, d.status,
 (SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id AND a.claim_token IS NOT NULL),
 (SELECT COUNT(*) FROM event_delivery_attempts a WHERE a.delivery_id=d.delivery_id AND a.closure_kind='settled' AND a.open_marker=FALSE)
 FROM event_deliveries d WHERE d.event_id=$1 AND d.subscriber_type='node'`, publication.event.ID()).Scan(&deliveryID, &status, &attempts, &settled); err != nil {
		t.Fatal(err)
	}
	if status != "delivered" || attempts != 1 || settled != 1 {
		t.Fatalf("publication did not acquire and settle one real persisted claim: delivery=%s status=%s attempts=%d settled=%d", deliveryID, status, attempts, settled)
	}
	return deliveryID
}
