package conformance

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/notifyallchildren"
	"github.com/google/uuid"
)

func publishSemanticProofRunWithPins(t *testing.T, f *semanticProofFixture, pins []durabledata.ExplicitPin) {
	t.Helper()
	eventType := events.EventType(f.source.ResolveFlowEventReference(notifyallchildren.OwnerFlowID, "portfolio.opened"))
	apiEndpoint, err := bus.NewOrdinaryFlowAPIEventPublicationEndpoint(f.source, notifyallchildren.OwnerFlowID, string(eventType))
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	payload, err := json.Marshal(map[string]any{"portfolio_id": f.runID, "threshold": 75})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.RunCreatingRootIngressWithMode(eventID, eventType, notifyallchildren.OwnerFlowID, "", payload, 0, f.runID, "", events.EventEnvelope{}, time.Now().UTC(), executionmode.Live)
	initialEvent, err := json.Marshal(map[string]any{"event_name": event.Type(), "payload": json.RawMessage(event.Payload()), "emitter": "operator-api", "entity_id": "", "flow_instance": "", "source_event_id": ""})
	if err != nil {
		t.Fatal(err)
	}
	request := apiidempotency.Request{Method: "event.publish", Actor: apiidempotency.BearerActor("operator"), IdempotencyKey: eventID, RequestHash: "semantic-proof-" + eventID}
	completion := apiidempotency.Completion{ResourceID: eventID, Response: json.RawMessage(`{"event_id":"` + eventID + `"}`)}
	creation := durabledata.RunCreationCommand{RunID: f.runID, Actor: "operator", BundleHash: f.runtime.sourceArtifactFact.BundleHash(), EventID: eventID, InitialEvent: initialEvent, Data: durabledata.RunCreationDataEnvelope{Pins: pins}}
	result, replay, err := f.runtime.bus.PublishAPIEventWithRunCreationAcknowledged(f.ctx, event, &apiEndpoint, request, completion, &creation)
	if err != nil || replay || result.ResourceID != eventID {
		t.Fatalf("pinned public run creation: result=%+v replay=%v err=%v", result, replay, err)
	}
	waitNotifyAllChildrenRuntime(t, f.runtime, f.runID)
}

func readJobflowCorpus(t *testing.T) []byte {
	t.Helper()
	file, err := os.Open(filepath.Join("..", "..", "durabledata", "testdata", "jobflow-gems.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	if len(raw) != 666737 || hex.EncodeToString(digest[:]) != "7f91b3f892fd32e8605c9155bd4f7b4d90f4ff8dda1b556b1a68f0bb2d865a67" {
		t.Fatalf("jobflow source corpus changed: bytes=%d digest=%x", len(raw), digest)
	}
	return raw
}

// This variant exercises existing account routing with a production-scale
// cardinality. The exact-byte import is proved separately below.
func jobflowAccountRows(t *testing.T, runID string) []map[string]any {
	t.Helper()
	raw := readJobflowCorpus(t)
	rows := make([]map[string]any, 0, 1362)
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
	for scanner.Scan() {
		var source struct {
			Slug     string      `json:"slug"`
			EngRoles json.Number `json:"eng_roles"`
			GemScore json.Number `json:"gem_score"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &source); err != nil {
			t.Fatal(err)
		}
		roles, err := source.EngRoles.Int64()
		if err != nil {
			t.Fatal(err)
		}
		score, err := source.GemScore.Float64()
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, map[string]any{
			"portfolio_id": runID, "account_id": source.Slug, "eng_roles": roles,
			"gem_score": score, "external_id": "11111111-1111-4111-8111-111111111111",
			"eligible": true, "snapshot_threshold": 75,
		})
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1362 {
		t.Fatalf("jobflow source rows=%d, want 1362", len(rows))
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i]["account_id"].(string) < rows[j]["account_id"].(string) })
	for i := range rows {
		if i > 0 && rows[i-1]["account_id"] == rows[i]["account_id"] {
			t.Fatalf("jobflow source repeats business key %v", rows[i]["account_id"])
		}
		rows[i]["ordinal"] = i
		rows[i]["source_count"] = len(rows)
	}
	return rows
}

func TestFanOutPinnedResourceJobflow1362RoutesAndSettlesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSemanticProofFixture(t, backend)
			rows := jobflowAccountRows(t, f.runID)
			release := f.pauseAtEmptyScan(t)
			defer release()
			source := f.installPinnedResourceSource(t, rows)
			trigger := f.submit(t, semanticProofResourceEvent, semanticProofRows(1), 75)
			f.waitIntent(t, trigger, 0, "open")
			release()
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, f.runID, 2*time.Minute)
			f.assertOutcomes(t, trigger, rows, 75, nil)
			f.probe.mu.Lock()
			intent := f.probe.intents[trigger]
			f.probe.mu.Unlock()
			if intent.Source != source || intent.Request.Cardinality != 1362 || intent.Status != fanoutobligation.StatusOpen && intent.Status != fanoutobligation.StatusClosed {
				t.Fatalf("resource obligation escaped pinned version/cardinality: %+v", intent)
			}
			var delivered int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='delivered' AND subscriber_type='node'`, f.runID).Scan(&delivered); err != nil || delivered < 1362 {
				t.Fatalf("jobflow-derived rows did not settle through routed node deliveries: count=%d err=%v", delivered, err)
			}
			t.Logf("%s: %d imported rows, routed and settled", backend, len(rows))
		})
	}
}

func TestFanOutExactJobflow1362ImportRouteAndSettleBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			input := readJobflowCorpus(t)
			ref, err := durabledata.ParseDeclarationRef("portfolio", "portfolio/company.lead")
			if err != nil {
				t.Fatal(err)
			}
			var compiled durabledata.CompiledVersion
			var bundleHash string
			f := newSemanticProofFixtureWithPreparation(t, backend, func(f *semanticProofFixture) []durabledata.ExplicitPin {
				bundle, ok := semanticview.Bundle(f.source)
				if !ok {
					t.Fatal("exact jobflow proof requires bundle source")
				}
				bundleHash = bundle.SourceArtifact.BundleHash()
				declaration, ok := bundle.DurableDataDeclarationByRef(ref)
				if !ok || declaration.BusinessKey != "slug" {
					t.Fatalf("exact jobflow declaration = %+v", declaration)
				}
				var defects []durabledata.ValidationDefect
				compiled, defects = durabledata.CompileJSONL(ref, declaration.Schema, declaration.BusinessKey, input)
				if len(defects) != 0 || len(compiled.Rows) != 1362 {
					t.Fatalf("exact jobflow import: rows=%d defects=%+v", len(compiled.Rows), defects)
				}
				owner, ok := f.selected.(interface {
					ExecuteDataSourceOperation(ctx context.Context, command durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
				})
				if !ok {
					t.Fatal("selected store lacks exact durable-data import owner")
				}
				result, err := owner.ExecuteDataSourceOperation(f.ctx, durabledata.SourceCommand{
					Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator",
					BundleHash: bundleHash, Declaration: ref, ExpectedHead: durabledata.AbsentHead(),
					InputFormat: "jsonl", Input: input,
				})
				if err != nil || result.Candidate.VersionID != compiled.VersionID {
					t.Fatalf("exact jobflow version=%s want=%s err=%v", result.Candidate.VersionID, compiled.VersionID, err)
				}
				return []durabledata.ExplicitPin{{Declaration: ref, VersionID: result.Candidate.VersionID}}
			})
			owner, ok := f.selected.(interface {
				ExecuteDataSourceOperation(ctx context.Context, command durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
				LoadPinnedSource(ctx context.Context, runID, bundleHash string, ref durabledata.DeclarationRef) (durabledata.PinnedSource, error)
			})
			if !ok {
				t.Fatal("selected store lacks exact durable-data source owner")
			}
			release := f.pauseAtEmptyScan(t)
			defer release()
			pinned, err := owner.LoadPinnedSource(f.ctx, f.runID, bundleHash, ref)
			if err != nil || pinned.VersionID != compiled.VersionID || pinned.RowCount != 1362 {
				t.Fatalf("exact jobflow pin = %+v err=%v", pinned, err)
			}
			var dormant int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, f.runID).Scan(&dormant); err != nil || dormant != 0 {
				t.Fatalf("pin alone caused fan-out: intents=%d err=%v", dormant, err)
			}
			trigger := f.submit(t, semanticProofJobflowEvent, semanticProofRows(1), 75)
			f.waitIntent(t, trigger, 0, "open")
			semanticProofWait(t, func() (bool, error) {
				var status string
				err := f.db.QueryRowContext(f.ctx, `SELECT status FROM event_deliveries WHERE run_id=$1 AND event_id=$2`, f.runID, trigger).Scan(&status)
				return status == "delivered", err
			})
			old := f.runtime
			join := beginServingLifetimeJoin(old, nil)
			release()
			assertServingJoinComplete(t, join, old, nil)
			f.runtime = newNotifyAllChildrenRuntime(t, f.selected, f.db, f.source, time.Now, notifyAllChildrenRuntimeOptions{
				processTopology: f.topology,
				fanOutExecutor: func(pc *pipeline.PipelineCoordinator) startupownership.FanOutExecutor {
					f.probe.PipelineCoordinator = pc
					return f.probe
				},
			})
			if err := f.runtime.manager.Run(managedConformanceExecutionContextForBundle(t, f.ctx, "jobflow-pinned-source-restart", f.runtime.sourceArtifactFact)); err != nil {
				t.Fatal(err)
			}
			f.runtime.fanOutServing.Wake()
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, f.runID, 3*time.Minute)
			rows, err := f.db.QueryContext(f.ctx, `SELECT o.ordinal,o.outcome_kind,e.payload_bytes,e.event_id
				FROM fan_out_outcomes o JOIN event_deliveries d ON d.delivery_id=o.triggering_delivery_id
				JOIN events e ON e.event_id=o.event_id WHERE o.run_id=$1 AND d.event_id=$2 ORDER BY o.ordinal`, f.runID, trigger)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			count := 0
			publishedOrdinals := make(map[string]int, len(compiled.Rows))
			for rows.Next() {
				var ordinal int
				var kind string
				var payload []byte
				var eventID string
				if err := rows.Scan(&ordinal, &kind, &payload, &eventID); err != nil {
					t.Fatal(err)
				}
				var actual, expected any
				if err := json.Unmarshal(payload, &actual); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(compiled.Rows[count].Canonical, &expected); err != nil {
					t.Fatal(err)
				}
				if ordinal != count || kind != "committed" || !reflect.DeepEqual(actual, expected) {
					t.Fatalf("jobflow row %d: ordinal=%d kind=%s payload=%s want=%s", count, ordinal, kind, payload, compiled.Rows[count].Canonical)
				}
				publishedOrdinals[eventID] = ordinal
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if count != 1362 {
				t.Fatalf("jobflow committed outcomes=%d, want 1362", count)
			}
			var delivered int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id
				WHERE d.run_id=$1 AND e.event_name=$2 AND d.status='delivered'`, f.runID, "portfolio/company.lead").Scan(&delivered); err != nil || delivered != 1362 {
				t.Fatalf("jobflow exact routed settlements=%d want=1362 err=%v", delivered, err)
			}
			readback, ok := f.selected.(interface {
				ListOperatorEvents(context.Context, operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error)
			})
			if !ok {
				t.Fatal("selected store lacks public event projection")
			}
			seen := make(map[string]bool, len(compiled.Rows))
			cursor := ""
			for {
				page, err := readback.ListOperatorEvents(f.ctx, operatorread.OperatorEventListOptions{
					Filter: operatorread.OperatorEventListFilter{RunID: f.runID, EventName: "portfolio/company.lead"},
					Limit:  200, Cursor: cursor,
				})
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range page.Events {
					ordinal, exists := publishedOrdinals[event.EventID]
					if !exists || seen[event.EventID] || event.EventName != "portfolio/company.lead" || len(event.Deliveries) != 1 || event.Deliveries[0].Status != "delivered" {
						t.Fatalf("public readback changed or repeated row event: %+v", event)
					}
					var expected any
					if err := json.Unmarshal(compiled.Rows[ordinal].Canonical, &expected); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(event.Payload, expected) {
						t.Fatalf("public row %d payload=%v want=%v", ordinal, event.Payload, expected)
					}
					seen[event.EventID] = true
				}
				if page.NextCursor == "" {
					break
				}
				if page.NextCursor == cursor {
					t.Fatal("public event pagination did not advance")
				}
				cursor = page.NextCursor
			}
			if len(seen) != len(compiled.Rows) {
				t.Fatalf("public projection returned %d/%d row events", len(seen), len(compiled.Rows))
			}
			t.Logf("%s: exact %d-row JSONL imported, pinned, published and settled", backend, count)
		})
	}
}

func TestFanOutResourceTriggerWithoutPinFailsBeforeIntentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSemanticProofFixture(t, backend)
			trigger := f.submit(t, semanticProofJobflowEvent, semanticProofRows(1), 75)
			semanticProofWait(t, func() (bool, error) {
				var status string
				err := f.db.QueryRowContext(f.ctx, `SELECT status FROM event_deliveries WHERE run_id=$1 AND event_id=$2`, f.runID, trigger).Scan(&status)
				return status == "dead_letter", err
			})
			var intents, outputs int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, f.runID).Scan(&intents); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2`, f.runID, "portfolio/company.lead").Scan(&outputs); err != nil {
				t.Fatal(err)
			}
			if intents != 0 || outputs != 0 {
				t.Fatalf("unpinned source persisted work: intents=%d outputs=%d", intents, outputs)
			}
		})
	}
}

func TestFanOutPinnedKeylessEmptyAndDuplicateStreamsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, test := range []struct {
			name    string
			input   []byte
			count   int
			starts  int
			trigger string
		}{
			{"empty", nil, 0, 1, semanticProofKeylessEvent},
			{"duplicate-rows-new-trigger", []byte("{\"value\":\"same\"}\n{\"value\":\"same\"}\n"), 2, 2, semanticProofKeylessEvent},
			{"selected-rule-unselected-pin", []byte("{\"value\":\"same\"}\n"), 1, 1, semanticProofRuleDataEvent},
			{"on-complete", []byte("{\"value\":\"same\"}\n"), 1, 1, semanticProofCompleteDataEvent},
		} {
			t.Run(backend+"/"+test.name, func(t *testing.T) {
				ref, err := durabledata.ParseDeclarationRef("portfolio", "portfolio/company.keyless")
				if err != nil {
					t.Fatal(err)
				}
				var version durabledata.VersionID
				f := newSemanticProofFixtureWithPreparation(t, backend, func(f *semanticProofFixture) []durabledata.ExplicitPin {
					bundle, ok := semanticview.Bundle(f.source)
					if !ok {
						t.Fatal("keyless proof requires compiled bundle")
					}
					declaration, ok := bundle.DurableDataDeclarationByRef(ref)
					if !ok || declaration.BusinessKey != "" {
						t.Fatalf("keyless declaration = %+v", declaration)
					}
					compiled, defects := durabledata.CompileJSONL(ref, declaration.Schema, declaration.BusinessKey, test.input)
					if len(defects) != 0 || len(compiled.Rows) != test.count {
						t.Fatalf("keyless import rows=%d defects=%+v", len(compiled.Rows), defects)
					}
					owner := f.selected.(interface {
						ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
					})
					result, err := owner.ExecuteDataSourceOperation(f.ctx, durabledata.SourceCommand{
						Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: bundle.SourceArtifact.BundleHash(),
						Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: test.input,
					})
					if err != nil || result.Candidate.VersionID != compiled.VersionID {
						t.Fatalf("keyless import version=%s want=%s err=%v", result.Candidate.VersionID, compiled.VersionID, err)
					}
					version = result.Candidate.VersionID
					return []durabledata.ExplicitPin{{Declaration: ref, VersionID: version}}
				})
				var dormant int
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1`, f.runID).Scan(&dormant); err != nil || dormant != 0 {
					t.Fatalf("pin alone issued keyless work: %d err=%v", dormant, err)
				}
				for attempt := 0; attempt < test.starts; attempt++ {
					trigger := f.submit(t, test.trigger, semanticProofRows(1), 75)
					waitNotifyAllChildrenRuntimeWithin(t, f.runtime, f.runID, 30*time.Second)
					rows, err := f.db.QueryContext(f.ctx, `SELECT o.ordinal,o.outcome_kind,e.payload_bytes
						FROM fan_out_outcomes o JOIN event_deliveries d ON d.delivery_id=o.triggering_delivery_id
						JOIN events e ON e.event_id=o.event_id WHERE o.run_id=$1 AND d.event_id=$2 ORDER BY o.ordinal`, f.runID, trigger)
					if err != nil {
						t.Fatal(err)
					}
					count := 0
					for rows.Next() {
						var ordinal int
						var kind string
						var payload []byte
						if err := rows.Scan(&ordinal, &kind, &payload); err != nil {
							t.Fatal(err)
						}
						if ordinal != count || kind != "committed" || string(payload) != `{"value":"same"}` {
							t.Fatalf("keyless outcome %d: ordinal=%d kind=%s payload=%s", count, ordinal, kind, payload)
						}
						count++
					}
					if err := rows.Close(); err != nil {
						t.Fatal(err)
					}
					if count != test.count {
						t.Fatalf("keyless outcomes=%d want=%d", count, test.count)
					}
				}
				var intents, delivered int
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND source_resource_version_id=$2`, f.runID, version).Scan(&intents); err != nil {
					t.Fatal(err)
				}
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='portfolio/company.keyless' AND d.status='delivered'`, f.runID).Scan(&delivered); err != nil {
					t.Fatal(err)
				}
				if intents != test.starts || delivered != test.count*test.starts {
					t.Fatalf("keyless stream multiplicity: intents=%d delivered=%d want=%d/%d", intents, delivered, test.starts, test.count*test.starts)
				}
			})
		}
	}
}

func TestFanOutPinnedResourceUsesCompiledParentChildConnectBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ref, err := durabledata.ParseDeclarationRef("portfolio", "portfolio/account.registered")
			if err != nil {
				t.Fatal(err)
			}
			f := newSemanticProofFixtureWithSourcePreparation(t, backend, semanticProofSourceWithCrossFlow(t, true), func(f *semanticProofFixture) []durabledata.ExplicitPin {
				bundle, ok := semanticview.Bundle(f.source)
				if !ok {
					t.Fatal("cross-flow proof requires compiled bundle")
				}
				declaration, ok := bundle.DurableDataDeclarationByRef(ref)
				if !ok || declaration.BusinessKey != "account_id" {
					t.Fatalf("cross-flow declaration = %+v", declaration)
				}
				var input bytes.Buffer
				for index, accountID := range []string{"gamma", "alpha", "beta"} {
					row := map[string]any{
						"portfolio_id": f.runID, "account_id": accountID, "eng_roles": index + 1,
						"gem_score": 7.5, "external_id": uuid.NewString(), "eligible": true,
						"ordinal": index, "source_count": 3, "snapshot_threshold": 75,
					}
					encoded, err := json.Marshal(row)
					if err != nil {
						t.Fatal(err)
					}
					input.Write(encoded)
					input.WriteByte('\n')
				}
				compiled, defects := durabledata.CompileJSONL(ref, declaration.Schema, declaration.BusinessKey, input.Bytes())
				if len(defects) != 0 || len(compiled.Rows) != 3 {
					t.Fatalf("cross-flow keyed import: rows=%d defects=%+v", len(compiled.Rows), defects)
				}
				owner := f.selected.(interface {
					ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
				})
				result, err := owner.ExecuteDataSourceOperation(f.ctx, durabledata.SourceCommand{
					Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: bundle.SourceArtifact.BundleHash(),
					Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: input.Bytes(),
				})
				if err != nil || result.Candidate.VersionID != compiled.VersionID {
					t.Fatalf("cross-flow import version=%s want=%s err=%v", result.Candidate.VersionID, compiled.VersionID, err)
				}
				return []durabledata.ExplicitPin{{Declaration: ref, VersionID: result.Candidate.VersionID}}
			})
			trigger := f.submit(t, semanticProofResourceEvent, semanticProofRows(1), 75)
			waitNotifyAllChildrenRuntimeWithin(t, f.runtime, f.runID, 30*time.Second)
			rows, err := f.db.QueryContext(f.ctx, `SELECT o.ordinal,e.payload_bytes FROM fan_out_outcomes o
				JOIN event_deliveries d ON d.delivery_id=o.triggering_delivery_id JOIN events e ON e.event_id=o.event_id
				WHERE o.run_id=$1 AND d.event_id=$2 ORDER BY o.ordinal`, f.runID, trigger)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for rows.Next() {
				var ordinal int
				var payload []byte
				if err := rows.Scan(&ordinal, &payload); err != nil {
					t.Fatal(err)
				}
				var row struct {
					AccountID string `json:"account_id"`
				}
				if err := json.Unmarshal(payload, &row); err != nil {
					t.Fatal(err)
				}
				if ordinal != count || row.AccountID != []string{"alpha", "beta", "gamma"}[count] {
					t.Fatalf("cross-flow keyed row %d: ordinal=%d payload=%s", count, ordinal, payload)
				}
				count++
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			var children, delivered int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='account'`, f.runID).Scan(&children); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND e.event_name='portfolio/account.registered' AND d.status='delivered'`, f.runID).Scan(&delivered); err != nil {
				t.Fatal(err)
			}
			if count != 3 || children != 3 || delivered != 3 {
				t.Fatalf("compiled parent-child route: outcomes=%d children=%d delivered=%d", count, children, delivered)
			}
		})
	}
}

func TestFanOutPinnedResourceForkBeforeIntentRefusesUnsupportedReplayWithoutMutationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ref, err := durabledata.ParseDeclarationRef("portfolio", "portfolio/company.keyless")
			if err != nil {
				t.Fatal(err)
			}
			var original durabledata.VersionID
			f := newSemanticProofFixtureWithPreparation(t, backend, func(f *semanticProofFixture) []durabledata.ExplicitPin {
				bundle, ok := semanticview.Bundle(f.source)
				if !ok {
					t.Fatal("fork source requires compiled bundle")
				}
				owner := f.selected.(interface {
					ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
				})
				result, err := owner.ExecuteDataSourceOperation(f.ctx, durabledata.SourceCommand{
					Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator",
					BundleHash: bundle.SourceArtifact.BundleHash(), Declaration: ref,
					ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte("{\"value\":\"original\"}\n"),
				})
				if err != nil {
					t.Fatal(err)
				}
				original = result.Candidate.VersionID
				return []durabledata.ExplicitPin{{Declaration: ref, VersionID: original}}
			})
			bundle, _ := semanticview.Bundle(f.source)
			owner := f.selected.(interface {
				ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
				MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
			})
			alternate, err := owner.ExecuteDataSourceOperation(f.ctx, durabledata.SourceCommand{
				Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator",
				BundleHash: bundle.SourceArtifact.BundleHash(), Declaration: ref,
				ExpectedHead: durabledata.VersionHead(original), InputFormat: "jsonl", Input: []byte("{\"value\":\"alternate\"}\n"),
			})
			if err != nil || alternate.Candidate.VersionID == original {
				t.Fatalf("alternate import: version=%s err=%v", alternate.Candidate.VersionID, err)
			}
			var firstEvent string
			if err := f.db.QueryRowContext(f.ctx, `SELECT event_id FROM events WHERE run_id=$1 ORDER BY created_at,event_id LIMIT 1`, f.runID).Scan(&firstEvent); err != nil {
				t.Fatal(err)
			}
			carriage, err := semanticview.CompileOriginalLoopCarriage(f.source)
			if err != nil {
				t.Fatal(err)
			}
			var beforeRuns, beforePins, beforeIntents int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM runs`).Scan(&beforeRuns); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM resource_version_pins`).Scan(&beforePins); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_intents`).Scan(&beforeIntents); err != nil {
				t.Fatal(err)
			}
			_, err = owner.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{
				SourceRunID: f.runID, At: firstEvent, OriginalLoopCarriage: carriage,
				DataPinOverrides: []durabledata.ExplicitPin{{Declaration: ref, VersionID: alternate.Candidate.VersionID}},
			})
			if err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported) ||
				!strings.Contains(err.Error(), runfork.RunForkBlockerFlowRouteHistoryUnproven) {
				t.Fatalf("unsupported pre-intent fork refusal = %v", err)
			}
			var afterRuns, afterPins, afterIntents int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM runs`).Scan(&afterRuns); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM resource_version_pins`).Scan(&afterPins); err != nil {
				t.Fatal(err)
			}
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_intents`).Scan(&afterIntents); err != nil {
				t.Fatal(err)
			}
			if beforeRuns != afterRuns || beforePins != afterPins || beforeIntents != afterIntents {
				t.Fatalf("unsupported fork mutated domain rows: runs %d/%d pins %d/%d intents %d/%d", beforeRuns, afterRuns, beforePins, afterPins, beforeIntents, afterIntents)
			}
		})
	}
}
