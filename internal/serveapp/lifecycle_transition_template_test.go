package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

type lifecycleTemplateSibling struct {
	side, flow, instance, entity, revision, gateInstance, gateEntity string
	decision                                                         map[string]any
	gate                                                             gateruntime.Activation
}

func startLifecycleTemplateRuntime(t *testing.T, backend servedparity.Backend, root string) servedControlProofRuntime {
	t.Helper()
	store := "sqlite"
	if backend == servedparity.BackendExplicitPostgres {
		store = "postgres"
	}
	// This larger source repeats full boot verification; no verifier is disabled.
	_, start := lifecycleRestartHarness(t, store, root, 3*time.Minute)
	_, rt := start()
	return rt
}

func readLifecycleTemplateGate(t *testing.T, rt servedControlProofRuntime, runID, entityID, flow string) gateruntime.Activation {
	t.Helper()
	var raw string
	if err := rt.DB.QueryRow(`SELECT CAST(accumulator AS TEXT) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, runID, entityID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var buckets map[string]map[string]any
	if err := json.Unmarshal([]byte(raw), &buckets); err != nil {
		t.Fatal(err)
	}
	gate, found, err := gateruntime.Load(buckets, flow, "review_decision")
	if err != nil || !found {
		t.Fatalf("gate flow=%s: found=%t err=%v accumulator=%s", flow, found, err, raw)
	}
	return gate
}

func requireLifecycleTemplateInstance(t *testing.T, rt servedControlProofRuntime, runID, instance, flow string) {
	t.Helper()
	var template string
	if err := rt.DB.QueryRow(`SELECT flow_template FROM flow_instances WHERE run_id=$1 AND instance_path=$2`, runID, instance).Scan(&template); err != nil {
		t.Fatal(err)
	}
	if template != flow {
		t.Fatalf("instance %s template=%s want=%s", instance, template, flow)
	}
}

func requireLifecycleTemplateEntity(t *testing.T, rt servedControlProofRuntime, runID, flow, state string) (string, string) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); {
		var entity, instance string
		err := rt.DB.QueryRow(`SELECT e.entity_id,e.flow_instance FROM entity_state e JOIN flow_instances f ON f.run_id=e.run_id AND f.instance_path=e.flow_instance WHERE e.run_id=$1 AND f.flow_template=$2 AND e.current_state=$3`, runID, flow, state).Scan(&entity, &instance)
		if err == nil {
			return entity, instance
		}
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("logical template %s did not reach %s\n%s", flow, state, servedEventPublishDebugSummary(t, rt.DB, rt.Backend, runID))
	return "", ""
}

func logLifecycleTemplateCompletionRows(t *testing.T, rt servedControlProofRuntime, runID string) {
	t.Helper()
	queries := []struct{ label, sql string }{
		{"events", `SELECT event_id,event_name,CAST(payload AS TEXT),routing_source_kind,CAST(source_route AS TEXT),CAST(target_route AS TEXT),CAST(route_settlement AS TEXT) FROM events WHERE run_id=$1 AND (event_name LIKE '%work.completed' OR event_name='mailbox.card_decided') ORDER BY created_at,event_id`},
		{"deliveries", `SELECT d.delivery_id,d.event_id,d.subscriber_id,d.status,COALESCE(d.reason_code,''),CAST(d.delivery_target_route AS TEXT) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE e.run_id=$1 AND (e.event_name LIKE '%work.completed' OR e.event_name='mailbox.card_decided') ORDER BY d.created_at,d.delivery_id`},
		{"receipts", `SELECT r.event_id,r.subscriber_id,r.outcome,COALESCE(r.reason_code,''),CAST(r.side_effects AS TEXT) FROM event_receipts r JOIN events e ON e.event_id=r.event_id WHERE e.run_id=$1 AND (e.event_name LIKE '%work.completed' OR e.event_name='mailbox.card_decided') ORDER BY r.processed_at`},
	}
	for _, q := range queries {
		rows, err := rt.DB.Query(q.sql, runID)
		if err != nil {
			t.Logf("TEMPLATE_COMPLETION %s query error: %v", q.label, err)
			continue
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			values := make([]sql.NullString, len(columns))
			args := make([]any, len(columns))
			for i := range values {
				args[i] = &values[i]
			}
			if err := rows.Scan(args...); err != nil {
				t.Fatal(err)
			}
			t.Logf("TEMPLATE_COMPLETION run=%s table=%s columns=%v row=%v", runID, q.label, columns, values)
			count++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		t.Logf("TEMPLATE_COMPLETION run=%s table=%s count=%d", runID, q.label, count)
	}
}

// Both siblings reuse the same public template key and all local lifecycle names.
// Inputs go through the root's compiled connects, never a seeded instance row.
func prepareLifecycleTemplateSiblings(t *testing.T, rt servedControlProofRuntime) (string, string, []lifecycleTemplateSibling) {
	t.Helper()
	var runID, sourceEvent string
	var siblings []lifecycleTemplateSibling
	for _, side := range []string{"left", "right"} {
		s := lifecycleTemplateSibling{side: side, flow: "outer/" + side}
		publish := func(command, key string, payload map[string]any) servedEventPublishRPCResult {
			payload["case_id"] = "shared"
			params := map[string]any{"event_name": side + ".command." + command, "payload": payload, "idempotency_key": side + "-" + key}
			if runID == "" {
				params["bundle_hash"] = rt.BundleHash
			} else {
				params["run_id"], params["source_event_id"] = runID, sourceEvent
			}
			return requireServedEventPublishRPCResult(t, rt.Endpoint, params)
		}
		seed := publish("seed", "seed", map[string]any{"seed": true})
		if runID != "" && seed.RunID != runID {
			t.Fatal("sibling escaped its run")
		}
		runID, sourceEvent = seed.RunID, seed.EventID
		s.entity, s.instance = requireLifecycleTemplateEntity(t, rt, runID, s.flow, "drafting")
		requireLifecycleTemplateInstance(t, rt, runID, s.instance, s.flow)
		var keyed operatorread.OperatorEntityFull
		requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": s.entity}, &keyed)
		if keyed.Fields["case_id"] != "shared" || s.instance == s.flow {
			t.Fatalf("actual template key/instance missing: entity=%#v instance=%s", keyed, s.instance)
		}
		for attempt := 1; attempt <= 2; attempt++ {
			loop := readLifecycleLoop(t, rt, runID, s.entity)
			if loop.Attempt != attempt {
				t.Fatalf("%s loop attempt=%d want=%d", side, loop.Attempt, attempt)
			}
			publish("admit", fmt.Sprintf("admit-%d", attempt), map[string]any{"revision_id": loop.RevisionID})
			requireLifecycleTemplateEntity(t, rt, runID, s.flow, "review")
			publish("repeat", fmt.Sprintf("repeat-%d", attempt), map[string]any{"revision_id": loop.RevisionID})
			state := "drafting"
			if attempt == 2 {
				state = "escaped"
			}
			requireLifecycleTemplateEntity(t, rt, runID, s.flow, state)
		}
		s.revision = readLifecycleLoop(t, rt, runID, s.entity).RevisionID
		s.gateEntity, s.gateInstance = requireLifecycleTemplateEntity(t, rt, runID, s.flow+"/sink", "review")
		requireLifecycleTemplateInstance(t, rt, runID, s.gateInstance, s.flow+"/sink")
		var rawConfig string
		if err := rt.DB.QueryRow(`SELECT CAST(config AS TEXT) FROM flow_instances WHERE run_id=$1 AND instance_path=$2`, runID, s.gateInstance).Scan(&rawConfig); err != nil {
			t.Fatal(err)
		}
		var nestedConfig map[string]any
		if err := json.Unmarshal([]byte(rawConfig), &nestedConfig); err != nil {
			t.Fatal(err)
		}
		if nestedConfig["parent_flow_id"] != s.flow || nestedConfig["parent_flow_instance"] != s.instance || nestedConfig["parent_entity_id"] != s.entity {
			t.Fatalf("nested receiver lost exact concrete parent: %s", rawConfig)
		}
		var entity operatorread.OperatorEntityFull
		requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": s.gateEntity}, &entity)
		if entity.Fields["revision_id"] != s.revision {
			t.Fatalf("%s nested gate consumed wrong revision: %#v", side, entity)
		}
		history := readLifecycleTransitionHistory(t, rt, runID, s.entity)
		if len(history) != 5 {
			t.Fatalf("%s loop history=%#v", side, history)
		}
		for i, record := range history {
			compiled, ok := record.Evidence.Compiled()
			if !ok || compiled.FlowID() != s.flow || compiled.Edge().LoopID != "revision" {
				t.Fatalf("%s history[%d] borrowed template identity: %#v", side, i, record)
			}
			if i == 2 && compiled.Edge().Source != "loop.repeat" || i == 4 && compiled.Edge().Source != "loop.escape" {
				t.Fatalf("%s ordinary/cap selection history[%d]=%#v", side, i, compiled)
			}
		}
		s.gate = readLifecycleTemplateGate(t, rt, runID, s.gateEntity, s.flow+"/sink")
		var cards struct {
			Items []struct {
				DecisionCard struct {
					CardID string `json:"card_id"`
				} `json:"decision_card"`
			} `json:"items"`
		}
		requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.list", map[string]any{"run_id": runID, "entity_id": s.gateEntity, "status": "pending"}, &cards)
		if len(cards.Items) != 1 || cards.Items[0].DecisionCard.CardID != s.gate.CardID {
			t.Fatalf("%s filtered mailbox does not identify exact activation: %#v gate=%#v", side, cards, s.gate)
		}
		s.decision = lifecycleDecisionParamsForCard(t, rt, s.gate.CardID, "approve")
		s.decision["idempotency_key"] = side + "-template-decide"
		t.Logf("TEMPLATE_PENDING run=%s side=%s instance=%s entity=%s revision=%s gate_instance=%s gate_entity=%s activation=%s card=%s routes=%s", runID, side, s.instance, s.entity, s.revision, s.gateInstance, s.gateEntity, s.gate.ActivationID, s.gate.CardID, s.gate.RoutesJSON)
		siblings = append(siblings, s)
	}
	if siblings[0].entity == siblings[1].entity || siblings[0].revision == siblings[1].revision || siblings[0].gateEntity == siblings[1].gateEntity || siblings[0].gate.CardID == siblings[1].gate.CardID || siblings[0].gate.ActivationID == siblings[1].gate.ActivationID {
		t.Fatal("same-key sibling lifecycle identities aliased")
	}
	return runID, sourceEvent, siblings
}

func requireLifecycleTemplateVerdict(t *testing.T, rt servedControlProofRuntime, runID string, s lifecycleTemplateSibling) {
	t.Helper()
	requireLifecycleTemplateEntity(t, rt, runID, s.flow+"/sink", "approved")
	final, _ := requireLifecycleTemplateEntity(t, rt, runID, s.flow+"/sink/final", "done")
	var entity operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": final}, &entity)
	if entity.Fields["result"] != s.side {
		t.Fatalf("%s verdict reached wrong concrete consumer: %#v", s.side, entity)
	}
	history := readLifecycleTransitionHistory(t, rt, runID, s.gateEntity)
	if len(history) != 2 {
		t.Fatalf("%s gate history=%#v", s.side, history)
	}
	frozen, err := gateruntime.RouteFor(s.gate.RoutesJSON, "approve")
	if err != nil {
		t.Fatal(err)
	}
	compiled, ok := history[1].Evidence.Compiled()
	if !ok || !reflect.DeepEqual(compiled, frozen.Transition) || compiled.FlowID() != s.flow+"/sink" || compiled.Edge().DecisionID != "review_decision" || compiled.Edge().Verdict != "approve" {
		t.Fatalf("%s gate lost exact frozen source: history=%#v frozen=%#v", s.side, history, frozen)
	}
	gate := readLifecycleTemplateGate(t, rt, runID, s.gateEntity, s.flow+"/sink")
	if gate.CardID != s.gate.CardID || gate.ActivationID != s.gate.ActivationID || gate.RoutesJSON != s.gate.RoutesJSON || gate.DecisionEventID != history[1].TriggerEventID {
		t.Fatalf("%s gate receipt split activation/history: %#v", s.side, gate)
	}
	requireLifecycleEventCount(t, rt, runID, s.gateInstance+"/work.completed", 1)
	requireLifecycleEventCount(t, rt, runID, s.flow+"/sink/work.completed", 0)
	t.Logf("TEMPLATE_DECIDED run=%s side=%s card=%s event=%s final=%s compiled=%#v", runID, s.side, gate.CardID, gate.DecisionEventID, final, compiled)
}

func completeLifecycleTemplateSiblings(t *testing.T, rt servedControlProofRuntime, runID string, siblings []lifecycleTemplateSibling) {
	t.Helper()
	start := make(chan struct{})
	var workers sync.WaitGroup
	for _, s := range siblings {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			var result map[string]any
			requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", s.decision, &result)
		}()
	}
	close(start)
	workers.Wait()
	logLifecycleTemplateCompletionRows(t, rt, runID)
	if t.Failed() {
		t.FailNow()
	}
	for _, s := range siblings {
		requireLifecycleTemplateVerdict(t, rt, runID, s)
	}
}

func TestServedCompiledTransitionNestedTemplatesFirstJourneyOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyLifecycleNestedTemplates(t)
			isolateCLIAPIConfigEnv(t)
			var verifyOut, verifyErr bytes.Buffer
			config := writeServeRuntimeTestConfig(t)
			if code := cliapp.Execute(context.Background(), []string{"verify", root, "--config", config}, &verifyOut, &verifyErr, nil, nil); code != 0 {
				t.Fatalf("verify nested templates: code=%d\n%s\n%s", code, &verifyOut, &verifyErr)
			}
			rt := startLifecycleTemplateRuntime(t, backend, root)
			runID, sourceEvent, siblings := prepareLifecycleTemplateSiblings(t, rt)
			completeLifecycleTemplateSiblings(t, rt, runID, siblings)
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, runID)
			var count int
			if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1`, runID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 7 {
				t.Fatalf("root plus two template/second-template/final chains: entities=%d", count)
			}
			for _, s := range siblings {
				requireLifecycleEventCount(t, rt, runID, s.instance+"/loop.escaped", 1)
				requireLifecycleEventCount(t, rt, runID, s.flow+"/loop.escaped", 0)
			}
			before := lifecycleStoredSnapshot(t, rt, runID)
			for _, s := range siblings {
				var duplicate map[string]any
				requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", s.decision, &duplicate)
			}
			if lifecycleStoredSnapshot(t, rt, runID) != before {
				t.Fatal("same-key template verdict replay mutated persisted history/state")
			}
			requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.finished", "run_id": runID, "source_event_id": sourceEvent, "payload": map[string]any{"seed": true}, "idempotency_key": "template-driver-finish"})
			requireLifecycleFlowEntity(t, rt, runID, "", "done")
		})
	}
}

func TestServedCompiledTransitionTemplateSiblingForkOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startLifecycleTemplateRuntime(t, backend, canonicalrouting.CopyLifecycleNestedTemplates(t))
			runID, sourceEvent, siblings := prepareLifecycleTemplateSiblings(t, rt)
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, runID)
			requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": runID, "idempotency_key": "template-pause"})
			frontier := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.observed", "run_id": runID, "source_event_id": sourceEvent, "payload": map[string]any{"seed": true}, "idempotency_key": "template-frontier"})
			before := lifecycleStoredSnapshot(t, rt, runID)
			params := map[string]any{"source_run_id": runID, "fork_event_id": frontier.EventID, "allow_source_freeze": true, "idempotency_key": "template-fork"}
			var fork, duplicate apiv1.RunForkExecutionResult
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &fork)
			requireServedJSONRPCResult(t, rt.Endpoint, "run.fork", params, &duplicate)
			if fork.ForkRunID == "" || fork.ForkRunID == runID || fork.ForkRunID != duplicate.ForkRunID || fork.ExecutedEventCount != 1 {
				t.Fatalf("template fork=%#v duplicate=%#v", fork, duplicate)
			}
			var children []lifecycleTemplateSibling
			for _, parent := range siblings {
				child := parent
				// Fork remaps durable storage identity; select by the exact logical
				// template within this fork, not by a guessed remapping algorithm.
				if err := rt.DB.QueryRow(`SELECT e.entity_id,e.flow_instance FROM entity_state e JOIN flow_instances f ON f.run_id=e.run_id AND f.instance_path=e.flow_instance WHERE e.run_id=$1 AND f.flow_template=$2 AND e.current_state='review'`, fork.ForkRunID, parent.flow+"/sink").Scan(&child.gateEntity, &child.gateInstance); err != nil {
					t.Fatal(err)
				}
				child.gate = readLifecycleTemplateGate(t, rt, fork.ForkRunID, child.gateEntity, child.flow+"/sink")
				if child.gate.CardID == parent.gate.CardID || child.gate.ActivationID == parent.gate.ActivationID || child.gateEntity == parent.gateEntity || child.gateInstance == parent.gateInstance || child.gate.RoutesJSON != parent.gate.RoutesJSON || child.gate.BundleHash != parent.gate.BundleHash {
					t.Fatalf("fork did not remint ownership while preserving exact frozen source: parent=%#v child=%#v", parent, child)
				}
				child.decision = lifecycleDecisionParamsForCard(t, rt, child.gate.CardID, "approve")
				child.decision["idempotency_key"] = child.side + "-fork-decide"
				t.Logf("TEMPLATE_FORK source_run=%s fork_run=%s side=%s parent_card=%s child_card=%s parent_activation=%s child_activation=%s parent_instance=%s child_instance=%s routes=%s", runID, fork.ForkRunID, child.side, parent.gate.CardID, child.gate.CardID, parent.gate.ActivationID, child.gate.ActivationID, parent.gateInstance, child.gateInstance, child.gate.RoutesJSON)
				children = append(children, child)
			}
			if children[0].gate.CardID == children[1].gate.CardID || children[0].gate.ActivationID == children[1].gate.ActivationID || children[0].gateInstance == children[1].gateInstance {
				t.Fatal("fork sibling gates aliased")
			}
			completeLifecycleTemplateSiblings(t, rt, fork.ForkRunID, children)
			waitServedRunDeliveryQuiescence(t, rt.DB, rt.Backend, fork.ForkRunID)
			if lifecycleStoredSnapshot(t, rt, runID) != before {
				t.Fatal("fork sibling verdict changed parent state/history")
			}
			for _, parent := range siblings {
				requireLifecycleEventCount(t, rt, runID, parent.flow+"/sink/work.completed", 0)
				pending := readLifecycleTemplateGate(t, rt, runID, parent.gateEntity, parent.flow+"/sink")
				if !reflect.DeepEqual(pending, parent.gate) {
					t.Fatalf("fork mutated parent frozen gate: before=%#v after=%#v", parent.gate, pending)
				}
				params := lifecycleDecisionParamsForCard(t, rt, parent.gate.CardID, "approve")
				if params["observed_content_hash"] != parent.decision["observed_content_hash"] {
					t.Fatal("fork changed parent card contents")
				}
			}
		})
	}
}
