package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestIssue2394NestedCapacityOneHandoffBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			proveNestedCapacityOneHandoff(t, backend, []string{"sibling-c", "sibling-a", "sibling-b"}, false, 5*time.Second, 30*time.Second)
		})
	}
}

func TestIssue2394NestedDirectBarrierMembershipBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			proveNestedCapacityOneHandoff(t, backend, []string{"sibling-c", "sibling-a", "sibling-b"}, true, 5*time.Second, 30*time.Second)
		})
	}
}

func TestIssue2394NestedManyIntentRetainedHandoffBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			accounts := make([]string, 18)
			for i := range accounts {
				accounts[i] = fmt.Sprintf("sibling-%02d", i)
			}
			childEntryTimeout := 5 * time.Second
			if fanOutRaceBuild {
				// Gate A 5749347759 preserves every subsequent phase deadline.
				childEntryTimeout = time.Minute
			}
			finalDrainTimeout := 30 * time.Second
			if backend == "sqlite" && fanOutRaceBuild {
				// Lead exception 5752572474 changes only M29's SQLite race drain.
				finalDrainTimeout = time.Minute
			}
			proveNestedCapacityOneHandoff(t, backend, accounts, true, childEntryTimeout, finalDrainTimeout)
		})
	}
}

func proveNestedCapacityOneHandoff(t *testing.T, backend string, accounts []string, holdChild bool, childEntryTimeout, finalDrainTimeout time.Duration, rejection ...*nestedPreparedRejection) {
	t.Helper()
	source := loadCanonicalRoutingSource(t, canonicalrouting.CopyNotifyAllChildrenNestedServing(t))
	probe := newNestedServingProbe(t)
	if len(rejection) == 1 {
		probe.rejection = rejection[0]
	}
	var gates []*nestedChildHandlerGate
	if holdChild {
		gates = append(gates, newNestedChildHandlerGate(t, "account.task.requested"))
	}
	rt, db := newNestedServingRuntime(t, backend, source, nil, probe, gates...)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("nested serving failure counters: %+v", probe.snapshot())
			dumpNotifyAllChildrenRuntimeState(t, context.Background(), rt.selected, db)
			for _, entry := range rt.diagnostics.snapshot() {
				t.Logf("nested runtime diagnostic: %+v", entry)
				if entry.Failure != nil {
					t.Logf("nested typed failure: %+v", *entry.Failure)
				}
			}
		}
	})
	runID := uuid.NewString()
	ctx := correlation.WithRunID(testAuthorActivityContextForBundle(context.Background(), rt.sourceArtifactFact), runID)
	if err := rt.manager.Run(managedConformanceExecutionContextForBundle(t, ctx, "nested-capacity-one", rt.sourceArtifactFact)); err != nil {
		t.Fatal(err)
	}
	publishNotifyAllChildrenRunCreatingEvent(t, ctx, rt, source, runID, "portfolio.opened", map[string]any{"portfolio_id": "portfolio-main"})
	if probe.rejection != nil {
		publishNotifyAllChildrenEventAsync(t, ctx, rt, source, runID, "portfolio.accounts.register.requested", map[string]any{"portfolio_id": "portfolio-main", "account_ids": accounts})
		probe.rejection.assertReleasedBeforeRetry(t, ctx, db, probe, runID, len(accounts))
	} else {
		publishNotifyAllChildrenEvent(t, ctx, rt, source, runID, "portfolio.accounts.register.requested", map[string]any{"portfolio_id": "portfolio-main", "account_ids": accounts})
	}
	waitNotifyAllChildrenRuntime(t, rt, runID)
	before := probe.snapshot()
	probe.armPostCommitHold(t)
	notifyID := publishNotifyAllChildrenEventAsync(t, ctx, rt, source, runID, "portfolio.notify.requested", map[string]any{"portfolio_id": "portfolio-main", "command": "nested-proof"})
	held := waitNestedServingPostCommit(t, probe)
	if held.ParentEvent != notifyID || held.Publications != len(accounts) {
		t.Fatalf("held real parent commit=%+v, want trigger=%s cardinality=%d", held, notifyID, len(accounts))
	}
	counts := probe.snapshot()
	if counts.Started != before.Started+1 || counts.Returned != before.Returned {
		t.Fatalf("capacity-one postcommit permit escaped: before=%+v held=%+v", before, counts)
	}
	parents := loadNotifyAllChildrenItemEvents(t, ctx, rt.selected, db, runID, notifyID)
	assertNotifyAllChildrenItemSequence(t, parents, accounts)
	childEntryDeadline := time.Now().Add(childEntryTimeout)
	probe.releaseHeld()
	if holdChild {
		signal := gates[0].waitUntil(t, childEntryDeadline)
		t.Logf("parent release to held-child entry=%s bound=%s race=%t", childEntryTimeout-time.Until(childEntryDeadline), childEntryTimeout, fanOutRaceBuild)
		deadline := time.Now().Add(5 * time.Second)
		var parentStatus string
		for time.Now().Before(deadline) {
			if err := db.QueryRowContext(ctx, `SELECT status FROM fan_out_obligation_barriers WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`, held.Key.RunID, held.Key.TriggeringDeliveryID, held.Key.ElementRef.FlowPath, held.Key.ElementRef.Family, held.Key.ElementRef.SemanticPath).Scan(&parentStatus); err != nil {
				t.Fatal(err)
			}
			if parentStatus == "fired" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if parentStatus != "fired" {
			t.Fatalf("parent direct barrier waited on held grandchild: %s", parentStatus)
		}
		assertNestedExactBarrier(t, ctx, db, held.Key, "fired", fanoutbarrier.Summary{Total: len(accounts), Succeeded: len(accounts)})
		var intents, owed int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(cardinality-cursor),0) FROM fan_out_intents WHERE run_id=$1`, runID).Scan(&intents, &owed); err != nil {
			t.Fatal(err)
		}
		if intents != len(accounts)+2 || owed != 2*(len(accounts)-1) {
			t.Fatalf("held real child backlog: intents=%d owed=%d want=%d/%d", intents, owed, len(accounts)+2, 2*(len(accounts)-1))
		}
		view, err := nestedPublicReader(t, rt.selected).LoadOperatorEvent(ctx, signal.EventID)
		if err != nil || len(view.Deliveries) != 1 || view.Deliveries[0].Terminal || view.NoDelivery != nil {
			t.Fatalf("held actual child public disposition=%+v err=%v", view, err)
		}
		assertNestedPredecessorsSettledWhileChildHeld(t, ctx, db, nestedPublicReader(t, rt.selected), parents, signal.EventID)
		effectName := eventidentity.ExternalizeForFlow(view.Deliveries[0].Target.FlowInstance, []string{"account.task.completed"}, "account.task.completed")
		if ids := nestedEventIDs(t, ctx, db, runID, effectName, signal.EventID); len(ids) != 0 {
			t.Fatalf("held child published business effects early: %v", ids)
		}
		// Let escaped sibling/per-ordinal work become observable while the
		// actual child handler and same-budget postcommit caller remain held.
		time.Sleep(200 * time.Millisecond)
		counts := probe.snapshot()
		if counts.Active != 1 || counts.PeakActive != 1 || counts.CommitPlans != 2 || counts.Carriers != 2 || counts.Started != before.Started+2 {
			t.Fatalf("held child escaped finite owner bounds: %+v", counts)
		}
		probe.publications.mu.Lock()
		live, peak, lifeErr := len(probe.publications.live), probe.publications.peak, probe.publications.err
		probe.publications.mu.Unlock()
		if live != 2 || peak > 32 || lifeErr != nil {
			t.Fatalf("held actual child publication lifetime live=%d peak=%d err=%v", live, peak, lifeErr)
		}
		t.Logf("held child actual event=%s handler=%s: parent direct barrier fired; durable intents=%d owed=%d live plans/carriers=%d highwater=%d", signal.EventID, signal.SubscriberID, intents, owed, live, peak)
		gates[0].open()
	}
	drainStarted := time.Now()
	defer func() {
		if t.Failed() {
			t.Logf("final-drain observation elapsed=%s original_target=30s merge_ceiling=%s backend=%s race=%t", time.Since(drainStarted), finalDrainTimeout, backend, fanOutRaceBuild)
		}
	}()
	waitNotifyAllChildrenRuntimeWithin(t, rt, runID, finalDrainTimeout)
	t.Logf("final drain elapsed=%s original_target=30s merge_ceiling=%s backend=%s race=%t; original performance obligation remains open in #2394", time.Since(drainStarted), finalDrainTimeout, backend, fanOutRaceBuild)
	assertNestedServingDrained(t, probe)

	reader := nestedPublicReader(t, rt.selected)
	for _, parent := range parents {
		assertNestedDeliveredEvent(t, ctx, reader, parent.ID, 1)
	}
	assertNestedSiblingTaskEffects(t, ctx, rt.selected, db, runID,
		"account.task.requested", "account.task.completed", parents)
	assertNestedExactBarrier(t, ctx, db, held.Key, "fired", fanoutbarrier.Summary{Total: len(accounts), Succeeded: len(accounts)})
	completionID := loadNotifyAllChildrenSingleEventID(t, ctx, rt.selected, db, runID, source.ResolveFlowEventReference("portfolio", "portfolio.notify.completed"))
	completion, err := reader.LoadOperatorEvent(ctx, completionID)
	if err != nil || completion.Payload["total"] != float64(len(accounts)) || completion.Payload["succeeded"] != float64(len(accounts)) {
		t.Fatalf("parent direct-membership public summary=%+v err=%v", completion, err)
	}
	for _, field := range []string{"dead_lettered", "no_route", "semantic_rejected", "canceled"} {
		if completion.Payload[field] != float64(0) {
			t.Fatalf("parent public completion %s=%#v, want 0", field, completion.Payload[field])
		}
	}
	if completion.NoDelivery == nil || len(completion.Deliveries) != 0 {
		t.Fatalf("parent completion must have exact external settlement: %+v", completion)
	}
	var childCompletions []string
	for _, parent := range parents {
		view := assertNestedDeliveredEvent(t, ctx, reader, parent.ID, 1)
		name := eventidentity.ExternalizeForFlow(view.Deliveries[0].Target.FlowInstance, []string{"account.tasks.completed"}, "account.tasks.completed")
		completionRows, err := db.QueryContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2`, runID, name)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for completionRows.Next() {
			var id string
			if err := completionRows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			childCompletions = append(childCompletions, id)
			count++
		}
		err = completionRows.Err()
		completionRows.Close()
		if err != nil || count != 1 {
			t.Fatalf("nested sibling %s completion name=%s count=%d err=%v", parent.AccountID, name, count, err)
		}
	}
	if err != nil || len(childCompletions) != len(accounts) {
		t.Fatalf("nested public completions=%v err=%v", childCompletions, err)
	}
	seenCompletions := map[string]bool{}
	for _, id := range childCompletions {
		view, err := reader.LoadOperatorEvent(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		account, _ := view.Payload["account_id"].(string)
		if account == "" || seenCompletions[account] || view.NoDelivery == nil || len(view.Deliveries) != 0 {
			t.Fatalf("nested public completion identity/settlement=%+v", view)
		}
		seenCompletions[account] = true
		for field, want := range map[string]float64{"total": 2, "succeeded": 2, "dead_lettered": 0, "no_route": 0, "semantic_rejected": 0, "canceled": 0} {
			if view.Payload[field] != want {
				t.Fatalf("nested sibling %s public completion %s=%#v, want %v", account, field, view.Payload[field], want)
			}
		}
	}
	for _, account := range accounts {
		if !seenCompletions[account] {
			t.Fatalf("missing exact sibling public completion %s", account)
		}
	}

	// Every nested turn must acquire the single permit only after the real
	// parent caller has returned from its actual finalization/dispatch.
	var parentReturned time.Time
	nested := map[string]nestedServingReceipt{}
	for i := 0; i < probe.snapshot().Returned; i++ {
		r := <-probe.receipts
		if r.Err != nil {
			if probe.rejection != nil && probe.rejection.matchesReceipt(r) {
				continue
			}
			t.Fatalf("nested finite caller failed: %+v", r)
		}
		if r.Key == held.Key {
			parentReturned = r.ReturnedAt
		}
		for _, parent := range parents {
			if r.ParentEvent == parent.ID {
				if _, duplicate := nested[parent.ID]; duplicate {
					t.Fatalf("duplicate nested finite turn for %s", parent.ID)
				}
				nested[parent.ID] = r
			}
		}
	}
	if parentReturned.IsZero() || len(nested) != len(parents) {
		t.Fatalf("actual nested permit receipts: parent_return=%s nested=%+v", parentReturned, nested)
	}
	for _, parent := range parents {
		r := nested[parent.ID]
		if r.LoadedAt.Before(parentReturned) || r.Publications != 2 || r.Key == held.Key {
			t.Fatalf("nested intent requires independent post-parent turn: parent=%+v nested=%+v", held, r)
		}
		assertNestedExactBarrier(t, ctx, db, r.Key, "fired", fanoutbarrier.Summary{Total: 2, Succeeded: 2})
	}
	rows, err := db.QueryContext(ctx, `SELECT flow_instance,current_state,fields FROM entity_state WHERE run_id=$1`, runID)
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]string{}
	for rows.Next() {
		var path, state string
		var raw []byte
		if err := rows.Scan(&path, &state, &raw); err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		if task, ok := fields["task"].(string); ok {
			account, _ := fields["account_id"].(string)
			key := account + "/" + task
			if state != "completed" || states[key] != "" || path == "" || fields["task_key"] != account+":"+task {
				t.Fatalf("nested task state identity=%s path=%s state=%s duplicate=%s", key, path, state, states[key])
			}
			states[key] = path
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(states) != 2*len(accounts) {
		t.Fatalf("nested actual task state count=%d err=%v", len(states), err)
	}
	for _, account := range accounts {
		for _, task := range []string{"prepare", "publish"} {
			if states[account+"/"+task] == "" {
				t.Fatalf("missing independent nested state %s/%s", account, task)
			}
		}
	}
	summary, err := rt.selected.FanOutRunSummary(ctx, runID, time.Now().UTC())
	if err != nil || summary.BlocksCompletion() || summary.BarrierTerminal != len(accounts)+1 {
		t.Fatalf("nested final public summary=%+v err=%v", summary, err)
	}
	if probe.rejection != nil {
		probe.rejection.assertFinal(t, probe, 4*len(accounts))
	}
	t.Logf("M18 actual capacity-one nested serving: %d distinct child intents, %d terminal task entities/effects, %d exact direct barriers; parent handoff returned=%s", len(accounts), 2*len(accounts), len(accounts)+1, parentReturned)
}
