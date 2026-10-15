package serveapp

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type templateForkSourceEvidence struct {
	headers map[string]pipeline.WorkflowTargetPersistenceRecord
	cards   map[string]decisioncard.Card
	changes []decisioncard.Change
}

func readTemplateForkSourceEvidence(t *testing.T, ctx context.Context, reader receiverProofStateReader, cards decisioncard.Store, runID string, siblings []lifecycleTemplateSibling) templateForkSourceEvidence {
	t.Helper()
	result := templateForkSourceEvidence{headers: make(map[string]pipeline.WorkflowTargetPersistenceRecord), cards: make(map[string]decisioncard.Card)}
	headers, err := reader.ListWorkflowInstances(ctx, runID)
	if err != nil || len(headers) != 8 {
		t.Fatalf("source header census: count=%d err=%v", len(headers), err)
	}
	for _, header := range headers {
		owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(header.WorkflowName, flowidentity.LogicalInstanceID(header.StorageRef), header.StorageRef))
		if err != nil {
			t.Fatal(err)
		}
		record, err := reader.LoadWorkflowTargetPersistence(ctx, owner, identity.NormalizeEntityID(header.EntityID))
		if err != nil || record.Validate(owner.Route, identity.NormalizeEntityID(header.EntityID)) != nil || !record.Presence.Constructed() {
			t.Fatalf("source header evidence: %s %+v %v", header.EntityID, record, err)
		}
		if _, exists := result.headers[header.EntityID]; exists {
			t.Fatal("source header census repeated an entity")
		}
		result.headers[header.EntityID] = record
	}
	for _, sibling := range siblings {
		card, err := cards.GetDecisionCard(ctx, sibling.gate.CardID)
		if err != nil || card.Validate() != nil {
			t.Fatalf("source card evidence: %+v %v", card, err)
		}
		result.cards[card.CardID] = card
	}
	var after int64
	for {
		changes, err := cards.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{After: after, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		if len(changes) == 0 {
			break
		}
		for _, change := range changes {
			if change.Sequence <= after {
				t.Fatal("decision change reader failed to advance")
			}
			after = change.Sequence
			if change.RunID == runID {
				result.changes = append(result.changes, change)
			}
		}
	}
	return result
}

// Only the two exact source gate dispositions may change at source freeze.
// Configuration, fields and all other header/accumulator values remain exact.
func requireTemplateForkFreezeDelta(t *testing.T, before, after templateForkSourceEvidence, source operatorread.RunHeader, fork apiv1.RunForkExecutionResult, siblings []lifecycleTemplateSibling) []lifecycleTemplateSibling {
	t.Helper()
	if source.RunID != fork.SourceRunID || source.Status != "forked" || source.EndedAt == nil || source.EndedAt.IsZero() ||
		source.ContinuedAsRunID != fork.ForkRunID || !fork.SourceFrozen || fork.SourceRunStatus != source.Status || source.Failure != nil {
		t.Fatalf("fork source lifecycle disagrees with acknowledged freeze: source=%+v fork=%+v", source, fork)
	}
	if len(before.headers) != 8 || len(after.headers) != len(before.headers) || len(before.cards) != 2 || len(after.cards) != 2 {
		t.Fatal("source freeze changed exact header/card cardinality")
	}
	frozen := append([]lifecycleTemplateSibling(nil), siblings...)
	byEntity := make(map[string]gateruntime.Activation, 2)
	for i := range frozen {
		gate := frozen[i].gate
		if gate.Status != gateruntime.StatusOpen || !gate.Freeze("run_forked", *source.EndedAt) {
			t.Fatal("source freeze lacks its original pending gate")
		}
		frozen[i].gate = gate
		byEntity[frozen[i].gateEntity] = gate
		original, found := before.cards[gate.CardID]
		if !found || original.Status != decisioncard.StatusPending {
			t.Fatal("source freeze lacks its original pending card")
		}
		expected := original
		expected.Status, expected.SupersededReason, expected.UpdatedAt = decisioncard.StatusSuperseded, "run_forked", *source.EndedAt
		if actual, found := after.cards[gate.CardID]; !found || !reflect.DeepEqual(expected, actual) {
			t.Fatalf("source freeze changed more than exact card supersession: expected=%+v actual=%+v", expected, actual)
		}
	}
	for entityID, original := range before.headers {
		actual, found := after.headers[entityID]
		if !found {
			t.Fatalf("source freeze removed header %s", entityID)
		}
		expected := original
		if gate, changed := byEntity[entityID]; changed {
			var accumulator map[string]map[string]any
			if err := canonicaljson.DecodePreservingNumberLexemes(original.Lifecycle.State.Accumulator, &accumulator); err != nil {
				t.Fatal(err)
			}
			if err := gateruntime.Store(accumulator, gate); err != nil {
				t.Fatal(err)
			}
			var actualAccumulator map[string]map[string]any
			if err := canonicaljson.DecodePreservingNumberLexemes(actual.Lifecycle.State.Accumulator, &actualAccumulator); err != nil || !reflect.DeepEqual(accumulator, actualAccumulator) {
				t.Fatalf("source freeze changed non-gate accumulator facts: entity=%s expected=%+v actual=%+v err=%v", entityID, accumulator, actualAccumulator, err)
			}
			// The exact permitted new JSON value was checked above; JSONB encoding
			// is not an additional semantic gate change. Other raw columns stay exact.
			expected.Lifecycle.State.Accumulator = actual.Lifecycle.State.Accumulator
			expected.Lifecycle.State.Revision++
			expected.Lifecycle.State.UpdatedAt = *source.EndedAt
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("source freeze changed unrelated header/field/configuration facts: entity=%s expected=%+v actual=%+v", entityID, expected, actual)
		}
	}
	if len(after.changes) != len(before.changes)+2 || !reflect.DeepEqual(before.changes, after.changes[:len(before.changes)]) {
		t.Fatal("source supersession did not append exactly two changes to immutable history")
	}
	seen := make(map[string]bool, 2)
	for _, change := range after.changes[len(before.changes):] {
		if _, found := before.cards[change.CardID]; !found || seen[change.CardID] || change.ChangeType != decisioncard.ChangeSuperseded ||
			change.RunID != source.RunID || !change.CreatedAt.Equal(*source.EndedAt) || !reflect.DeepEqual(change.Payload.Interface(), map[string]any{"reason": "run_forked"}) {
			t.Fatalf("source supersession evidence differs from the committed freeze: %+v", change)
		}
		seen[change.CardID] = true
	}
	if len(seen) != 2 {
		t.Fatalf("source supersession lost a pending card: %v", seen)
	}
	return frozen
}
