package serveapp

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
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

// A live handoff pauses its source without ending instances or closing cards.
func requireTemplateForkFreezeDelta(t *testing.T, before, after templateForkSourceEvidence, source operatorread.RunHeader, fork apiv1.RunForkExecutionResult, siblings []lifecycleTemplateSibling) []lifecycleTemplateSibling {
	t.Helper()
	if source.RunID != fork.SourceRunID || source.Status != "paused" || source.EndedAt != nil ||
		source.ContinuedAsRunID != "" || fork.SourceFrozen || fork.SourceRunStatus != source.Status || source.Failure != nil {
		t.Fatalf("fork source lifecycle disagrees with acknowledged handoff: source=%+v fork=%+v", source, fork)
	}
	if len(before.headers) != 8 || len(after.headers) != len(before.headers) || len(before.cards) != 2 || len(after.cards) != 2 {
		t.Fatal("source freeze changed exact header/card cardinality")
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("live handoff changed the paused source's headers, cards or immutable history: before=%+v after=%+v", before, after)
	}
	return append([]lifecycleTemplateSibling(nil), siblings...)
}
