package serveapp

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

type servedEntityFilterList struct {
	Items []struct {
		Kind string `json:"kind"`
		Card struct {
			CardID     string             `json:"card_id"`
			RunID      string             `json:"run_id"`
			Status     string             `json:"status"`
			AnchorKind string             `json:"anchor_kind"`
			Scope      decisioncard.Scope `json:"scope"`
		} `json:"decision_card"`
	} `json:"items"`
	NextCursor string `json:"next_cursor"`
	Unread     *int   `json:"unread_informational_notices"`
}

func startServedMailboxEntityFilterProof(t *testing.T, backend servedparity.Backend) (servedControlProofRuntime, []string, []string, func(map[string]any) servedEntityFilterList, map[string]bool) {
	t.Helper()
	rt := startServedControlProofRuntimeWithFixture(t, backend, func(t *testing.T) string { return canonicalrouting.CopyMailboxEntityFilter(t) })
	var runs, entities, events []string
	for i := 0; i < 3; i++ {
		published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
			"event_name": "review.requested", "bundle_hash": rt.BundleHash,
			"payload": map[string]any{"item_id": uuid.NewString()}, "idempotency_key": uuid.NewString(),
		})
		if !published.NewRunCreated || published.RunID == "" || published.EventID == "" {
			t.Fatalf("event.publish = %+v, want new run", published)
		}
		waitForServedEventPublishNodeDeliveryLifecycleForNode(t, rt.DB, rt.Backend, published.RunID, published.EventID, identitytest.RootNode(t, "reviewer").Key(), rt.Probe)
		entities = append(entities, requireServedEventPublishEntityState(t, rt.DB, rt.Backend, published.RunID, "", "waiting"))
		runs = append(runs, published.RunID)
		events = append(events, published.EventID)
	}
	// Keep the global unread count nonzero outside the selected entity.
	notice := runtimetools.MailboxItem{EventID: events[2], EntityID: entities[2], FlowInstance: runs[2],
		Type: runtimetools.NotifyHumanMailboxItemType, Priority: "normal", Status: "pending",
		Summary: "independent informational notice", Context: json.RawMessage(`{}`)}
	ctx := servedControlProofAuthorActivityContext(t, rt)
	var err error
	if rt.SQLite != nil {
		_, err = rt.SQLite.InsertMailboxItem(ctx, notice)
	} else {
		_, err = rt.Postgres.InsertMailboxItem(ctx, notice)
	}
	if err != nil {
		t.Fatal(err)
	}
	list := func(params map[string]any) servedEntityFilterList {
		t.Helper()
		var result servedEntityFilterList
		requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.list", params, &result)
		if result.Items == nil || result.Unread == nil || *result.Unread != 1 {
			t.Fatalf("mailbox.list envelope/count = %+v", result)
		}
		return result
	}
	all := list(map[string]any{})
	if len(all.Items) != 4 || all.NextCursor != "" {
		t.Fatalf("unfiltered mailbox = %+v, want three real gates and one notice", all)
	}
	wantCards := map[string]bool{}
	for _, item := range all.Items {
		if item.Kind == decisioncard.KindDecisionCard {
			wantCards[item.Card.CardID] = true
		}
	}
	return rt, runs, entities, list, wantCards
}

func TestServedMailboxListEntityFilterOnBothStores(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.RootIngress)
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt, runs, entities, list, wantCards := startServedMailboxEntityFilterProof(t, backend)
			for _, params := range []map[string]any{
				{"entity_id": entities[0]},
				{"entity_id": entities[0], "run_id": runs[0], "status": "pending", "anchor_kind": "stage_gate", "limit": 1},
			} {
				result := list(params)
				if len(result.Items) != 1 || result.NextCursor != "" {
					t.Fatalf("entity-filtered mailbox = %+v, want one gate", result)
				}
				item := result.Items[0]
				if item.Kind != decisioncard.KindDecisionCard || !wantCards[item.Card.CardID] || item.Card.RunID != runs[0] || item.Card.Scope.EntityID != entities[0] || item.Card.AnchorKind != "stage_gate" || item.Card.Status != "pending" {
					t.Fatalf("entity-filtered mailbox leaked another owner: %+v", item)
				}
			}
			for _, params := range []map[string]any{
				{"entity_id": uuid.NewString()},
				{"entity_id": entities[0], "run_id": runs[1]},
				{"entity_id": entities[0], "status": "decided", "anchor_kind": "stage_gate"},
				{"entity_id": entities[0], "anchor_kind": "human_task"},
			} {
				if result := list(params); len(result.Items) != 0 || result.NextCursor != "" {
					t.Fatalf("nonmatching mailbox filters %v leaked %+v", params, result)
				}
			}
			rpcErr := requireServedJSONRPCError(t, rt.Endpoint, "mailbox.list", map[string]any{"entity_id": entities[0], "cursor": "not-a-cursor"})
			if rpcErr.Code != -32602 {
				t.Fatalf("malformed cursor = %+v, want invalid params", rpcErr)
			}
		})
	}
}
