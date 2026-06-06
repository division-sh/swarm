package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_V1MailboxReadDecisionAndIdempotencyOwners(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	runID := uuid.NewString()
	if err := s.AppendEvent(ctx, events.NewRootIngressEvent(sourceEventID,
		"review.requested", "", "", json.RawMessage(`{"request":true}`), 0, runID, "", events.EventEnvelope{}, time.Time{}).
		WithEntityID(entityID).WithFlowInstance("main/review")); err != nil {
		t.Fatalf("append source event: %v", err)
	}

	firstID, err := s.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:   sourceEventID,
		EntityID:  entityID,
		FromAgent: "review-agent",
		Type:      "review_request",
		Priority:  "high",
		Context:   []byte(`{"title":"check"}`),
		Summary:   "review this",
	})
	if err != nil {
		t.Fatalf("insert first mailbox item: %v", err)
	}
	secondID, err := s.InsertMailboxItem(ctx, runtimetools.MailboxItem{
		EventID:   sourceEventID,
		EntityID:  entityID,
		FromAgent: "review-agent",
		Type:      "approval",
		Priority:  "critical",
		Context:   []byte(`{"title":"approve"}`),
		Summary:   "approve this",
	})
	if err != nil {
		t.Fatalf("insert second mailbox item: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE mailbox SET flow_instance = 'main/review' WHERE item_id IN ($1::uuid, $2::uuid)`, firstID, secondID); err != nil {
		t.Fatalf("set flow instance: %v", err)
	}

	page1, cursor, err := s.ListV1MailboxItems(ctx, MailboxV1ListOptions{
		Status:   "pending",
		RunID:    runID,
		EntityID: entityID,
		Type:     "review_request",
		Priority: "high",
		Limit:    1,
	})
	if err != nil {
		t.Fatalf("list v1 mailbox page1: %v", err)
	}
	if len(page1) != 1 || page1[0].MailboxID != firstID || page1[0].Priority != "high" || page1[0].SourceFlow != "main/review" {
		t.Fatalf("unexpected page1=%#v", page1)
	}
	if cursor != "" {
		t.Fatalf("single filtered item produced cursor %q", cursor)
	}

	pageAll, cursor, err := s.ListV1MailboxItems(ctx, MailboxV1ListOptions{Status: "pending", Limit: 1})
	if err != nil {
		t.Fatalf("list all page1: %v", err)
	}
	if len(pageAll) != 1 || cursor == "" {
		t.Fatalf("page all len=%d cursor=%q", len(pageAll), cursor)
	}
	page2, next, err := s.ListV1MailboxItems(ctx, MailboxV1ListOptions{Status: "pending", Cursor: cursor, Limit: 1})
	if err != nil {
		t.Fatalf("list all page2: %v", err)
	}
	if len(page2) != 1 || page2[0].MailboxID == pageAll[0].MailboxID || next != "" {
		t.Fatalf("page2=%#v next=%q page1=%#v", page2, next, pageAll)
	}

	detail, err := s.GetV1MailboxItem(ctx, firstID)
	if err != nil {
		t.Fatalf("get v1 mailbox: %v", err)
	}
	if detail.Item.Payload["title"] != "check" || len(detail.History) != 1 || detail.History[0].Action != "created" {
		t.Fatalf("unexpected detail=%#v", detail)
	}

	if _, err := s.DecideV1MailboxItem(ctx, MailboxV1DecisionRequest{
		MailboxID:    firstID,
		Action:       "approved",
		ActorTokenID: "actor-1",
		Now:          now,
	}); !errors.Is(err, ErrMailboxV1ApprovalRouteUnconfigured) {
		t.Fatalf("approve without route error = %v, want route unconfigured", err)
	}

	outcome, err := s.DecideV1MailboxItem(ctx, MailboxV1DecisionRequest{
		MailboxID:         firstID,
		Action:            "approved",
		ActorTokenID:      "actor-1",
		DecisionPayload:   json.RawMessage(`{"ok":true}`),
		Now:               now,
		ApprovalEventType: "mailbox.item_decided",
	})
	if err != nil {
		t.Fatalf("approve with route: %v", err)
	}
	if !outcome.Result.OK || outcome.Result.Status != "decided" || outcome.Result.DownstreamEventID == "" || outcome.ApprovalEvent == nil {
		t.Fatalf("unexpected approve outcome=%#v", outcome)
	}
	var approvalPayload map[string]any
	if err := json.Unmarshal(outcome.ApprovalEvent.Payload(), &approvalPayload); err != nil {
		t.Fatalf("decode approval event payload: %v", err)
	}
	if _, ok := approvalPayload["payload"]; ok {
		t.Fatalf("approval event retained retired payload field: %#v", approvalPayload)
	}
	mailboxPayload := approvalPayload["mailbox_payload"].(map[string]any)
	if mailboxPayload["title"] != "check" {
		t.Fatalf("approval event mailbox_payload = %#v, want title check", mailboxPayload)
	}
	decisionPayload := approvalPayload["decision_payload"].(map[string]any)
	if decisionPayload["ok"] != true {
		t.Fatalf("approval event decision_payload = %#v, want ok true", decisionPayload)
	}
	if _, err := s.DecideV1MailboxItem(ctx, MailboxV1DecisionRequest{
		MailboxID:         firstID,
		Action:            "approved",
		ActorTokenID:      "actor-1",
		Now:               now,
		ApprovalEventType: "mailbox.item_decided",
	}); err == nil {
		t.Fatal("second approve error = nil, want already decided")
	} else {
		var already *MailboxV1AlreadyDecidedError
		if !errors.As(err, &already) || already.ExistingDecision != "approved" {
			t.Fatalf("second approve error = %v, want approved already-decided", err)
		}
	}

	pastID, err := s.InsertMailboxItem(ctx, runtimetools.MailboxItem{Type: "approval", Summary: "later"})
	if err != nil {
		t.Fatalf("insert defer mailbox item: %v", err)
	}
	if _, err := s.DecideV1MailboxItem(ctx, MailboxV1DecisionRequest{
		MailboxID:    pastID,
		Action:       "deferred",
		ActorTokenID: "actor-1",
		DeferUntil:   now.Add(-time.Second),
		Now:          now,
	}); err == nil {
		t.Fatal("defer in past error = nil, want invalid defer")
	} else {
		var invalid *MailboxV1InvalidDeferUntilError
		if !errors.As(err, &invalid) || invalid.Reason != "in_past" {
			t.Fatalf("defer in past error = %v, want in_past", err)
		}
	}
	if _, err := s.DecideV1MailboxItem(ctx, MailboxV1DecisionRequest{
		MailboxID:    pastID,
		Action:       "deferred",
		ActorTokenID: "actor-1",
		DeferUntil:   now.Add(time.Hour),
		Now:          now,
	}); err != nil {
		t.Fatalf("defer future: %v", err)
	}
	deferred, err := s.GetV1MailboxItem(ctx, pastID)
	if err != nil {
		t.Fatalf("get deferred: %v", err)
	}
	if deferred.Item.Status != "deferred" || deferred.Item.Decision != "deferred" {
		t.Fatalf("deferred projection = %#v", deferred.Item)
	}
}

func TestPostgresStore_APIIdempotencyReplaysAndConflicts(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	s := &PostgresStore{DB: db}
	ctx := context.Background()
	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	req := APIIdempotencyRequest{
		Method:         "mailbox.approve",
		ActorTokenID:   "actor-1",
		IdempotencyKey: "idem-1",
		RequestHash:    "sha256:first",
		ResourceID:     "mailbox-1",
		Now:            now,
	}
	calls := 0
	first, replay, err := s.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
		calls++
		return APIIdempotencyCompletion{ResourceID: "mailbox-1", Response: json.RawMessage(`{"ok":true,"n":1}`)}, nil
	})
	if err != nil || replay || calls != 1 || string(first.Response) == "" {
		t.Fatalf("first idempotency completion=%#v replay=%v calls=%d err=%v", first, replay, calls, err)
	}
	second, replay, err := s.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
		calls++
		return APIIdempotencyCompletion{ResourceID: "mailbox-1", Response: json.RawMessage(`{"ok":true,"n":2}`)}, nil
	})
	if err != nil || !replay || calls != 1 || !sameJSON(first.Response, second.Response) {
		t.Fatalf("replay completion=%s replay=%v calls=%d err=%v", second.Response, replay, calls, err)
	}
	req.RequestHash = "sha256:second"
	if _, _, err := s.WithAPIIdempotency(ctx, req, func(context.Context) (APIIdempotencyCompletion, error) {
		calls++
		return APIIdempotencyCompletion{}, nil
	}); err == nil {
		t.Fatal("conflicting idempotency request error = nil")
	} else {
		var conflict *APIIdempotencyConflictError
		if !errors.As(err, &conflict) || conflict.OriginalRequestHash != "sha256:first" || conflict.ConflictingRequestHash != "sha256:second" {
			t.Fatalf("conflict error = %#v", err)
		}
	}
}

func sameJSON(left, right json.RawMessage) bool {
	var l any
	var r any
	if err := json.Unmarshal(left, &l); err != nil {
		return false
	}
	if err := json.Unmarshal(right, &r); err != nil {
		return false
	}
	return reflect.DeepEqual(l, r)
}
