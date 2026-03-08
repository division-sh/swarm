package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"empireai/internal/config"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDashboardMutationContracts_MailboxDecisions(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	verticalID := uuid.NewString()
	approveID := uuid.NewString()
	moreDataID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Payroll Mexico', 'payroll-mexico', 'mexico', 'ready_for_review', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
		VALUES
			($1::uuid, $3::uuid, 'empire-coordinator', 'vertical_approval', 'critical', 'pending', '{}'::jsonb, 'approve this vertical', now() - interval '10 minutes'),
			($2::uuid, $3::uuid, 'empire-coordinator', 'vertical_approval', 'normal', 'pending', '{}'::jsonb, 'need more detail', now() - interval '5 minutes')
	`, approveID, moreDataID, verticalID); err != nil {
		t.Fatalf("seed mailbox: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	t.Run("approve", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{
			"mailbox_id": approveID,
			"action":     "approve",
			"notes":      "ship it",
		}))
		if w.Code != http.StatusOK {
			t.Fatalf("mailbox approve status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		if body["ok"] != true {
			t.Fatalf("expected ok=true, got %#v", body["ok"])
		}
		if body["id"] != approveID {
			t.Fatalf("expected mailbox id %s, got %#v", approveID, body["id"])
		}
		if body["status"] != "approved" || body["decision"] != "approve" {
			t.Fatalf("unexpected decision payload: %#v", body)
		}

		var status, decision, notes string
		if err := db.QueryRowContext(ctx, `
			SELECT status, decision, decision_notes
			FROM mailbox
			WHERE id = $1::uuid
		`, approveID).Scan(&status, &decision, &notes); err != nil {
			t.Fatalf("load mailbox row: %v", err)
		}
		if status != "approved" || decision != "approve" || notes != "ship it" {
			t.Fatalf("unexpected mailbox row status=%q decision=%q notes=%q", status, decision, notes)
		}

		var mailboxDecisionEvents, itemDecidedEvents, verticalApprovedEvents int
		_ = db.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE type = 'mailbox.decision'`).Scan(&mailboxDecisionEvents)
		_ = db.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE type = 'mailbox.item_decided'`).Scan(&itemDecidedEvents)
		_ = db.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE type = 'vertical.approved' AND vertical_id = $1::uuid`, verticalID).Scan(&verticalApprovedEvents)
		if mailboxDecisionEvents < 1 || itemDecidedEvents < 1 || verticalApprovedEvents < 1 {
			t.Fatalf("expected decision side-effect events, got mailbox.decision=%d mailbox.item_decided=%d vertical.approved=%d", mailboxDecisionEvents, itemDecidedEvents, verticalApprovedEvents)
		}
	})

	t.Run("more-data", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{
			"mailbox_id": moreDataID,
			"action":     "more-data",
			"notes":      "need customer proof",
		}))
		if w.Code != http.StatusOK {
			t.Fatalf("mailbox more-data status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		if body["status"] != "more_data" || body["decision"] != "more_data" {
			t.Fatalf("unexpected more-data response: %#v", body)
		}

		var status, decision string
		if err := db.QueryRowContext(ctx, `
			SELECT status, decision
			FROM mailbox
			WHERE id = $1::uuid
		`, moreDataID).Scan(&status, &decision); err != nil {
			t.Fatalf("load mailbox row: %v", err)
		}
		if status != "more_data" || decision != "more_data" {
			t.Fatalf("unexpected mailbox row status=%q decision=%q", status, decision)
		}

		var needsMoreDataEvents int
		_ = db.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE type = 'vertical.needs_more_data' AND vertical_id = $1::uuid`, verticalID).Scan(&needsMoreDataEvents)
		if needsMoreDataEvents < 1 {
			t.Fatalf("expected vertical.needs_more_data event, got %d", needsMoreDataEvents)
		}
	})
}

func TestDashboardMutationContracts_TaskActions(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	cfg := &config.Config{
		Budget: config.BudgetConfig{
			HumanTasks: config.HumanTasksConfig{
				MaxTasksPerWeek: 5,
				BudgetReset:     "monday",
			},
		},
	}

	verticalID := uuid.NewString()
	claimTaskID := uuid.NewString()
	rejectTaskID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Brazil Payroll', 'brazil-payroll', 'brazil', 'researching', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO human_tasks (
			id, requesting_agent, vertical_id, category, description, priority, status, created_at
		) VALUES
			($1::uuid, 'empire-coordinator', $3::uuid, 'verification', 'Call the operator', 'high', 'pending_review', now() - interval '1 hour'),
			($2::uuid, 'empire-coordinator', $3::uuid, 'verification', 'Review the pack', 'medium', 'approved', now() - interval '40 minutes')
	`, claimTaskID, rejectTaskID, verticalID); err != nil {
		t.Fatalf("seed tasks: %v", err)
	}

	srv := NewServer(db, cfg, pg, pg, nil)
	h := srv.Handler()

	t.Run("claim", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/tasks/"+claimTaskID+"/claim", map[string]any{
			"assigned_to": "founder",
		}))
		if w.Code != http.StatusOK {
			t.Fatalf("task claim status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		if body["ok"] != true || body["task_id"] != claimTaskID || body["status"] != "assigned" || body["assigned_to"] != "founder" {
			t.Fatalf("unexpected claim response: %#v", body)
		}

		var status, assignedTo string
		if err := db.QueryRowContext(ctx, `
			SELECT status, COALESCE(assigned_to, '')
			FROM human_tasks
			WHERE id = $1::uuid
		`, claimTaskID).Scan(&status, &assignedTo); err != nil {
			t.Fatalf("load claimed task: %v", err)
		}
		if status != "assigned" || assignedTo != "founder" {
			t.Fatalf("unexpected claimed task status=%q assigned_to=%q", status, assignedTo)
		}
	})

	t.Run("complete", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/tasks/"+claimTaskID+"/complete", map[string]any{
			"result_text":      "customer confirmed demand",
			"outcome":          "success",
			"follow_up_needed": true,
		}))
		if w.Code != http.StatusOK {
			t.Fatalf("task complete status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		if body["ok"] != true || body["task_id"] != claimTaskID || body["status"] != "completed" {
			t.Fatalf("unexpected complete response: %#v", body)
		}

		var status, result, outcome string
		var followUp bool
		if err := db.QueryRowContext(ctx, `
			SELECT status, COALESCE(result, ''), COALESCE(outcome, ''), COALESCE(follow_up_needed, false)
			FROM human_tasks
			WHERE id = $1::uuid
		`, claimTaskID).Scan(&status, &result, &outcome, &followUp); err != nil {
			t.Fatalf("load completed task: %v", err)
		}
		if status != "completed" || result != "customer confirmed demand" || outcome != "success" || !followUp {
			t.Fatalf("unexpected completed task status=%q result=%q outcome=%q follow_up=%v", status, result, outcome, followUp)
		}
	})

	t.Run("reject", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/tasks/"+rejectTaskID+"/reject", map[string]any{
			"reason": "need more context first",
		}))
		if w.Code != http.StatusOK {
			t.Fatalf("task reject status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		if body["ok"] != true || body["task_id"] != rejectTaskID || body["status"] != "deferred" {
			t.Fatalf("unexpected reject response: %#v", body)
		}
		requireMapKey(t, body, "requeue_date")

		var status string
		var reviewDecision []byte
		if err := db.QueryRowContext(ctx, `
			SELECT status, review_decision
			FROM human_tasks
			WHERE id = $1::uuid
		`, rejectTaskID).Scan(&status, &reviewDecision); err != nil {
			t.Fatalf("load deferred task: %v", err)
		}
		if status != "deferred" {
			t.Fatalf("expected deferred task status, got %q", status)
		}
		decision := decodeJSONMap(t, reviewDecision)
		if decision["decision"] != "deferred" || decision["human_reason"] != "need more context first" {
			t.Fatalf("unexpected review decision payload: %#v", decision)
		}

		var assignedEvents, completedEvents, deferredEvents int
		_ = db.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE type = 'human_task.assigned' AND vertical_id = $1::uuid`, verticalID).Scan(&assignedEvents)
		_ = db.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE type = 'human_task.completed' AND vertical_id = $1::uuid`, verticalID).Scan(&completedEvents)
		_ = db.QueryRowContext(ctx, `SELECT count(*) FROM events WHERE type = 'human_task.deferred' AND vertical_id = $1::uuid`, verticalID).Scan(&deferredEvents)
		if assignedEvents < 1 || completedEvents < 1 || deferredEvents < 1 {
			t.Fatalf("expected task lifecycle events, got assigned=%d completed=%d deferred=%d", assignedEvents, completedEvents, deferredEvents)
		}
	})

	t.Run("stats reflect completion outcome", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/tasks/stats", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("task stats status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		byCategory := requireMap(t, requireMapKey(t, body, "by_category"), "by_category")
		verification := requireMap(t, requireMapKey(t, byCategory, "verification"), "verification stats")
		if got, ok := verification["success"].(float64); !ok || got < 1 {
			t.Fatalf("expected verification.success >= 1, got %#v", verification["success"])
		}
	})
}

func TestDashboardContractDegradedStates(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	srv := NewServer(db, &config.Config{}, pg, nil, nil)
	h := srv.Handler()

	t.Run("pipeline shards without shards table", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/pipeline/shards", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("pipeline shards status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		scans := requireList(t, requireMapKey(t, body, "scans"), "scans")
		if len(scans) != 0 {
			t.Fatalf("expected empty scans list, got %d", len(scans))
		}
	})

	t.Run("digest without mailbox store", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/digest", nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("digest status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		requireMapKey(t, body, "error")
	})

	t.Run("mailbox decide without store", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{
			"mailbox_id": uuid.NewString(),
			"action":     "approve",
		}))
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("mailbox decide status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		requireMapKey(t, body, "error")
	})

	t.Run("conversations empty list", func(t *testing.T) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/conversations", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("conversations status=%d body=%s", w.Code, w.Body.String())
		}
		body := decodeJSONMap(t, w.Body.Bytes())
		conversations := requireList(t, requireMapKey(t, body, "conversations"), "conversations")
		if len(conversations) != 0 {
			t.Fatalf("expected empty conversations list, got %d", len(conversations))
		}
	})
}
