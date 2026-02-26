package runtime

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"empireai/internal/models"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func resp(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Header:     http.Header{"Content-Type": []string{"text/html"}},
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}

func seedVertical(t *testing.T, db *sql.DB, slug string, credsJSON string) string {
	t.Helper()
	id := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'V', $2, 'us', 'discovered', 'factory', $3::jsonb, now(), now())
	`, id, slug, credsJSON); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	return id
}

func TestAuthorizeRoutingAndManage_Branches(t *testing.T) {
	target := models.AgentConfig{Role: "backend-agent"}

	// chief-of-staff restrictions.
	if err := authorizeRouting(models.AgentConfig{Role: "chief-of-staff"}, target, "active"); err == nil {
		t.Fatalf("expected CoS to be blocked unless proposed")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "chief-of-staff"}, target, "proposed"); err != nil {
		t.Fatalf("expected CoS proposed ok: %v", err)
	}

	// domain authorization.
	if err := authorizeRouting(models.AgentConfig{Role: "vp-growth"}, target, "active"); err == nil {
		t.Fatalf("expected vp-growth to be blocked for eng target")
	}
	if err := authorizeRouting(models.AgentConfig{Role: "cto-agent"}, target, "active"); err != nil {
		t.Fatalf("expected cto-agent ok for eng target: %v", err)
	}

	// manage restrictions.
	if err := authorizeManage(models.AgentConfig{Role: "vp-product", VerticalID: "v1"}, "vp-growth", "v1"); err == nil {
		t.Fatalf("expected vp-product blocked for growth role")
	}
	if err := authorizeManage(models.AgentConfig{Role: "vp-product", VerticalID: "v1"}, "backend-agent", "v1"); err != nil {
		t.Fatalf("expected vp-product can manage product roles (backend is allowed list): %v", err)
	}
	if err := authorizeManage(models.AgentConfig{Role: "opco-ceo", VerticalID: "v1"}, "vp-product", "v2"); err == nil {
		t.Fatalf("expected cross-vertical block")
	}
	if err := authorizeManage(models.AgentConfig{Role: "empire-coordinator", VerticalID: "v1"}, "vp-product", "v2"); err != nil {
		t.Fatalf("coordinator bypass: %v", err)
	}
}

func TestDefaultExternalCredentialEnv_Branches(t *testing.T) {
	t.Setenv("REGISTRAR_API_ENDPOINT", "https://reg.example")
	t.Setenv("REGISTRAR_API_KEY", "rk")
	t.Setenv("CLOUDFLARE_API_ENDPOINT", "")
	t.Setenv("CLOUDFLARE_API_TOKEN", "cfk")
	t.Setenv("WHATSAPP_API_ENDPOINT", "https://wa.example")
	t.Setenv("WHATSAPP_API_KEY", "wak")
	t.Setenv("INSTAGRAM_API_ENDPOINT", "https://ig.example")
	t.Setenv("INSTAGRAM_API_KEY", "igk")

	if got := defaultExternalCredentialEnv("domain_purchase"); got["endpoint"] != "https://reg.example" || got["api_key"] != "rk" {
		t.Fatalf("domain creds mismatch: %#v", got)
	}
	if got := defaultExternalCredentialEnv("dns_configure"); !strings.Contains(got["endpoint"], "cloudflare.com") || got["api_key"] != "cfk" {
		t.Fatalf("dns creds mismatch: %#v", got)
	}
	if got := defaultExternalCredentialEnv("whatsapp_business_api"); got["endpoint"] != "https://wa.example" || got["api_key"] != "wak" {
		t.Fatalf("wa creds mismatch: %#v", got)
	}
	if got := defaultExternalCredentialEnv("instagram_api"); got["endpoint"] != "https://ig.example" || got["api_key"] != "igk" {
		t.Fatalf("ig creds mismatch: %#v", got)
	}
	if got := defaultExternalCredentialEnv("unknown"); len(got) != 0 {
		t.Fatalf("expected empty map")
	}
}

func TestExecInstagramHandleCheck_Availability(t *testing.T) {
	orig := http.DefaultClient.Transport
	t.Cleanup(func() { http.DefaultClient.Transport = orig })

	http.DefaultClient.Transport = rtFunc(func(r *http.Request) (*http.Response, error) {
		if !strings.Contains(r.URL.Host, "www.instagram.com") {
			return nil, errors.New("unexpected host")
		}
		if strings.Contains(r.URL.Path, "available_handle") {
			return resp(404, "not found"), nil
		}
		return resp(200, "ok"), nil
	})

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out, err := ex.execInstagramHandleCheck(ctx, models.AgentConfig{ID: "a"}, map[string]any{"handle": "@available_handle"})
	if err != nil {
		t.Fatalf("expected ok: %v", err)
	}
	m := out.(map[string]any)
	if m["available"] != true {
		t.Fatalf("expected available=true, got %#v", m)
	}

	out, err = ex.execInstagramHandleCheck(ctx, models.AgentConfig{ID: "a"}, map[string]any{"handle": "taken_handle"})
	if err != nil {
		t.Fatalf("expected ok: %v", err)
	}
	m = out.(map[string]any)
	if m["available"] != false {
		t.Fatalf("expected available=false, got %#v", m)
	}

	if _, err := ex.execInstagramHandleCheck(ctx, models.AgentConfig{}, map[string]any{"handle": ""}); err == nil {
		t.Fatalf("expected handle required error")
	}
	if _, err := ex.execInstagramHandleCheck(ctx, models.AgentConfig{}, map[string]any{"handle": "bad!!"}); err == nil {
		t.Fatalf("expected invalid format error")
	}
}

func TestExecEmailAPI_CredentialAndSendBranches(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	_ = dsn

	verticalID := seedVertical(t, db, "emailco", `{
		"email": {
			"smtp_addr": "127.0.0.1:1",
			"from": "noreply@example.com",
			"username": "u",
			"password": "p"
		}
	}`)

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	actor := models.AgentConfig{ID: "opco-ceo-" + verticalID, Role: "opco-ceo", VerticalID: verticalID}
	// Missing recipients branch.
	if _, err := ex.execEmailAPI(ctx, actor, map[string]any{"to": []string{}}); err == nil {
		t.Fatalf("expected recipient error")
	}
	// SendMail error path (connection refused) should be returned.
	if _, err := ex.execEmailAPI(ctx, actor, map[string]any{"to": []string{"a@example.com"}, "subject": "s", "body": "b"}); err == nil {
		t.Fatalf("expected send failure")
	}

	// Missing configured smtp/from.
	vertical2 := seedVertical(t, db, "emailco2", `{"email":{}}`)
	actor2 := models.AgentConfig{ID: "a2", Role: "opco-ceo", VerticalID: vertical2}
	if _, err := ex.execEmailAPI(ctx, actor2, map[string]any{"to": []string{"a@example.com"}, "subject": "s", "body": "b"}); err == nil {
		t.Fatalf("expected missing credential error")
	}
}

func TestRestrictedDevOpsTools_RejectNonHoldingDevOps(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ctx := context.Background()
	actor := models.AgentConfig{ID: "a", Role: "opco-ceo"}

	if _, err := ex.execNginxReload(ctx, actor, nil); err == nil {
		t.Fatalf("expected nginx restriction error")
	}
	if _, err := ex.execSystemdControl(ctx, actor, map[string]any{"action": "restart", "unit": "empireai-x"}); err == nil {
		t.Fatalf("expected systemd restriction error")
	}
	if _, err := ex.execCertbotExecute(ctx, actor, map[string]any{"domain": "example.com"}); err == nil {
		t.Fatalf("expected certbot restriction error")
	}
}

func TestDecryptCredentialValue_NoKeyLeavesEncrypted(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	_ = dsn
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)
	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "")
	in := map[string]any{"token": "enc::abc"}
	out := ex.decryptCredentialMap(context.Background(), in)
	if out["token"].(string) != "enc::abc" {
		t.Fatalf("expected encrypted value to remain when key missing")
	}
	// ensure os import used.
	_ = os.ErrInvalid
}

func TestExecSystemdControl_ValidationBranches(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ctx := context.Background()
	actor := models.AgentConfig{ID: "hd", Role: "holding-devops"}

	// Unsupported action should fail before calling systemctl.
	if _, err := ex.execSystemdControl(ctx, actor, map[string]any{"action": "bogus", "unit": "empireai-x"}); err == nil {
		t.Fatalf("expected unsupported action error")
	}
	// Unit prefix validation should fail before calling systemctl.
	if _, err := ex.execSystemdControl(ctx, actor, map[string]any{"action": "restart", "unit": "nginx"}); err == nil {
		t.Fatalf("expected unit prefix error")
	}
}

func TestExecCertbotExecute_ValidationBranches(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ctx := context.Background()
	actor := models.AgentConfig{ID: "hd", Role: "holding-devops"}
	if _, err := ex.execCertbotExecute(ctx, actor, map[string]any{"domain": ""}); err == nil {
		t.Fatalf("expected domain required error")
	}
}

func TestRedactTelemetryValue_NestedAndSensitive(t *testing.T) {
	in := map[string]any{
		"token":    "secret-token-value",
		"password": "pw",
		"notes":    "payment confirmed ch_abcdef123456",
		"meta": map[string]any{
			"Authorization": "Bearer X",
			"count":         2,
			"items":         []any{"a", map[string]any{"api_key": "k"}},
		},
	}
	out := redactTelemetryValue(in).(map[string]any)
	if out["token"] != "[REDACTED]" || out["password"] != "[REDACTED]" {
		t.Fatalf("expected sensitive keys redacted: %#v", out)
	}
	meta := out["meta"].(map[string]any)
	if meta["Authorization"] != "[REDACTED]" {
		t.Fatalf("expected nested auth redacted: %#v", meta)
	}
	if strings.Contains(asString(out["notes"]), "ch_abcdef123456") || !strings.Contains(asString(out["notes"]), "[PAYMENT_REF]") {
		t.Fatalf("expected payment ref redacted, got %#v", out["notes"])
	}
}

func TestLoadExternalCredentials_MergesSections(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	verticalID := seedVertical(t, db, "credco", `{
		"whatsapp": {"endpoint":"w","token":"t"},
		"instagram": {"endpoint":"i","api_key":"k"},
		"registrar": {"endpoint":"r","api_key":"rk"},
		"dns": {"endpoint":"d","api_key":"dk"},
		"whatsapp_name_check": {"endpoint":"n","api_key":"nk"}
	}`)

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)
	ctx := context.Background()
	actor := models.AgentConfig{ID: "a", Role: "opco-ceo", VerticalID: verticalID}

	check := func(tool string, wantKey string) {
		creds, err := ex.loadExternalCredentials(ctx, actor.VerticalID, tool)
		if err != nil {
			t.Fatalf("loadExternalCredentials %s: %v", tool, err)
		}
		if strings.TrimSpace(asString(creds[wantKey])) == "" {
			t.Fatalf("expected %s in creds for %s: %#v", wantKey, tool, creds)
		}
	}
	check("whatsapp_business_api", "endpoint")
	check("instagram_api", "api_key")
	check("domain_availability_check", "api_key")
	check("dns_configure", "endpoint")
	check("whatsapp_name_check", "api_key")
}

func TestDecryptCredentialValue_Success(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)
	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "k")

	var encoded string
	if err := db.QueryRowContext(context.Background(), `
		SELECT encode(pgp_sym_encrypt('plain', 'k'), 'base64')
	`).Scan(&encoded); err != nil {
		t.Fatalf("encode: %v", err)
	}
	got := ex.decryptCredentialValue(context.Background(), "enc::"+strings.TrimSpace(encoded))
	if got.(string) != "plain" {
		t.Fatalf("expected decrypted plain, got %#v", got)
	}
}

type mailboxStub struct {
	last MailboxItem
}

func (m *mailboxStub) InsertMailboxItem(_ context.Context, item MailboxItem) (string, error) {
	m.last = item
	if strings.TrimSpace(item.ID) == "" {
		return "mb1", nil
	}
	return item.ID, nil
}
func (m *mailboxStub) ListMailboxItems(context.Context, string, int) ([]MailboxItem, error) {
	return nil, nil
}
func (m *mailboxStub) CountMailboxItems(context.Context, string) (int, error) { return 0, nil }
func (m *mailboxStub) GetMailboxItem(context.Context, string) (MailboxItem, error) {
	return MailboxItem{}, nil
}
func (m *mailboxStub) DecideMailboxItem(context.Context, string, string, string, string) error {
	return nil
}
func (m *mailboxStub) ExpireMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}
func (m *mailboxStub) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}
func (m *mailboxStub) MarkMailboxItemNotified(context.Context, string) error { return nil }

type scheduleStoreStub2 struct{ upsert int }

func (s *scheduleStoreStub2) UpsertSchedule(context.Context, Schedule) error { s.upsert++; return nil }
func (s *scheduleStoreStub2) CancelSchedule(context.Context, string, string) error {
	return nil
}
func (s *scheduleStoreStub2) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}
func (s *scheduleStoreStub2) MarkScheduleFired(context.Context, Schedule) error { return nil }

func TestToolExecutor_AgentHireFireReconfigure_And_ScheduleAndMailbox(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	scheduler := NewScheduler(func(Schedule) {})
	t.Cleanup(func() { scheduler.Stop() })

	store := &scheduleStoreStub2{}
	ex := NewRuntimeToolExecutor(bus, scheduler, nil, store)
	mb := &mailboxStub{}
	ex.SetMailboxStore(mb)

	// Manager without store/factory uses generic agents for tests.
	manager := NewAgentManager(bus, nil)
	manager.Run(context.Background())
	t.Cleanup(func() { _ = manager.Shutdown() })
	ex.SetManager(manager)

	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding", VerticalID: "v1"}

	// Hire.
	out, err := ex.execAgentHire(actor, map[string]any{"config": map[string]any{"id": "a1", "role": "vp-product"}})
	if err != nil {
		t.Fatalf("hire: %v", err)
	}
	if out.(map[string]any)["status"] != "hired" {
		t.Fatalf("unexpected hire out: %#v", out)
	}

	// Reconfigure.
	out, err = ex.execAgentReconfigure(actor, map[string]any{"agent_id": "a1", "config": map[string]any{"mode": "holding"}})
	if err != nil {
		t.Fatalf("reconfigure: %v", err)
	}
	if out.(map[string]any)["status"] != "reconfigured" {
		t.Fatalf("unexpected reconfigure out: %#v", out)
	}

	// Fire.
	out, err = ex.execAgentFire(actor, map[string]any{"agent_id": "a1"})
	if err != nil {
		t.Fatalf("fire: %v", err)
	}
	if out.(map[string]any)["status"] != "fired" {
		t.Fatalf("unexpected fire out: %#v", out)
	}

	// Schedule: validate at parsing + persistence call.
	if _, err := ex.execSchedule(actor, map[string]any{"event_type": "timer.x", "at": "bad"}); err == nil {
		t.Fatalf("expected invalid at error")
	}
	if _, err := ex.execSchedule(actor, map[string]any{"agent_id": "other", "event_type": "timer.x"}); err == nil {
		t.Fatalf("expected schedule for self only error")
	}
	if _, err := ex.execSchedule(models.AgentConfig{ID: "a", Role: "opco-ceo", Mode: "operating", VerticalID: "v1"}, map[string]any{"vertical_id": "v2", "event_type": "timer.x"}); err == nil {
		t.Fatalf("expected cross-vertical schedule error")
	}
	if _, err := ex.execSchedule(models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding", VerticalID: "v1"}, map[string]any{"event_type": "timer.x", "mode": "cron", "cron": "@every 1h"}); err != nil {
		t.Fatalf("schedule ok: %v", err)
	}
	if store.upsert == 0 {
		t.Fatalf("expected schedule store upsert")
	}

	// Mailbox send: requires authorized role and type.
	if _, err := ex.execMailboxSend(models.AgentConfig{ID: "x", Role: "qa-agent", VerticalID: "v1"}, map[string]any{"type": "review"}); err == nil {
		t.Fatalf("expected mailbox auth error")
	}
	if _, err := ex.execMailboxSend(models.AgentConfig{ID: "x", Role: "opco-ceo", VerticalID: "v1"}, map[string]any{"priority": "normal"}); err == nil {
		t.Fatalf("expected mailbox type required")
	}
	if _, err := ex.execMailboxSend(models.AgentConfig{ID: "x", Role: "opco-ceo", VerticalID: "v1"}, map[string]any{"vertical_id": "v2", "type": "review"}); err == nil {
		t.Fatalf("expected cross-vertical mailbox error")
	}
	out, err = ex.execMailboxSend(models.AgentConfig{ID: "x", Role: "opco-ceo", VerticalID: "v1"}, map[string]any{"type": "review", "context": map[string]any{"token": "secret"}})
	if err != nil {
		t.Fatalf("mailbox send: %v", err)
	}
	if out.(map[string]any)["status"] != "queued" {
		t.Fatalf("unexpected mailbox out: %#v", out)
	}
	if mb.last.Type != "review" || mb.last.Status != "pending" {
		t.Fatalf("unexpected mailbox item: %#v", mb.last)
	}
}

func TestDecodeToolInput_ErrorBranch(t *testing.T) {
	var out struct{}
	if err := decodeToolInput(func() {}, &out); err == nil {
		t.Fatalf("expected marshal error for func")
	}
}

func TestNormalizeSQLValue_Bytes(t *testing.T) {
	if got := normalizeSQLValue([]byte("x")); got.(string) != "x" {
		t.Fatalf("expected string from bytes, got %#v", got)
	}
	if got := normalizeSQLValue(123); got.(int) != 123 {
		t.Fatalf("expected passthrough, got %#v", got)
	}
}

func TestExecSQLExecute_ReadOnly(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	verticalID := seedVertical(t, db, "acme", `{}`)
	// Create the vertical schema + a table inside it.
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS "acme_schema"`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS "acme_schema".t (id INT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)

	actor := models.AgentConfig{ID: "a", Role: "opco-ceo", Mode: "operating", VerticalID: verticalID}

	// Non-select should be rejected.
	if _, err := ex.execSQLExecute(ctx, actor, map[string]any{"query": `INSERT INTO t (id,v) VALUES (1,'x')`}); err == nil {
		t.Fatalf("expected insert rejection")
	}

	// Seed row directly so select path can be validated.
	if _, err := db.ExecContext(ctx, `INSERT INTO "acme_schema".t (id, v) VALUES (1, 'x')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	// Select path.
	out, err := ex.execSQLExecute(ctx, actor, map[string]any{"query": `SELECT id, v FROM t ORDER BY id`})
	if err != nil {
		t.Fatalf("execSQLExecute select: %v", err)
	}
	rows := out.(map[string]any)["rows"].([]map[string]any)
	if len(rows) != 1 || rows[0]["v"] != "x" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}

func TestExecAgentMessage_TargetValidationBranches(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ex := NewRuntimeToolExecutor(bus, NewScheduler(func(Schedule) {}), nil)
	t.Cleanup(func() { ex.scheduler.Stop() })

	manager := NewAgentManager(bus, nil)
	_ = manager.SpawnAgent(models.AgentConfig{ID: "t1", Role: "vp-product", Mode: "operating", VerticalID: "v1"})
	_ = manager.SpawnAgent(models.AgentConfig{ID: "t2", Role: "vp-product", Mode: "operating", VerticalID: "v2"})
	ex.SetManager(manager)

	actor := models.AgentConfig{ID: "a", Role: "opco-ceo", Mode: "operating", VerticalID: "v1"}
	ctx := context.Background()

	// Missing targets.
	if _, err := ex.execAgentMessage(ctx, actor, map[string]any{"message": "hi"}); err == nil {
		t.Fatalf("expected missing target error")
	}
	// Unknown target.
	if _, err := ex.execAgentMessage(ctx, actor, map[string]any{"target_agent_id": "nope", "message": "hi"}); err == nil {
		t.Fatalf("expected unknown target error")
	}
	// Cross-vertical blocked in operating mode.
	if _, err := ex.execAgentMessage(ctx, actor, map[string]any{"target_agent_id": "t2", "message": "hi"}); err == nil {
		t.Fatalf("expected cross-vertical error")
	}
	// Successful direct publish to same vertical.
	if _, err := ex.execAgentMessage(ctx, actor, map[string]any{"target_agent_ids": []string{"t1", "t1"}, "message": "hi"}); err != nil {
		t.Fatalf("expected ok: %v", err)
	}
}

func TestExecAgentMessage_AuthorityAndManagementChain(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ex := NewRuntimeToolExecutor(bus, NewScheduler(func(Schedule) {}), nil)
	t.Cleanup(func() { ex.scheduler.Stop() })

	manager := NewAgentManager(bus, nil)
	_ = manager.SpawnAgent(models.AgentConfig{ID: "vp-product-v1", Role: "vp-product", Mode: "operating", VerticalID: "v1"})
	_ = manager.SpawnAgent(models.AgentConfig{ID: "vp-growth-v1", Role: "vp-growth", Mode: "operating", VerticalID: "v1"})
	_ = manager.SpawnAgent(models.AgentConfig{ID: "cto-v1", Role: "cto-agent", Mode: "operating", VerticalID: "v1", ParentAgent: "vp-product-v1"})
	_ = manager.SpawnAgent(models.AgentConfig{ID: "backend-v1", Role: "backend-agent", Mode: "operating", VerticalID: "v1", ParentAgent: "cto-v1"})
	ex.SetManager(manager)

	ctx := context.Background()

	// vp-product -> vp-growth is disallowed (CoS bridge model).
	actor := models.AgentConfig{ID: "vp-product-v1", Role: "vp-product", Mode: "operating", VerticalID: "v1"}
	if _, err := ex.execAgentMessage(ctx, actor, map[string]any{"target_agent_id": "vp-growth-v1", "message": "sync"}); err == nil {
		t.Fatalf("expected authority rejection for vp-product -> vp-growth")
	}

	// vp-product -> backend is allowed through management chain (vp-product -> cto -> backend).
	if _, err := ex.execAgentMessage(ctx, actor, map[string]any{"target_agent_id": "backend-v1", "message": "prioritize bug"}); err != nil {
		t.Fatalf("expected management-chain authorization, got: %v", err)
	}

	// worker -> manager escalation is allowed.
	worker := models.AgentConfig{ID: "backend-v1", Role: "backend-agent", Mode: "operating", VerticalID: "v1"}
	if _, err := ex.execAgentMessage(ctx, worker, map[string]any{"target_agent_id": "vp-product-v1", "message": "blocked on product decision"}); err != nil {
		t.Fatalf("expected upward escalation authorization, got: %v", err)
	}
}

func TestAuthorizeToolUsage_AllowedToolsConfig(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	t.Cleanup(func() { ex.scheduler.Stop() })
	actor := models.AgentConfig{
		ID:   "a",
		Role: "worker",
		Config: []byte(`{
			"allowed_tools": ["agent_message"]
		}`),
	}
	ctx := WithActor(context.Background(), actor)
	// Allowed.
	if _, err := ex.Execute(ctx, "agent_message", map[string]any{"target_agent_id": "x"}); err == nil {
		// It will fail later because bus/manager isn't configured, but must pass allowlist gate.
	}
	// Blocked.
	if _, err := ex.Execute(ctx, "schedule", map[string]any{}); err == nil {
		t.Fatalf("expected tool not allowed error")
	}
}

func TestAuthorizeMailboxSend_RoleCoverage(t *testing.T) {
	for _, role := range []string{
		"validation-coordinator",
		"vp-growth",
		"support-agent",
		"marketing-agent",
	} {
		if err := authorizeMailboxSend(models.AgentConfig{Role: role}); err != nil {
			t.Fatalf("expected role %s to be allowed mailbox_send: %v", role, err)
		}
	}
}
