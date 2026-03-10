package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"empireai/internal/events"
	runtimeproductpolicy "empireai/internal/runtime/productpolicy"
	"github.com/google/uuid"
)

type ScanCampaignBus interface {
	Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event
	Publish(ctx context.Context, evt events.Event) error
	PublishDirect(ctx context.Context, evt events.Event, recipients []string) error
}

type ScanCampaignTransitionInput struct {
	EventID       string
	EventType     string
	Handler       string
	PipelineType  string
	PipelineID    string
	Action        string
	StateBefore   any
	StateAfter    any
	EventsEmitted []string
	DropReason    string
	Error         string
	Duration      time.Duration
}

type ScanCampaignHooks struct {
	Warnf                    func(component, format string, args ...any)
	RecordTransition         func(ctx context.Context, db *sql.DB, in ScanCampaignTransitionInput) error
	EnsureDirectiveGeography func(ctx context.Context, db *sql.DB, name, country, region string) (string, error)
	ScanPolicy               ScanPolicy
	Now                      func() time.Time
}

// ScanCampaignManager maintains the scan_campaigns queue.
type ScanCampaignManager struct {
	bus                ScanCampaignBus
	store              ScanCampaignPersistence
	db                 *sql.DB
	hooks              ScanCampaignHooks
	maxPendingMailbox  int
	budgetPaused       bool
	backpressurePaused bool
}

func controlPlaneDirectiveRecipient() string {
	return strings.TrimSpace(runtimeproductpolicy.ControlPlaneAgentID())
}

const DefaultCampaignTimeCap = 24 * time.Hour

func NewScanCampaignManager(bus ScanCampaignBus, store ScanCampaignPersistence, hooks ScanCampaignHooks, db ...*sql.DB) *ScanCampaignManager {
	var sqlDB *sql.DB
	if len(db) > 0 {
		sqlDB = db[0]
	}
	if hooks.EnsureDirectiveGeography == nil {
		hooks.EnsureDirectiveGeography = EnsureDirectiveGeography
	}
	if hooks.ScanPolicy == nil {
		hooks.ScanPolicy = defaultWorkflowModule().ScanPolicy()
	}
	return &ScanCampaignManager{
		bus:               bus,
		store:             store,
		db:                sqlDB,
		hooks:             hooks,
		maxPendingMailbox: 5,
	}
}

func (m *ScanCampaignManager) scanPolicy() ScanPolicy {
	if m == nil || m.hooks.ScanPolicy == nil {
		return defaultWorkflowModule().ScanPolicy()
	}
	return m.hooks.ScanPolicy
}

func (m *ScanCampaignManager) Run(ctx context.Context) {
	if m == nil || m.bus == nil || m.store == nil {
		return
	}
	ch := m.subscribe()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	m.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				m.resetFlags()
				ch = m.subscribe()
				continue
			}
			m.onEvent(ctx, evt)
		case <-ticker.C:
			m.tick(ctx)
		}
	}
}

func (m *ScanCampaignManager) OnEventForTest(ctx context.Context, evt events.Event) {
	m.onEvent(ctx, evt)
}

func (m *ScanCampaignManager) OnDirectiveForTest(ctx context.Context, evt events.Event) {
	m.onDirective(ctx, evt)
}

func (m *ScanCampaignManager) TickForTest(ctx context.Context) {
	m.tick(ctx)
}

func (m *ScanCampaignManager) EmitCampaignCompletedIfDoneForTest(ctx context.Context, campaignID string, discoveries int, sourceEventID string) bool {
	return m.emitCampaignCompletedIfDone(ctx, campaignID, discoveries, sourceEventID)
}

func (m *ScanCampaignManager) PendingMailboxCountForTest(ctx context.Context) (int, error) {
	return m.pendingMailboxCount(ctx)
}

func (m *ScanCampaignManager) ShouldPauseForBackpressureForTest(ctx context.Context) bool {
	return m.shouldPauseForBackpressure(ctx)
}

func (m *ScanCampaignManager) ResetFlagsForTest()              { m.resetFlags() }
func (m *ScanCampaignManager) SetBudgetPausedForTest(v bool)   { m.budgetPaused = v }
func (m *ScanCampaignManager) BudgetPausedForTest() bool       { return m.budgetPaused }
func (m *ScanCampaignManager) BackpressurePausedForTest() bool { return m.backpressurePaused }

func (m *ScanCampaignManager) subscribe() <-chan events.Event {
	return m.bus.Subscribe("scan-campaign-manager",
		events.EventType("system.directive"),
		events.EventType("scan.completed"),
		events.EventType("vertical.killed"),
		events.EventType("mailbox.item_decided"),
		events.EventType("vertical.resumed"),
		events.EventType("budget.throttle"),
		events.EventType("budget.resumed"),
		events.EventType("runtime.reset"),
	)
}

func (m *ScanCampaignManager) onEvent(ctx context.Context, evt events.Event) {
	start := m.now()
	switch string(evt.Type) {
	case "runtime.reset":
		m.resetFlags()
		return
	case "system.directive":
		m.onDirective(ctx, evt)
		m.tick(ctx)
	case "budget.throttle":
		m.budgetPaused = true
		if n, err := m.store.PauseQueuedScanCampaigns(ctx); err == nil && n > 0 {
			log.Printf("scan campaigns paused count=%d", n)
		}
	case "budget.resumed":
		m.budgetPaused = false
		if !m.backpressurePaused {
			if n, err := m.store.ResumePausedScanCampaigns(ctx); err == nil && n > 0 {
				log.Printf("scan campaigns resumed count=%d", n)
			}
		}
	case "scan.completed":
		var payload map[string]any
		_ = json.Unmarshal(evt.Payload, &payload)
		campaignID := strings.TrimSpace(asString(payload["campaign_id"]))
		discoveries := asIntLocal(payload["discoveries_count"])
		if discoveries == 0 {
			discoveries = asIntLocal(payload["verticals_discovered"])
		}
		emitted := make([]string, 0, 1)
		if campaignID != "" {
			if err := m.store.MarkScanCampaignCompleted(ctx, campaignID, discoveries); err != nil {
				m.recordTransition(ctx, ScanCampaignTransitionInput{
					EventID: evt.ID, EventType: string(evt.Type), Handler: "scanCampaign.onScanCompleted",
					PipelineType: "campaign", PipelineID: campaignID, Action: "error", Error: err.Error(), Duration: m.now().Sub(start),
				})
				return
			}
			if m.emitCampaignCompletedIfDone(ctx, campaignID, discoveries, evt.ID) {
				emitted = append(emitted, "campaign.completed")
			}
			m.recordTransition(ctx, ScanCampaignTransitionInput{
				EventID: evt.ID, EventType: string(evt.Type), Handler: "scanCampaign.onScanCompleted",
				PipelineType: "campaign", PipelineID: campaignID, Action: "consumed",
				StateAfter:    map[string]any{"status": "completed", "discoveries_count": discoveries},
				EventsEmitted: emitted, Duration: m.now().Sub(start),
			})
		} else {
			m.recordTransition(ctx, ScanCampaignTransitionInput{
				EventID: evt.ID, EventType: string(evt.Type), Handler: "scanCampaign.onScanCompleted",
				PipelineType: "scan", PipelineID: evt.ID, Action: "dropped", DropReason: "scan.completed missing campaign_id", Duration: m.now().Sub(start),
			})
		}
		m.tick(ctx)
	case "vertical.killed", "mailbox.item_decided", "vertical.resumed":
		m.tick(ctx)
	}
}

func (m *ScanCampaignManager) resetFlags() {
	m.budgetPaused = false
	m.backpressurePaused = false
}

func (m *ScanCampaignManager) emitCampaignCompletedIfDone(ctx context.Context, campaignID string, discoveries int, sourceEventID string) bool {
	if m == nil || m.db == nil {
		return false
	}
	campaignID = strings.TrimSpace(campaignID)
	if campaignID == "" {
		return false
	}
	var geographyID, mode, directiveID, priority string
	var strategic json.RawMessage
	if err := m.db.QueryRowContext(ctx, `
		SELECT geography_id::text, mode, COALESCE(directive_id::text, ''), COALESCE(priority, ''), COALESCE(strategic_context, '{}'::jsonb)
		FROM scan_campaigns
		WHERE id = $1::uuid
	`, campaignID).Scan(&geographyID, &mode, &directiveID, &priority, &strategic); err != nil {
		return false
	}
	var remaining int
	if err := m.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM scan_campaigns
		WHERE geography_id = $1::uuid
		  AND status IN ('queued','active','paused')
	`, geographyID).Scan(&remaining); err != nil {
		return false
	}
	if remaining > 0 {
		return false
	}
	payload := map[string]any{
		"campaign_id":       campaignID,
		"geography_id":      geographyID,
		"completed_mode":    strings.TrimSpace(mode),
		"discoveries_count": discoveries,
		"priority":          strings.TrimSpace(priority),
		"source_event_id":   strings.TrimSpace(sourceEventID),
		"directive_id":      strings.TrimSpace(directiveID),
		"strategic_context": parsePayloadMapRaw(strategic),
	}
	if err := m.bus.Publish(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("campaign.completed"), SourceAgent: "scan-campaign-manager", Payload: mustJSONBytes(payload), CreatedAt: m.now()}); err != nil {
		m.warnf("scan-campaign-manager", "failed to publish campaign.completed campaign_id=%s source_event_id=%s: %v", strings.TrimSpace(campaignID), strings.TrimSpace(sourceEventID), err)
	}
	return true
}

func (m *ScanCampaignManager) tick(ctx context.Context) {
	m.enforceCampaignTimeCap(ctx, m.now())
	if m.shouldPauseForBackpressure(ctx) {
		return
	}
	_, _ = m.store.RequeueDueRescans(ctx, m.now())
	c, ok, err := m.store.ClaimNextDueScanCampaign(ctx)
	if err != nil || !ok {
		return
	}
	geoLabel := ""
	if label, err := m.store.LookupGeographyLabel(ctx, c.GeographyID); err == nil {
		geoLabel = label
	}
	if strings.TrimSpace(geoLabel) == "" {
		geoLabel = "unspecified"
	}
	strategicContext := parsePayloadMapRaw(c.StrategicContext)
	corpusPath := m.scanPolicy().ExtractCorpusPathFromStrategicContext(strategicContext)
	payload := map[string]any{
		"campaign_id":         c.ID,
		"geography_id":        c.GeographyID,
		"geography":           geoLabel,
		"mode":                c.Mode,
		"taxonomy_categories": c.Categories,
		"priority":            c.Priority,
		"depth":               "full",
		"directive_id":        strings.TrimSpace(c.DirectiveID),
		"strategic_context":   strategicContext,
		"campaign_context": map[string]any{
			"modes":             []string{strings.TrimSpace(c.Mode)},
			"strategic_context": strings.TrimSpace(asString(strategicContext["directive_text"])),
			"directive_id":      strings.TrimSpace(c.DirectiveID),
		},
	}
	if corpusPath != "" {
		payload["corpus_path"] = corpusPath
	}
	if err := m.bus.Publish(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("scan.requested"), SourceAgent: "scan-campaign-manager", Payload: mustJSONBytes(payload), CreatedAt: m.now()}); err != nil {
		m.warnf("scan-campaign-manager", "failed to publish scan.requested campaign_id=%s geography_id=%s mode=%s: %v", strings.TrimSpace(c.ID), strings.TrimSpace(c.GeographyID), strings.TrimSpace(c.Mode), err)
	}
}

func (m *ScanCampaignManager) enforceCampaignTimeCap(ctx context.Context, now time.Time) {
	if m == nil || m.db == nil || m.store == nil {
		return
	}
	if now.IsZero() {
		now = m.now()
	}
	rows, err := m.db.QueryContext(ctx, `
		SELECT id::text, COALESCE(discoveries, 0)
		FROM scan_campaigns
		WHERE status IN ('queued', 'active', 'paused')
		  AND deadline_at IS NOT NULL
		  AND deadline_at <= $1
		ORDER BY deadline_at ASC, created_at ASC
		LIMIT 100
	`, now)
	if err != nil {
		m.warnf("scan-campaign-manager", "campaign time_cap query failed: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var campaignID string
		var discoveries int
		if err := rows.Scan(&campaignID, &discoveries); err != nil {
			m.warnf("scan-campaign-manager", "campaign time_cap row scan failed: %v", err)
			continue
		}
		campaignID = strings.TrimSpace(campaignID)
		if campaignID == "" {
			continue
		}
		if err := m.store.MarkScanCampaignCompleted(ctx, campaignID, discoveries); err != nil {
			m.warnf("scan-campaign-manager", "campaign time_cap mark completed failed campaign_id=%s: %v", campaignID, err)
			continue
		}
		_ = m.emitCampaignCompletedIfDone(ctx, campaignID, discoveries, "campaign_time_cap_exceeded")
	}
	if err := rows.Err(); err != nil {
		m.warnf("scan-campaign-manager", "campaign time_cap row iteration failed: %v", err)
	}
}

func (m *ScanCampaignManager) shouldPauseForBackpressure(ctx context.Context) bool {
	if m == nil {
		return true
	}
	if m.budgetPaused {
		return true
	}
	if m.db == nil || m.maxPendingMailbox <= 0 {
		return false
	}
	pending, err := m.pendingMailboxCount(ctx)
	if err != nil {
		return false
	}
	if pending >= m.maxPendingMailbox {
		if !m.backpressurePaused {
			if n, err := m.store.PauseQueuedScanCampaigns(ctx); err == nil && n > 0 {
				log.Printf("scan campaigns paused by mailbox backpressure count=%d pending=%d", n, pending)
			}
		}
		m.backpressurePaused = true
		return true
	}
	if m.backpressurePaused {
		if n, err := m.store.ResumePausedScanCampaigns(ctx); err == nil && n > 0 {
			log.Printf("scan campaigns resumed after mailbox backpressure clear count=%d pending=%d", n, pending)
		}
		m.backpressurePaused = false
	}
	return false
}

func (m *ScanCampaignManager) pendingMailboxCount(ctx context.Context) (int, error) {
	if m == nil || m.db == nil {
		return 0, nil
	}
	var pending int
	if err := m.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE status = 'pending' AND type = 'vertical_approval'`).Scan(&pending); err != nil {
		return 0, err
	}
	return pending, nil
}

func (m *ScanCampaignManager) onDirective(ctx context.Context, evt events.Event) {
	if m == nil || m.db == nil || m.store == nil {
		return
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		m.warnf("scan-campaign-manager", "failed to parse directive payload event_id=%s: %v", strings.TrimSpace(evt.ID), err)
	}
	text := strings.TrimSpace(asString(payload["directive_text"]))
	if text == "" {
		text = strings.TrimSpace(asString(payload["message"]))
	}
	if text == "" {
		return
	}
	parsed := m.scanPolicy().ParseDirective(text)
	if m.scanPolicy().IsComplexDirectiveText(text) {
		forwardedPayload := map[string]any{
			"directive_text":   text,
			"timestamp":        m.now().Format(time.RFC3339),
			"forwarded_by":     "scan-campaign-manager",
			"original_event":   evt.ID,
			"parsed_directive": parsed,
		}
		recipient := controlPlaneDirectiveRecipient()
		if err := m.bus.PublishDirect(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("system.directive"), SourceAgent: "scan-campaign-manager", Payload: mustJSONBytes(forwardedPayload), CreatedAt: m.now()}, []string{recipient}); err != nil {
			m.warnf("scan-campaign-manager", "failed to forward complex directive to control-plane agent event_id=%s: %v", strings.TrimSpace(evt.ID), err)
		}
		return
	}
	mode, explicitMode := parsed.Mode, parsed.ExplicitMode
	geoName, country, region := parsed.Geography, parsed.Country, parsed.Region
	corpusPath, err := m.scanPolicy().ResolveDirectiveCorpusPath(mode, parsed, payload)
	if err != nil {
		m.warnf("scan-campaign-manager", "directive corpus resolution failed event_id=%s: %v", strings.TrimSpace(evt.ID), err)
		return
	}
	geoID, err := m.hooks.EnsureDirectiveGeography(ctx, m.db, geoName, country, region)
	if err != nil {
		log.Printf("scan campaign directive geography resolution failed: %v", err)
		return
	}
	strategic := map[string]any{"directive_text": text, "parsed": parsed}
	if sentBy := strings.TrimSpace(asString(payload["sent_by"])); sentBy != "" {
		strategic["sent_by"] = sentBy
	}
	if corpusPath != "" {
		strategic["corpus_path"] = corpusPath
	}
	deadline := m.now().Add(DefaultCampaignTimeCap)
	for _, nextMode := range CampaignModesForDirective(mode, explicitMode) {
		if err := m.ensureQueuedCampaign(ctx, geoID, nextMode, strings.TrimSpace(evt.ID), mustJSONBytes(strategic), &deadline); err != nil {
			log.Printf("queue follow-on campaign failed geography=%s mode=%s err=%v", geoName, nextMode, err)
		}
	}
}

func (m *ScanCampaignManager) ensureQueuedCampaign(ctx context.Context, geographyID, mode, directiveID string, strategic json.RawMessage, deadline *time.Time) error {
	if m == nil || m.db == nil || m.store == nil {
		return nil
	}
	geographyID = strings.TrimSpace(geographyID)
	mode = runtimeproductpolicy.NormalizeScanMode(mode)
	if geographyID == "" || mode == "" {
		return nil
	}
	var exists bool
	if err := m.db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM scan_campaigns WHERE geography_id = $1::uuid AND mode = $2 AND status IN ('queued','active','paused') LIMIT 1
		)
	`, geographyID, mode).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	_, err := m.store.CreateScanCampaign(ctx, CreateScanCampaignInput{GeographyID: geographyID, DirectiveID: strings.TrimSpace(directiveID), Mode: mode, Priority: "normal", Status: "queued", StrategicContext: strategic, DeadlineAt: deadline})
	return err
}

func RemainingCampaignModes(initialMode string) []string {
	modes := runtimeproductpolicy.CampaignModesForDirective(initialMode, false)
	if len(modes) <= 1 {
		return []string{}
	}
	return append([]string(nil), modes[1:]...)
}

func CampaignModesForDirective(initialMode string, explicit bool) []string {
	modes := runtimeproductpolicy.CampaignModesForDirective(initialMode, explicit)
	return append([]string(nil), modes...)
}

func ParseDirectiveMode(text string) (mode string, explicit bool) {
	return runtimeproductpolicy.ParseDirectiveMode(text)
}

func EnsureDirectiveGeography(ctx context.Context, db *sql.DB, name, country, region string) (string, error) {
	if db == nil {
		return "", nil
	}
	name = strings.TrimSpace(name)
	country = strings.TrimSpace(country)
	region = strings.TrimSpace(region)
	if name == "" {
		name = "unspecified"
	}
	if country == "" {
		country = name
	}
	var id string
	err := db.QueryRowContext(ctx, `
		SELECT id::text FROM geographies WHERE lower(name) = lower($1) ORDER BY created_at DESC LIMIT 1
	`, name).Scan(&id)
	if err == nil && strings.TrimSpace(id) != "" {
		return id, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	id = uuid.NewString()
	scanCfg := mustJSONBytes(map[string]any{"source": "scan_campaign_manager.directive", "geography": name, "country": country, "region": region, "recorded_at": time.Now().UTC().Format(time.RFC3339)})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, scan_config, created_at)
		VALUES ($1::uuid, $2, $3, NULLIF($4,''), $5::jsonb, now())
	`, id, name, country, region, string(scanCfg)); err != nil {
		return "", err
	}
	return id, nil
}

func (m *ScanCampaignManager) now() time.Time {
	if m != nil && m.hooks.Now != nil {
		return m.hooks.Now().UTC()
	}
	return time.Now().UTC()
}

func (m *ScanCampaignManager) warnf(component, format string, args ...any) {
	if m != nil && m.hooks.Warnf != nil {
		m.hooks.Warnf(component, format, args...)
		return
	}
	log.Printf("%s: %s", strings.TrimSpace(component), fmt.Sprintf(format, args...))
}

func (m *ScanCampaignManager) recordTransition(ctx context.Context, in ScanCampaignTransitionInput) {
	if m == nil || m.db == nil || m.hooks.RecordTransition == nil {
		return
	}
	_ = m.hooks.RecordTransition(ctx, m.db, in)
}

func parsePayloadMapRaw(raw json.RawMessage) map[string]any {
	payload := map[string]any{}
	if len(raw) == 0 {
		return payload
	}
	_ = json.Unmarshal(raw, &payload)
	return payload
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return ""
	}
}

func asIntLocal(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case string:
		t = strings.TrimSpace(t)
		if t == "" {
			return 0
		}
		if n, err := strconv.Atoi(t); err == nil {
			return n
		}
	}
	return 0
}

func mustJSONBytes(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
