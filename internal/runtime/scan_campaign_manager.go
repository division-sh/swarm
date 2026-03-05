package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

// ScanCampaignManager maintains the scan_campaigns queue (spec v2.0 GAP 3).
// It is a lightweight runtime loop: claim queued campaigns, emit scan.requested,
// and react to scan.completed + budget throttle/resume events.
type ScanCampaignManager struct {
	bus                *EventBus
	store              ScanCampaignPersistence
	db                 *sql.DB
	maxPendingMailbox  int
	budgetPaused       bool
	backpressurePaused bool
}

func NewScanCampaignManager(bus *EventBus, store ScanCampaignPersistence, db ...*sql.DB) *ScanCampaignManager {
	var sqlDB *sql.DB
	if len(db) > 0 {
		sqlDB = db[0]
	}
	return &ScanCampaignManager{
		bus:               bus,
		store:             store,
		db:                sqlDB,
		maxPendingMailbox: 5,
	}
}

func (m *ScanCampaignManager) Run(ctx context.Context) {
	if m == nil || m.bus == nil || m.store == nil {
		return
	}

	ch := m.subscribe()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Kick once on startup.
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
	start := time.Now()
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
		campaignID, _ := payload["campaign_id"].(string)
		campaignID = strings.TrimSpace(campaignID)
		discoveries := asInt(payload["discoveries_count"])
		if discoveries == 0 {
			// Backward-compatible fallback for scan.completed payloads that still
			// use verticals_discovered naming from pipeline coordinator snapshots.
			discoveries = asInt(payload["verticals_discovered"])
		}
		emitted := make([]string, 0, 1)
		if campaignID != "" {
			if err := m.store.MarkScanCampaignCompleted(ctx, campaignID, discoveries); err != nil {
				_ = RecordPipelineTransition(ctx, m.db, PipelineTransitionInput{
					EventID:      evt.ID,
					EventType:    string(evt.Type),
					Handler:      "scanCampaign.onScanCompleted",
					PipelineType: "campaign",
					PipelineID:   campaignID,
					Action:       "error",
					Error:        err.Error(),
					Duration:     time.Since(start),
				})
				return
			}
			if m.emitCampaignCompletedIfDone(ctx, campaignID, discoveries, evt.ID) {
				emitted = append(emitted, "campaign.completed")
			}
			_ = RecordPipelineTransition(ctx, m.db, PipelineTransitionInput{
				EventID:      evt.ID,
				EventType:    string(evt.Type),
				Handler:      "scanCampaign.onScanCompleted",
				PipelineType: "campaign",
				PipelineID:   campaignID,
				Action:       "consumed",
				StateAfter: map[string]any{
					"status":            "completed",
					"discoveries_count": discoveries,
				},
				EventsEmitted: emitted,
				Duration:      time.Since(start),
			})
		} else {
			_ = RecordPipelineTransition(ctx, m.db, PipelineTransitionInput{
				EventID:      evt.ID,
				EventType:    string(evt.Type),
				Handler:      "scanCampaign.onScanCompleted",
				PipelineType: "scan",
				PipelineID:   evt.ID,
				Action:       "dropped",
				DropReason:   "scan.completed missing campaign_id",
				Duration:     time.Since(start),
			})
		}
		// Fire the next queued campaign.
		m.tick(ctx)
	case "vertical.killed", "mailbox.item_decided", "vertical.resumed":
		// Backpressure may clear when mailbox items are decided/killed.
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
		"strategic_context": parsePayloadMap(strategic),
	}
	if err := m.bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("campaign.completed"),
		SourceAgent: "scan-campaign-manager",
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}); err != nil {
		runtimeWarn(
			"scan-campaign-manager",
			"failed to publish campaign.completed campaign_id=%s source_event_id=%s: %v",
			strings.TrimSpace(campaignID),
			strings.TrimSpace(sourceEventID),
			err,
		)
	}
	return true
}

func (m *ScanCampaignManager) tick(ctx context.Context) {
	if m.shouldPauseForBackpressure(ctx) {
		return
	}
	// Requeue due rescans first.
	_, _ = m.store.RequeueDueRescans(ctx, time.Now().UTC())

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
	strategicContext := parsePayloadMap(c.StrategicContext)
	corpusPath := extractCorpusPathFromStrategicContext(strategicContext)

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
	if err := m.bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scan.requested"),
		SourceAgent: "scan-campaign-manager",
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now(),
	}); err != nil {
		runtimeWarn(
			"scan-campaign-manager",
			"failed to publish scan.requested campaign_id=%s geography_id=%s mode=%s: %v",
			strings.TrimSpace(c.ID),
			strings.TrimSpace(c.GeographyID),
			strings.TrimSpace(c.Mode),
			err,
		)
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
		runtimeWarn(
			"scan-campaign-manager",
			"failed to parse directive payload event_id=%s: %v",
			strings.TrimSpace(evt.ID),
			err,
		)
	}
	text := strings.TrimSpace(asString(payload["directive_text"]))
	if text == "" {
		text = strings.TrimSpace(asString(payload["message"]))
	}
	if text == "" {
		return
	}
	parsed := (DirectiveParser{}).Parse(text)

	if isComplexDirectiveText(text) {
		// Runtime couldn't parse deterministically; forward only this directive
		// to Empire Coordinator for interpretation.
		forwardedPayload := map[string]any{
			"directive_text":   text,
			"timestamp":        time.Now().UTC().Format(time.RFC3339),
			"forwarded_by":     "scan-campaign-manager",
			"original_event":   evt.ID,
			"parsed_directive": parsed,
		}
		if err := m.bus.PublishDirect(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("system.directive"),
			SourceAgent: "scan-campaign-manager",
			Payload:     mustJSON(forwardedPayload),
			CreatedAt:   time.Now(),
		}, []string{"empire-coordinator"}); err != nil {
			runtimeWarn(
				"scan-campaign-manager",
				"failed to forward complex directive to empire-coordinator event_id=%s: %v",
				strings.TrimSpace(evt.ID),
				err,
			)
		}
		return
	}

	mode, explicitMode := parsed.Mode, parsed.ExplicitMode
	geoName, country, region := parsed.Geography, parsed.Country, parsed.Region
	corpusPath := strings.TrimSpace(asString(payload["corpus_path"]))
	if corpusPath == "" {
		corpusPath = strings.TrimSpace(parsed.CorpusPath)
	}
	if mode == "corpus" && corpusPath == "" {
		runtimeWarn(
			"scan-campaign-manager",
			"directive requested corpus mode without corpus_path event_id=%s",
			strings.TrimSpace(evt.ID),
		)
		return
	}
	geoID, err := ensureDirectiveGeography(ctx, m.db, geoName, country, region)
	if err != nil {
		log.Printf("scan campaign directive geography resolution failed: %v", err)
		return
	}

	strategic := map[string]any{
		"directive_text": text,
		"parsed":         parsed,
	}
	if sentBy := strings.TrimSpace(asString(payload["sent_by"])); sentBy != "" {
		strategic["sent_by"] = sentBy
	}
	if corpusPath != "" {
		strategic["corpus_path"] = corpusPath
	}
	deadline := time.Now().UTC().Add(24 * time.Hour)

	for _, nextMode := range campaignModesForDirective(mode, explicitMode) {
		if err := m.ensureQueuedCampaign(ctx, geoID, nextMode, strings.TrimSpace(evt.ID), mustJSON(strategic), &deadline); err != nil {
			log.Printf("queue follow-on campaign failed geography=%s mode=%s err=%v", geoName, nextMode, err)
		}
	}
}

func (m *ScanCampaignManager) ensureQueuedCampaign(ctx context.Context, geographyID, mode, directiveID string, strategic json.RawMessage, deadline *time.Time) error {
	if m == nil || m.db == nil || m.store == nil {
		return nil
	}
	geographyID = strings.TrimSpace(geographyID)
	mode = normalizeScanMode(mode)
	if geographyID == "" || mode == "" {
		return nil
	}

	var exists bool
	if err := m.db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM scan_campaigns
			WHERE geography_id = $1::uuid
			  AND mode = $2
			  AND status IN ('queued','active','paused')
			LIMIT 1
		)
	`, geographyID, mode).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}

	_, err := m.store.CreateScanCampaign(ctx, CreateScanCampaignInput{
		GeographyID:      geographyID,
		DirectiveID:      strings.TrimSpace(directiveID),
		Mode:             mode,
		Priority:         "normal",
		Status:           "queued",
		StrategicContext: strategic,
		DeadlineAt:       deadline,
	})
	return err
}

func remainingCampaignModes(initialMode string) []string {
	cycle := []string{"saas_gap", "saas_trend", "local_services"}
	initialMode = normalizeScanMode(initialMode)
	if initialMode == "corpus" {
		// Corpus directives are self-contained and should not fan into scan cycles.
		return []string{}
	}
	if initialMode == "" {
		initialMode = "saas_gap"
	}
	idx := 0
	for i, mode := range cycle {
		if mode == initialMode {
			idx = i
			break
		}
	}
	out := make([]string, 0, len(cycle)-1)
	for i := idx + 1; i < len(cycle); i++ {
		out = append(out, cycle[i])
	}
	return out
}

func campaignModesForDirective(initialMode string, explicit bool) []string {
	initialMode = normalizeScanMode(initialMode)
	if initialMode == "" {
		initialMode = "saas_gap"
	}
	if explicit {
		return []string{initialMode}
	}
	modes := []string{initialMode}
	modes = append(modes, remainingCampaignModes(initialMode)...)
	return modes
}

func parseDirectiveMode(text string) (mode string, explicit bool) {
	t := strings.ToLower(strings.TrimSpace(text))
	if t == "" {
		return "saas_gap", false
	}
	switch {
	case strings.Contains(t, "corpus_path"),
		strings.Contains(t, " mode corpus"),
		strings.HasPrefix(t, "corpus"),
		strings.Contains(t, ".jsonl"),
		strings.Contains(t, ", corpus"),
		strings.Contains(t, " corpus "):
		return "corpus", true
	case strings.Contains(t, "automation_micro"),
		(strings.Contains(t, "automation") && strings.Contains(t, "micro")):
		return "saas_gap", true
	case strings.Contains(t, "local_services"), strings.Contains(t, "local service"):
		return "local_services", true
	case strings.Contains(t, "saas_trend"), (strings.Contains(t, "saas") && strings.Contains(t, "trend")):
		return "saas_trend", true
	case strings.Contains(t, "saas_gap"), strings.Contains(t, "gap scan"):
		return "saas_gap", true
	default:
		return "saas_gap", false
	}
}

func extractCorpusPathFromStrategicContext(strategic map[string]any) string {
	if len(strategic) == 0 {
		return ""
	}
	if path := strings.TrimSpace(asString(strategic["corpus_path"])); path != "" {
		return path
	}
	parsed, ok := strategic["parsed"].(map[string]any)
	if !ok {
		return ""
	}
	return strings.TrimSpace(asString(parsed["corpus_path"]))
}

func isComplexDirectiveText(text string) bool {
	t := strings.ToLower(strings.TrimSpace(text))
	if t == "" {
		return false
	}
	complexHints := []string{
		"latam",
		"across",
		"countries",
		"country with",
		"internet penetration",
		"focus on",
		"exclude",
		"excluding",
		"greater than",
		"less than",
		">",
		"<",
		"compared to",
	}
	for _, hint := range complexHints {
		if strings.Contains(t, hint) {
			return true
		}
	}
	return false
}

var directiveInPattern = regexp.MustCompile(`(?i)\bin\s+([a-z][a-z\s-]{2,})`)

func parseDirectiveGeography(text string) (name, country, region string) {
	raw := strings.TrimSpace(text)
	if raw == "" {
		return "unspecified", "unspecified", ""
	}
	lower := strings.ToLower(raw)
	known := map[string]string{
		"paraguay":  "Paraguay",
		"argentina": "Argentina",
		"brazil":    "Brazil",
		"mexico":    "Mexico",
		"chile":     "Chile",
		"peru":      "Peru",
		"colombia":  "Colombia",
		"uruguay":   "Uruguay",
	}
	for needle, label := range known {
		if strings.Contains(lower, needle) {
			return label, label, ""
		}
	}
	m := directiveInPattern.FindStringSubmatch(raw)
	if len(m) == 2 {
		part := sanitizeGeographyPhrase(m[1])
		if part != "" {
			return part, part, ""
		}
	}
	return "unspecified", "unspecified", ""
}

func sanitizeGeographyPhrase(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	lower := strings.ToLower(v)
	for _, cut := range []string{" for ", " with ", " using ", " where ", ".", ","} {
		if idx := strings.Index(lower, cut); idx >= 0 {
			v = strings.TrimSpace(v[:idx])
			lower = strings.ToLower(v)
		}
	}
	if v == "" {
		return ""
	}
	parts := strings.Fields(strings.ToLower(v))
	for i := range parts {
		if len(parts[i]) == 0 {
			continue
		}
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, " ")
}

func ensureDirectiveGeography(ctx context.Context, db *sql.DB, name, country, region string) (string, error) {
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
		SELECT id::text
		FROM geographies
		WHERE lower(name) = lower($1)
		ORDER BY created_at DESC
		LIMIT 1
	`, name).Scan(&id)
	if err == nil && strings.TrimSpace(id) != "" {
		return id, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}

	id = uuid.NewString()
	scanCfg := mustJSON(map[string]any{
		"source":      "scan_campaign_manager.directive",
		"geography":   name,
		"country":     country,
		"region":      region,
		"recorded_at": time.Now().UTC().Format(time.RFC3339),
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, scan_config, created_at)
		VALUES ($1::uuid, $2, $3, NULLIF($4,''), $5::jsonb, now())
	`, id, name, country, region, string(scanCfg)); err != nil {
		return "", err
	}
	return id, nil
}

func asInt(v any) int {
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
		// best effort
		if n, err := strconv.Atoi(t); err == nil {
			return n
		}
	}
	return 0
}
