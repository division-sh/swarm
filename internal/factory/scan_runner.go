package factory

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"strings"
	"time"

	"empireai/internal/events"
	"empireai/internal/runtime"
	runtimebus "empireai/internal/runtime/bus"
	runtimetools "empireai/internal/runtime/tools"
	"github.com/google/uuid"
)

// ScanRequestedRunner executes scan.requested events using the built-in Pipeline.
// This provides a deterministic "Discovery Coordinator" execution path so that
// scan campaigns can run without relying on LLM behavior.
type ScanRequestedRunner struct {
	db      *sql.DB
	events  runtimebus.EventStore
	mailbox runtimetools.MailboxPersistence
	bus     *runtime.EventBus
}

func NewScanRequestedRunner(db *sql.DB, eventStore runtimebus.EventStore, mailbox runtimetools.MailboxPersistence, bus *runtime.EventBus) *ScanRequestedRunner {
	return &ScanRequestedRunner{db: db, events: eventStore, mailbox: mailbox, bus: bus}
}

func (r *ScanRequestedRunner) Run(ctx context.Context) {
	if r == nil || r.db == nil || r.bus == nil {
		return
	}
	ch := r.bus.Subscribe("scan-requested-runner", events.EventType("scan.requested"))
	for {
		select {
		case <-ctx.Done():
			return
		case evt := <-ch:
			r.handle(ctx, evt)
		}
	}
}

func (r *ScanRequestedRunner) handle(ctx context.Context, evt events.Event) {
	start := time.Now()
	var payload map[string]any
	_ = json.Unmarshal(evt.Payload, &payload)
	geography := strings.TrimSpace(asString(payload["geography"]))
	if geography == "" {
		geography = strings.TrimSpace(asString(payload["geography_label"]))
	}
	if geography == "" {
		geography = strings.TrimSpace(evt.VerticalID)
	}

	depth := strings.TrimSpace(asString(payload["depth"]))
	if depth == "" {
		depth = "full"
	}
	count := asInt(payload["count"])
	if count <= 0 {
		count = 3
	}

	p := NewPipeline(r.db, r.events, r.mailbox)
	mode := strings.TrimSpace(asString(payload["mode"]))
	if mode == "" {
		mode = "local_services"
	}
	pipelineID := strings.TrimSpace(asString(payload["campaign_id"]))
	if _, err := uuid.Parse(pipelineID); err != nil {
		pipelineID = evt.ID
	}
	sum, err := p.RunScanWithMode(ctx, geography, depth, mode, payload["taxonomy_categories"], count)
	latency := time.Since(start)
	if err != nil {
		log.Printf("scan.requested failed geography=%s depth=%s err=%v", geography, depth, err)
		_ = runtime.RecordPipelineTransition(ctx, r.db, runtime.PipelineTransitionInput{
			EventID:      evt.ID,
			EventType:    string(evt.Type),
			Handler:      "scanRunner.handleScanRequested",
			PipelineType: "scan",
			PipelineID:   pipelineID,
			Action:       "error",
			Error:        err.Error(),
			StateBefore: map[string]any{
				"geography": geography,
				"mode":      mode,
				"depth":     depth,
			},
			Duration: latency,
		})
		return
	}
	_ = runtime.RecordPipelineTransition(ctx, r.db, runtime.PipelineTransitionInput{
		EventID:      evt.ID,
		EventType:    string(evt.Type),
		Handler:      "scanRunner.handleScanRequested",
		PipelineType: "scan",
		PipelineID:   pipelineID,
		Action:       "consumed",
		StateBefore: map[string]any{
			"geography": geography,
			"mode":      mode,
			"depth":     depth,
		},
		StateAfter: map[string]any{
			"discoveries_count": sum.Discovered,
			"scored_count":      sum.Scored,
			"ready_for_review":  sum.ReadyForReview,
			"killed_count":      sum.Killed,
		},
		EventsEmitted: []string{"scan.completed"},
		Duration:      latency,
	})

	out := map[string]any{
		"campaign_id":        strings.TrimSpace(asString(payload["campaign_id"])),
		"geography":          geography,
		"depth":              depth,
		"mode":               mode,
		"categories_scanned": payload["taxonomy_categories"],
		"discoveries_count":  sum.Discovered,
		"scored_count":       sum.Scored,
		"ready_for_review":   sum.ReadyForReview,
		"killed_count":       sum.Killed,
		"latency_ms":         int(latency / time.Millisecond),
	}
	if err := r.bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scan.completed"),
		SourceAgent: "discovery-coordinator",
		Payload:     mustJSON(out),
		CreatedAt:   time.Now(),
	}); err != nil {
		log.Printf("scan.completed publish failed campaign=%s err=%v", strings.TrimSpace(asString(payload["campaign_id"])), err)
	}
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	default:
		return 0
	}
}
