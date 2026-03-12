package corpusobs

import (
	"context"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	runtimescanmode "empireai/internal/runtime/scanmode"
	runtimesharedjson "empireai/internal/runtime/sharedjson"
)

type TurnMeta struct {
	EventID      string
	EventType    string
	AgentID      string
	VerticalID   string
	ScanID       string
	CampaignID   string
	AssignedAt   time.Time
	BatchSize    int
	PayloadBytes int
}

type EmitSnapshot struct {
	FirstEmitAt       time.Time
	EmitCount         int
	ScanCompleteEmits int
}

var emitTracker = struct {
	mu      sync.Mutex
	byEvent map[string]EmitSnapshot
}{
	byEvent: map[string]EmitSnapshot{},
}

func TurnMetaFromEvent(evt events.Event) (TurnMeta, bool) {
	if strings.TrimSpace(string(evt.Type)) != "market_research.scan_assigned" {
		return TurnMeta{}, false
	}
	payload := parsePayloadMap(evt.Payload)
	if normalizeScanMode(asString(payload["mode"])) != "corpus" {
		return TurnMeta{}, false
	}
	meta := TurnMeta{
		EventID:      strings.TrimSpace(evt.ID),
		EventType:    strings.TrimSpace(string(evt.Type)),
		AgentID:      strings.TrimSpace(evt.SourceAgent),
		VerticalID:   strings.TrimSpace(evt.EntityID()),
		ScanID:       strings.TrimSpace(asString(payload["scan_id"])),
		CampaignID:   strings.TrimSpace(asString(payload["campaign_id"])),
		AssignedAt:   evt.CreatedAt.UTC(),
		BatchSize:    corpusSignalsCount(payload["corpus_signals"]),
		PayloadBytes: len(evt.Payload),
	}
	return meta, true
}

func RecordEmitFromContext(ctx context.Context, toolName string, at time.Time) (TurnMeta, EmitSnapshot, bool) {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" || !strings.HasPrefix(toolName, "emit_") {
		return TurnMeta{}, EmitSnapshot{}, false
	}
	inbound, ok := runtimebus.InboundEventFromContext(ctx)
	if !ok {
		return TurnMeta{}, EmitSnapshot{}, false
	}
	meta, ok := TurnMetaFromEvent(inbound)
	if !ok || strings.TrimSpace(meta.EventID) == "" {
		return TurnMeta{}, EmitSnapshot{}, false
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	emitTracker.mu.Lock()
	defer emitTracker.mu.Unlock()
	snapshot := emitTracker.byEvent[meta.EventID]
	if snapshot.FirstEmitAt.IsZero() {
		snapshot.FirstEmitAt = at.UTC()
	}
	snapshot.EmitCount++
	if toolName == "emit_market_research_scan_complete" {
		snapshot.ScanCompleteEmits++
	}
	emitTracker.byEvent[meta.EventID] = snapshot
	return meta, snapshot, true
}

func ConsumeEmitSnapshot(eventID string) EmitSnapshot {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return EmitSnapshot{}
	}
	emitTracker.mu.Lock()
	defer emitTracker.mu.Unlock()
	snapshot := emitTracker.byEvent[eventID]
	delete(emitTracker.byEvent, eventID)
	return snapshot
}

func corpusSignalsCount(v any) int {
	switch t := v.(type) {
	case []any:
		return len(t)
	case []map[string]any:
		return len(t)
	default:
		return 0
	}
}

func parsePayloadMap(raw []byte) map[string]any {
	return runtimesharedjson.ParsePayloadMap(raw)
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	default:
		return ""
	}
}

func normalizeScanMode(raw string) string {
	return runtimescanmode.NormalizeMode(raw)
}
