package runtime

import (
	"context"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
)

type corpusTurnMeta struct {
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

type corpusEmitSnapshot struct {
	FirstEmitAt       time.Time
	EmitCount         int
	ScanCompleteEmits int
}

var corpusEmitTracker = struct {
	mu      sync.Mutex
	byEvent map[string]corpusEmitSnapshot
}{
	byEvent: map[string]corpusEmitSnapshot{},
}

func corpusTurnMetaFromEvent(evt events.Event) (corpusTurnMeta, bool) {
	if strings.TrimSpace(string(evt.Type)) != "market_research.scan_assigned" {
		return corpusTurnMeta{}, false
	}
	payload := parsePayloadMap(evt.Payload)
	if normalizeScanMode(asString(payload["mode"])) != "corpus" {
		return corpusTurnMeta{}, false
	}
	meta := corpusTurnMeta{
		EventID:      strings.TrimSpace(evt.ID),
		EventType:    strings.TrimSpace(string(evt.Type)),
		AgentID:      strings.TrimSpace(evt.SourceAgent),
		VerticalID:   strings.TrimSpace(evt.VerticalID),
		ScanID:       strings.TrimSpace(asString(payload["scan_id"])),
		CampaignID:   strings.TrimSpace(asString(payload["campaign_id"])),
		AssignedAt:   evt.CreatedAt.UTC(),
		BatchSize:    corpusSignalsCount(payload["corpus_signals"]),
		PayloadBytes: len(evt.Payload),
	}
	return meta, true
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

func recordCorpusEmitFromContext(ctx context.Context, toolName string, at time.Time) (corpusTurnMeta, corpusEmitSnapshot, bool) {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" || !strings.HasPrefix(toolName, "emit_") {
		return corpusTurnMeta{}, corpusEmitSnapshot{}, false
	}
	inbound, ok := InboundEventFromContext(ctx)
	if !ok {
		return corpusTurnMeta{}, corpusEmitSnapshot{}, false
	}
	meta, ok := corpusTurnMetaFromEvent(inbound)
	if !ok || strings.TrimSpace(meta.EventID) == "" {
		return corpusTurnMeta{}, corpusEmitSnapshot{}, false
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	corpusEmitTracker.mu.Lock()
	defer corpusEmitTracker.mu.Unlock()
	snapshot := corpusEmitTracker.byEvent[meta.EventID]
	if snapshot.FirstEmitAt.IsZero() {
		snapshot.FirstEmitAt = at.UTC()
	}
	snapshot.EmitCount++
	if toolName == "emit_market_research_scan_complete" {
		snapshot.ScanCompleteEmits++
	}
	corpusEmitTracker.byEvent[meta.EventID] = snapshot
	return meta, snapshot, true
}

func consumeCorpusEmitSnapshot(eventID string) corpusEmitSnapshot {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return corpusEmitSnapshot{}
	}
	corpusEmitTracker.mu.Lock()
	defer corpusEmitTracker.mu.Unlock()
	snapshot := corpusEmitTracker.byEvent[eventID]
	delete(corpusEmitTracker.byEvent, eventID)
	return snapshot
}
