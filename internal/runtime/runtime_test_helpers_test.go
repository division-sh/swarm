package runtime

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
)

func assertNoEventType(t *testing.T, ch <-chan events.Event, typ string, d time.Duration) {
	t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case evt := <-ch:
			if string(evt.Type) == typ {
				t.Fatalf("unexpected event type %s", typ)
			}
		case <-timer.C:
			return
		}
	}
}

func extractSystemPromptForTest(cfg models.AgentConfig) string {
	if len(cfg.Config) == 0 || !json.Valid(cfg.Config) {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(cfg.Config, &obj); err != nil {
		return ""
	}
	if v, ok := obj["system_prompt"].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}
