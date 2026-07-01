package sessions

import (
	"strings"
	"testing"
)

func TestResolveAuthoredAgentMemoryModeDerivesRuntimeScope(t *testing.T) {
	tests := []struct {
		raw       string
		wantMode  RuntimeMode
		wantScope SessionScope
	}{
		{raw: "task", wantMode: RuntimeModeTask, wantScope: ""},
		{raw: "session", wantMode: RuntimeModeSession, wantScope: SessionScopeFlow},
		{raw: "session_per_entity", wantMode: RuntimeModeSessionPerEntity, wantScope: SessionScopeEntity},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			mode, scope, err := ResolveAuthoredAgentMemoryMode(tt.raw)
			if err != nil {
				t.Fatalf("ResolveAuthoredAgentMemoryMode(%q): %v", tt.raw, err)
			}
			if mode != tt.wantMode || scope != tt.wantScope {
				t.Fatalf("ResolveAuthoredAgentMemoryMode(%q) = (%q, %q), want (%q, %q)", tt.raw, mode, scope, tt.wantMode, tt.wantScope)
			}
		})
	}
}

func TestResolveAuthoredAgentMemoryModeRejectsLegacyAndUnknownValues(t *testing.T) {
	tests := []struct {
		raw      string
		contains string
	}{
		{raw: "", contains: "mode is required"},
		{raw: "stateless", contains: "retired"},
		{raw: "global", contains: "reserved"},
		{raw: "forever", contains: "invalid mode"},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			if _, _, err := ResolveAuthoredAgentMemoryMode(tt.raw); err == nil || !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("ResolveAuthoredAgentMemoryMode(%q) error = %v, want %q", tt.raw, err, tt.contains)
			}
		})
	}
}
