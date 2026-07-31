package contracts

import (
	"strings"
	"testing"
)

func TestInputPinResolutionGeneratedSourceModeMatrix(t *testing.T) {
	for _, mode := range []string{FlowInputResolutionModeSelect, FlowInputResolutionModeSelectOrCreate, FlowInputResolutionModeFanIn, FlowInputResolutionModeReply} {
		t.Run(mode, func(t *testing.T) {
			_, err := ResolveFlowInputInstanceSource(mode, FlowInputCarrySourceGeneratedUUID)
			if err == nil || !strings.Contains(err.Error(), "only valid for resolution mode create") {
				t.Fatalf("ResolveFlowInputInstanceSource error = %v, want create-only rejection", err)
			}
		})
	}
}

func TestInputPinResolutionCreateSourceMatrix(t *testing.T) {
	tests := []struct {
		path string
		kind FlowInputInstanceSourceKind
	}{
		{path: FlowInputCarrySourceGeneratedUUID, kind: FlowInputInstanceSourceGeneratedUUID},
		{path: FlowInputCarrySourceEventID, kind: FlowInputInstanceSourceEventID},
		{path: "payload.external_account_id", kind: FlowInputInstanceSourcePayload},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			source, err := ResolveFlowInputInstanceSource(FlowInputResolutionModeCreate, tc.path)
			if err != nil {
				t.Fatalf("ResolveFlowInputInstanceSource: %v", err)
			}
			if source.Kind != tc.kind || source.Path != tc.path {
				t.Fatalf("source = %#v, want %s/%s", source, tc.kind, tc.path)
			}
		})
	}
}

func TestInputPinResolutionRejectsUnknownGeneratedSource(t *testing.T) {
	for _, path := range []string{"generated.ulid", "generated.sequence", "generated.UUID"} {
		if _, err := ResolveFlowInputInstanceSource(FlowInputResolutionModeCreate, path); err == nil || !strings.Contains(err.Error(), "only generated.uuid is supported") {
			t.Fatalf("ResolveFlowInputInstanceSource(%q) error = %v, want closed generated namespace", path, err)
		}
	}
}

func TestInputPinResolutionSelectingSourceMustBeTopLevelPayload(t *testing.T) {
	for _, path := range []string{"event.id", "payload", "payload.account.id", "payload.", "account_id"} {
		if _, err := ResolveFlowInputInstanceSource(FlowInputResolutionModeSelect, path); err == nil {
			t.Fatalf("ResolveFlowInputInstanceSource(select, %q) succeeded, want fail-closed", path)
		}
	}
}
