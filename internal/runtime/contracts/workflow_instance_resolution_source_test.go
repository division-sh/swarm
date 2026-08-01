package contracts

import (
	"strings"
	"testing"
)

func TestInputPinResolutionGeneratedSourceModeMatrix(t *testing.T) {
	for _, mode := range []FlowInputResolutionMode{FlowInputResolutionModeSelect, FlowInputResolutionModeSelectOrCreate, FlowInputResolutionModeFanIn, FlowInputResolutionModeReply} {
		t.Run(mode.String(), func(t *testing.T) {
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
	for _, path := range []string{"event.id", "payload", "payload.account.id", "payload.", "payload. account_id", "account_id"} {
		if _, err := ResolveFlowInputInstanceSource(FlowInputResolutionModeSelect, path); err == nil {
			t.Fatalf("ResolveFlowInputInstanceSource(select, %q) succeeded, want fail-closed", path)
		}
	}
}

func TestInputPinResolutionPayloadSourceUsesCanonicalPathSpelling(t *testing.T) {
	source, err := ResolveFlowInputInstanceSource(FlowInputResolutionModeSelect, "  payload.account_id  ")
	if err != nil {
		t.Fatalf("ResolveFlowInputInstanceSource: %v", err)
	}
	if source.Path != "payload.account_id" {
		t.Fatalf("source path = %q, want canonical payload.account_id", source.Path)
	}
	if _, err := ResolveFlowInputInstanceSource(FlowInputResolutionModeSelect, "payload. account_id"); err == nil {
		t.Fatal("internally spaced payload source succeeded, want fail-closed canonical spelling")
	}
}

func TestResolveFlowInputInstanceSourceTypeUsesAuthoritativeSourceEvidence(t *testing.T) {
	field, err := ParseTemplateInstanceField("account_id")
	if err != nil {
		t.Fatalf("ParseTemplateInstanceField: %v", err)
	}
	instance := TemplateInstanceContract{
		FlowID: "account",
		Field:  field,
		PrimaryEntity: PrimaryEntityContract{
			FlowID: "account",
			Contract: EntityContract{Fields: map[string]EntityFieldDecl{
				"account_id": {Type: "text"},
			}},
			Types: TypeCatalogDocument{},
		},
	}
	bundle := &WorkflowContractBundle{Events: map[string]EventCatalogEntry{
		"account.ready": {Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{
			"account_id": {Type: "integer"},
		}}},
	}}

	for _, tc := range []struct {
		name      string
		carryType string
		want      string
	}{
		{name: "omitted annotation cannot hide mismatch", want: "actual type integer is incompatible with receiver"},
		{name: "dishonest annotation cannot replace source evidence", carryType: "text", want: "actual type integer is incompatible with declared carry type text"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pin := FlowInputEventPin{
				Name:       "account_ready",
				Event:      "account.ready",
				Resolution: FlowInputPinResolution{Mode: FlowInputResolutionModeSelect},
				Carries: FlowInputPinCarries{
					"account_id": {From: "payload.account_id", Type: tc.carryType},
				},
			}
			_, err := bundle.ResolveFlowInputInstanceSourceType(bundle, "account", pin, instance)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ResolveFlowInputInstanceSourceType error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestResolveFlowInputInstanceSourceTypeAcceptsScalarAliasesAndIntrinsicUUID(t *testing.T) {
	field, err := ParseTemplateInstanceField("account_id")
	if err != nil {
		t.Fatalf("ParseTemplateInstanceField: %v", err)
	}
	instance := TemplateInstanceContract{
		FlowID: "account",
		Field:  field,
		PrimaryEntity: PrimaryEntityContract{
			FlowID: "account",
			Contract: EntityContract{Fields: map[string]EntityFieldDecl{
				"account_id": {Type: "uuid"},
			}},
		},
	}
	bundle := &WorkflowContractBundle{Events: map[string]EventCatalogEntry{
		"account.ready": {Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{
			"external_account_id": {Type: "string"},
		}}},
	}}

	for _, tc := range []struct {
		name string
		mode FlowInputResolutionMode
		from string
		kind FlowInputInstanceSourceKind
	}{
		{name: "payload string to uuid", mode: FlowInputResolutionModeSelect, from: "payload.external_account_id", kind: FlowInputInstanceSourcePayload},
		{name: "generated uuid", mode: FlowInputResolutionModeCreate, from: FlowInputCarrySourceGeneratedUUID, kind: FlowInputInstanceSourceGeneratedUUID},
		{name: "event id", mode: FlowInputResolutionModeCreate, from: FlowInputCarrySourceEventID, kind: FlowInputInstanceSourceEventID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pin := FlowInputEventPin{
				Name:       "account_ready",
				Event:      "account.ready",
				Resolution: FlowInputPinResolution{Mode: tc.mode},
				Carries: FlowInputPinCarries{
					"account_id": {From: tc.from, Type: "text"},
				},
			}
			evidence, err := bundle.ResolveFlowInputInstanceSourceType(bundle, "account", pin, instance)
			if err != nil {
				t.Fatalf("ResolveFlowInputInstanceSourceType: %v", err)
			}
			if evidence.Source.Kind != tc.kind {
				t.Fatalf("source kind = %q, want %q", evidence.Source.Kind, tc.kind)
			}
		})
	}
}
