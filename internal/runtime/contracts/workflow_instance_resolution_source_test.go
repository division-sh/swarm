package contracts

import (
	"strings"
	"testing"
)

func TestInputPinResolutionGeneratedSourceModeMatrix(t *testing.T) {
	for _, mode := range []FlowInputResolutionMode{FlowInputResolutionModeSelect, FlowInputResolutionModeSelectOrCreate, FlowInputResolutionModeFanIn, FlowInputResolutionModeReply} {
		t.Run(FlowInputResolutionModeCode(mode), func(t *testing.T) {
			_, err := ResolveFlowInputInstanceSource(mode, FlowInputInstanceSourceGeneratedUUIDPath)
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
		{path: FlowInputInstanceSourceGeneratedUUIDPath, kind: FlowInputInstanceSourceGeneratedUUID},
		{path: FlowInputInstanceSourceEventIDPath, kind: FlowInputInstanceSourceEventID},
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

func TestResolveFlowInputInstanceSourceTypeRejectsProducerReceiverTypeMismatch(t *testing.T) {
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
	events := map[string]EventCatalogEntry{
		"account.ready": {Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{
			"account_id": {Type: "integer"},
		}, Required: []string{"account_id"}}},
	}
	bundle := instanceResolutionTestBundle(events)
	pin := mustCompileInputPinForTest(t, "account", "account.ready")
	_, err = bundle.ResolveFlowInputInstanceSourceType(nil, "account", pin, instance)
	if err == nil || !strings.Contains(err.Error(), "key_types_incompatible") {
		t.Fatalf("ResolveFlowInputInstanceSourceType error = %v, want producer/receiver mismatch", err)
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
	events := map[string]EventCatalogEntry{
		"account.ready": {Payload: EventPayloadSpec{Properties: map[string]EventFieldSpec{
			"external_account_id": {Type: "string"},
		}, Required: []string{"external_account_id"}}},
	}
	bundle := instanceResolutionTestBundle(events)

	for _, tc := range []struct {
		name string
		mode FlowInputResolutionMode
		from string
		kind FlowInputInstanceSourceKind
	}{
		{name: "payload string to uuid", mode: FlowInputResolutionModeSelect, from: "payload.external_account_id", kind: FlowInputInstanceSourcePayload},
		{name: "generated uuid", mode: FlowInputResolutionModeCreate, from: FlowInputInstanceSourceGeneratedUUIDPath, kind: FlowInputInstanceSourceGeneratedUUID},
		{name: "event id", mode: FlowInputResolutionModeCreate, from: FlowInputInstanceSourceEventIDPath, kind: FlowInputInstanceSourceEventID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pin, err := CompileFlowInputPin(
				FlowPinCompilationContext{FlowID: "account", FlowPath: "account"},
				FlowInputEventPin{Event: "account.ready", Resolution: FlowInputPinResolution{Mode: tc.mode, From: tc.from}},
			)
			if err != nil {
				t.Fatalf("CompileFlowInputPin: %v", err)
			}
			evidence, err := bundle.ResolveFlowInputInstanceSourceType(nil, "account", pin, instance)
			if err != nil {
				t.Fatalf("ResolveFlowInputInstanceSourceType: %v", err)
			}
			if evidence.Source.Kind != tc.kind {
				t.Fatalf("source kind = %q, want %q", evidence.Source.Kind, tc.kind)
			}
		})
	}
}

func instanceResolutionTestBundle(events map[string]EventCatalogEntry) *WorkflowContractBundle {
	return &WorkflowContractBundle{
		Events: events,
		projectContracts: map[string]ProjectContractView{
			".": {Paths: ProjectPackagePaths{Key: ".", ProjectEventsFile: "events.yaml"}, Events: events},
		},
	}
}

func TestRequireInstanceSourceTypesCompatiblePreservesIntegerConstraints(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		target  string
		wantErr bool
	}{
		{name: "integer identity", source: "integer", target: "integer"},
		{name: "integer widens to number", source: "integer", target: "number"},
		{name: "number identity", source: "number", target: "number"},
		{name: "number cannot narrow to integer", source: "number", target: "integer", wantErr: true},
		{name: "numeric alias cannot narrow to bigint", source: "numeric", target: "bigint", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := requireInstanceSourceTypesCompatible(
				CatalogTypeReference{Type: tc.source},
				CatalogTypeReference{Type: tc.target},
			)
			if (err != nil) != tc.wantErr {
				t.Fatalf("requireInstanceSourceTypesCompatible(%q, %q) error = %v, wantErr %v", tc.source, tc.target, err, tc.wantErr)
			}
		})
	}
}
