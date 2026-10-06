package pipeline

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func constructionPublicationFieldsFixture(t *testing.T) (workflowInitialMaterializationProjection, flowidentity.RunScopedFlowInstance) {
	t.Helper()
	const run = "11111111-1111-4111-8111-111111111111"
	const path = "account/item"
	entity := flowidentity.EntityID(path)
	owner, err := flowidentity.NewRunScopedFlowInstance(run, flowidentity.StoredRoute("account", "item", path))
	if err != nil {
		t.Fatal(err)
	}
	return workflowInitialMaterializationProjection{
		Version: workflowInitialMaterializationProjectionVersion,
		RunID:   run, FlowInstance: path, EntityID: entity, WorkflowName: "account", WorkflowVersion: "1",
		OccurredAt:    time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC),
		CreatingInput: FlowConstructionInput{EventID: "22222222-2222-4222-8222-222222222222", Input: "account.initialized"},
		Persisted: workflowInstancePersistedProjection{
			Fields:  map[string]any{"integer": int64(7), "double": float64(7), "nested": map[string]any{"values": []any{int64(7), float64(7), nil}}, "literal": "${x}"},
			Control: workflowInstancePersistedControl{StorageRef: path, EntityID: entity},
		},
		Readiness: &DynamicFlowRuntimeReadinessPlan{
			RunID: run, WorkflowVersion: "1", BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), ExecutionMode: executionmode.Live,
			Identity: flowidentity.Instance{TemplateID: "account", ScopeKey: "account", InstanceID: "item", InstancePath: path, EntityID: entity, HasStoredPath: true},
		},
	}, owner
}

func TestFlowConstructionPublicationFieldsPreservesKindsAndIsolation(t *testing.T) {
	receipt, owner := constructionPublicationFieldsFixture(t)
	raw, err := canonicaljson.MarshalPreservingNumberKinds(receipt)
	if err != nil {
		t.Fatal(err)
	}
	before := bytes.Clone(raw)
	fields, err := FlowConstructionPublicationFields(raw, owner, receipt.EntityID, receipt.CreatingInput.EventID)
	if err != nil || !reflect.DeepEqual(fields, receipt.Persisted.Fields) {
		t.Fatalf("initial field projection lost number kinds: %#v err=%v", fields, err)
	}
	fields["integer"] = int64(99)
	fields["nested"].(map[string]any)["values"].([]any)[0] = int64(99)
	again, err := FlowConstructionPublicationFields(raw, owner, receipt.EntityID, receipt.CreatingInput.EventID)
	if err != nil || !reflect.DeepEqual(again, receipt.Persisted.Fields) || !bytes.Equal(raw, before) {
		t.Fatalf("caller mutation changed immutable evidence: %#v err=%v", again, err)
	}
	for _, empty := range []map[string]any{nil, {}} {
		receipt.Persisted.Fields = empty
		raw, err := canonicaljson.MarshalPreservingNumberKinds(receipt)
		if err != nil {
			t.Fatal(err)
		}
		got, err := FlowConstructionPublicationFields(raw, owner, receipt.EntityID, receipt.CreatingInput.EventID)
		if err != nil || !reflect.DeepEqual(got, empty) {
			t.Fatalf("fieldless/empty evidence changed: %#v err=%v", got, err)
		}
	}
}

func TestFlowConstructionPublicationDiscoversDetachedCreatingInput(t *testing.T) {
	receipt, owner := constructionPublicationFieldsFixture(t)
	raw, err := canonicaljson.MarshalPreservingNumberKinds(receipt)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := ProjectFlowConstructionPublication(raw, owner, receipt.EntityID)
	if err != nil || evidence.CreatingInput != receipt.CreatingInput || !reflect.DeepEqual(evidence.Fields, receipt.Persisted.Fields) {
		t.Fatalf("receipt discovery changed original provenance or precise fields: %#v %v", evidence, err)
	}
	evidence.CreatingInput.EventID = "33333333-3333-4333-8333-333333333333"
	evidence.CreatingInput.Input = "different.input"
	evidence.Fields["nested"].(map[string]any)["values"].([]any)[0] = int64(99)
	again, err := ProjectFlowConstructionPublication(raw, owner, receipt.EntityID)
	if err != nil || again.CreatingInput != receipt.CreatingInput || !reflect.DeepEqual(again.Fields, receipt.Persisted.Fields) {
		t.Fatalf("caller mutation changed original provenance or fields: %#v %v", again, err)
	}
	if err := ValidateFlowConstructionPublication(raw, owner, receipt.EntityID, evidence.CreatingInput.EventID); err == nil {
		t.Fatal("discovery weakened rejection of a later incoming publication")
	}
}

func TestFlowConstructionPublicationObservesTypedAbsenceButNeverMatchesAnEvent(t *testing.T) {
	receipt, owner := constructionPublicationFieldsFixture(t)
	receipt.CreatingInput = FlowConstructionInput{}
	raw, err := canonicaljson.MarshalPreservingNumberKinds(receipt)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := ProjectFlowConstructionPublication(raw, owner, receipt.EntityID)
	if err != nil || evidence.CreatingInput != (FlowConstructionInput{}) || !reflect.DeepEqual(evidence.Fields, receipt.Persisted.Fields) {
		t.Fatalf("explicit no-publication receipt lost provenance or fields: %#v %v", evidence, err)
	}
	for _, event := range []string{"", "22222222-2222-4222-8222-222222222222", "unrelated"} {
		if err := ValidateFlowConstructionPublication(raw, owner, receipt.EntityID, event); err == nil {
			t.Fatalf("typed absence falsely matched publication %q", event)
		}
		if fields, err := FlowConstructionPublicationFields(raw, owner, receipt.EntityID, event); err == nil || fields != nil {
			t.Fatalf("known-event fields accepted no-publication receipt for %q: %#v %v", event, fields, err)
		}
	}
	for _, variant := range []string{"missing", "null", "partial", "unknown"} {
		t.Run(variant, func(t *testing.T) {
			var document map[string]json.RawMessage
			if err := json.Unmarshal(raw, &document); err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "missing":
				delete(document, "creating_input")
			case "null":
				document["creating_input"] = json.RawMessage(`null`)
			case "partial":
				document["creating_input"] = json.RawMessage(`{"event_id":"","input":"task.create"}`)
			case "unknown":
				document["creating_input"] = json.RawMessage(`{"event_id":"","input":"","unknown":true}`)
			}
			invalid, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if evidence, err := ProjectFlowConstructionPublication(invalid, owner, receipt.EntityID); err == nil || !reflect.DeepEqual(evidence, FlowConstructionPublicationEvidence{}) {
				t.Fatalf("%s provenance became typed absence: %#v %v", variant, evidence, err)
			}
		})
	}
}

func TestFlowConstructionPublicationFieldsSharesStrictReceiptAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*workflowInitialMaterializationProjection)
	}{
		{"version", func(r *workflowInitialMaterializationProjection) { r.Version = 1 }},
		{"foreign_run", func(r *workflowInitialMaterializationProjection) { r.RunID = "33333333-3333-4333-8333-333333333333" }},
		{"foreign_entity", func(r *workflowInitialMaterializationProjection) { r.EntityID = "33333333-3333-4333-8333-333333333333" }},
		{"foreign_instance", func(r *workflowInitialMaterializationProjection) { r.FlowInstance = "account/foreign" }},
		{"foreign_template", func(r *workflowInitialMaterializationProjection) { r.WorkflowName = "other" }},
		{"missing_time", func(r *workflowInitialMaterializationProjection) { r.OccurredAt = time.Time{} }},
		{"foreign_creating_event", func(r *workflowInitialMaterializationProjection) {
			r.CreatingInput.EventID = "33333333-3333-4333-8333-333333333333"
		}},
		{"foreign_header", func(r *workflowInitialMaterializationProjection) { r.Persisted.Control.StorageRef = "account/foreign" }},
		{"missing_readiness", func(r *workflowInitialMaterializationProjection) { r.Readiness = nil }},
		{"foreign_readiness_run", func(r *workflowInitialMaterializationProjection) {
			r.Readiness.RunID = "33333333-3333-4333-8333-333333333333"
		}},
		{"foreign_readiness_entity", func(r *workflowInitialMaterializationProjection) {
			r.Readiness.Identity.EntityID = "33333333-3333-4333-8333-333333333333"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receipt, owner := constructionPublicationFieldsFixture(t)
			event := receipt.CreatingInput.EventID
			entity := receipt.EntityID
			tc.change(&receipt)
			raw, err := canonicaljson.MarshalPreservingNumberKinds(receipt)
			if err != nil {
				t.Fatal(err)
			}
			validation := ValidateFlowConstructionPublication(raw, owner, entity, event)
			fields, err := FlowConstructionPublicationFields(raw, owner, entity, event)
			if validation == nil || err == nil || fields != nil || validation.Error() != err.Error() {
				t.Fatalf("projection bypassed canonical refusal: fields=%#v err=%v validation=%v", fields, err, validation)
			}
			evidence, discoveryErr := ProjectFlowConstructionPublication(raw, owner, entity)
			if tc.name == "foreign_creating_event" {
				if discoveryErr != nil || evidence.CreatingInput != receipt.CreatingInput {
					t.Fatalf("discovery must report the stored event independently of the caller: %#v %v", evidence, discoveryErr)
				}
			} else if discoveryErr == nil || !reflect.DeepEqual(evidence, FlowConstructionPublicationEvidence{}) || discoveryErr.Error() != validation.Error() {
				t.Fatalf("discovery bypassed canonical receipt admission: %#v %v validation=%v", evidence, discoveryErr, validation)
			}
		})
	}
	receipt, owner := constructionPublicationFieldsFixture(t)
	valid, err := canonicaljson.MarshalPreservingNumberKinds(receipt)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{
		nil, []byte(`{`), []byte(`{}`), append(bytes.Clone(valid), []byte(` {}`)...),
		append([]byte(`{"unrecognized":true,`), valid[1:]...),
		append([]byte(`{"version":2,`), valid[1:]...),
		bytes.Replace(valid, []byte(`"integer":7`), []byte(`"integer":9007199254740993`), 1),
	} {
		if fields, err := FlowConstructionPublicationFields(raw, owner, receipt.EntityID, receipt.CreatingInput.EventID); err == nil || fields != nil {
			t.Fatalf("malformed receipt became initial fields: %s -> %#v err=%v", raw, fields, err)
		}
		if evidence, err := ProjectFlowConstructionPublication(raw, owner, receipt.EntityID); err == nil || !reflect.DeepEqual(evidence, FlowConstructionPublicationEvidence{}) {
			t.Fatalf("malformed receipt became creation coordinates: %s -> %#v err=%v", raw, evidence, err)
		}
	}
}
