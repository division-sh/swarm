package pipeline

import (
	"bytes"
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
	}
}
