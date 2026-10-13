package pipeline

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func constructionReceiptFixture(t *testing.T) (FlowConstructionReceipt, flowidentity.RunScopedFlowInstance) {
	t.Helper()
	receipt, owner := constructionPublicationFieldsFixture(t)
	receipt.Identity.ParentEntityID = "77777777-7777-4777-8777-777777777777"
	receipt.Identity.ParentRoute = flowidentity.ParentRoute{FlowID: "parent", FlowInstance: "parent/keyed", EntityID: receipt.Identity.ParentEntityID}
	control := &receipt.Persisted.Control
	control.WorkflowVersion, control.EntityType, control.Slug, control.Name = "1", "account", "original-slug", "original-name"
	control.InstanceKind, control.TemplateVersion, control.Status = "template", "1", "active"
	control.ParentFlowID, control.ParentFlowInstance, control.ParentEntityID = receipt.Identity.ParentRoute.FlowID, receipt.Identity.ParentRoute.FlowInstance, receipt.Identity.ParentEntityID
	control.TransitionHistory = []WorkflowTransitionRecord{lifecycleTransitionRecordFixtureForTest(t, "account", "before", "initial", receipt.CreatingInput.EventID, receipt.OccurredAt)}
	receipt.Persisted.Bookkeeping = map[string]any{"initial": map[string]any{"integer": int64(7), "double": float64(7)}}
	receipt.Persisted.Accumulator = map[string]any{"members": []any{"original-member", int64(7), float64(7)}}
	receipt.Persisted.Gates = map[string]bool{"original-gate": true}
	receipt.CreationEvent = &DynamicFlowRuntimeCreationEventPlan{
		EventID: "33333333-3333-4333-8333-333333333333", EventType: "account.created", RunID: receipt.RunID,
		ParentEventID: receipt.CreatingInput.EventID, ExecutionMode: executionmode.Live,
		Payload: []byte(`{"double":7.0,"integer":7}`), CreatedAt: receipt.OccurredAt,
		DeliveryContext: events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "original-reply"}},
	}
	return receipt, owner
}

func constructionReceiptRaw(t *testing.T, receipt FlowConstructionReceipt) []byte {
	t.Helper()
	raw, err := canonicaljson.MarshalPreservingNumberKinds(receipt)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestFlowConstructionReceiptV3IndependentIdentityAndOccurrence(t *testing.T) {
	receipt, owner := constructionReceiptFixture(t)
	raw, err := EncodeFlowConstructionReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeFlowConstructionReceipt(raw, owner, receipt.EntityID)
	if err != nil || !reflect.DeepEqual(got, receipt) {
		t.Fatalf("immutable receipt lost evidence: %#v, %v", got, err)
	}
	if bytes.Contains(raw, []byte(`"readiness"`)) || !bytes.Contains(raw, []byte(`"version":3`)) {
		t.Fatalf("receipt retained operational readiness or old version: %s", raw)
	}
	evidence, err := ProjectFlowConstructionPublication(raw, owner, receipt.EntityID)
	if err != nil || evidence.Identity != receipt.Identity || evidence.CreatingInput != receipt.CreatingInput {
		t.Fatalf("publication projection depends on readiness: %#v, %v", evidence, err)
	}
	if err := ValidateFlowConstructionPublication(raw, owner, receipt.EntityID, receipt.CreationEvent.EventID); err == nil {
		t.Fatal("outgoing occurrence substituted for incoming construction")
	}
	got.Persisted.Fields["nested"].(map[string]any)["values"].([]any)[0] = int64(99)
	got.Persisted.Bookkeeping["initial"].(map[string]any)["integer"] = int64(99)
	got.Persisted.Accumulator["members"].([]any)[0] = "changed"
	got.Persisted.Gates["original-gate"] = false
	got.Persisted.Control.TransitionHistory[0].From = "changed"
	got.CreationEvent.Payload[0] = 'x'
	got.CreationEvent.DeliveryContext.Reply.ID = "changed"
	if !bytes.Equal(raw, constructionReceiptRaw(t, receipt)) {
		t.Fatal("decoded nested values alias immutable evidence")
	}
}

func TestFlowConstructionReceiptV3StoredHeaderUsesRetainedInstanceID(t *testing.T) {
	receipt, _ := constructionPublicationFieldsFixture(t)
	receipt.Identity.InstanceID = "retained-logical-id-not-path-tail"
	receipt.Persisted.Control.InstanceID = receipt.Identity.InstanceID
	raw, err := EncodeFlowConstructionReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeStoredFlowConstructionReceipt(raw, receipt.RunID, receipt.EntityID, receipt.FlowInstance, receipt.WorkflowName)
	if err != nil || !reflect.DeepEqual(got, receipt) {
		t.Fatalf("fixed header invented an instance ID instead of retaining evidence: %#v, %v", got, err)
	}
}

func TestFlowConstructionReceiptV3StoredHeaderRejectsMismatches(t *testing.T) {
	receipt, _ := constructionPublicationFieldsFixture(t)
	raw := constructionReceiptRaw(t, receipt)
	for _, header := range []struct {
		name, run, entity, path, workflow string
	}{
		{"missing_run", "", receipt.EntityID, receipt.FlowInstance, receipt.WorkflowName},
		{"missing_entity", receipt.RunID, "", receipt.FlowInstance, receipt.WorkflowName},
		{"missing_path", receipt.RunID, receipt.EntityID, "", receipt.WorkflowName},
		{"missing_workflow", receipt.RunID, receipt.EntityID, receipt.FlowInstance, ""},
		{"foreign_run", "44444444-4444-4444-8444-444444444444", receipt.EntityID, receipt.FlowInstance, receipt.WorkflowName},
		{"foreign_entity", receipt.RunID, receipt.RunID, receipt.FlowInstance, receipt.WorkflowName},
		{"foreign_path", receipt.RunID, receipt.EntityID, "account/other", receipt.WorkflowName},
		{"foreign_workflow", receipt.RunID, receipt.EntityID, receipt.FlowInstance, "other"},
		{"nonexact_run", " " + receipt.RunID, receipt.EntityID, receipt.FlowInstance, receipt.WorkflowName},
	} {
		t.Run(header.name, func(t *testing.T) {
			got, err := DecodeStoredFlowConstructionReceipt(raw, header.run, header.entity, header.path, header.workflow)
			if err == nil || !reflect.DeepEqual(got, FlowConstructionReceipt{}) {
				t.Fatalf("contradictory header admitted: %#v, %v", got, err)
			}
		})
	}
}

func TestFlowConstructionReceiptV3StoredHeaderDelegatesStrictAdmission(t *testing.T) {
	receipt, owner := constructionPublicationFieldsFixture(t)
	raw := string(constructionReceiptRaw(t, receipt))
	for _, scenario := range []struct{ name, raw string }{
		{"missing_identity", strings.Replace(raw, `"identity":`, `"IDENTITY":`, 1)},
		{"null_identity", strings.Replace(raw, `"identity":{`, `"identity":null,"ignored":{`, 1)},
		{"missing_instance_id", strings.Replace(raw, `"InstanceID":`, `"INSTANCE_ID":`, 1)},
		{"empty_instance_id", strings.Replace(raw, `"InstanceID":"item"`, `"InstanceID":""`, 1)},
		{"missing_scope", strings.Replace(raw, `"ScopeKey":`, `"SCOPE_KEY":`, 1)},
		{"empty_scope", strings.Replace(raw, `"ScopeKey":"account"`, `"ScopeKey":""`, 1)},
		{"null_parent", strings.Replace(raw, `"ParentRoute":{`, `"ParentRoute":null,"ignored_parent":{`, 1)},
		{"version_two", strings.Replace(raw, `"version":3`, `"version":2`, 1)},
		{"legacy_readiness", strings.TrimSuffix(raw, "}") + `,"readiness":{}}`},
		{"duplicate_identity", strings.TrimSuffix(raw, "}") + `,"identity":null}`},
		{"invalid_initial_state", strings.Replace(raw, `"initial_state":"initial"`, `"initial_state":""`, 1)},
		{"trailing_value", raw + `{}`},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			got, err := DecodeStoredFlowConstructionReceipt([]byte(scenario.raw), receipt.RunID, receipt.EntityID, receipt.FlowInstance, receipt.WorkflowName)
			if err == nil || !reflect.DeepEqual(got, FlowConstructionReceipt{}) {
				t.Fatalf("stored header bypassed strict receipt admission: %#v, %v", got, err)
			}
			if _, err := DecodeFlowConstructionReceipt([]byte(scenario.raw), owner, receipt.EntityID); err == nil {
				t.Fatal("negative fixture did not contradict the canonical receipt decoder")
			}
		})
	}
}

func TestFlowConstructionReceiptV3RejectsMissingAndAliasedEvidence(t *testing.T) {
	receipt, owner := constructionReceiptFixture(t)
	raw := constructionReceiptRaw(t, receipt)
	var original map[string]json.RawMessage
	if err := json.Unmarshal(raw, &original); err != nil {
		t.Fatal(err)
	}
	for name := range original {
		t.Run("missing_"+name, func(t *testing.T) {
			var document map[string]json.RawMessage
			if err := json.Unmarshal(raw, &document); err != nil {
				t.Fatal(err)
			}
			delete(document, name)
			invalid, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := DecodeFlowConstructionReceipt(invalid, owner, receipt.EntityID); err == nil || !reflect.DeepEqual(got, FlowConstructionReceipt{}) {
				t.Fatalf("missing evidence repaired: %#v, %v", got, err)
			}
		})
	}
	for _, scenario := range []struct{ name, raw string }{
		{"version_two", strings.Replace(string(raw), `"version":3`, `"version":2`, 1)},
		{"embedded_readiness", strings.TrimSuffix(string(raw), "}") + `,"readiness":{}}`},
		{"null_identity", strings.Replace(string(raw), `"identity":{`, `"identity":null,"ignored":{`, 1)},
		{"duplicate_identity", strings.TrimSuffix(string(raw), "}") + `,"identity":null}`},
		{"escaped_duplicate_identity", strings.TrimSuffix(string(raw), "}") + `,"ident\u0069ty":null}`},
		{"top_case_alias", strings.TrimSuffix(string(raw), "}") + `,"VERSION":3}`},
		{"identity_case_alias", strings.Replace(string(raw), `"TemplateID":`, `"templateid":`, 1)},
		{"parent_case_alias", strings.Replace(string(raw), `"FlowInstance":"parent/keyed"`, `"flowinstance":"parent/keyed"`, 1)},
		{"control_case_alias", strings.Replace(string(raw), `"storage_ref":`, `"STORAGE_REF":`, 1)},
		{"occurrence_case_alias", strings.Replace(string(raw), `"event_type":`, `"EVENT_TYPE":`, 1)},
		{"missing_occurrence_payload", strings.Replace(string(raw), `"payload":`, `"PAYLOAD":`, 1)},
		{"missing_initial_bookkeeping", strings.Replace(string(raw), `"bookkeeping":`, `"BOOKKEEPING":`, 1)},
		{"duplicate_initial_field", strings.Replace(string(raw), `"integer":7`, `"integer":7,"integer":8`, 1)},
		{"trailing_object", string(raw) + `{}`}, {"null_root", `null`}, {"array_root", `[]`},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			if got, err := DecodeFlowConstructionReceipt([]byte(scenario.raw), owner, receipt.EntityID); err == nil || !reflect.DeepEqual(got, FlowConstructionReceipt{}) {
				t.Fatalf("hostile receipt admitted: %#v, %v", got, err)
			}
		})
	}
}

func TestFlowConstructionReceiptV3RejectsIdentityAndOccurrenceCorruption(t *testing.T) {
	for _, scenario := range []struct {
		name string
		edit func(*FlowConstructionReceipt)
	}{
		{"missing_identity", func(r *FlowConstructionReceipt) { r.Identity = flowidentity.Instance{} }},
		{"missing_template", func(r *FlowConstructionReceipt) { r.Identity.TemplateID = "" }},
		{"missing_scope", func(r *FlowConstructionReceipt) { r.Identity.ScopeKey = "" }},
		{"missing_instance", func(r *FlowConstructionReceipt) { r.Identity.InstanceID = "" }},
		{"missing_stored_path", func(r *FlowConstructionReceipt) { r.Identity.HasStoredPath = false }},
		{"foreign_entity", func(r *FlowConstructionReceipt) { r.Identity.EntityID = r.RunID }},
		{"foreign_parent", func(r *FlowConstructionReceipt) { r.Identity.ParentEntityID = r.RunID }},
		{"missing_parent", func(r *FlowConstructionReceipt) { r.Identity.ParentRoute = flowidentity.ParentRoute{} }},
		{"foreign_control_parent", func(r *FlowConstructionReceipt) { r.Persisted.Control.ParentEntityID = r.RunID }},
		{"missing_initial_state", func(r *FlowConstructionReceipt) { r.InitialState = "" }},
		{"invalid_source", func(r *FlowConstructionReceipt) { r.BundleHash = "invalid" }},
		{"invalid_mode", func(r *FlowConstructionReceipt) { r.ExecutionMode = "invalid" }},
		{"foreign_occurrence_run", func(r *FlowConstructionReceipt) { r.CreationEvent.RunID = r.Identity.EntityID }},
		{"missing_occurrence_type", func(r *FlowConstructionReceipt) { r.CreationEvent.EventType = "" }},
		{"invalid_occurrence_identity", func(r *FlowConstructionReceipt) { r.CreationEvent.EventID = "invalid" }},
		{"missing_occurrence_parent", func(r *FlowConstructionReceipt) { r.CreationEvent.ParentEventID = "" }},
		{"wrong_occurrence_mode", func(r *FlowConstructionReceipt) { r.CreationEvent.ExecutionMode = executionmode.Mock }},
		{"duplicate_occurrence_payload", func(r *FlowConstructionReceipt) { r.CreationEvent.Payload = []byte(`{"key":1,"key":2}`) }},
		{"incoming_equals_outgoing", func(r *FlowConstructionReceipt) { r.CreationEvent.EventID = r.CreatingInput.EventID }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			receipt, owner := constructionReceiptFixture(t)
			scenario.edit(&receipt)
			invalid, err := json.Marshal(receipt)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeFlowConstructionReceipt(invalid, owner, receipt.EntityID); err == nil {
				t.Fatal("corrupt receipt admitted")
			}
			if _, err := EncodeFlowConstructionReceipt(receipt); err == nil {
				t.Fatal("encoder bypassed strict semantic receipt admission")
			}
		})
	}
}

func childConstructionReceiptMapping(t *testing.T, receipt FlowConstructionReceipt) (flowidentity.RunScopedFlowInstance, flowidentity.Instance, FlowConstructionInput, *DynamicFlowRuntimeCreationEventPlan) {
	t.Helper()
	const childRun = "44444444-4444-4444-8444-444444444444"
	identity := receipt.Identity
	identity.EntityID = "55555555-5555-4555-8555-555555555555"
	identity.ParentEntityID = "88888888-8888-4888-8888-888888888888"
	identity.ParentRoute.EntityID = identity.ParentEntityID
	owner, err := flowidentity.NewRunScopedFlowInstance(childRun, identity.Route())
	if err != nil {
		t.Fatal(err)
	}
	input := receipt.CreatingInput
	if input.EventID != "" {
		input.EventID = "66666666-6666-4666-8666-666666666666"
	}
	var occurrence *DynamicFlowRuntimeCreationEventPlan
	if receipt.CreationEvent != nil {
		mapped := *receipt.CreationEvent
		mapped.EventID, mapped.RunID, mapped.ParentEventID = "99999999-9999-4999-8999-999999999999", childRun, input.EventID
		mapped.Payload = bytes.Clone(receipt.CreationEvent.Payload)
		mapped.DeliveryContext = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "child-reply"}}
		occurrence = &mapped
	}
	return owner, identity, input, occurrence
}

func TestFlowConstructionReceiptV3ChildProjectionKeepsExactInitialization(t *testing.T) {
	receipt, _ := constructionReceiptFixture(t)
	before := constructionReceiptRaw(t, receipt)
	owner, identity, input, occurrence := childConstructionReceiptMapping(t, receipt)
	got, err := ProjectFlowConstructionReceipt(receipt, owner, identity, input, occurrence)
	if err != nil {
		t.Fatal(err)
	}
	if got.RunID != owner.RunID || got.Identity != identity || got.EntityID != identity.EntityID || got.CreatingInput != input || !reflect.DeepEqual(got.CreationEvent, occurrence) {
		t.Fatalf("explicit child correspondence lost: %#v", got)
	}
	if got.InitialState != receipt.InitialState || got.OccurredAt != receipt.OccurredAt || got.BundleHash != receipt.BundleHash || got.WorkflowVersion != receipt.WorkflowVersion ||
		!reflect.DeepEqual(got.Persisted.Fields, receipt.Persisted.Fields) || !reflect.DeepEqual(got.Persisted.Bookkeeping, receipt.Persisted.Bookkeeping) ||
		!reflect.DeepEqual(got.Persisted.Gates, receipt.Persisted.Gates) || !reflect.DeepEqual(got.Persisted.Accumulator, receipt.Persisted.Accumulator) {
		t.Fatal("child projection replaced initial values with current state")
	}
	wantControl := receipt.Persisted.Control
	wantControl.EntityID, wantControl.ParentEntityID = identity.EntityID, identity.ParentEntityID
	if !reflect.DeepEqual(got.Persisted.Control, wantControl) || got.Identity.ParentEntityID == flowidentity.EntityID(got.Identity.ParentRoute.FlowInstance) {
		t.Fatal("child projection lost exact keyed ancestry or changed unrelated controls")
	}
	got.Persisted.Fields["nested"].(map[string]any)["values"].([]any)[0] = int64(99)
	got.Persisted.Control.TransitionHistory[0].From = "changed"
	got.CreationEvent.Payload[0] = 'x'
	got.CreationEvent.DeliveryContext.Reply.ID = "changed"
	if !bytes.Equal(before, constructionReceiptRaw(t, receipt)) || occurrence.Payload[0] == 'x' || occurrence.DeliveryContext.Reply.ID != "child-reply" {
		t.Fatal("child projection aliases source or supplied occurrence")
	}
}

func TestFlowConstructionReceiptV3ChildProjectionRejectsInventedEvidence(t *testing.T) {
	for _, scenario := range []struct {
		name string
		edit func(*flowidentity.Instance, *FlowConstructionInput, **DynamicFlowRuntimeCreationEventPlan)
	}{
		{"different_template", func(i *flowidentity.Instance, _ *FlowConstructionInput, _ **DynamicFlowRuntimeCreationEventPlan) {
			i.TemplateID = "other"
		}},
		{"missing_parent", func(i *flowidentity.Instance, _ *FlowConstructionInput, _ **DynamicFlowRuntimeCreationEventPlan) {
			i.ParentRoute = flowidentity.ParentRoute{}
		}},
		{"different_input", func(_ *flowidentity.Instance, i *FlowConstructionInput, _ **DynamicFlowRuntimeCreationEventPlan) {
			i.Input = "other.input"
		}},
		{"missing_incoming", func(_ *flowidentity.Instance, i *FlowConstructionInput, _ **DynamicFlowRuntimeCreationEventPlan) {
			*i = FlowConstructionInput{}
		}},
		{"unmapped_incoming", func(_ *flowidentity.Instance, i *FlowConstructionInput, _ **DynamicFlowRuntimeCreationEventPlan) {
			i.EventID = "22222222-2222-4222-8222-222222222222"
		}},
		{"missing_outgoing", func(_ *flowidentity.Instance, _ *FlowConstructionInput, e **DynamicFlowRuntimeCreationEventPlan) {
			*e = nil
		}},
		{"different_outgoing_type", func(_ *flowidentity.Instance, _ *FlowConstructionInput, e **DynamicFlowRuntimeCreationEventPlan) {
			(*e).EventType = "other.created"
		}},
		{"unmapped_outgoing", func(_ *flowidentity.Instance, _ *FlowConstructionInput, e **DynamicFlowRuntimeCreationEventPlan) {
			(*e).EventID = "33333333-3333-4333-8333-333333333333"
		}},
		{"different_initial_payload", func(_ *flowidentity.Instance, _ *FlowConstructionInput, e **DynamicFlowRuntimeCreationEventPlan) {
			(*e).Payload = []byte(`{"current":true}`)
		}},
		{"foreign_outgoing_run", func(_ *flowidentity.Instance, _ *FlowConstructionInput, e **DynamicFlowRuntimeCreationEventPlan) {
			(*e).RunID = "11111111-1111-4111-8111-111111111111"
		}},
		{"wrong_outgoing_parent", func(_ *flowidentity.Instance, _ *FlowConstructionInput, e **DynamicFlowRuntimeCreationEventPlan) {
			(*e).ParentEventID = "11111111-1111-4111-8111-111111111111"
		}},
		{"lost_return_binding", func(_ *flowidentity.Instance, _ *FlowConstructionInput, e **DynamicFlowRuntimeCreationEventPlan) {
			(*e).DeliveryContext = events.DeliveryContext{}
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			receipt, _ := constructionReceiptFixture(t)
			before := constructionReceiptRaw(t, receipt)
			owner, identity, input, occurrence := childConstructionReceiptMapping(t, receipt)
			scenario.edit(&identity, &input, &occurrence)
			got, err := ProjectFlowConstructionReceipt(receipt, owner, identity, input, occurrence)
			if err == nil || !reflect.DeepEqual(got, FlowConstructionReceipt{}) || !bytes.Equal(before, constructionReceiptRaw(t, receipt)) {
				t.Fatalf("invalid correspondence admitted or mutated evidence: %#v, %v", got, err)
			}
		})
	}
}

func TestFlowConstructionReceiptV3ExplicitAbsenceAndFieldlessProjection(t *testing.T) {
	receipt, _ := constructionReceiptFixture(t)
	receipt.CreatingInput, receipt.CreationEvent = FlowConstructionInput{}, nil
	receipt.Persisted.Fields, receipt.Persisted.Control.EntityType = nil, ""
	owner, identity, input, occurrence := childConstructionReceiptMapping(t, receipt)
	got, err := ProjectFlowConstructionReceipt(receipt, owner, identity, input, occurrence)
	if err != nil || got.CreatingInput != (FlowConstructionInput{}) || got.CreationEvent != nil || got.Persisted.Fields != nil {
		t.Fatalf("projection invented publication or fields: %#v, %v", got, err)
	}
	input.EventID = "66666666-6666-4666-8666-666666666666"
	if _, err := ProjectFlowConstructionReceipt(receipt, owner, identity, input, nil); err == nil {
		t.Fatal("explicit no-publication construction accepted invented ingress")
	}
}

func TestFlowConstructionReceiptV3ChildReturnAdmission(t *testing.T) {
	receipt, _ := constructionReceiptFixture(t)
	declaration, err := timeridentity.NewJoinRef(identitytest.FlowNode(t, "account", "join"), "item.done", "initial", "items")
	if err != nil {
		t.Fatal(err)
	}
	entry := timeridentity.StageEntryRef{
		RunID: receipt.RunID, FlowScope: receipt.Identity.ScopeKey, InstanceID: receipt.Identity.InstanceID,
		InstancePath: receipt.Identity.InstancePath, EntityID: receipt.EntityID, Stage: "initial", Cause: "construction",
	}
	bound, err := declaration.BindStageEntry(entry, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	receipt.CreationEvent.DeliveryContext.Joins = []events.JoinAdmissionReceipt{{Ref: bound, Disposition: events.JoinAdmissionBound}}
	owner, identity, input, occurrence := childConstructionReceiptMapping(t, receipt)
	entry.RunID, entry.EntityID = owner.RunID, identity.EntityID
	mapped, err := declaration.BindStageEntry(entry, bound.Generation())
	if err != nil {
		t.Fatal(err)
	}
	occurrence.DeliveryContext.Joins = []events.JoinAdmissionReceipt{{Ref: mapped, Disposition: events.JoinAdmissionBound}}
	got, err := ProjectFlowConstructionReceipt(receipt, owner, identity, input, occurrence)
	if err != nil || len(got.CreationEvent.DeliveryContext.Joins) != 1 || !got.CreationEvent.DeliveryContext.Joins[0].Ref.Equal(mapped) {
		t.Fatalf("mapped return admission lost: %#v, %v", got, err)
	}
	occurrence.DeliveryContext.Joins[0].Ref = bound
	if _, err := ProjectFlowConstructionReceipt(receipt, owner, identity, input, occurrence); err == nil {
		t.Fatal("source bound return admission became child authority")
	}
}

func TestFlowConstructionReceiptV3PersistenceRecordSeparatesReadiness(t *testing.T) {
	receipt, owner := constructionPublicationFieldsFixture(t)
	readiness := DynamicFlowRuntimeReadinessPlan{
		Identity: receipt.Identity, RunID: receipt.RunID, BundleHash: receipt.BundleHash,
		WorkflowVersion: receipt.WorkflowVersion, ExecutionMode: receipt.ExecutionMode,
		CreationEvent: &DynamicFlowRuntimeCreationEventPlan{
			EventID: "33333333-3333-4333-8333-333333333333", EventType: "account.created", RunID: receipt.RunID,
			ParentEventID: receipt.CreatingInput.EventID, ExecutionMode: receipt.ExecutionMode,
			Payload: []byte(`{"double":7.0,"integer":7}`), CreatedAt: receipt.OccurredAt,
		},
	}
	plan := FlowInstanceActivationPlan{
		Identity: receipt.Identity, Readiness: readiness, CreatingInput: receipt.CreatingInput, OccurredAt: receipt.OccurredAt,
		Instance: WorkflowInstance{
			StorageRef: receipt.FlowInstance, InstanceID: receipt.Identity.InstanceID, EntityID: receipt.EntityID, EntityType: "account",
			WorkflowName: receipt.WorkflowName, WorkflowVersion: receipt.WorkflowVersion, Mode: "template", Status: "active",
			CurrentState: receipt.InitialState, Fields: receipt.Persisted.Fields, StageDefined: true,
			EnteredStageAt: receipt.OccurredAt, CreatedAt: receipt.OccurredAt,
		},
	}
	record, err := plan.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeFlowConstructionReceipt(record.InitialMaterialization, owner, receipt.EntityID)
	if err != nil || got.Version != 3 || got.Identity != plan.Identity || !reflect.DeepEqual(got.CreationEvent, readiness.CreationEvent) || !reflect.DeepEqual(got.Persisted.Fields, receipt.Persisted.Fields) {
		t.Fatalf("producer lost independent immutable evidence: %#v, %v", got, err)
	}
	if bytes.Contains(record.InitialMaterialization, []byte(`"readiness"`)) || len(record.Readiness) == 0 || record.ReadinessPlanHash == "" {
		t.Fatal("receipt embeds readiness or producer removed the separate runtime plan")
	}
}
