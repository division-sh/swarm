package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func testRuntimePayloadAdmitter(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) runtimebus.PayloadAdmitter {
	t.Helper()
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatalf("bundle source fact: %v", err)
	}
	return NewRuntimePayloadAdmitter(nil, semanticview.Wrap(bundle), fact)
}

func admitRuntimePayload(admitter runtimebus.PayloadAdmitter, eventType string, payload json.RawMessage) (events.Event, error) {
	event := eventtest.RunCreatingRootIngress("", events.EventType(eventType), "", "", payload, 0, "", "", events.EventEnvelope{}, time.Time{})
	admission, err := admitter(context.Background(), event, events.AuthoredPayloadProducer("."))
	if err != nil {
		return events.Event{}, err
	}
	return events.ApplyPayloadAdmission(event, admission)
}

func booleanPayloadBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadRootPayloadBundle(t, "task.completed:\n  ok: boolean\n", "")
}

func loadRootPayloadBundle(t *testing.T, eventsYAML, typesYAML string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	for name, contents := range map[string]string{
		"manifest.yaml": "version: 1.0.0\nplatform_version: \">=0.7.0 <0.8.0\"\n",
		"schema.yaml":   "stages: []\n",
		"events.yaml":   eventsYAML,
		"types.yaml":    typesYAML,
	} {
		if contents == "" {
			continue
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load payload admission fixture: %v", err)
	}
	return bundle
}

func TestEventBusRejectsMalformedFailureBeforeRuntimeLog(t *testing.T) {
	logger := NewRuntimeLogger(nil, executionposture.Live, nil)
	eventBus, err := newRuntimeEventBus(nil, runtimebus.DurableDependencies{}, nil, logger, nil, executionposture.Live, runtimecorrelation.SourceArtifactFact{}, "", nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("newRuntimeEventBus: %v", err)
	}

	err = eventBus.LogRuntime(testAuthorActivityContext(context.Background()), runtimepipeline.RuntimeLogEntry{
		Component: "test",
		Action:    "malformed_failure",
		Failure: &runtimefailures.Envelope{
			SchemaVersion: "forged",
			Class:         runtimefailures.ClassConnectorFailure,
		},
	})
	if err == nil {
		t.Fatal("LogRuntime() accepted malformed failure evidence")
	}
}

func TestRuntimePayloadAdmitter_AllowsValidSchemaPayload(t *testing.T) {
	t.Parallel()

	admitter := testRuntimePayloadAdmitter(t, booleanPayloadBundle(t))
	event, err := admitRuntimePayload(admitter, "task.completed", []byte(`{"ok":true}`))
	if err != nil {
		t.Fatalf("admit valid payload: %v", err)
	}
	admission, ok := event.PayloadAdmission()
	if !ok {
		t.Fatal("admitted event is missing payload schema evidence")
	}
	if admission.Binding().SchemaClass() != events.PayloadSchemaAuthored {
		t.Fatalf("payload schema class = %q, want authored", admission.Binding().SchemaClass())
	}
	// A valid producer scope with no event schema is distinct from missing scope.
	schemaLess, err := admitRuntimePayload(admitter, "fixture.untyped", []byte(`{"ok":true}`))
	if err != nil {
		t.Fatalf("admit genuinely schema-less payload in declared root: %v", err)
	}
	if admission, found := schemaLess.PayloadAdmission(); !found || admission.Binding().SchemaClass() != events.PayloadSchemaSchemaLess {
		t.Fatal("valid schema-less root event lost its admission")
	}

	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "tests/tier5-flow-lifecycle/test-timer-fire"), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	timerAdmitter := testRuntimePayloadAdmitter(t, bundle)
	source, err := events.NewRootRoutingSource("timer-entity")
	if err != nil {
		t.Fatal(err)
	}
	for _, eventType := range []events.EventType{"timer.check", "timer.undeclared"} {
		timer := eventtest.RuntimeControlWithRoutingSource(uuid.NewString(), eventType, "runtime.workflow_timer", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{EntityID: "timer-entity"}, source, time.Time{})
		proof, err := timerAdmitter(context.Background(), timer, events.EventPayloadProducer(timer, "."))
		if eventType == "timer.undeclared" {
			if err == nil || !strings.Contains(err.Error(), "not declared by flow") || proof.Valid() {
				t.Fatalf("undeclared timer payload admission: %v", err)
			}
			continue
		}
		if err != nil || !proof.Binding().Producer().IsAuthored() || proof.Binding().FlowID() != "." {
			t.Fatalf("authored timer payload admission: %v", err)
		}
		if _, err := events.ApplyPayloadAdmission(timer, proof); err != nil {
			t.Fatalf("apply authored timer payload: %v", err)
		}
	}
}

func TestRuntimePayloadAdmitter_ClassifiesGeneratedActivitySchema(t *testing.T) {
	t.Parallel()

	repo := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyGeneratedActivity(t, false, true)
	toolPath := filepath.Join(root, "tools.yaml")
	tool, err := os.ReadFile(toolPath)
	if err != nil {
		t.Fatal(err)
	}
	tool = bytes.Replace(tool, []byte("      delivered: {type: boolean}"), []byte("      delivered: {type: boolean}\n      explicit_double: {type: number}"), 1)
	if err := os.WriteFile(toolPath, tool, 0600); err != nil {
		t.Fatal(err)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load generated activity fixture: %v", err)
	}
	event, err := admitRuntimePayload(testRuntimePayloadAdmitter(t, bundle), "send.succeeded", []byte(`{
		"activity_id":"send",
		"tool":"send",
		"result":{"delivered":true,"explicit_double":8.0}
	}`))
	if err != nil {
		t.Fatalf("admit generated activity payload: %v", err)
	}
	admission, ok := event.PayloadAdmission()
	if !ok || admission.Binding().SchemaClass() != events.PayloadSchemaGenerated {
		t.Fatalf("generated payload schema class = %q/%v", admission.Binding().SchemaClass(), ok)
	}
	var executionPayload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(admission.Payload(), &executionPayload); err != nil {
		t.Fatal(err)
	}
	if executionPayload["result"].(map[string]any)["explicit_double"] != json.Number("8.0") {
		t.Fatalf("generated result schema erased execution kind: %s", admission.Payload())
	}
}

func TestRuntimePayloadAdmitter_ClassifiesPlatformCatalogSchema(t *testing.T) {
	t.Parallel()

	bundle := booleanPayloadBundle(t)
	payload := []byte(`{
		"card_id":"00000000-0000-4000-8000-000000000001",
		"anchor_kind":"stage_gate",
		"anchor":{},
		"decision_id":"decision-1",
		"verdict":"approve",
		"fields":{},
		"card_content_hash":"card-hash",
		"decision_schema_hash":"schema-hash",
		"bundle_hash":"bundle-hash"
	}`)
	event, err := admitRuntimePayload(testRuntimePayloadAdmitter(t, bundle), "mailbox.card_decided", payload)
	if err != nil {
		t.Fatalf("admit platform catalog payload: %v", err)
	}
	admission, ok := event.PayloadAdmission()
	if !ok || admission.Binding().SchemaClass() != events.PayloadSchemaPlatform {
		t.Fatalf("platform payload schema class = %q/%v", admission.Binding().SchemaClass(), ok)
	}

	// Reach: an actual diagnostic resolves reference law through its platform
	// producer, never through an empty authored coordinate or structural schema.
	admit := testRuntimePayloadAdmitter(t, bundle)
	capture := &runtimeLogPersistenceCapture{}
	logger := NewRuntimeLogger(capture, executionposture.Live, admit)
	if err := logger.Log(context.Background(), RuntimeLogEntry{Level: "info", Message: "catalog reference", Component: "runtime", Action: "proof"}); err != nil {
		t.Fatal(err)
	}
	if len(capture.records) != 1 {
		t.Fatalf("diagnostic records=%d", len(capture.records))
	}
	record := capture.records[0]
	binding := record.PayloadAdmission.Binding()
	if binding.SchemaClass() != events.PayloadSchemaPlatform || !binding.Producer().IsPlatform() || binding.Reference() != events.RuntimeLogSchemaReference || binding.FlowID() != "" || binding.SchemaDigest() == canonicaljson.HashBytes([]byte("{}")) {
		t.Fatalf("diagnostic lost explicit reference authority: %+v", binding)
	}
	diagnostic := eventtest.DiagnosticDirect("", events.EventTypePlatformRuntimeLog, "runtime", "", record.Payload, 0, "", "", events.EventEnvelope{}, time.Time{})
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	projection, err := AdmitEffectiveSourceProjection(EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: fact})
	if err != nil {
		t.Fatal(err)
	}
	for name, source := range map[string]semanticview.Source{"declaration": semanticview.Wrap(bundle), "effective": projection.Source()} {
		resolved, err := semanticview.ResolveProducerEventSchema(source, events.PlatformPayloadProducer(diagnostic), diagnostic)
		if err != nil || resolved.HasSchema || resolved.HasCompiled || resolved.Reference.Reference() != binding.Reference() || resolved.Reference.Digest() != binding.SchemaDigest() {
			t.Fatalf("%s reference/schema disagreement: %+v err=%v", name, resolved, err)
		}
		if authored := semanticview.ResolveEventSchema(source, "", string(diagnostic.Type())); authored.HasSchema || authored.Reference.Digest() != "" {
			t.Fatalf("%s recovered authority from empty authored flow", name)
		}
	}
	boot := eventtest.RuntimeControl("", "platform.boot", "runtime", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Time{})
	for name, producer := range map[string]events.PayloadProducer{
		"missing": {}, "empty_authored": events.AuthoredPayloadProducer(""), "dot_authored": events.AuthoredPayloadProducer("."),
		"foreign_event_class": events.PlatformPayloadProducer(boot),
	} {
		if proof, err := admit(context.Background(), diagnostic, producer); err == nil || proof.Valid() {
			t.Errorf("%s gained diagnostic authority", name)
		}
	}
	if _, err := admit(context.Background(), boot, events.PlatformPayloadProducer(boot)); err == nil {
		t.Fatal("invalid boot payload gained diagnostic validation privileges")
	}
	for _, payload := range []string{`{}`, `{"log_level":1,"message":"bad","details":{"component":"runtime","action":"proof"}}`, `{"log_level":"info","message":"bad","details":{"component":"runtime","action":"proof","error":"legacy"}}`} {
		bad := eventtest.DiagnosticDirect("", events.EventTypePlatformRuntimeLog, "runtime", "", []byte(payload), 0, "", "", events.EventEnvelope{}, time.Time{})
		if proof, err := admit(context.Background(), bad, events.PlatformPayloadProducer(bad)); err == nil || proof.Valid() {
			t.Errorf("invalid diagnostic payload admitted: %s", payload)
		}
	}
	if _, err := events.ApplyPayloadAdmission(boot, record.PayloadAdmission); err == nil {
		t.Fatal("diagnostic binding applied to another event/class")
	}
	input := events.PayloadSchemaBindingInput{BundleHash: binding.BundleHash(), EventKey: binding.EventKey(), SchemaDigest: binding.SchemaDigest(), SchemaClass: binding.SchemaClass()}
	restored, err := events.RestoreEventPayloadSchemaBinding(input, diagnostic.AdmissionClass(), diagnostic.Type(), diagnostic.Producer())
	if err != nil || !restored.Equal(binding) {
		t.Fatalf("explicit reference binding lost on restoration: %v", err)
	}
	if _, err := events.NewPlatformPayloadSchemaBinding(input, events.PlatformPayloadProducer(diagnostic), "wrong-reference"); err == nil {
		t.Fatal("substituted reference admitted")
	}
	labelOnly, err := events.NewPayloadSchemaBinding(input)
	if err != nil {
		t.Fatal(err)
	}
	labelAdmission, err := events.NewPayloadAdmission(record.Payload, labelOnly)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ApplyPayloadAdmission(diagnostic, labelAdmission); err == nil {
		t.Fatal("platform label alone gained reference authority")
	}
	for name, mutate := range map[string]func(*events.PayloadSchemaBindingInput){
		"foreign_event": func(in *events.PayloadSchemaBindingInput) { in.EventKey = "platform.boot" },
		"borrowed_flow": func(in *events.PayloadSchemaBindingInput) { in.FlowID = "." },
		"schema_less":   func(in *events.PayloadSchemaBindingInput) { in.SchemaClass = events.PayloadSchemaSchemaLess },
	} {
		bad := input
		mutate(&bad)
		if _, err := events.RestoreEventPayloadSchemaBinding(bad, diagnostic.AdmissionClass(), diagnostic.Type(), diagnostic.Producer()); err == nil {
			t.Errorf("%s restored reference authority", name)
		}
	}

	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassChainDepthExceeded, "chain_depth_exceeded", "pipeline", "emit", nil), "pipeline", "emit")
	deadLetter := map[string]any{
		"original_event": "task.completed", "original_payload": map[string]any{"ok": true},
		"failure": failure, "retry_count": 0, "chain_depth": 51, "timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	}
	deadLetterPayload, err := json.Marshal(deadLetter)
	if err != nil {
		t.Fatal(err)
	}
	deadLetterEvent := eventtest.RuntimeDiagnostic("", "platform.dead_letter", "runtime", "", deadLetterPayload, 0, "", "", events.EventEnvelope{}, time.Time{})
	deadLetterAdmission, err := admit(context.Background(), deadLetterEvent, events.PlatformPayloadProducer(deadLetterEvent))
	if err != nil {
		t.Fatalf("admit unparented dead letter through catalog law: %v", err)
	}
	deadLetterBinding := deadLetterAdmission.Binding()
	if deadLetterBinding.Reference() != events.DeadLetterSchemaReference || deadLetterBinding.SchemaDigest() == binding.SchemaDigest() {
		t.Fatal("dead letter did not retain its own catalog law")
	}
	if _, err := events.ApplyPayloadAdmission(deadLetterEvent, deadLetterAdmission); err != nil {
		t.Fatal(err)
	}
	deadLetterInput := events.PayloadSchemaBindingInput{BundleHash: deadLetterBinding.BundleHash(), EventKey: deadLetterBinding.EventKey(), SchemaClass: deadLetterBinding.SchemaClass(), SchemaDigest: deadLetterBinding.SchemaDigest()}
	if restored, err := events.RestoreEventPayloadSchemaBinding(deadLetterInput, deadLetterEvent.AdmissionClass(), deadLetterEvent.Type(), deadLetterEvent.Producer()); err != nil || !restored.Equal(deadLetterBinding) {
		t.Fatalf("dead-letter reference restoration: %v", err)
	}
	wrongClass := eventtest.RuntimeControl("", "platform.dead_letter", "runtime", "", deadLetterPayload, 0, "", "", events.EventEnvelope{}, time.Time{})
	if _, err := admit(context.Background(), wrongClass, events.PlatformPayloadProducer(wrongClass)); err == nil {
		t.Fatal("dead-letter reference accepted the wrong admission class")
	}
	for field, invalid := range map[string]any{"failure": map[string]any{}, "original_payload": []any{}, "retry_count": "0", "source": map[string]any{"flow_id": 17}} {
		bad := make(map[string]any, len(deadLetter))
		for key, value := range deadLetter {
			bad[key] = value
		}
		bad[field] = invalid
		raw, err := json.Marshal(bad)
		if err != nil {
			t.Fatal(err)
		}
		badEvent := eventtest.RuntimeDiagnostic("", "platform.dead_letter", "runtime", "", raw, 0, "", "", events.EventEnvelope{}, time.Time{})
		if _, err := admit(context.Background(), badEvent, events.PlatformPayloadProducer(badEvent)); err == nil {
			t.Errorf("dead-letter reference accepted invalid %s", field)
		}
	}
}

func TestRuntimePayloadAdmitter_SelectedForkValidatesTargetSchemaWithoutChangingHistoricalBytes(t *testing.T) {
	t.Parallel()

	payload := []byte("{\n  \"ok\": true\n}")
	lineage, err := events.NewSelectedForkLineage(
		uuid.NewString(), uuid.NewString(), uuid.NewString(), "selected-contract", "", executionmode.Live,
	)
	if err != nil {
		t.Fatalf("selected-fork lineage: %v", err)
	}
	event, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{
		Facts: events.EventFacts{
			ID: uuid.NewString(), Type: "task.completed",
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "selected-contract"},
			Payload:  payload, CreatedAt: time.Now().UTC(), ExecutionMode: executionmode.Live,
		},
		Lineage: lineage,
	})
	if err != nil {
		t.Fatalf("selected-fork event: %v", err)
	}
	admission, err := testRuntimePayloadAdmitter(t, booleanPayloadBundle(t))(context.Background(), event, events.AuthoredPayloadProducer("."))
	if err != nil {
		t.Fatalf("admit selected-fork payload: %v", err)
	}
	if !bytes.Equal(admission.Payload(), payload) {
		t.Fatalf("selected-fork admitted payload = %q, want exact historical bytes %q", admission.Payload(), payload)
	}
}

func TestRuntimePayloadAdmitter_RejectsNonObjectJSON(t *testing.T) {
	t.Parallel()

	admitter := testRuntimePayloadAdmitter(t, booleanPayloadBundle(t))
	if _, err := admitRuntimePayload(admitter, "task.completed", []byte(`[]`)); err == nil {
		t.Fatal("expected non-object JSON payload to be rejected")
	}
}

func TestRuntimePayloadAdmitter_RejectsSchemaMismatch(t *testing.T) {
	t.Parallel()

	admitter := testRuntimePayloadAdmitter(t, booleanPayloadBundle(t))
	// Reach: invalid or absent producer authority must not turn schema refusal
	// into schema-less approval, even when the supplied payload is otherwise valid.
	for _, flow := range []string{"", " ", " .", ". ", "./", "child/../root", "foreign", "foreign/child"} {
		for _, payload := range []json.RawMessage{[]byte(`{"ok":true}`), []byte(`{"ok":"yes"}`)} {
			event := eventtest.RunCreatingRootIngress("", "task.completed", "", "", payload, 0, "", "", events.EventEnvelope{}, time.Time{})
			admission, err := admitter(context.Background(), event, events.AuthoredPayloadProducer(flow))
			if err == nil || !reflect.DeepEqual(admission, events.PayloadAdmission{}) {
				t.Errorf("invalid producer coordinate %q gained payload admission: valid=%t err=%v", flow, admission.Valid(), err)
			}
		}
	}
	if _, err := admitRuntimePayload(admitter, "task.completed", []byte(`{"ok":"yes"}`)); err == nil {
		t.Fatal("expected schema-invalid payload to be rejected")
	}
}

func TestRuntimePayloadAdmitter_AllowsUndeclaredCanonicalContextFields(t *testing.T) {
	t.Parallel()

	admitter := testRuntimePayloadAdmitter(t, booleanPayloadBundle(t))
	_, err := admitRuntimePayload(admitter, "task.completed", []byte(`{
			"ok": true,
			"entity_id": "ent-1",
			"flow_instance": "flow/inst-1",
			"trigger_event_type": "task.started",
			"current_state": "running"
		}`))
	if err != nil {
		t.Fatalf("admit extra canonical context: %v", err)
	}
}

func TestRuntimePayloadAdmitter_RejectsTriggerSchemaFieldWhenTargetSchemaDisallowsIt(t *testing.T) {
	t.Parallel()

	bundle := booleanPayloadBundle(t)
	bundle.Events["task.started"] = runtimecontracts.EventCatalogEntry{Payload: runtimecontracts.EventPayloadSpec{
		Properties: map[string]runtimecontracts.EventFieldSpec{"score": {Type: "integer"}},
	}}
	admitter := testRuntimePayloadAdmitter(t, bundle)
	_, err := admitRuntimePayload(admitter, "task.completed", []byte(`{
			"ok": true,
			"score": 10,
			"trigger_event_type": "task.started"
		}`))
	if err == nil {
		t.Fatal("expected trigger-schema-only field to be rejected by target schema validation")
	}
}

func TestRuntimePayloadAdmitter_RejectsUndeclaredCallerPayloadFieldWhenAdditionalPropertiesFalse(t *testing.T) {
	t.Parallel()

	admitter := testRuntimePayloadAdmitter(t, booleanPayloadBundle(t))
	_, err := admitRuntimePayload(admitter, "task.completed", []byte(`{
			"ok": true,
			"surprise": "x"
		}`))
	if err == nil {
		t.Fatal("expected undeclared caller payload field to be rejected")
	}
}

func TestRuntimePayloadAdmitter_RejectsScalarAliasUUIDViolation(t *testing.T) {
	t.Parallel()

	source := semanticview.Wrap(loadRootPayloadBundle(t,
		"task.completed:\n  trace_id: TraceID\n",
		"scalars:\n  TraceID: uuid\n",
	))
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle.SourceArtifact == nil {
		t.Fatal("payload admission fixture requires its admitted source artifact")
	}
	fact, err := runtimecorrelation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatalf("bundle source fact: %v", err)
	}
	admitter := NewRuntimePayloadAdmitter(nil, source, fact)
	if _, err := admitRuntimePayload(admitter, "task.completed", []byte(`{"trace_id":"not-a-uuid"}`)); err == nil {
		t.Fatal("expected scalar-alias uuid violation to be rejected")
	}
}
