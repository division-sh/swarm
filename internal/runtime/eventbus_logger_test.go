package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
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

const payloadAdmissionTestBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func testRuntimePayloadAdmitter(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) runtimebus.PayloadAdmitter {
	t.Helper()
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(payloadAdmissionTestBundleHash)
	if err != nil {
		t.Fatalf("bundle source fact: %v", err)
	}
	return NewRuntimePayloadAdmitter(nil, semanticview.Wrap(bundle), fact)
}

func admitRuntimePayload(admitter runtimebus.PayloadAdmitter, eventType string, payload json.RawMessage) (events.Event, error) {
	event := eventtest.RunCreatingRootIngress("", events.EventType(eventType), "", "", payload, 0, "", "", events.EventEnvelope{}, time.Time{})
	admission, err := admitter(context.Background(), event, "")
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
		"package.yaml": "name: payload-admission-proof\nversion: \"1.0.0\"\nplatform_version: \">=0.7.0 <0.8.0\"\nflows: []\n",
		"schema.yaml":  "name: payload-admission-proof\n",
		"events.yaml":  eventsYAML,
		"types.yaml":   typesYAML,
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
	eventBus, err := newRuntimeEventBus(nil, runtimebus.DurableDependencies{}, nil, logger, nil, executionposture.Live, runtimecorrelation.BundleSourceFact{}, "", nil, nil, nil, nil, nil, nil, nil, nil)
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
}

func TestRuntimePayloadAdmitter_ClassifiesGeneratedActivitySchema(t *testing.T) {
	t.Parallel()

	repo := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyGeneratedActivity(t, false, true)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load generated activity fixture: %v", err)
	}
	event, err := admitRuntimePayload(testRuntimePayloadAdmitter(t, bundle), "send.succeeded", []byte(`{
		"activity_id":"send",
		"tool":"send",
		"effect_class":"read_only",
		"attempt":1,
		"result":{"delivered":true}
	}`))
	if err != nil {
		t.Fatalf("admit generated activity payload: %v", err)
	}
	admission, ok := event.PayloadAdmission()
	if !ok || admission.Binding().SchemaClass() != events.PayloadSchemaGenerated {
		t.Fatalf("generated payload schema class = %q/%v", admission.Binding().SchemaClass(), ok)
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
	admission, err := testRuntimePayloadAdmitter(t, booleanPayloadBundle(t))(context.Background(), event, "")
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
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(payloadAdmissionTestBundleHash)
	if err != nil {
		t.Fatalf("bundle source fact: %v", err)
	}
	admitter := NewRuntimePayloadAdmitter(nil, source, fact)
	if _, err := admitRuntimePayload(admitter, "task.completed", []byte(`{"trace_id":"not-a-uuid"}`)); err == nil {
		t.Fatal("expected scalar-alias uuid violation to be rejected")
	}
}
