package bootverify_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtime "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/scenarioderivation"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRun_ProviderTransportEntityIDDoesNotElectCanonicalPrimary(t *testing.T) {
	root := t.TempDir()
	for label, body := range map[string]string{
		"manifest.yaml":        "version: 1.0.0\nplatform_version: '*'\n",
		"schema.yaml":          "stages: []\n",
		"matrix/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"matrix/schema.yaml":   "stages: []\npins: {inputs: [inbound.telegram.raw, inbound.intercom.raw]}\n",
		"matrix/entities.yaml": "matrix_service:\n  service_id: {type: text, initial: standing}\n",
		"matrix/nodes.yaml": `matrix-sink:
  execution_type: system_node
  subscribes_to: [inbound.telegram.raw, inbound.intercom.raw]
  event_handlers:
    inbound.telegram.raw:
      data_accumulation:
        writes: [{target_field: service_id, value: "standing"}]
    inbound.intercom.raw:
      data_accumulation:
        writes: [{target_field: service_id, value: "standing"}]
`,
		"matrix/telegram/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw], verify_secret: webhook_signing_secret}, cannot: [bypass journal]}\n",
		"matrix/telegram/trigger.yaml":  "transport: webhook\nsecret: {required: true}\nsignature: {type: token_equality, header: X-Telegram-Token}\ndelivery_id: {json_path: $.update_id, required: true}\nevent_type: {literal: update, required: true}\nevent_name: {literal: raw}\n",
		"matrix/intercom/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\ncapabilities: {can: {emit_events: [raw], verify_secret: webhook_signing_secret}, cannot: [bypass journal]}\n",
		"matrix/intercom/trigger.yaml":  "transport: webhook\nsecret: {required: true}\nsignature: {type: hmac_sha1, header: X-Hub-Signature, prefix: 'sha1=', signed_payload: raw_body}\ndelivery_id: {json_path: $.id, required: true}\nevent_type: {json_path: $.topic, required: true}\nevent_name: {literal: raw}\n",
	} {
		path := filepath.Join(root, filepath.FromSlash(label))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	source := lifecycleSurfaceSource(t, root)
	report := bootverify.Run(context.Background(), source, bootverify.Options{})
	for _, finding := range report.Errors() {
		if finding.CheckID == "flow_boundary_create_entity_validation" {
			t.Fatalf("provider transport entity_id acquired primary-entity authority: %#v", report.Errors())
		}
	}
}

func TestCompiledLifecycleEmitterStrictValidationSurface(t *testing.T) {
	for _, tc := range []struct {
		name, event string
		root        func(*testing.T) string
		blocked     bool
	}{
		{"gate_local", "work.completed", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleGateLocal)
		}, false},
		{"gate_dangling", "work.completed", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitterStatic(t, canonicalrouting.LifecycleStaticGateDanglingStrict)
		}, true},
		{"loop_connected", "loop.escaped", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitter(t, canonicalrouting.LifecycleLoopConnected)
		}, false},
		{"loop_dangling", "loop.escaped", func(t *testing.T) string {
			return canonicalrouting.CopyLifecycleEmitterStatic(t, canonicalrouting.LifecycleStaticLoopDangling)
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := lifecycleSurfaceSource(t, tc.root(t))
			credentialStore, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range map[string]string{
				"telegram.telegram_bot_token":     "fixture-token",
				"telegram.webhook_signing_secret": "fixture-signing-secret",
			} {
				if err := credentialStore.Set(context.Background(), key, value); err != nil {
					t.Fatal(err)
				}
			}
			result, err := runtime.ValidateWorkflowContractSurface(context.Background(), source, runtime.WorkflowContractValidationOptions{
				ExecutionPosture: executionposture.Live, FatalBootWarnings: true, StrictEmitSchemas: true,
				Credentials: credentialStore, ProviderCredentials: credentialStore,
			})
			if !tc.blocked {
				if err != nil {
					t.Fatalf("valid lifecycle source rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "[BLOCKER] event_consumer_exists") || !strings.Contains(err.Error(), tc.event) {
				t.Fatalf("strict surface did not reject dangling lifecycle event: %v", err)
			}
			for _, finding := range result.BootReport.Findings {
				if finding.CheckID == "event_consumer_exists" && strings.Contains(finding.Location, tc.event) && finding.Severity != bootverify.SeveritySemanticDriftWarn {
					t.Fatalf("surface escalation changed diagnostic severity: %#v", finding)
				}
			}
		})
	}
}

func TestLifecycleProducerDoesNotGrantScenarioInputAuthority(t *testing.T) {
	for _, variant := range []canonicalrouting.LifecycleEmitterVariant{canonicalrouting.LifecycleGateLocal, canonicalrouting.LifecycleLoopConnected} {
		source := lifecycleSurfaceSource(t, canonicalrouting.CopyLifecycleEmitter(t, variant))
		bundle, _ := semanticview.Bundle(source)
		hash, err := runtimecontracts.BundleHash(bundle)
		if err != nil {
			t.Fatal(err)
		}
		fact, err := correlation.NewSourceArtifactFact(hash)
		if err != nil {
			t.Fatal(err)
		}
		// This source-only fixture has no overlay. Bind the scenario profile to
		// its actual bundle fact without treating the bundle ID as a digest.
		digest, err := canonicaljson.Hash(map[string]any{"source_bundle_hash": hash})
		if err != nil {
			t.Fatal(err)
		}
		identity, err := scenarioexecution.NewEffectiveSourceIdentity(fact, digest)
		if err != nil {
			t.Fatal(err)
		}
		plans, err := scenarioderivation.Compile(source, identity, scenarioderivation.Request{FlowID: ".", AllInputs: true})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, plan := range plans {
			got = append(got, plan.PinName)
		}
		want := []string{"work.requested"}
		event := "work.completed"
		if variant == canonicalrouting.LifecycleLoopConnected {
			want = []string{"work.requested", "loop.start", "loop.admit", "loop.repeat", "loop.close"}
			event = "loop.escaped"
		}
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("scenario inputs = %q, want %q", got, want)
		}
		if _, err := scenarioderivation.Compile(source, identity, scenarioderivation.Request{FlowID: ".", Input: event}); err == nil || !strings.Contains(err.Error(), "has no public input matching") {
			t.Fatalf("producer-only scenario input admitted: %v", err)
		}
	}
}

func TestCompiledLifecycleEmitterInvalidPlansRejectedByValidationSurface(t *testing.T) {
	for _, tc := range []struct {
		name, check, detail string
		variant             canonicalrouting.LifecycleEmitterStaticVariant
	}{
		{"gate_unknown", "stage_gate_validation", "absent.event", canonicalrouting.LifecycleStaticGateUnknownEvent},
		{"gate_wrong_flow", "stage_gate_validation", "work.completed", canonicalrouting.LifecycleStaticGateWrongFlow},
		{"gate_bad_field", "stage_gate_validation", "unknown", canonicalrouting.LifecycleStaticGateBadField},
		{"gate_missing_field", "stage_gate_validation", "result", canonicalrouting.LifecycleStaticGateMissingField},
		{"loop_unknown", "loop_validation", "absent.event", canonicalrouting.LifecycleStaticLoopUnknownEvent},
		{"loop_wrong_flow", "loop_validation", "loop.escaped", canonicalrouting.LifecycleStaticLoopWrongFlow},
		{"loop_bad_field", "loop_validation", "unknown", canonicalrouting.LifecycleStaticLoopBadField},
		{"loop_missing_field", "loop_validation", "revision_id", canonicalrouting.LifecycleStaticLoopMissingField},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := lifecycleSurfaceSource(t, canonicalrouting.CopyLifecycleEmitterStatic(t, tc.variant))
			_, err := runtime.ValidateWorkflowContractSurface(context.Background(), source, runtime.WorkflowContractValidationOptions{ExecutionPosture: executionposture.Live, FatalBootWarnings: true, StrictEmitSchemas: true})
			if err == nil || !strings.Contains(err.Error(), tc.check) || !strings.Contains(err.Error(), tc.detail) {
				t.Fatalf("invalid lifecycle plan reached validation success: %v", err)
			}
		})
	}
}

func lifecycleSurfaceSource(t *testing.T, root string) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	projection, err := runtime.AdmitEffectiveSourceProjection(runtime.EffectiveSourceProjectionRequest{Source: semanticview.Wrap(bundle), SourceArtifactFact: fact})
	if err != nil {
		t.Fatal(err)
	}
	return projection.Source()
}
