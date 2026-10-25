package serveapp

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"gopkg.in/yaml.v3"
)

func copyClockMixedIngress(t *testing.T) string {
	t.Helper()
	root := canonicalrouting.CopyStandingRootTreePublic(t)
	path := filepath.Join(root, "schema.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := yaml.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	schema["schedules"] = map[string]any{"poll": map[string]any{"every": "1h", "emit": "poll.tick"}}
	pins := schema["pins"].(map[string]any)
	pins["outputs"] = append(pins["outputs"].([]any), "poll.tick")
	updated, err := yaml.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	writeWorkflowValidationFixtureFile(t, path, string(updated))
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "poll.tick:\n")
	return root
}

func TestServedClockMixedEnabledBindingsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", path)
			file, err := credentials.NewFileStore(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"telegram.telegram_bot_token", "telegram.webhook_signing_secret"} {
				if err := file.Set(context.Background(), key, "clock-binding-secret"); err != nil {
					t.Fatal(err)
				}
			}
			_, start := clockDeploymentHarness(t, backend, copyClockMixedIngress(t))
			process, served := start()
			rt := servedTestProcessRuntime(t, process)
			statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 2 {
				t.Fatalf("independently enabled binding inventory=%+v err=%v", statuses, err)
			}
			rootRun, rootService, sibling := "", "", ""
			for _, status := range statuses {
				if !status.RestartDisposition.Executable() {
					t.Fatalf("enabled binding was not executable: %+v", status)
				}
				if status.FlowPath == "." {
					rootRun, rootService = status.RunID, status.ServiceID
				} else if status.FlowPath == "beta" {
					sibling = status.ServiceID
				}
			}
			if rootRun == "" || sibling == "" {
				t.Fatal("mixed bindings did not acquire their distinct generations")
			}
			before := readServedClockHeader(t, served.Endpoint, rootRun).ClockSchedules[0]
			if before.Status != genericschedule.StatusActive || !before.RetainsRun {
				t.Fatalf("enabled clock lacked retention: %+v", before)
			}
			invokeServedStandingOperation(t, served.Endpoint, "standing.suspend", rootService, "mixed-enabled-park")
			parked := readServedClockHeader(t, served.Endpoint, rootRun).ClockSchedules[0]
			if parked.ActivationID != before.ActivationID || parked.Status != genericschedule.StatusParked || parked.RetainsRun {
				t.Fatalf("mixed service failed exact parking: %+v", parked)
			}
			statuses, err = rt.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, status := range statuses {
				if status.ServiceID == sibling {
					found = true
					if !status.RestartDisposition.Executable() {
						t.Fatalf("root suspension withdrew sibling ingress: %+v", status)
					}
				}
			}
			if !found {
				t.Fatal("root suspension erased sibling binding")
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("mixed binding shutdown=%d\n%s", code, process.outputString())
			}
		})
	}
}

func TestFiniteHostCannotRearmIndependentlyStandingClockBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := canonicalrouting.CopyClockDeployment(t, false)
			path := filepath.Join(root, "schema.yaml")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var schema map[string]any
			if err := yaml.Unmarshal(raw, &schema); err != nil {
				t.Fatal(err)
			}
			schema["pins"].(map[string]any)["inputs"] = []string{"inbound.partner.raw"}
			updated, err := yaml.Marshal(schema)
			if err != nil {
				t.Fatal(err)
			}
			writeWorkflowValidationFixtureFile(t, path, string(updated))
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "poll.tick:\n")
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "consumer:\n  execution_type: system_node\n  subscribes_to: [inbound.partner.raw]\n  event_handlers:\n    inbound.partner.raw:\n      guard: {check: true}\n")
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "partner/manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\ncapabilities:\n  can:\n    emit_events: [raw]\n    verify_secret: webhook_signing_secret\n  cannot: [execute code]\n")
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "partner/trigger.yaml"), "transport: webhook\npayload_object_required: true\nsecret: {required: true}\nsignature: {type: token_equality, header: X-Partner-Token}\ndelivery_id: {json_path: $.id, required: true}\nevent_type: {literal: update, required: true}\nevent_name: {literal: raw}\n")
			credentialPath := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			file, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Set(t.Context(), "partner.webhook_signing_secret", "finite-clock-secret"); err != nil {
				t.Fatal(err)
			}
			opts, start := clockDeploymentHarness(t, backend, root)
			first, served := start()
			rt := servedTestProcessRuntime(t, first)
			statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 {
				t.Fatalf("mixed declaration generation=%+v err=%v", statuses, err)
			}
			status := statuses[0]
			invokeServedStandingOperation(t, served.Endpoint, "standing.suspend", status.ServiceID, "mixed-clock-park")
			parked := readServedClockHeader(t, served.Endpoint, status.RunID)
			if parked.ClockSchedules[0].Status != genericschedule.StatusParked {
				t.Fatalf("mixed clock did not park: %+v", parked)
			}
			if code := first.stop(); code != 0 {
				t.Fatalf("mixed deployment shutdown=%d", code)
			}
			opts.LocalRun = true
			finite, local := start()
			localBefore := readServedClockHeader(t, local.Endpoint, status.RunID)
			if !reflect.DeepEqual(localBefore.ClockSchedules, parked.ClockSchedules) {
				t.Fatalf("finite host changed an existing independent clock: before=%+v current=%+v", parked, localBefore)
			}
			for _, command := range []string{"standing.suspend", "standing.resume", "standing.reset"} {
				response := requestServedJSONRPC(t, local.Endpoint, command, map[string]any{
					"service_id": status.ServiceID, "idempotency_key": "finite-clock-" + command,
				})
				if response.Error == nil {
					t.Fatalf("finite host accepted clock mutation %s: %+v", command, response)
				}
				if current := readServedClockHeader(t, local.Endpoint, status.RunID); !reflect.DeepEqual(current, localBefore) {
					t.Fatalf("finite-host operator acquired deployment clock authority: response=%+v before=%+v current=%+v", response, localBefore, current)
				}
			}
			if code := finite.stop(); code != 0 {
				t.Fatalf("finite mixed host shutdown=%d", code)
			}
		})
	}
}
