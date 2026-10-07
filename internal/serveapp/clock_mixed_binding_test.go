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

func TestServedClockRetainsWithoutIngressCredentialsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := copyClockMixedIngress(t)
			credentialPath := filepath.Join(t.TempDir(), "clock-credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			file, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			opts, start := clockDeploymentHarness(t, backend, root)
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			first, served := start()
			rt := servedTestProcessRuntime(t, first)
			statuses, err := rt.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 {
				t.Fatalf("mixed binding inventory=%+v err=%v", statuses, err)
			}
			var rootRun, rootService, siblingService string
			for _, status := range statuses {
				if status.FlowPath == "." {
					rootRun, rootService = status.RunID, status.ServiceID
					if !status.RestartDisposition.Executable() {
						t.Fatalf("disabled ingress hid an enabled clock: %+v", status)
					}
				} else {
					t.Fatalf("clock invented an unbound sibling generation: %+v", status)
				}
			}
			dormant, err := rt.IneligibleStandingIngress()
			if err != nil || len(dormant) != 2 {
				t.Fatalf("disabled provider bindings disappeared: dormant=%+v err=%v", dormant, err)
			}
			initial := readServedClockHeader(t, served.Endpoint, rootRun).ClockSchedules[0]
			if !initial.RetainsRun || initial.Status != genericschedule.StatusActive {
				t.Fatalf("enabled clock failed independent retention: %+v", initial)
			}
			if code := first.stop(); code != 0 {
				t.Fatalf("dormant-ingress clock shutdown=%d", code)
			}
			for _, key := range []string{"telegram_bot_token", "webhook_signing.alpha", "webhook_signing.beta"} {
				if err := file.Set(context.Background(), key, "clock-binding-secret"); err != nil {
					t.Fatal(err)
				}
			}
			second, restarted := start()
			rt = servedTestProcessRuntime(t, second)
			statuses, err = rt.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 2 {
				t.Fatalf("enabled ingress inventory=%+v err=%v", statuses, err)
			}
			for _, status := range statuses {
				if status.FlowPath == "beta" {
					siblingService = status.ServiceID
				} else if status.FlowPath == "." && (status.RunID != rootRun || status.ServiceID != rootService) {
					t.Fatalf("credential enabling replaced the clock's standing owner: %+v", status)
				}
				if !status.RestartDisposition.Executable() || status.Generation != 1 {
					t.Fatalf("real credentials failed binding readiness: %+v", status)
				}
			}
			if siblingService == "" {
				t.Fatal("real sibling ingress binding did not acquire its own generation")
			}
			readiness, err := rt.Manager.InspectDynamicFlowRuntimeReadinessForSource(t.Context(), rt.Options.SourceArtifactFact)
			if err != nil || len(readiness.CurrentPending) != 0 || len(readiness.SourceTransitionRequired) != 0 {
				t.Fatalf("credential enabling left unfinished startup topology: %+v err=%v", readiness, err)
			}
			completedRuns := make(map[string]bool)
			for _, item := range readiness.CurrentCompleted {
				completedRuns[item.Plan.RunID] = true
			}
			for _, status := range statuses {
				if !completedRuns[status.RunID] {
					t.Fatalf("ready service lacks completed topology for its exact run: %+v", status)
				}
			}
			current := readServedClockHeader(t, restarted.Endpoint, rootRun).ClockSchedules[0]
			if current.ActivationID != initial.ActivationID || !current.RetainsRun || !current.InitialDueAt.Equal(initial.InitialDueAt) {
				t.Fatalf("ingress enabling replaced clock authority: before=%+v current=%+v", initial, current)
			}
			invokeServedStandingOperation(t, restarted.Endpoint, "standing.suspend", rootService, "mixed-bindings-park")
			if clock := readServedClockHeader(t, restarted.Endpoint, rootRun).ClockSchedules[0]; clock.Status != genericschedule.StatusParked || clock.RetainsRun {
				t.Fatalf("suspended mixed service retained clock execution: %+v", clock)
			}
			statuses, err = rt.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			for _, status := range statuses {
				if status.ServiceID == siblingService && !status.RestartDisposition.Executable() {
					t.Fatalf("clock suspension withdrew another enabled binding: %+v", status)
				}
			}
			if code := second.stop(); code != 0 {
				t.Fatalf("mixed binding joined shutdown=%d", code)
			}
		})
	}
}

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
			for _, key := range []string{"telegram_bot_token", "webhook_signing.alpha", "webhook_signing.beta"} {
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
			writeWorkflowValidationFixtureFile(t, path, "activation: standing\n"+string(raw))
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
