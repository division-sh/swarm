package serveapp

import (
	"context"
	"os"
	"path/filepath"
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
			statuses, err := readServedLandingRuns(t.Context(), rt)
			if err != nil || len(statuses) != 2 {
				t.Fatalf("mixed binding inventory=%+v err=%v", statuses, err)
			}
			var rootRun string
			for _, status := range statuses {
				if status.State != "live" {
					t.Fatalf("disabled ingress hid its flow run: %+v", status)
				}
				if status.Scope.FlowPath == "." {
					rootRun = status.RunID
				} else if status.Scope.FlowPath != "beta" {
					t.Fatalf("clock selected an undeclared flow: %+v", status)
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
			statuses, err = readServedLandingRuns(t.Context(), rt)
			if err != nil || len(statuses) != 2 {
				t.Fatalf("enabled ingress inventory=%+v err=%v", statuses, err)
			}
			for _, status := range statuses {
				if status.Scope.FlowPath == "." && status.RunID != rootRun {
					t.Fatalf("credential enabling replaced the clock's standing owner: %+v", status)
				}
				if status.State != "live" {
					t.Fatalf("real credentials failed binding readiness: %+v", status)
				}
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
			statuses, err := readServedLandingRuns(t.Context(), rt)
			if err != nil || len(statuses) != 2 {
				t.Fatalf("independently enabled binding inventory=%+v err=%v", statuses, err)
			}
			rootRun, sibling := "", ""
			for _, status := range statuses {
				if status.State != "live" {
					t.Fatalf("enabled binding was not executable: %+v", status)
				}
				if status.Scope.FlowPath == "." {
					rootRun = status.RunID
				} else if status.Scope.FlowPath == "beta" {
					sibling = status.RunID
				}
			}
			if rootRun == "" || sibling == "" || rootRun == sibling {
				t.Fatal("mixed bindings did not acquire their distinct generations")
			}
			before := readServedClockHeader(t, served.Endpoint, rootRun).ClockSchedules[0]
			if before.Status != genericschedule.StatusActive || !before.RetainsRun {
				t.Fatalf("enabled clock lacked retention: %+v", before)
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("mixed binding shutdown=%d\n%s", code, process.outputString())
			}
		})
	}
}
