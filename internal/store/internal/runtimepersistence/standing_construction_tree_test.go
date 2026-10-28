package runtimepersistence

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestA9KeyedStandingGenerationDoesNotConstructBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, keyed := range []string{".", "parent"} {
			t.Run(backend+"/"+keyed, func(t *testing.T) {
				documents := map[string]string{
					"manifest.yaml":                "version: 1.0.0\nplatform_version: '*'\n",
					"parent/manifest.yaml":         "version: 1.0.0\nplatform_version: '*'\n",
					"parent/service/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
					"schema.yaml":                  "stages: []\n",
					"parent/schema.yaml":           "stages: []\n",
					"parent/service/schema.yaml":   "stages: []\n",
				}
				documents[filepath.Join(keyed, "schema.yaml")] += "instance: tenant\n"
				documents[filepath.Join(keyed, "entities.yaml")] = "owner:\n  tenant: text\n"
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, documents, nil)
				source := semanticview.Wrap(f.bundle)
				fact, found := correlation.SourceArtifactFactFromContext(f.ctx)
				if !found {
					t.Fatal("source authority is missing")
				}
				req := pipeline.LiveTargetMutationRequest{ObservedAt: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), Targets: []pipeline.LiveTargetMutation{{
					FlowPath:   "parent/service",
					Activation: pipeline.FlowInstanceActivationRequest{ContractBundle: source, OccurredAt: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)},
				}}}
				results, finish, err := f.workflows.PrepareLiveTargets(f.ctx, req, f.manager)
				if err != nil || len(results) != 1 || finish == nil {
					t.Fatalf("keyed declaration failed to retain a generation: results=%+v completion=%t err=%v", results, finish != nil, err)
				}
				result := results[0]
				if result.Reconciliation.State != runlifecycle.StateLive || !result.Reconciliation.Source.Matches(fact) || result.Instance != (flowidentity.Instance{}) {
					t.Fatalf("binding and receiver identity were conflated: %+v", result)
				}
				instances, err := f.workflows.ListWorkflowInstances(f.ctx, result.Reconciliation.RunID)
				if err != nil {
					t.Fatal(err)
				}
				wantHeaders := 0
				if keyed == "parent" {
					wantHeaders = 1 // The genuine keyless root, never a keyed placeholder.
				}
				if len(instances) != wantHeaders {
					t.Fatalf("enabled generation manufactured receivers: %+v", instances)
				}
				for _, instance := range instances {
					if instance.WorkflowName != "." || instance.StorageRef != result.Reconciliation.RunID {
						t.Fatalf("unexpected eager receiver: %+v", instance)
					}
				}
				if err := finish(); err != nil {
					t.Fatal(err)
				}
				landing, found, err := f.store.(runlifecycle.LandingRunReader).LoadLandingRun(f.ctx, runlifecycle.LiveRunScope{FlowPath: "parent/service"})
				if err != nil || !found || landing.RunID != result.Reconciliation.RunID || landing.Scope != result.Reconciliation.Scope || landing.State != runlifecycle.StateLive || !landing.Source.Matches(fact) {
					t.Fatalf("zero-instance binding readback changed authority: %+v err=%v", landing, err)
				}
			})
		}
	}
}

func TestStandingPreparationVerifiesCompleteRootTreeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, damage := range []string{"intact", "missing_sibling", "missing_fields", "foreign_parent"} {
			t.Run(backend+"/"+damage, func(t *testing.T) {
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
					"manifest.yaml":                "version: 1.0.0\nplatform_version: '*'\n",
					"service/manifest.yaml":        "version: 1.0.0\nplatform_version: '*'\n",
					"service/detail/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
					"receiver/manifest.yaml":       "version: 1.0.0\nplatform_version: '*'\n",
					"deferred/manifest.yaml":       "version: 1.0.0\nplatform_version: '*'\n",
					"schema.yaml":                  "stages: []\n# fixture: standing-root-tree\n",
					"service/schema.yaml":          "stages: []\n",
					"service/detail/schema.yaml":   "stages: []\n",
					"receiver/schema.yaml":         "stages: []\n",
					"receiver/entities.yaml":       "receipt:\n  marker: {type: text, initial: unchanged}\n",
					"deferred/schema.yaml":         "instance: job_id\npins:\n  inputs:\n    - job.started\n",
					"deferred/entities.yaml":       "job:\n  job_id: text\n",
					"deferred/events.yaml":         "job.started:\n  job_id: text\n",
				}, nil)
				runID := correlation.RunIDFromContext(f.ctx)
				req := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
				var err error
				req.Instance, err = flowidentity.StandingForGeneration(req.ContractBundle, ".", runID)
				if err != nil {
					t.Fatal(err)
				}
				created, finish, err := f.manager.PrepareStandingFlowInstance(f.ctx, req)
				if err != nil || !created || finish == nil {
					t.Fatalf("prepare root tree: created=%t completion=%t err=%v", created, finish != nil, err)
				}
				for table, want := range map[string]int{"flow_instances": 4, "entity_state": 1, "workflow_instance_initial_materializations": 4, "flow_instance_runtime_readiness": 4} {
					var count int
					if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", runID).Scan(&count); err != nil || count != want {
						t.Fatalf("complete root tree %s=%d want=%d err=%v", table, count, want, err)
					}
				}
				switch damage {
				case "missing_sibling":
					_, err = f.db.ExecContext(f.ctx, `DELETE FROM flow_instances WHERE run_id=$1 AND instance_path='receiver'`, runID)
				case "missing_fields":
					_, err = f.db.ExecContext(f.ctx, `DELETE FROM entity_state WHERE run_id=$1 AND flow_instance='receiver'`, runID)
				case "foreign_parent":
					var raw []byte
					if err := f.db.QueryRowContext(f.ctx, `SELECT config FROM flow_instances WHERE run_id=$1 AND instance_path='receiver'`, runID).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					var config map[string]json.RawMessage
					if err := json.Unmarshal(raw, &config); err != nil {
						t.Fatal(err)
					}
					config["parent_flow_instance"] = json.RawMessage(`"foreign"`)
					encoded, encodeErr := json.Marshal(config)
					if encodeErr != nil {
						t.Fatal(encodeErr)
					}
					_, err = f.db.ExecContext(f.ctx, `UPDATE flow_instances SET config=$1 WHERE run_id=$2 AND instance_path='receiver'`, string(encoded), runID)
				}
				if err != nil {
					t.Fatal(err)
				}
				before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				created, finish, err = f.manager.PrepareStandingFlowInstance(f.ctx, req)
				if damage == "intact" {
					if err != nil || created || finish == nil {
						t.Fatalf("reuse root tree: created=%t completion=%t err=%v", created, finish != nil, err)
					}
				} else if err == nil || created || finish != nil {
					t.Fatalf("damaged tree must refuse without repair: created=%t completion=%t err=%v", created, finish != nil, err)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
					t.Fatal("standing reuse/refusal changed durable construction history")
				}
				if damage == "missing_sibling" && !strings.Contains(err.Error(), "complete workflow target persistence is required") {
					t.Fatalf("missing descendant lost named refusal: %v", err)
				}
			})
		}
	}
}
