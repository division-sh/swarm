package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestStandingKeyedAncestryRefusesBeforeMutationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, keyed := range []string{".", "parent"} {
			t.Run(backend+"/"+keyed, func(t *testing.T) {
				documents := map[string]string{
					"schema.yaml":                "name: root\n",
					"parent/schema.yaml":         "name: parent\n",
					"parent/service/schema.yaml": "name: service\nactivation: standing\n",
				}
				documents[filepath.Join(keyed, "schema.yaml")] += "instance: tenant\n"
				documents[filepath.Join(keyed, "entities.yaml")] = "owner:\n  tenant: text\n"
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, documents, nil)
				source := semanticview.Wrap(f.bundle)
				fact, found := correlation.SourceArtifactFactFromContext(f.ctx)
				if !found {
					t.Fatal("source authority is missing")
				}
				serviceID := flowidentity.StandingServiceID("parent/service")
				instance := flowidentity.StandingForService(source, "parent/service", serviceID)
				req := pipeline.StandingTargetMutationRequest{ObservedAt: f.request("unused", "unused", "unused").OccurredAt, Targets: []pipeline.StandingTargetMutation{{
					Candidate:  pipeline.StandingServiceCandidate{BindingEnabled: true, ServiceID: serviceID, FlowPath: "parent/service", InstanceID: instance.InstanceID, EntityID: instance.EntityID, Source: fact},
					Activation: pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: instance},
				}}}
				before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
				results, finish, err := f.workflows.PrepareStandingTargets(f.ctx, req, f.manager)
				if err == nil || !strings.Contains(err.Error(), "keyless no-argument signature") || results != nil || finish != nil {
					t.Fatalf("keyed ancestry accepted: results=%+v completion=%t err=%v", results, finish != nil, err)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
					t.Fatal("keyed ancestry refusal mutated the selected store")
				}
			})
		}
	}
}

type failSecondStandingTreeOwner struct {
	*manager.AgentManager
	preparations int
}

func (o *failSecondStandingTreeOwner) PrepareStandingFlowInstance(ctx context.Context, req pipeline.FlowInstanceActivationRequest) (bool, func() error, error) {
	o.preparations++
	if o.preparations == 2 {
		return false, nil, errors.New("second tree construction failed")
	}
	return o.AgentManager.PrepareStandingFlowInstance(ctx, req)
}

func TestStandingTreeConstructionFailureDoesNotPublishPartialSetBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml":       "name: standing-atomic-set\n",
				"alpha/schema.yaml": "name: alpha\nactivation: standing\n",
				"beta/schema.yaml":  "name: beta\nactivation: standing\n",
			}, nil)
			source := semanticview.Wrap(f.bundle)
			fact, found := correlation.SourceArtifactFactFromContext(f.ctx)
			if !found {
				t.Fatal("source authority is missing")
			}
			req := pipeline.StandingTargetMutationRequest{ObservedAt: f.request("unused", "unused", "unused").OccurredAt}
			for _, flowID := range []string{"alpha", "beta"} {
				serviceID := flowidentity.StandingServiceID(flowID)
				instance := flowidentity.StandingForService(source, flowID, serviceID)
				req.Targets = append(req.Targets, pipeline.StandingTargetMutation{
					Candidate:  pipeline.StandingServiceCandidate{BindingEnabled: true, ServiceID: serviceID, FlowPath: flowID, InstanceID: instance.InstanceID, EntityID: instance.EntityID, Source: fact},
					Activation: pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: instance, OccurredAt: req.ObservedAt},
				})
			}
			owner := &failSecondStandingTreeOwner{AgentManager: f.manager}
			results, finish, err := f.workflows.PrepareStandingTargets(f.ctx, req, owner)
			if err == nil || !strings.Contains(err.Error(), "second tree construction failed") || owner.preparations != 2 || results != nil || finish != nil {
				t.Fatalf("partial standing construction: preparations=%d results=%+v completion=%t err=%v", owner.preparations, results, finish != nil, err)
			}
			var published int
			if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM standing_services WHERE publication_sequence > 0`).Scan(&published); err != nil || published != 0 {
				t.Fatalf("partial set was published: count=%d err=%v", published, err)
			}
		})
	}
}

func TestDormantStandingPreparationDoesNotConstructBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml":         "name: dormant-root\n",
				"service/schema.yaml": "name: service\nactivation: standing\n",
			}, nil)
			source := semanticview.Wrap(f.bundle)
			fact, found := correlation.SourceArtifactFactFromContext(f.ctx)
			if !found {
				t.Fatal("source authority is missing")
			}
			serviceID := flowidentity.StandingServiceID("service")
			instance := flowidentity.StandingForService(source, "service", serviceID)
			request := pipeline.StandingTargetMutationRequest{ObservedAt: f.request("unused", "unused", "unused").OccurredAt, Targets: []pipeline.StandingTargetMutation{{
				Candidate: pipeline.StandingServiceCandidate{BindingEnabled: false, BindingBlockReason: runlifecycle.StandingBindingCredentialsAbsent,
					ServiceID: serviceID, FlowPath: "service", InstanceID: instance.InstanceID, EntityID: instance.EntityID, Source: fact},
				Activation: pipeline.FlowInstanceActivationRequest{ContractBundle: source, Instance: instance},
			}}}
			owner := &failSecondStandingTreeOwner{AgentManager: f.manager}
			before := snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")
			for attempt := 0; attempt < 2; attempt++ {
				results, finish, err := f.workflows.PrepareStandingTargets(f.ctx, request, owner)
				if err != nil || len(results) != 1 || finish == nil {
					t.Fatalf("dormant preparation: results=%+v finish=%t err=%v", results, finish != nil, err)
				}
				if result := results[0]; result.Created || result.Reconciliation.RunID != "" || result.Reconciliation.Generation != 0 || result.Reconciliation.RestartDisposition.Executable() || result.PublicationSequence != 0 || result.Instance.InstancePath != "" {
					t.Fatalf("dormant declaration acquired execution evidence: %+v", result)
				}
				if err := finish(); err != nil || owner.preparations != 0 {
					t.Fatalf("dormant preparation invoked constructor: count=%d err=%v", owner.preparations, err)
				}
			}
			if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend == "postgres")) {
				t.Fatal("dormant declaration mutated construction/execution evidence")
			}
			for _, table := range []string{"standing_services", "standing_service_generations", "standing_service_journal"} {
				var count int
				if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("fresh dormant %s count=%d err=%v", table, count, err)
				}
			}
		})
	}
}

func TestStandingPreparationVerifiesCompleteRootTreeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, damage := range []string{"intact", "missing_sibling", "missing_fields", "foreign_parent"} {
			t.Run(backend+"/"+damage, func(t *testing.T) {
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
					"schema.yaml":                "name: standing-root-tree\n",
					"service/schema.yaml":        "name: service\nactivation: standing\n",
					"service/detail/schema.yaml": "name: detail\n",
					"receiver/schema.yaml":       "name: receiver\n",
					"receiver/entities.yaml":     "receipt:\n  marker: {type: text, initial: unchanged}\n",
					"deferred/schema.yaml":       "name: deferred\ninstance: job_id\npins:\n  inputs:\n    - job.started\n",
					"deferred/entities.yaml":     "job:\n  job_id: text\n",
					"deferred/events.yaml":       "job.started:\n  job_id: text\n",
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
