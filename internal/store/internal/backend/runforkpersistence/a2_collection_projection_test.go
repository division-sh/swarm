package runforkpersistence

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packadmission"
	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func a2ForkMapFixture(t *testing.T) (runfork.RunForkPlan, rc.FanOutCompiledPlan, semanticview.OriginalLoopCarriage) {
	t.Helper()
	root := canonicalrouting.CopyForkFanOutCarrier(t, false, false)
	if err := os.WriteFile(filepath.Join(root, "events.yaml"), []byte("items.ready:\n  items: 'map[text][integer]'\nitems.child:\n  value: text\nbatch.completed:\n  total: integer\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	nodes, err := os.ReadFile(filepath.Join(root, "nodes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nodes.yaml"), []byte(strings.Replace(string(nodes), "items_from: payload.items", "items_from: entity.items", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "entities.yaml"), []byte("root:\n  items: 'map[text][integer]'\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := rc.LoadWorkflowContractBundleWithOptions(repo, root, rc.DefaultPlatformSpecFile(repo), rc.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	original, err := semanticview.CompileOriginalLoopCarriage(source)
	if err != nil {
		t.Fatal(err)
	}
	node, err := identity.AdmitExecutableNodeDeclaration(".", "fan-out-source")
	if err != nil {
		t.Fatal(err)
	}
	plans := source.FanOutPlansForHandler(node, "items.ready")
	if len(plans) != 1 {
		t.Fatalf("want one actual compiled map fan-out plan, got %d", len(plans))
	}
	compiled := plans[0]
	run, event, delivery, mutation := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	producer, err := events.NewRootRoutingSource(run)
	if err != nil {
		t.Fatal(err)
	}
	ref := fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEntityField, RunID: run, EntityID: run, Field: "items", MutationID: mutation}
	requestSource := ref
	requestSource.MutationID = ""
	now := time.Now().UTC().Truncate(time.Microsecond)
	intent := fanoutobligation.Intent{
		Request: fanoutobligation.IntentRequest{Key: fanoutobligation.IntentKey{RunID: run, TriggeringDeliveryID: delivery, ElementRef: compiled.Ref.ElementRef}, PlanRef: compiled.Ref, Source: requestSource, Cardinality: 3,
			Capsule: fanoutobligation.Capsule{SourceProjection: compiled.SemanticEvidence(), NodeKey: node.Key(), ExecutionFlowID: ".", EntityID: run, HandlerEventKey: "items.ready", Route: flowidentity.StoredRoute(".", run, run), ProducerSource: producer, Lineage: events.EventLineage{RunID: run, ParentEventID: event, ExecutionMode: executionmode.Live}},
		},
		Source: ref, Cursor: 1, Status: fanoutobligation.StatusOpen, NextChunkSize: fanoutobligation.InitialChunkSize, CreatedAt: now, UpdatedAt: now,
	}
	if err := intent.Validate(); err != nil {
		t.Fatal(err)
	}
	plan := runfork.RunForkPlan{SourceRunID: run, Entities: []runfork.RunForkEntityState{{EntityID: run, MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, FlowInstance: run}}},
		FanOutObligations: []runfork.RunForkFanOutObligation{{Intent: intent, Outcomes: []fanoutobligation.Outcome{{Ordinal: 0, Kind: fanoutobligation.OutcomeCommitted, SourceEventID: uuid.NewString(), InheritedDisposition: "no_route", CreatedAt: now}}}},
	}
	return plan, compiled, original
}

// Reduced exact-reader controls complement the actual retained-source reader;
// they do not claim a full fork materializer/publication/boot receipt.
func TestA2RetainedMapSourceForkOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := fanOutTimestampReaderDatabase(t, backend)
			plan, compiled, original := a2ForkMapFixture(t)
			intent := plan.FanOutObligations[0].Intent
			child := uuid.NewString()
			refs, err := resolveRunForkFanOutPlanRefs(plan, compiled.Ref.BundleHash, []rc.FanOutPlanRef{compiled.Ref})
			if err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			capsule, _, err := projectRunForkFanOutCapsule(context.Background(), tx, false, child, plan, plan.FanOutObligations[0], original)
			if err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(capsule.SourceProjection, intent.Request.Capsule.SourceProjection) {
				t.Fatal("fork changed immutable admitted projection")
			}
			if capsule.EntityID != child || capsule.Lineage.ParentEventID != intent.Request.Capsule.Lineage.ParentEventID {
				t.Fatal("execution projection changed source provenance")
			}
			raw, err := fanoutobligation.MarshalCapsule(capsule)
			if err != nil {
				t.Fatal(err)
			}
			source := intent.Source
			if _, err := db.Exec(`INSERT INTO fan_out_intents (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,source_kind,source_run_id,source_entity_id,source_field,source_mutation_id,cardinality,cursor,status,next_chunk_size,capsule,claim_generation) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,3,1,'open',$13,$14,0)`, child, intent.Request.Key.TriggeringDeliveryID, compiled.Ref.ElementRef.FlowPath, compiled.Ref.ElementRef.Family, compiled.Ref.ElementRef.SemanticPath, compiled.Ref.BundleHash, compiled.Ref.SemanticDigest, string(source.Kind), source.RunID, source.EntityID, source.Field, source.MutationID, fanoutobligation.InitialChunkSize, string(raw)); err != nil {
				t.Fatal(err)
			}
			outcome := plan.FanOutObligations[0].Outcomes[0]
			if _, err := db.Exec(`INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,source_event_id,inherited_disposition,created_at) VALUES ($1,$2,$3,$4,$5,0,'committed',$6,'no_route',$7)`, child, intent.Request.Key.TriggeringDeliveryID, compiled.Ref.ElementRef.FlowPath, compiled.Ref.ElementRef.Family, compiled.Ref.ElementRef.SemanticPath, outcome.SourceEventID, outcome.CreatedAt); err != nil {
				t.Fatal(err)
			}
			for repeat := 0; repeat < 2; repeat++ {
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				err = requireExactMaterializedRunForkFanOut(context.Background(), tx, backend == "postgres", child, plan, refs, original, "", nil, nil)
				if rollback := tx.Rollback(); rollback != nil {
					t.Fatal(rollback)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, tc := range []struct {
				name, column string
				value        any
			}{
				{"mode", "capsule", func() string {
					c := capsule
					c.SourceProjection = c.SourceProjection.Clone()
					c.SourceProjection.CollectionProjection.Kind = rc.CollectionListItems
					b, _ := fanoutobligation.MarshalCapsule(c)
					return string(b)
				}()},
				{"semantic_digest", "semantic_digest", "wrong"},
				{"source_revision", "source_mutation_id", uuid.NewString()},
				{"source_run", "source_run_id", uuid.NewString()},
				{"cardinality", "cardinality", 4},
				{"cursor", "cursor", 2},
			} {
				t.Run(tc.name, func(t *testing.T) {
					tx, err := db.BeginTx(context.Background(), nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					if _, err := tx.Exec(`UPDATE fan_out_intents SET `+tc.column+`=$1 WHERE run_id=$2`, tc.value, child); err != nil {
						t.Fatal(err)
					}
					if err := requireExactMaterializedRunForkFanOut(context.Background(), tx, backend == "postgres", child, plan, refs, original, "", nil, nil); err == nil {
						t.Fatal("contradictory retained plan/source accepted")
					}
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
				})
			}
			items, err := intent.Request.ProjectSource(map[string]any{"z": []any{1}, " a ": []any{2}, "a": []any{3}})
			if err != nil || !reflect.DeepEqual(items[1:], []any{"a", "z"}) {
				t.Fatalf("fork lexical range: %#v %v", items, err)
			}
		})
	}
}

func TestA2ForkPlanProofRejectsProjectionDrift(t *testing.T) {
	plan, compiled, _ := a2ForkMapFixture(t)
	if _, err := resolveRunForkFanOutPlanRefs(plan, compiled.Ref.BundleHash, nil); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*runfork.RunForkPlan){
		func(p *runfork.RunForkPlan) {
			p.FanOutObligations[0].Intent.Request.Capsule.SourceProjection = rc.FanOutPlanSemantics{}
		},
		func(p *runfork.RunForkPlan) {
			p.FanOutObligations[0].Intent.Request.Capsule.SourceProjection.CollectionProjection.Kind = rc.CollectionListItems
		},
		func(p *runfork.RunForkPlan) { p.FanOutObligations[0].Intent.Request.Source.Field = "other" },
	} {
		raw, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		var bad runfork.RunForkPlan
		if err := json.Unmarshal(raw, &bad); err != nil {
			t.Fatal(err)
		}
		change(&bad)
		if _, err := resolveRunForkFanOutPlanRefs(bad, compiled.Ref.BundleHash, []rc.FanOutPlanRef{compiled.Ref}); err == nil {
			t.Fatal("fork proof accepted contradictory admitted projection")
		}
	}
	selected := compiled.Ref
	selected.BundleHash = "bundle-v2:sha256:" + strings.Repeat("2", 64)
	if _, err := resolveRunForkFanOutPlanRefs(plan, selected.BundleHash, []rc.FanOutPlanRef{selected}); err != nil {
		t.Fatal(err)
	}
	selected.SemanticDigest = "wrong"
	if _, err := resolveRunForkFanOutPlanRefs(plan, selected.BundleHash, []rc.FanOutPlanRef{selected}); err == nil {
		t.Fatal("fork accepted changed semantic interpretation")
	}
}
