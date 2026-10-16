package runtimepersistence

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/operatorsurface"
)

type operationalSnapshotPipeline struct {
	operatorsurface.PipelineObligationSource
	beforeInstances func()
}

func (p operationalSnapshotPipeline) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	p.beforeInstances()
	return p.PipelineObligationSource.ListFlowInstances(ctx, scope)
}

func TestRunOperationalReportUsesOneSnapshotAcrossCommitBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, cut := range []string{"before_snapshot", "during_snapshot"} {
				t.Run(cut, func(t *testing.T) {
					f := newSnapshotOwnershipFixture(t, backend, false, false)
					// Permit the real writer to commit while a WAL reader retains
					// its earlier snapshot; this is not a simulated row iterator.
					f.db.SetMaxOpenConns(4)
					reader := f.store.(operatorread.RunReader)
					before, err := reader.LoadRunDebugReport(f.ctx, f.runID, operatorread.RunDebugQueryOptions{})
					if err != nil {
						t.Fatal(err)
					}
					writes := 0
					advance := func() { writes++; f.advance(t) }
					if cut == "before_snapshot" {
						advance()
					} else {
						switch s := f.store.(type) {
						case *PostgresStore:
							reader, err = operatorsurface.NewRunPostgres(s.backend, s.requireCurrentSchema,
								operationalSnapshotPipeline{s.pipelinePostgresOwner, advance}, s.timerObligationPostgresReader,
								s.operatorObservabilityPostgres, s.sourceArtifactOwner)
						case *SQLiteRuntimeStore:
							reader, err = operatorsurface.NewRunSQLite(s.backend, s.requireCurrentSchema, nil,
								operationalSnapshotPipeline{s.pipelineSQLiteOwner, advance}, s.timerObligationSQLiteReader,
								s.operatorObservabilitySQLite, s.sourceArtifactOwner)
						default:
							t.Fatalf("unexpected selected owner %T", f.store)
						}
						if err != nil {
							t.Fatal(err)
						}
					}
					report, err := reader.LoadRunDebugReport(f.ctx, f.runID, operatorread.RunDebugQueryOptions{})
					if err != nil {
						t.Fatal(err)
					}
					wantStage, wantCount := "ready", before.EventCount
					if cut == "before_snapshot" {
						wantStage, wantCount = "later", before.EventCount+1
					}
					assertOperationalSnapshotCut(t, report, wantStage, wantCount)
					after, err := f.store.(operatorread.RunReader).LoadRunDebugReport(f.ctx, f.runID, operatorread.RunDebugQueryOptions{})
					if err != nil {
						t.Fatal(err)
					}
					assertOperationalSnapshotCut(t, after, "later", before.EventCount+1)
					if writes != 1 {
						t.Fatalf("diagnostic changed the controlled writer count: %d", writes)
					}
				})
			}
		})
	}
}

func assertOperationalSnapshotCut(t *testing.T, report operatorread.RunDebugReport, stage string, eventCount int) {
	t.Helper()
	if report.OperationalFacts == nil || !report.OperationalFacts.Complete || report.EventCount != eventCount || report.Header.EventCount != eventCount {
		t.Fatalf("incoherent event/header/fact snapshot: %+v", report)
	}
	for _, instance := range report.OperationalFacts.Instances {
		if instance.FlowInstance == "owner/one" {
			if instance.Stage != stage {
				t.Fatalf("header stage crossed snapshot: %s, want %s", instance.Stage, stage)
			}
			return
		}
	}
	t.Fatal("snapshot omitted constructed owner/one")
}
