package runtimepersistence

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/internal/operatorsurface"
)

type operationalSnapshotPipeline struct {
	operatorsurface.PipelineObligationSource
	beforeInstances func()
}

type operationalSnapshotSources struct {
	sourceartifact.Reader
	mode  string
	reads int
}

func (s *operationalSnapshotSources) GetSourceArtifact(ctx context.Context, hash string) (sourceartifact.Persisted, error) {
	s.reads++
	stored, err := s.Reader.GetSourceArtifact(ctx, hash)
	if err != nil {
		return stored, err
	}
	switch s.mode {
	case "missing":
		return sourceartifact.Persisted{}, sourceartifact.ErrNotFound
	case "corrupt":
		stored.SourceBlob = []byte("corrupt retained row")
	case "foreign":
		stored.BundleHash = strings.Repeat("a", 64)
	}
	return stored, nil
}

func (p operationalSnapshotPipeline) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	p.beforeInstances()
	return p.PipelineObligationSource.ListFlowInstances(ctx, scope)
}

func TestRunOperationalReportUsesOneSnapshotAcrossCommitBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, cut := range []string{"before_snapshot", "during_snapshot", "during_warm_snapshot"} {
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
						armed := cut != "during_warm_snapshot"
						writeDuringRead := func() {
							if armed {
								advance()
							}
						}
						switch s := f.store.(type) {
						case *PostgresStore:
							reader, err = operatorsurface.NewRunPostgres(s.backend, s.requireCurrentSchema,
								operationalSnapshotPipeline{s.pipelinePostgresOwner, writeDuringRead}, s.timerObligationPostgresReader,
								s.operatorObservabilityPostgres, s.sourceArtifactOwner)
						case *SQLiteRuntimeStore:
							reader, err = operatorsurface.NewRunSQLite(s.backend, s.requireCurrentSchema, nil,
								operationalSnapshotPipeline{s.pipelineSQLiteOwner, writeDuringRead}, s.timerObligationSQLiteReader,
								s.operatorObservabilitySQLite, s.sourceArtifactOwner)
						default:
							t.Fatalf("unexpected selected owner %T", f.store)
						}
						if err != nil {
							t.Fatal(err)
						}
						if !armed {
							warm, err := reader.LoadRunDebugReport(f.ctx, f.runID, operatorread.RunDebugQueryOptions{})
							if err != nil {
								t.Fatal(err)
							}
							assertOperationalSnapshotCut(t, warm, "ready", before.EventCount)
							armed = true
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
			t.Run("warm_source_refusals_and_fresh_progress", func(t *testing.T) {
				f := newSnapshotOwnershipFixture(t, backend, false, false)
				var reader operatorread.RunReader
				sources := &operationalSnapshotSources{}
				var err error
				switch s := f.store.(type) {
				case *PostgresStore:
					sources.Reader = s.sourceArtifactOwner
					reader, err = operatorsurface.NewRunPostgres(s.backend, s.requireCurrentSchema, s.pipelinePostgresOwner, s.timerObligationPostgresReader, s.operatorObservabilityPostgres, sources)
				case *SQLiteRuntimeStore:
					sources.Reader = s.sourceArtifactOwner
					reader, err = operatorsurface.NewRunSQLite(s.backend, s.requireCurrentSchema, nil, s.pipelineSQLiteOwner, s.timerObligationSQLiteReader, s.operatorObservabilitySQLite, sources)
				default:
					t.Fatalf("unexpected selected owner %T", f.store)
				}
				if err != nil {
					t.Fatal(err)
				}
				before, err := reader.LoadRunDebugReport(f.ctx, f.runID, operatorread.RunDebugQueryOptions{})
				if err != nil {
					t.Fatal(err)
				}
				assertOperationalSnapshotCut(t, before, "ready", before.EventCount)
				f.advance(t)
				for _, mode := range []string{"corrupt", "foreign", "missing", "restored"} {
					sources.mode = mode
					report, err := reader.LoadRunDebugReport(f.ctx, f.runID, operatorread.RunDebugQueryOptions{})
					switch mode {
					case "corrupt":
						if err == nil || !strings.Contains(err.Error(), "decode source artifact") {
							t.Fatalf("warm corrupt source escaped exact refusal: %v", err)
						}
					case "foreign":
						if err == nil || err.Error() != "operational source contradicts exact run bundle" {
							t.Fatalf("warm source association escaped refusal: %v", err)
						}
					case "missing":
						if err != nil || report.OperationalFacts == nil || report.OperationalFacts.Complete || report.EventCount != before.EventCount+1 {
							t.Fatalf("missing source fabricated complete or cached facts: %+v / %v", report.OperationalFacts, err)
						}
					case "restored":
						if err != nil {
							t.Fatal(err)
						}
						assertOperationalSnapshotCut(t, report, "later", before.EventCount+1)
					}
				}
				if sources.reads != 5 {
					t.Fatalf("report bypassed retained source read: %d", sources.reads)
				}
				cancelled, cancel := context.WithCancel(f.ctx)
				cancel()
				if _, err := reader.LoadRunDebugReport(cancelled, f.runID, operatorread.RunDebugQueryOptions{}); !errors.Is(err, context.Canceled) {
					t.Fatalf("warm report ignored cancellation: %v", err)
				}
				after, err := reader.LoadRunDebugReport(f.ctx, f.runID, operatorread.RunDebugQueryOptions{})
				if err != nil {
					t.Fatal(err)
				}
				assertOperationalSnapshotCut(t, after, "later", before.EventCount+1)
			})
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
