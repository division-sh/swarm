package runforkpersistence_test

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	nativefork "github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	artifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"modernc.org/sqlite"
)

type selectedRootedSourceReader interface {
	LoadSelectedForkRootedSource(context.Context, string) (sourceartifact.RootedSource, error)
	LoadRunRootedSource(context.Context, string) (sourceartifact.RootedSource, error)
}

func TestS1SelectedForkSourceRequiresWholeRetainedResponsibilityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newSelectedRootedSourceFixture(t, backend)
			reader, ok := fixture.owner.(selectedRootedSourceReader)
			if !ok {
				t.Fatal("selected fork owner cannot restore its exact rooted responsibility before compilation")
			}
			ctx := context.Background()
			requireSource := func(t *testing.T) {
				t.Helper()
				loaded, err := reader.LoadSelectedForkRootedSource(ctx, fixture.childID)
				if err != nil || loaded.Validate() != nil || !loaded.Admission().Equal(fixture.target.Admission()) {
					t.Fatalf("retained child selected another root: root=%+v err=%v", loaded.Admission().Reference(), err)
				}
				if _, present := loaded.Artifact().RootAdmission(); present {
					t.Fatal("restored content became filesystem root authority")
				}
			}
			requireSource(t)
			t.Run("otherwise-valid-operation-other-root", func(t *testing.T) {
				fixture.replaceOperationRoot(t, fixture.parent.Admission().Reference())
				loaded, err := reader.LoadSelectedForkRootedSource(ctx, fixture.childID)
				fixture.replaceOperationRoot(t, fixture.target.Admission().Reference())
				if err == nil || loaded.Artifact() != nil || loaded.Admission() != (sourceartifact.RootAdmission{}) {
					t.Fatalf("valid permanent operation crossed child root: %+v, %v", loaded.Admission().Reference(), err)
				}
				requireSource(t)
			})
			for _, cell := range []struct {
				name            string
				change, restore func(*testing.T)
			}{
				{"child-root", func(t *testing.T) { fixture.reviseChildRoot(t, fixture.parent) }, func(t *testing.T) { fixture.reviseChildRoot(t, fixture.target) }},
				{"child-point", func(t *testing.T) { fixture.corruptChildRevision(t, 8) }, func(t *testing.T) { fixture.corruptChildRevision(t, 7) }},
			} {
				t.Run(cell.name, func(t *testing.T) {
					cell.change(t)
					loaded, err := reader.LoadSelectedForkRootedSource(ctx, fixture.childID)
					cell.restore(t)
					if err == nil || loaded.Artifact() != nil || loaded.Admission() != (sourceartifact.RootAdmission{}) {
						t.Fatalf("crossed child responsibility returned compiler authority: root=%+v err=%v", loaded.Admission().Reference(), err)
					}
					requireSource(t)
				})
			}
			for _, cell := range []struct {
				name, statement   string
				changed, original any
			}{
				{"binding-root", `UPDATE run_fork_selected_contract_bindings SET source_root_directory=$1 WHERE fork_run_id=$2`, fixture.parent.Admission().RootDirectory(), fixture.target.Admission().RootDirectory()},
				{"binding-point", `UPDATE run_fork_selected_contract_bindings SET fork_revision=$1 WHERE fork_run_id=$2`, int64(8), int64(7)},
				{"missing-binding", `UPDATE run_fork_selected_contract_bindings SET fork_run_id=$1 WHERE binding_id=$2`, fixture.parentID, fixture.childID},
				{"missing-root", `UPDATE source_root_admissions SET root_directory=$1 WHERE root_directory=$2`, "retired-selected", fixture.target.Admission().RootDirectory()},
				{"corrupt-content", `UPDATE source_artifacts SET source_blob=$1 WHERE bundle_hash=$2`, []byte("not a source artifact"), fixture.target.Artifact().LogicalBlob()},
			} {
				t.Run(cell.name, func(t *testing.T) {
					id := fixture.childID
					if cell.name == "missing-binding" {
						id = fixture.bindingID
					} else if cell.name == "missing-root" {
						id = fixture.target.Admission().RootDirectory()
					} else if cell.name == "corrupt-content" {
						id = fixture.target.Artifact().BundleHash()
					}
					fixture.changeEvidence(t, cell.statement, cell.changed, id)
					loaded, err := reader.LoadSelectedForkRootedSource(ctx, fixture.childID)
					if cell.name == "missing-root" {
						id = cell.changed.(string)
					}
					fixture.changeEvidence(t, cell.statement, cell.original, id)
					if err == nil || loaded.Artifact() != nil || loaded.Admission() != (sourceartifact.RootAdmission{}) {
						t.Fatalf("crossed responsibility returned compiler authority: root=%+v err=%v", loaded.Admission().Reference(), err)
					}
					requireSource(t)
				})
			}
			t.Run("original-is-not-selected-child", func(t *testing.T) {
				loaded, err := reader.LoadSelectedForkRootedSource(ctx, fixture.parentID)
				if err == nil || loaded.Artifact() != nil {
					t.Fatalf("original run gained selected target authority: %+v, %v", loaded.Admission().Reference(), err)
				}
			})
			t.Run("canceled", func(t *testing.T) {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				loaded, err := reader.LoadSelectedForkRootedSource(canceled, fixture.childID)
				if !errors.Is(err, context.Canceled) || loaded.Artifact() != nil {
					t.Fatalf("canceled reconstruction returned compiler authority: %+v, %v", loaded.Admission().Reference(), err)
				}
			})
			t.Run("missing-content", func(t *testing.T) {
				fixture.removeSourceContent(t)
				loaded, err := reader.LoadSelectedForkRootedSource(ctx, fixture.childID)
				if err == nil || loaded.Artifact() != nil || loaded.Admission() != (sourceartifact.RootAdmission{}) {
					t.Fatalf("deleted content reconstructed compiler authority from history: %+v, %v", loaded.Admission().Reference(), err)
				}
			})
		})
	}
}

// Metadata fixtures use the original fully composed native store and process.
// They do not create an executable selected grant or claim activation coverage.
type selectedRootedSourceStore interface {
	selectedRootedSourceReader
	runlifecycle.OperationOwner
	startupownership.Store
	sourceartifactfixture.RootedWriter
	LoadForkOperationByID(context.Context, string) (runfork.ForkOperationRecord, bool, error)
}

type selectedRootedSourceFixture struct {
	connector                                 *selectedRootReadConnector
	db                                        *sql.DB
	owner                                     selectedRootedSourceStore
	ctx                                       context.Context
	postgres                                  bool
	parent, target                            sourceartifact.RootedSource
	parentID, childID, bindingID, operationID string
	association                               func(*testing.T, string)
}

func TestS1SelectedForkSourceRefusesUnacknowledgedReadBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newSelectedRootedSourceFixture(t, backend)
			reader := f.owner.(selectedRootedSourceReader)
			for _, responsibility := range []struct {
				name, runID string
				load        func(context.Context, string) (sourceartifact.RootedSource, error)
				want        sourceartifact.RootAdmission
			}{
				{"selected", f.childID, reader.LoadSelectedForkRootedSource, f.target.Admission()},
				{"original", f.parentID, reader.LoadRunRootedSource, f.parent.Admission()},
			} {
				for _, cut := range []string{"closure-error", "canceled-after-commit"} {
					t.Run(responsibility.name+"/"+cut, func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						cause := errors.New("rooted fork read closure failed")
						calls := 0
						f.connector.afterCommit = func() error {
							calls++
							if cut == "canceled-after-commit" {
								cancel()
								return nil
							}
							return cause
						}
						loaded, err := responsibility.load(ctx, responsibility.runID)
						f.connector.afterCommit = nil
						want := cause
						if cut == "canceled-after-commit" {
							want = context.Canceled
						}
						if calls != 1 || !errors.Is(err, want) || loaded.Artifact() != nil || loaded.Admission() != (sourceartifact.RootAdmission{}) {
							t.Fatalf("failed read disclosed rooted evidence: calls=%d source=%+v err=%v", calls, loaded.Admission().Reference(), err)
						}
						if restored, err := responsibility.load(context.Background(), responsibility.runID); err != nil || !restored.Admission().Equal(responsibility.want) {
							t.Fatalf("read fault changed retained responsibility: %v", err)
						}
					})
				}
			}
		})
	}
}

func TestS1RetainedForkSourceReachesRootedCompilerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newSelectedRootedSourceFixture(t, backend)
			reader := fixture.owner.(selectedRootedSourceReader)
			for _, responsibility := range []struct {
				name, runID string
				load        func(context.Context, string) (sourceartifact.RootedSource, error)
				want        sourceartifact.RootAdmission
			}{
				{"original", fixture.parentID, reader.LoadRunRootedSource, fixture.parent.Admission()},
				{"selected", fixture.childID, reader.LoadSelectedForkRootedSource, fixture.target.Admission()},
			} {
				t.Run(responsibility.name, func(t *testing.T) {
					source, err := responsibility.load(context.Background(), responsibility.runID)
					if err != nil {
						t.Fatal(err)
					}
					if _, present := source.Artifact().RootAdmission(); present {
						t.Fatal("stored content invented a filesystem admission")
					}
					bundle, err := contracts.LoadWorkflowContractBundleFromRootedSource(filepath.Join("..", "..", "..", "..", ".."), source, "", contracts.WorkflowContractLoadOptions{})
					if err != nil {
						t.Fatal(err)
					}
					compiled, err := bundle.RootedSource()
					if err != nil || !compiled.Admission().Equal(responsibility.want) {
						t.Fatalf("native responsibility lost at compiler: %+v, %v", compiled.Admission().Reference(), err)
					}
					identity, err := contracts.BootBundleIdentity(bundle)
					if err != nil || identity.BundleHash != source.Artifact().BundleHash() || identity.WorkflowName != "." || identity.WorkflowVersion != identity.BundleHash {
						t.Fatalf("retained rooted compiler changed machine identity: %+v, %v", identity, err)
					}
				})
			}
		})
	}
}

func newSelectedRootedSourceFixture(t *testing.T, backend string) selectedRootedSourceFixture {
	t.Helper()
	f := selectedRootedSourceFixture{parentID: uuid.NewString(), childID: uuid.NewString(), operationID: uuid.NewString(), postgres: backend == "postgres"}
	var paths []string
	for _, name := range []string{"original", "selected"} {
		source, path := sourceartifactfixture.Rooted(t, name, sourceartifactfixture.Artifact())
		paths = append(paths, path)
		if name == "original" {
			f.parent = source
		} else {
			f.target = source
		}
	}
	if f.postgres {
		dsn, _, cleanup := testutil.StartEmptyPostgres(t)
		t.Cleanup(cleanup)
		f.connector = &selectedRootReadConnector{driver: &pq.Driver{}, dsn: dsn}
	} else {
		path := filepath.Join(t.TempDir(), "source.sqlite")
		f.connector = &selectedRootReadConnector{driver: &sqlite.Driver{}, dsn: "file:" + path + "?_pragma=" + url.QueryEscape("foreign_keys(ON)")}
	}
	f.db = sql.OpenDB(f.connector)
	t.Cleanup(func() {
		if err := f.db.Close(); err != nil {
			t.Error(err)
		}
	})
	if f.postgres {
		f.owner = storetest.AdmitPostgresRuntimeStore(t, f.db)
	} else {
		f.owner = storetest.AdmitSQLiteRuntimeStore(t, f.db)
	}
	request := startupownership.AcquireRequest{OwnerID: "rooted-source-proof", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}
	capability, err := f.owner.AcquireProcessCapability(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	f.ctx, err = startupownership.WithProcessMutationAuthority(context.Background(), capability)
	if err != nil {
		t.Fatal(err)
	}
	f.ctx = authoractivity.WithScope(f.ctx, authoractivity.BundleScope(request.RuntimeInstanceID, f.parent.Artifact().BundleHash()))
	for _, source := range []sourceartifact.RootedSource{f.parent, f.target} {
		if _, err := f.owner.EnsureRootedSource(f.ctx, source); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.owner.CreateRun(f.ctx, runlifecycle.CreateRequest{
		RunID: f.parentID, Origin: runlifecycle.DeploymentRunOrigin(),
		Source: sourceartifactfixture.FactFor(f.parent.Artifact()), RootAdmission: f.parent.Admission(), StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	f.createFork(t, f.childID, f.parentID, 7, f.target)
	f.bindingID, f.association = nativefork.BindRetainedSourceAssociationForTest(t, f.ctx, f.db, f.postgres,
		f.parent, f.target, f.parentID, f.childID, f.operationID, f.owner)
	for _, path := range paths {
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f selectedRootedSourceFixture) createFork(t *testing.T, runID, parentID string, revision int64, source sourceartifact.RootedSource) {
	t.Helper()
	origin, err := runlifecycle.ForkMaterializationRunOrigin(parentID, runlifecycle.ForkOriginPointDeploymentRevision, revision, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.CreateRun(f.ctx, runlifecycle.CreateRequest{
		RunID: runID, Origin: origin, Source: sourceartifactfixture.FactFor(source.Artifact()),
		RootAdmission: source.Admission(), StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.owner.TransitionActiveRun(f.ctx, runlifecycle.ActiveTransitionRequest{RunID: runID, State: runlifecycle.StatePaused}); err != nil {
		t.Fatal(err)
	}
}

func (f selectedRootedSourceFixture) reviseChildRoot(t *testing.T, source sourceartifact.RootedSource) {
	t.Helper()
	if _, err := f.owner.ReviseRunSource(f.ctx, runlifecycle.SourceRevisionRequest{RunID: f.childID, Source: sourceartifactfixture.FactFor(source.Artifact()), RootAdmission: source.Admission()}); err != nil {
		t.Fatal(err)
	}
}

func (f selectedRootedSourceFixture) corruptChildRevision(t *testing.T, revision int64) {
	t.Helper()
	origin, err := runlifecycle.ForkMaterializationRunOrigin(f.parentID, runlifecycle.ForkOriginPointDeploymentRevision, revision, "")
	if err != nil {
		t.Fatal(err)
	}
	if f.postgres {
		runlifecyclefixture.CorruptPostgresOrigin(t, context.Background(), f.db, f.childID, origin)
	} else {
		runlifecyclefixture.CorruptSQLiteOrigin(t, context.Background(), f.db, f.childID, origin)
	}
}

func (f selectedRootedSourceFixture) changeEvidence(t *testing.T, statement string, args ...any) {
	t.Helper()
	result, err := f.db.ExecContext(context.Background(), statement, args...)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("corrupt/restore exact retained evidence: affected=%d err=%v", count, err)
	}
}

func (f selectedRootedSourceFixture) replaceOperationRoot(t *testing.T, reference sourceartifact.RootAdmissionReference) {
	t.Helper()
	record, found, err := f.owner.LoadForkOperationByID(context.Background(), f.operationID)
	if err != nil || !found {
		t.Fatalf("load exact retained operation: found=%t err=%v", found, err)
	}
	request := record.Request
	request.TargetRootReference = reference
	canonical, semanticHash, err := request.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := canonicaljson.Bytes(canonical)
	if err != nil {
		t.Fatal(err)
	}
	f.changeEvidence(t, `UPDATE run_fork_operations SET request_json=$1, semantic_hash=$2 WHERE operation_id=$3`, string(raw), semanticHash, f.operationID)
}

func (f selectedRootedSourceFixture) removeSourceContent(t *testing.T) {
	t.Helper()
	tx, err := f.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	dialect := "sqlite"
	if f.postgres {
		dialect = "postgres"
	}
	if err := artifactstore.DeleteSourceArtifactForFixtureRefusalTx(context.Background(), tx, dialect, sourceartifactfixture.FactFor(f.target.Artifact())); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
