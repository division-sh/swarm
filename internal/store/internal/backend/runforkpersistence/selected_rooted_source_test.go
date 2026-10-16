package runforkpersistence

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
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	runstore "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	artifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil"
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
			for _, cell := range []struct {
				name, statement   string
				changed, original any
			}{
				{"child-root", `UPDATE runs SET source_root_directory=$1 WHERE run_id=$2`, fixture.parent.Admission().RootDirectory(), fixture.target.Admission().RootDirectory()},
				{"child-point", `UPDATE runs SET forked_from_revision=$1 WHERE run_id=$2`, int64(8), int64(7)},
				{"binding-root", `UPDATE run_fork_selected_contract_bindings SET source_root_directory=$1 WHERE fork_run_id=$2`, fixture.parent.Admission().RootDirectory(), fixture.target.Admission().RootDirectory()},
				{"binding-point", `UPDATE run_fork_selected_contract_bindings SET fork_revision=$1 WHERE fork_run_id=$2`, int64(8), int64(7)},
				{"missing-binding", `UPDATE run_fork_selected_contract_bindings SET fork_run_id=$1 WHERE binding_id=$2`, fixture.parentID, fixture.childID},
				{"missing-operation", `UPDATE run_fork_operations SET fork_run_id=$1 WHERE operation_id=$2`, fixture.parentID, fixture.childID},
				{"operation-point", `UPDATE run_fork_operations SET fork_revision=$1 WHERE operation_id=$2`, int64(8), int64(7)},
				{"missing-root", `UPDATE source_root_admissions SET root_directory=$1 WHERE root_directory=$2`, "retired-selected", fixture.target.Admission().RootDirectory()},
				{"corrupt-content", `UPDATE source_artifacts SET source_blob=$1 WHERE bundle_hash=$2`, []byte("not a source artifact"), fixture.target.Artifact().LogicalBlob()},
			} {
				t.Run(cell.name, func(t *testing.T) {
					id := fixture.childID
					if cell.name == "missing-binding" {
						id = fixture.bindingID
					} else if cell.name == "missing-operation" || cell.name == "operation-point" {
						id = fixture.operationID
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
		})
	}
}

// This fixture composes the native metadata owners on the real schema. It
// neither creates an executable selected grant nor claims activation coverage.
type selectedRootedSourceFixture struct {
	connector                                 *selectedRootReadConnector
	owner                                     any
	postgres                                  *postgresbackend.Backend
	sqlite                                    *sqlitebackend.Backend
	parent, target                            sourceartifact.RootedSource
	parentID, childID, bindingID, operationID string
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

func newSelectedRootedSourceFixture(t *testing.T, backend string) selectedRootedSourceFixture {
	t.Helper()
	f := selectedRootedSourceFixture{parentID: uuid.NewString(), childID: uuid.NewString(), operationID: uuid.NewString()}
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
	spec, err := contracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	plans, err := schemastore.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	ctx := authoractivity.WithScope(context.Background(), authoractivity.BundleScope(uuid.NewString(), f.parent.Artifact().BundleHash()))
	bootstrap := schemastore.SchemaBootstrapRequest{PlatformPlans: plans, Origin: schemastore.RuntimeStoreOrigin{SwarmVersion: "rooted-fork-source-proof", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()}}
	parentRequest := runlifecycle.CreateRequest{RunID: f.parentID, Origin: runlifecycle.DeploymentRunOrigin(), Source: sourceartifactfixture.FactFor(f.parent.Artifact()), RootAdmission: f.parent.Admission(), StartedAt: time.Now().UTC()}
	var write func(context.Context, func(context.Context, *mutationprotocol.Attempt) error) error
	if backend == "postgres" {
		dsn, _, cleanup := testutil.StartEmptyPostgres(t)
		t.Cleanup(cleanup)
		f.connector = &selectedRootReadConnector{driver: &pq.Driver{}, dsn: dsn}
		db := sql.OpenDB(f.connector)
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
		f.postgres, err = postgresbackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := schemastore.NewPostgres(f.postgres)
		if err != nil {
			t.Fatal(err)
		}
		if err := schema.BootstrapSchema(ctx, bootstrap); err != nil {
			t.Fatal(err)
		}
		runs, err := runstore.NewPostgres(f.postgres, schema.RequireCurrent, runhandoff.NewCandidateCoordinator())
		if err != nil {
			t.Fatal(err)
		}
		f.owner = &RunForkPostgresOwner{backend: f.postgres, requireCurrent: schema.RequireCurrent, RunLifecyclePostgresOwner: runs}
		write = func(ctx context.Context, body func(context.Context, *mutationprotocol.Attempt) error) error {
			result := mutationprotocol.RunPostgres(ctx, f.postgres, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
				return struct{}{}, body(ctx, attempt)
			})
			return result.Err()
		}
	} else {
		path := filepath.Join(t.TempDir(), "source.sqlite")
		f.connector = &selectedRootReadConnector{driver: &sqlite.Driver{}, dsn: "file:" + path + "?_pragma=" + url.QueryEscape("foreign_keys(ON)")}
		db := sql.OpenDB(f.connector)
		native, err := sqlitebackend.New(db)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := schemastore.NewSQLiteWithBackend(native, path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := schema.Close(); err != nil {
				t.Error(err)
			}
		})
		f.sqlite = native
		if err := schema.BootstrapSchema(ctx, bootstrap); err != nil {
			t.Fatal(err)
		}
		runs, err := runstore.NewSQLite(native, schema.RequireCurrent, runhandoff.NewCandidateCoordinator(), time.Now)
		if err != nil {
			t.Fatal(err)
		}
		f.owner = &RunForkSQLiteOwner{backend: native, requireCurrent: schema.RequireCurrent, RunLifecycleSQLiteOwner: runs}
		write = func(ctx context.Context, body func(context.Context, *mutationprotocol.Attempt) error) error {
			result := mutationprotocol.RunSQLite(ctx, native, "rooted fork source fixture", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
				return struct{}{}, body(ctx, attempt)
			})
			return result.Err()
		}
	}
	if err := write(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			for _, source := range []sourceartifact.RootedSource{f.parent, f.target} {
				var err error
				if f.postgres != nil {
					_, err = artifactstore.EnsurePostgresRootedSourceTx(ctx, tx, source, time.Now())
				} else {
					_, err = artifactstore.EnsureSQLiteRootedSourceTx(ctx, tx, source, time.Now())
				}
				if err != nil {
					return err
				}
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	switch owner := f.owner.(type) {
	case *RunForkPostgresOwner:
		if _, err := owner.RunLifecyclePostgresOwner.CreateRun(ctx, parentRequest); err != nil {
			t.Fatal(err)
		}
	case *RunForkSQLiteOwner:
		if _, err := owner.RunLifecycleSQLiteOwner.CreateRun(ctx, parentRequest); err != nil {
			t.Fatal(err)
		}
	}
	point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7}
	selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: f.target.Artifact().BundleHash()}
	if err := write(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		switch owner := f.owner.(type) {
		case *RunForkPostgresOwner:
			err = owner.InsertRunForkRunTx(ctx, attempt, f.childID, f.parentID, point, time.Now(), parentRequest.Source, f.target.Admission())
		case *RunForkSQLiteOwner:
			err = owner.InsertRunForkRunTx(ctx, attempt, f.childID, f.parentID, point, time.Now(), parentRequest.Source, f.target.Admission())
		}
		if err != nil {
			return err
		}
		return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			binding, err := insertRunForkSelectedContractBinding(ctx, tx, runfork.RunForkSelectedContractBindingRequest{ForkRunID: f.childID, SourceRunID: f.parentID, ForkPoint: point, ContractSelection: selection, TargetRootAdmission: f.target.Admission()}, time.Now())
			if err != nil {
				return err
			}
			f.bindingID = binding.BindingID
			_, _, err = bindForkOperationTx(ctx, tx, runfork.ForkOperationRequest{OperationID: f.operationID, Actor: "bearer:rooted-source-proof", TransportHash: "sha256:rooted-source-proof", SourceRunID: f.parentID, TargetBundleHash: f.target.Artifact().BundleHash(), TargetRootReference: f.target.Admission().Reference(), ContractSelection: selection, ResolvedPoint: &point}, f.childID, binding.BindingID, f.postgres != nil)
			return err
		})
	}); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f selectedRootedSourceFixture) changeEvidence(t *testing.T, statement string, value, id any) {
	t.Helper()
	var err error
	if f.postgres != nil {
		_, err = f.postgres.ExecContext(context.Background(), statement, value, id)
	} else {
		_, err = f.sqlite.ExecContext(context.Background(), statement, value, id)
	}
	if err != nil {
		t.Fatal(err)
	}
}
