package runforkpersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// Forward real SQL while observing admission and injecting a missing COMMIT
// acknowledgement. Durable state is checked through a separate connection.
type selectedWriterConnector struct {
	driver.Connector
	begins, commits, rollbacks int
	contextWrong               bool
	isolation                  driver.IsolationLevel
	afterExec                  func(string)
	afterCommit                func()
	commitError                error
}

func (p *selectedWriterConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c, err := p.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &selectedWriterConn{Conn: c, probe: p}, nil
}

type selectedWriterConn struct {
	driver.Conn
	probe *selectedWriterConnector
}

func (c *selectedWriterConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.probe.contextWrong = c.probe.contextWrong || ctx.Done() != nil
	result, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	if c.probe.afterExec != nil {
		c.probe.afterExec(query)
	}
	return result, err
}

func (c *selectedWriterConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.probe.contextWrong = c.probe.contextWrong || ctx.Done() != nil
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *selectedWriterConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}

func (c *selectedWriterConn) IsValid() bool { return c.Conn.(driver.Validator).IsValid() }

func (c *selectedWriterConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.probe.begins++
	c.probe.contextWrong = c.probe.contextWrong || ctx.Done() != nil
	c.probe.isolation = opts.Isolation
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &selectedWriterTx{Tx: tx, probe: c.probe}, nil
}

type selectedWriterTx struct {
	driver.Tx
	probe *selectedWriterConnector
}

func (tx *selectedWriterTx) Commit() error {
	tx.probe.commits++
	err := tx.Tx.Commit()
	if tx.probe.afterCommit != nil {
		tx.probe.afterCommit()
	}
	return errors.Join(err, tx.probe.commitError)
}

func (tx *selectedWriterTx) Rollback() error {
	tx.probe.rollbacks++
	return tx.Tx.Rollback()
}

func selectedWriterProbe(t *testing.T) (*RunForkPostgresOwner, *selectedWriterConnector, *sql.DB) {
	t.Helper()
	dsn, observer, _ := testutil.StartPostgres(t)
	return selectedWriterProbeDatabase(t, dsn, observer)
}

func selectedWriterProbeDatabase(t *testing.T, dsn string, observer *sql.DB) (*RunForkPostgresOwner, *selectedWriterConnector, *sql.DB) {
	t.Helper()
	connector, err := pq.NewConnector(dsn)
	if err != nil {
		t.Fatal(err)
	}
	probe := &selectedWriterConnector{Connector: connector}
	db := sql.OpenDB(probe)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	return &RunForkPostgresOwner{backend: backend, requireCurrent: func() error { return nil }}, probe, observer
}

type selectedClaimCleanupProbe struct {
	*postgresbackend.Backend
	cleanup error
}

func (p selectedClaimCleanupProbe) RunTransactionOutcome(ctx context.Context, fn func(context.Context, *sql.Tx) error) (bool, error) {
	committed, err := p.Backend.RunTransactionOutcome(ctx, fn)
	return committed, errors.Join(err, p.cleanup)
}

func TestSelectedForkClaimPreservesCommittedEvidence(t *testing.T) {
	for _, cut := range []string{"success", "before_begin", "before_commit", "commit_uncertain", "committed_cleanup_error"} {
		t.Run(cut, func(t *testing.T) {
			dsn, db, _ := testutil.StartEmptyPostgres(t)
			s, probe, _ := selectedWriterProbeDatabase(t, dsn, db)
			// Minimal relational fixture; the existing selected-store matrix covers
			// schema/admission parity. Here the real claim SQL and COMMIT are used.
			if _, err := db.Exec(`
				CREATE TABLE source_artifacts (bundle_hash text PRIMARY KEY);
				CREATE TABLE runs (run_id uuid PRIMARY KEY, status text, bundle_hash text,
					origin_kind text, trigger_event_id uuid, trigger_event_type text,
					origin_service_id uuid, origin_generation bigint,
					forked_from_run_id uuid, forked_from_event_id uuid, started_at timestamptz);
				CREATE TABLE run_fork_selected_contract_runtime_executions (
					execution_id uuid PRIMARY KEY, fork_run_id uuid, generation bigint,
					state text, execution_owner text, lease_expires_at timestamptz, updated_at timestamptz,
					admission_fingerprint text, container_plan_fingerprint text,
					actor_census_fingerprint text, effective_config_fingerprint text,
					preparation_binding jsonb, preparation_fingerprint text,
					declaration_plan_fingerprint text, executable_coordinate_fingerprint text
				);
				CREATE TABLE runtime_startup_authority_facts (
					authority_id uuid, authority_generation bigint, transition_ordinal bigint,
					state_version bigint, state text, owner_id text, boot_id uuid,
					runtime_instance_id uuid, backend text, acquisition_id uuid,
					acquisition_request_hash text, acquisition_kind text,
					predecessor_authority_id uuid, successor_authority_id uuid,
					snapshot jsonb, created_at timestamptz
				)`); err != nil {
				t.Fatal(err)
			}
			hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
			issued := runfork.SelectedContractRuntimeExecution{
				ExecutionID: uuid.NewString(), ForkRunID: uuid.NewString(), Generation: 1,
				SourceRunID: uuid.NewString(), ForkEventID: uuid.NewString(),
				ExecutionOwner: "issuer", LeaseExpiresAt: time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond),
				AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container",
				ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config",
				ExecutionMode: executionmode.Live, FenceGeneration: 1,
			}
			if _, err := db.Exec(`INSERT INTO source_artifacts VALUES ($1)`, hash); err != nil {
				t.Fatal(err)
			}
			source, err := correlation.NewSourceArtifactFact(hash)
			if err != nil {
				t.Fatal(err)
			}
			origin, err := runlifecycle.ForkMaterializationRunOrigin(issued.SourceRunID, issued.ForkEventID)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := runlifecyclefixture.PostgresCreateRunInMutation(context.Background(), tx, runlifecycle.CreateRequest{
				RunID: issued.ForkRunID, Source: source, Origin: origin, StartedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			preparation := selectedClaimPreparationFixture(t, db, issued, hash)
			issued.PreparationFingerprint, err = preparation.Fingerprint()
			if err != nil {
				t.Fatal(err)
			}
			issued.DeclarationPlanFingerprint = preparation.DeclarationPlanFingerprint
			issued.ExecutableCoordinateFingerprint, err = issued.CoordinateFingerprint()
			if err != nil {
				t.Fatal(err)
			}
			preparationJSON, err := json.Marshal(preparation)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO run_fork_selected_contract_runtime_executions VALUES ($1,$2,1,'prepared','issuer',$3,NULL,'admission','container','actors','config',$4,$5,$6,$7)`, issued.ExecutionID, issued.ForkRunID, issued.LeaseExpiresAt, string(preparationJSON), issued.PreparationFingerprint, issued.DeclarationPlanFingerprint, issued.ExecutableCoordinateFingerprint); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			primary := errors.New("selected claim exit failure")
			runner := selectedClaimCleanupProbe{Backend: s.backend}
			if cut == "before_begin" {
				cancel()
			}
			if cut == "before_commit" {
				probe.afterExec = func(query string) {
					if strings.Contains(query, "UPDATE run_fork_selected_contract_runtime_executions") {
						cancel()
					}
				}
			}
			if cut == "commit_uncertain" {
				probe.commitError = primary
			}
			if cut == "committed_cleanup_error" {
				// Fault at the existing outcome/result boundary, after real COMMIT.
				runner.cleanup = primary
			}
			authority, err := claimSelectedContractRuntimeExecutionPostgres(ctx, runner, issued, "consumer", time.Minute)
			wantEvidence := cut == "success" || cut == "committed_cleanup_error"
			if (authority.ID == issued.ExecutionID) != wantEvidence {
				t.Fatalf("authority ID=%s error=%v", authority.ID, err)
			}
			if (cut == "before_begin" || cut == "before_commit") && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if (cut == "commit_uncertain" || cut == "committed_cleanup_error") && !errors.Is(err, primary) {
				t.Fatalf("lost exit error: %v", err)
			}
			if cut == "success" && err != nil {
				t.Fatal(err)
			}
			var state string
			if err := db.QueryRow(`SELECT state FROM run_fork_selected_contract_runtime_executions`).Scan(&state); err != nil {
				t.Fatal(err)
			}
			wantState := "running"
			if cut == "before_begin" || cut == "before_commit" {
				wantState = "prepared"
			}
			if state != wantState || probe.commits > 1 || probe.contextWrong {
				t.Fatalf("state=%s want=%s commits=%d contextWrong=%t", state, wantState, probe.commits, probe.contextWrong)
			}
		})
	}
}

func selectedClaimPreparationFixture(t *testing.T, db *sql.DB, issued runfork.SelectedContractRuntimeExecution, hash string) runfork.SelectedForkPreparationBinding {
	t.Helper()
	authority, err := startupownership.NewColdAuthority(startupownership.AcquireRequest{
		OwnerID: "selected-claim-proof", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	}, "postgres_retained_session")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO runtime_startup_authority_facts VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULL,NULL,$13,$14)`,
		authority.AuthorityID, authority.AuthorityGeneration, authority.TransitionOrdinal, authority.StateVersion,
		authority.State, authority.OwnerID, authority.BootID, authority.RuntimeInstanceID, authority.Backend,
		authority.AcquisitionID, authority.AcquisitionRequestHash, authority.AcquisitionKind, string(raw), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	fingerprint := strings.Repeat("a", 64)
	return runfork.SelectedForkPreparationBinding{
		ForkRunID: issued.ForkRunID,
		SelectedForkPreparation: runfork.SelectedForkPreparation{
			PreparationID: uuid.NewString(), ProcessGeneration: authority.AuthorityGeneration,
			SourceRunID: issued.SourceRunID, ForkEventID: issued.ForkEventID,
			DeclarationPlanFingerprint: "sha256:" + fingerprint,
			Actors:                     []runfork.SelectedForkPreparedActor{},
			Coordinates: managedcapabilities.SelectedForkPreparationCoordinates{
				ProcessAuthorityID: authority.AuthorityID, ProcessOwnerID: authority.OwnerID,
				ProcessBootID: authority.BootID, BundleHash: hash,
				SourceFingerprint: fingerprint, AdmittedPlanFingerprint: fingerprint,
				ConfigurationFingerprint: fingerprint, CatalogFingerprint: fingerprint,
			},
		},
	}
}

func TestSelectedForkWriterPortsSettlement(t *testing.T) {
	for _, family := range []string{"activation", "materialization", "discard"} {
		for _, cut := range []string{"success", "before_begin", "during_sql", "callback_error", "panic", "commit_uncertain", "after_commit"} {
			t.Run(family+"/"+cut, func(t *testing.T) {
				s, probe, observer := selectedWriterProbe(t)
				if _, err := observer.Exec(`CREATE TABLE selected_writer_probe (n integer)`); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				primary := errors.New("selected writer independent failure")
				if cut == "before_begin" {
					cancel()
				}
				if cut == "during_sql" {
					probe.afterExec = func(query string) {
						if strings.Contains(query, "INSERT INTO selected_writer_probe") {
							cancel()
						}
					}
				}
				if cut == "after_commit" {
					probe.afterCommit = cancel
				}
				if cut == "commit_uncertain" {
					probe.commitError = primary
				}
				run := postgresRunForkSelectedContractActivationPort(s).runMutation
				wantIsolation := sql.LevelReadCommitted
				if family == "materialization" {
					run = postgresRunForkSelectedContractMaterializationPort(s).runMutation
				}
				if family == "discard" {
					wantIsolation = sql.LevelSerializable
					run = func(ctx context.Context, fn func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects) error) (bool, error) {
						err := postgresRunForkSelectedContractDiscardPort(s).runMutation(ctx, fn)
						return err == nil, err
					}
				}
				calls := 0
				var committed bool
				var err error
				var panicked any
				func() {
					defer func() { panicked = recover() }()
					committed, err = run(ctx, func(sqlCtx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation, _ *runforkrevision.Effects) error {
						calls++
						if _, err := tx.ExecContext(sqlCtx, `INSERT INTO selected_writer_probe VALUES (1)`); err != nil {
							return err
						}
						// This query must finish even when the preceding DML cancels
						// the caller; COMMIT admission still refuses that cancellation.
						var n int
						if err := tx.QueryRowContext(sqlCtx, `SELECT count(*) FROM selected_writer_probe`).Scan(&n); err != nil || n != 1 {
							t.Fatalf("owned result drain: rows=%d error=%v", n, err)
						}
						if cut == "callback_error" {
							cancel()
							return primary
						}
						if cut == "panic" {
							panic(primary)
						}
						return nil
					})
				}()
				wantCommitted := cut == "success" || cut == "after_commit"
				if committed != wantCommitted {
					t.Fatalf("committed=%t error=%v", committed, err)
				}
				if (cut == "before_begin" || cut == "during_sql" || cut == "callback_error") && !errors.Is(err, context.Canceled) {
					t.Fatalf("lost caller refusal: %v", err)
				}
				if (cut == "callback_error" || cut == "commit_uncertain") && !errors.Is(err, primary) {
					t.Fatalf("lost independent error: %v", err)
				}
				if cut == "panic" && panicked != primary || cut != "panic" && panicked != nil {
					t.Fatalf("panic identity: %v", panicked)
				}
				if wantCommitted && err != nil {
					t.Fatal(err)
				}
				wantCalls, wantCommits, wantRollbacks := 1, 0, 1
				if cut == "before_begin" {
					wantCalls, wantRollbacks = 0, 0
				} else if wantCommitted || cut == "commit_uncertain" {
					wantCommits, wantRollbacks = 1, 0
				}
				if calls != wantCalls || probe.begins != wantCalls || probe.commits != wantCommits || probe.rollbacks != wantRollbacks || probe.contextWrong {
					t.Fatalf("calls=%d begin=%d commit=%d rollback=%d contextWrong=%t", calls, probe.begins, probe.commits, probe.rollbacks, probe.contextWrong)
				}
				if wantCalls > 0 && probe.isolation != driver.IsolationLevel(wantIsolation) {
					t.Fatalf("isolation=%v want=%v", probe.isolation, wantIsolation)
				}
				var durable int
				if err := observer.QueryRow(`SELECT count(*) FROM selected_writer_probe`).Scan(&durable); err != nil {
					t.Fatal(err)
				}
				if durable != wantCommits {
					t.Fatalf("durable=%d want=%d", durable, wantCommits)
				}
			})
		}
	}
}

func TestSelectedForkActivationProjectsAcknowledgedOutcome(t *testing.T) {
	for _, committed := range []bool{false, true} {
		name := "uncertain"
		if committed {
			name = "committed_cleanup_error"
		}
		t.Run(name, func(t *testing.T) {
			primary := errors.New("activation settlement error")
			s := &RunForkPostgresOwner{requireCurrent: func() error { return nil }}
			port := postgresRunForkSelectedContractActivationPort(s)
			// Isolate the typed projection from SQL. The port settlement matrix
			// above separately proves the real transaction's acknowledged outcome.
			port.runMutation = func(context.Context, func(context.Context, *sql.Tx, *privateauthoractivity.Mutation, *runforkrevision.Effects) error) (bool, error) {
				return committed, primary
			}
			result, err := activateRunForkForSelectedContractExecution(context.Background(), runfork.RunForkSelectedContractExecutionActivateRequest{ForkRunID: uuid.NewString()}, port)
			if !errors.Is(err, primary) || result.Activated != committed || result.SourceFrozen != committed {
				t.Fatalf("activated=%t sourceFrozen=%t committed=%t error=%v", result.Activated, result.SourceFrozen, committed, err)
			}
			if committed && (result.ForkRunStatus != runfork.RunForkActivatedStatus || result.SourceRunStatus != runfork.RunForkSourceFrozenStatus) {
				t.Fatalf("committed statuses: %+v", result)
			}
		})
	}
}
