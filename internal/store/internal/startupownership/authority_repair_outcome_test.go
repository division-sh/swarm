package startupownership

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

type repairArgument func(driver.Value) bool

func (f repairArgument) Match(value driver.Value) bool { return f(value) }

type repairReleasePossession struct {
	release func() error
}

func (*repairReleasePossession) ProveCurrent(context.Context) error { return nil }
func (p *repairReleasePossession) Release() error                   { return p.release() }

// Exercise the real repair owner and fresh repair callback, injecting only at
// driver settlement and possession release. Callback success is not COMMIT proof.
func TestRepairAuthorityOutcomeAndJoinedRelease(t *testing.T) {
	for _, backendName := range []string{"postgres", "sqlite"} {
		for _, scenario := range []string{"ack", "ack_release_failure", "commit_failure", "callback_and_release_failure"} {
			t.Run(backendName+"/"+scenario, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				primary := errors.New("repair settlement failed")
				cleanup := errors.New("repair possession release failed")
				failRelease := strings.Contains(scenario, "release_failure")
				releaseEntered, releaseContinue := make(chan struct{}), make(chan struct{})
				t.Cleanup(func() { close(releaseContinue) })
				release := func() error {
					close(releaseEntered)
					<-releaseContinue
					return cleanup
				}
				var repair func(context.Context, runtimestartupownership.AuthorityRepairRequest) (runtimestartupownership.AuthorityRepairResult, error)
				if backendName == "postgres" {
					backend, err := postgresbackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					repair = (&StartupPostgresOwner{backend: backend, schemaGuard: func() error { return nil }}).RepairAuthority
					mock.ExpectQuery("SELECT pg_try_advisory_lock").WithArgs(runtimeSharedStoreOwnershipLock).
						WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(true))
				} else {
					backend, err := sqlitebackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					possession := &repairReleasePossession{release: func() error { return nil }}
					if failRelease {
						possession.release = release
					}
					repair = (&StartupSQLiteOwner{
						backend: backend, path: "injected-construction-possession", schemaGuard: func() error { return nil },
						backendIdentity: &SQLiteBackendIdentity{initial: possession},
					}).RepairAuthority
				}
				record := authorityHeadRecord{
					AuthorityID: uuid.NewString(), AuthorityGeneration: 1, TransitionOrdinal: 1, StateVersion: 1,
					State: "active", OwnerID: "corrupt-owner", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
					Backend: backendName, AcquisitionID: uuid.NewString(), AcquisitionRequestHash: strings.Repeat("0", 64),
					AcquisitionKind: "cold", Snapshot: json.RawMessage(`{}`), CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
				}
				digest, err := canonicaljson.Hash(record)
				if err != nil {
					t.Fatal(err)
				}
				req := runtimestartupownership.AuthorityRepairRequest{OperationID: uuid.NewString(), FindingsDigest: digest, Confirmed: true}
				mock.ExpectBegin()
				expectFence := func() {
					if backendName == "postgres" {
						mock.ExpectExec("INSERT INTO author_activity_order").WillReturnResult(sqlmock.NewResult(0, 1))
						mock.ExpectQuery("SELECT last_sequence FROM author_activity_order").WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(0))
					} else {
						mock.ExpectExec("INSERT OR IGNORE INTO author_activity_order").WillReturnResult(sqlmock.NewResult(0, 1))
						mock.ExpectExec("UPDATE author_activity_order").WillReturnResult(sqlmock.NewResult(0, 1))
						mock.ExpectQuery("SELECT last_sequence FROM author_activity_order").WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(0))
					}
				}
				expectFence()
				mock.ExpectQuery("SELECT request_hash,result FROM runtime_startup_authority_repairs").
					WithArgs(req.OperationID).WillReturnRows(sqlmock.NewRows([]string{"request_hash", "result"}))
				mock.ExpectQuery("SELECT .* FROM runtime_startup_authority_facts ORDER BY").WillReturnRows(sqlmock.NewRows([]string{
					"authority_id", "authority_generation", "transition_ordinal", "state_version", "state", "owner_id", "boot_id",
					"runtime_instance_id", "backend", "acquisition_id", "acquisition_request_hash", "acquisition_kind",
					"predecessor_authority_id", "successor_authority_id", "snapshot", "created_at",
				}).AddRow(record.AuthorityID, 1, 1, 1, record.State, record.OwnerID, record.BootID, record.RuntimeInstanceID,
					record.Backend, record.AcquisitionID, record.AcquisitionRequestHash, record.AcquisitionKind, nil, nil, "{}", record.CreatedAt))
				expectFence()
				mock.ExpectQuery("SELECT g.snapshot FROM runtime_generation_grants").WillReturnRows(sqlmock.NewRows([]string{"snapshot"}))
				previous := sqlmock.NewRows([]string{"snapshot"})
				args := make([]driver.Value, 17)
				for i := range args {
					args[i] = sqlmock.AnyArg()
				}
				args[15] = repairArgument(func(v driver.Value) bool { previous.AddRow(v); return true })
				expectFence()
				mock.ExpectExec("INSERT INTO runtime_startup_authority_facts").WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, 1))
				expectFence()
				mock.ExpectQuery("SELECT snapshot FROM runtime_startup_authority_facts").WillReturnRows(previous)
				mock.ExpectExec("INSERT INTO runtime_startup_authority_facts").WillReturnResult(sqlmock.NewResult(0, 1))
				insert := mock.ExpectExec("INSERT INTO runtime_startup_authority_repairs")
				if scenario == "callback_and_release_failure" {
					insert.WillReturnError(primary)
					mock.ExpectRollback()
				} else {
					insert.WillReturnResult(sqlmock.NewResult(0, 1))
					commit := mock.ExpectCommit()
					if scenario == "commit_failure" {
						commit.WillReturnError(primary)
					}
				}
				if backendName == "postgres" && scenario != "commit_failure" {
					unlock := mock.ExpectQuery("SELECT pg_advisory_unlock")
					if failRelease {
						unlock.WithArgs(repairArgument(func(v driver.Value) bool {
							_ = release()
							return v == runtimeSharedStoreOwnershipLock
						})).WillReturnError(cleanup)
					} else {
						unlock.WillReturnRows(sqlmock.NewRows([]string{"unlocked"}).AddRow(true))
					}
				}
				if backendName == "postgres" && (failRelease || scenario == "commit_failure") || backendName == "sqlite" && scenario == "commit_failure" {
					mock.ExpectClose()
				}
				type outcome struct {
					result runtimestartupownership.AuthorityRepairResult
					err    error
				}
				done := make(chan outcome, 1)
				go func() {
					result, err := repair(context.Background(), req)
					done <- outcome{result, err}
				}()
				if failRelease {
					select {
					case <-releaseEntered:
					case got := <-done:
						t.Fatalf("returned before release: %+v", got)
					case <-time.After(3 * time.Second):
						t.Fatal("release was not attempted")
					}
					select {
					case got := <-done:
						t.Fatalf("returned without joining release: %+v", got)
					default:
					}
					releaseContinue <- struct{}{}
				}
				select {
				case got := <-done:
					ack := scenario == "ack" || scenario == "ack_release_failure"
					if ack {
						if err := got.result.Validate(); err != nil || got.result.OperationID != req.OperationID || got.result.FindingsDigest != digest || got.result.AuthorityGeneration != 2 {
							t.Errorf("acknowledged repair lost: %+v (%v)", got.result, err)
						}
					} else if got.result != (runtimestartupownership.AuthorityRepairResult{}) {
						t.Errorf("unacknowledged repair exposed: %+v", got.result)
					}
					if !ack && !errors.Is(got.err, primary) {
						t.Errorf("lost primary error: %v", got.err)
					}
					if failRelease && !errors.Is(got.err, cleanup) {
						t.Errorf("lost release error: %v", got.err)
					}
					if scenario == "ack" && got.err != nil {
						t.Errorf("healthy repair failed: %v", got.err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("repair did not join")
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Error(err)
				}
			})
		}
	}
}
