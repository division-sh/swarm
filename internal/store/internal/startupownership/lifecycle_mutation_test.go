package startupownership

import (
	"context"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type lifecycleTestAgentSource struct{}

func (lifecycleTestAgentSource) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return nil, nil
}

func TestSQLiteSessionLifecycleTransitionStartsStoryBeforeDomainWrite(t *testing.T) {
	originalSession, _ := newSQLiteProofSession(t, 1)
	capability, err := runtimeownership.NewProcessCapability(originalSession)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	ctx, err := runtimeownership.WithProcessMutationAuthority(context.Background(), capability)
	if err != nil {
		t.Fatal(err)
	}
	original, err := runtimeownership.CaptureMutationAuthority(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stamp := original.ProcessAuthority().LeaseStamp()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	agents, err := storeagent.NewSQLite(backend, func() error { return nil }, lifecycleTestAgentSource{})
	if err != nil {
		t.Fatal(err)
	}
	// Statement/order model only: the separate native fixture retains possession.
	// These exact stamp projections do not authorize a second native store.
	mock.ExpectBegin()
	mock.ExpectExec("INSERT OR IGNORE INTO author_activity_order").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE author_activity_order").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT last_sequence FROM author_activity_order").WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(0))
	mock.ExpectQuery("SELECT generation, CAST\\(lease_id AS TEXT\\), owner_id").WillReturnRows(
		sqlmock.NewRows([]string{"generation", "lease_id", "owner_id", "boot_id", "runtime_instance_id", "backend"}).AddRow(
			int64(stamp.Generation), stamp.LeaseID, stamp.OwnerID, stamp.BootID, stamp.RuntimeInstanceID, stamp.Backend,
		),
	)
	mock.ExpectRollback()

	session := &sqliteSession{owner: &StartupSQLiteOwner{backend: backend, agents: agents}}
	result, err := session.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{})
	if err == nil || err.Error() != "agent identity run_id is required" {
		t.Fatalf("invalid lifecycle transition did not reach its domain refusal: %v", err)
	}
	if !reflect.DeepEqual(result, runtimemanager.AgentLifecycleTransitionResult{}) {
		t.Fatalf("unacknowledged lifecycle result exposed: %+v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
