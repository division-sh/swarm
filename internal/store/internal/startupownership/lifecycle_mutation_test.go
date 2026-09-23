package startupownership

import (
	"context"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	storeagent "github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

type lifecycleTestAgentSource struct{}

func (lifecycleTestAgentSource) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return nil, nil
}

func TestSQLiteSessionLifecycleTransitionStartsStoryBeforeDomainWrite(t *testing.T) {
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
	mock.ExpectBegin()
	mock.ExpectExec("INSERT OR IGNORE INTO author_activity_order").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE author_activity_order").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT last_sequence FROM author_activity_order").WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(0))
	mock.ExpectRollback()

	session := &sqliteSession{owner: &StartupSQLiteOwner{backend: backend, agents: agents}}
	result, err := session.CommitAgentLifecycleTransition(context.Background(), runtimemanager.AgentLifecycleTransition{})
	if err == nil {
		t.Fatal("invalid lifecycle transition unexpectedly succeeded")
	}
	if !reflect.DeepEqual(result, runtimemanager.AgentLifecycleTransitionResult{}) {
		t.Fatalf("unacknowledged lifecycle result exposed: %+v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
