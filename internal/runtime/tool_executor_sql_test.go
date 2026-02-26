package runtime

import (
	"context"
	"testing"

	"empireai/internal/models"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestRuntimeToolExecutor_SQLExecute_ReadOnlySelect(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	bus := NewEventBus(InMemoryEventStore{})
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	exec.SetSQLDB(db)

	verticalID := "11111111-1111-1111-1111-111111111111"
	ctx := WithActor(context.Background(), models.AgentConfig{
		ID:         "agent-1",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: verticalID,
	})

	// SELECT path uses derived schema from slug when available.
	mock.ExpectQuery("SELECT COALESCE\\(NULLIF\\(slug, ''\\), ''\\)\\s+FROM verticals").
		WithArgs(verticalID).
		WillReturnRows(sqlmock.NewRows([]string{"slug"}).AddRow("Acme"))
	mock.ExpectBegin()
	mock.ExpectExec("SET LOCAL search_path = \"acme_schema\"").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SET TRANSACTION READ ONLY").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SET LOCAL statement_timeout = '15s'").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("select 1 as x LIMIT 200").
		WillReturnRows(sqlmock.NewRows([]string{"x"}).AddRow([]byte("1")))
	mock.ExpectCommit()

	out, err := exec.Execute(ctx, "sql_execute", map[string]any{
		"query": "select 1 as x",
	})
	if err != nil {
		t.Fatalf("sql_execute select: %v", err)
	}
	m, _ := out.(map[string]any)
	rows, _ := m["rows"].([]map[string]any)
	if len(rows) != 1 || rows[0]["x"] != "1" {
		t.Fatalf("unexpected select rows: %#v", out)
	}
	if m["schema"] != "acme_schema" {
		t.Fatalf("expected schema acme_schema, got %#v", m["schema"])
	}
	if m["read_only"] != true {
		t.Fatalf("expected read_only=true, got %#v", m["read_only"])
	}

	// Non-select statements must be rejected.
	out, err = exec.Execute(ctx, "sql_execute", map[string]any{
		"query": "update t set a=1",
	})
	if err == nil {
		t.Fatalf("expected non-select query rejection, got out=%#v", out)
	}

	// Small direct helper coverage.
	_ = exec.ToolDefinitions()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestSanitizeSQLReadQuery_Guards(t *testing.T) {
	t.Run("appends default limit", func(t *testing.T) {
		q, err := sanitizeSQLReadQuery("select id from t")
		if err != nil {
			t.Fatalf("sanitize query: %v", err)
		}
		if q != "select id from t LIMIT 200" {
			t.Fatalf("unexpected normalized query: %q", q)
		}
	})

	t.Run("rejects non-select", func(t *testing.T) {
		if _, err := sanitizeSQLReadQuery("delete from t"); err == nil {
			t.Fatal("expected non-select rejection")
		}
	})

	t.Run("rejects schema qualified from clause", func(t *testing.T) {
		if _, err := sanitizeSQLReadQuery("select id from public.orders limit 10"); err == nil {
			t.Fatal("expected restricted schema rejection")
		}
	})

	t.Run("rejects quoted restricted schema", func(t *testing.T) {
		if _, err := sanitizeSQLReadQuery(`select id from "public".orders limit 10`); err == nil {
			t.Fatal("expected quoted restricted schema rejection")
		}
	})

	t.Run("rejects schema qualification with quoted identifier", func(t *testing.T) {
		if _, err := sanitizeSQLReadQuery(`select id from "tenant".orders`); err == nil {
			t.Fatal("expected schema-qualified quoted identifier rejection")
		}
	})

	t.Run("rejects schema qualification with spaced dot", func(t *testing.T) {
		if _, err := sanitizeSQLReadQuery(`select id from "tenant"   . orders`); err == nil {
			t.Fatal("expected schema-qualified reference rejection")
		}
	})

	t.Run("rejects oversized limit", func(t *testing.T) {
		if _, err := sanitizeSQLReadQuery("select id from t limit 9999"); err == nil {
			t.Fatal("expected limit rejection")
		}
	})
}
