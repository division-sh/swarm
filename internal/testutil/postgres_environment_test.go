package testutil

import (
	"bytes"
	"context"
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSharedPostgresStateRejectsBadDSNWithoutDockerFallback(t *testing.T) {
	state := &sharedPostgresState{}
	err := state.startLockedWithDSN("not-a-dsn")
	if err == nil {
		t.Fatal("expected invalid DSN failure")
	}
	message := err.Error()
	for _, want := range []string{
		"SWARM_TEST_POSTGRES_DSN is set but unusable",
		"Docker fallback is disabled",
		postgresTestSetupDoc,
		"parse postgres test DSN",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("error %q missing %q", message, want)
		}
	}
	if strings.Contains(message, "docker not found") || strings.Contains(message, "docker run") {
		t.Fatalf("invalid DSN reached Docker fallback: %q", message)
	}
}

func TestSharedPostgresStateReportsDockerFallbackOnce(t *testing.T) {
	var output bytes.Buffer
	state := &sharedPostgresState{reportWriter: &output}
	state.reportDockerFallback()
	state.reportDockerFallback()
	if got, want := output.String(), dockerPostgresFallbackLog+"\n"; got != want {
		t.Fatalf("fallback output = %q, want %q", got, want)
	}
}

func TestDockerPostgresRunArgsOwnDisposableSettings(t *testing.T) {
	args := dockerPostgresRunArgs("swarm-test")
	for _, pair := range [][]string{
		{"--tmpfs", "/var/lib/postgresql/data:rw"},
		{"-e", "POSTGRES_DB=postgres"},
		{"-c", "max_connections=300"},
		{"-c", "fsync=off"},
		{"-c", "synchronous_commit=off"},
		{"-c", "full_page_writes=off"},
	} {
		if !containsAdjacentStrings(args, pair[0], pair[1]) {
			t.Fatalf("docker args %q missing adjacent %q", args, pair)
		}
	}
}

func TestInspectTestPostgresSessionRejectsGSSBeforeLifecycleMutation(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`(?s)SELECT current_user,.*pg_catalog\.pg_stat_gssapi`).
		WillReturnRows(sqlmock.NewRows([]string{"current_user", "gss_authenticated"}).AddRow("tester", true))

	_, err = inspectTestPostgresSession(db)
	if err == nil || !strings.Contains(err.Error(), "used GSS authentication") {
		t.Fatalf("GSS error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet: %v", err)
	}
}

func TestInspectTestPostgresSessionReturnsServerRole(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(`(?s)SELECT current_user,.*pg_catalog\.pg_stat_gssapi`).
		WillReturnRows(sqlmock.NewRows([]string{"current_user", "gss_authenticated"}).AddRow("swarm_test", false))

	role, err := inspectTestPostgresSession(db)
	if err != nil {
		t.Fatalf("inspectTestPostgresSession: %v", err)
	}
	if role != "swarm_test" {
		t.Fatalf("role = %q, want swarm_test", role)
	}
}

func TestEnsurePublicSchemaGrantsAuthenticatedRole(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, statement := range []string{
		`CREATE SCHEMA IF NOT EXISTS public`,
		`GRANT ALL ON SCHEMA public TO "role-with-dash"`,
		`GRANT ALL ON SCHEMA public TO public`,
	} {
		mock.ExpectExec(regexp.QuoteMeta(statement)).WillReturnResult(sqlmock.NewResult(0, 0))
	}
	if err := ensurePublicSchema(context.Background(), db, "role-with-dash"); err != nil {
		t.Fatalf("ensurePublicSchema: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("ExpectationsWereMet: %v", err)
	}
}

func TestDockerSetupDiagnosticPreservesCause(t *testing.T) {
	cause := errors.New("dial unix /var/run/docker.sock: no such file")
	err := dockerPostgresSetupError(cause)
	if !strings.Contains(err.Error(), postgresTestSetupDoc) {
		t.Fatalf("Docker failure missing setup doc: %v", err)
	}
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), cause.Error()) {
		t.Fatalf("Docker failure lost technical cause: %v", err)
	}
}

func containsAdjacentStrings(values []string, first, second string) bool {
	for i := 0; i+1 < len(values); i++ {
		if slices.Equal(values[i:i+2], []string{first, second}) {
			return true
		}
	}
	return false
}
