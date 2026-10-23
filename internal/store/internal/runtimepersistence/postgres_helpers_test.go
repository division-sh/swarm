package runtimepersistence

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"

	"github.com/lib/pq"
)

func TestDSNFromConfigQuotesKeywordValues(t *testing.T) {
	cfg := config.DatabaseConfig{
		Host:    "127.0.0.1",
		Port:    5432,
		Name:    "swarm db",
		User:    "swarm user",
		SSLMode: "disable",
	}
	password := `has space 'quote' \slash user=other`

	dsn := DSNFromConfig(cfg, password)
	for _, want := range []string{
		`host='127.0.0.1'`,
		`dbname='swarm db'`,
		`sslmode='disable'`,
		`user='swarm user'`,
		`password='has space \'quote\' \\slash user=other'`,
	} {
		if !strings.Contains(dsn, want) {
			t.Fatalf("DSNFromConfig() = %q, want substring %q", dsn, want)
		}
	}
	if _, err := pq.NewConnector(dsn); err != nil {
		t.Fatalf("pq.NewConnector(%q): %v", dsn, err)
	}
}

func TestDSNFromConfigPinsDefaultNonSecretKeywordsAgainstPGEnv(t *testing.T) {
	t.Setenv("PGHOST", "env-host")
	t.Setenv("PGPORT", "15432")
	t.Setenv("PGDATABASE", "env-db")
	t.Setenv("PGUSER", "env-user")
	t.Setenv("PGSSLMODE", "require")

	dsn := DSNFromConfig(config.DatabaseConfig{}, "secret")
	for _, want := range []string{
		`host='127.0.0.1'`,
		"port=5432",
		`dbname='swarm'`,
		`sslmode='disable'`,
		`user='postgres'`,
	} {
		if !strings.Contains(dsn, want) {
			t.Fatalf("DSNFromConfig() = %q, want pinned default keyword %q", dsn, want)
		}
	}
	for _, notWant := range []string{"env-host", "15432", "env-db", "env-user", "require"} {
		if strings.Contains(dsn, notWant) {
			t.Fatalf("DSNFromConfig() = %q, leaked PG env value %q", dsn, notWant)
		}
	}
	if _, err := pq.NewConnector(dsn); err != nil {
		t.Fatalf("pq.NewConnector(%q): %v", dsn, err)
	}
}
