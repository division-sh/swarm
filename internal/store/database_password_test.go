package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
)

type mapCredentialStore map[string]string

func (m mapCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	value, ok := m[strings.TrimSpace(key)]
	return value, ok, nil
}

func (mapCredentialStore) Set(context.Context, string, string) error {
	return errors.New("read-only test store")
}

func (mapCredentialStore) List(context.Context) ([]string, error) {
	return nil, nil
}

func (mapCredentialStore) Delete(context.Context, string) error {
	return errors.New("read-only test store")
}

func TestResolveDatabasePasswordRejectsImplicitEnvFallbacks(t *testing.T) {
	resolver := DatabasePasswordResolver{
		LookupEnv: func(key string) (string, bool) {
			switch key {
			case "SWARM_DB_PASSWORD":
				return "swarm-env-password", true
			case "PGPASSWORD":
				return "pg-env-password", true
			default:
				return "", false
			}
		},
	}

	_, err := resolver.Resolve(context.Background(), config.DatabaseConfig{})
	if err == nil || !strings.Contains(err.Error(), "postgres store requires exactly one database password source") {
		t.Fatalf("Resolve error = %v, want fail-closed missing source guidance", err)
	}
}

func TestResolveDatabasePasswordReadsOnlyDeclaredEnv(t *testing.T) {
	resolver := DatabasePasswordResolver{
		LookupEnv: func(key string) (string, bool) {
			values := map[string]string{
				"DB_PASSWORD":       "declared-password",
				"SWARM_DB_PASSWORD": "swarm-env-password",
				"PGPASSWORD":        "pg-env-password",
			}
			value, ok := values[key]
			return value, ok
		},
	}

	got, err := resolver.Resolve(context.Background(), config.DatabaseConfig{PasswordEnv: "DB_PASSWORD"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != "declared-password" {
		t.Fatalf("password = %q, want declared env value", got)
	}
}

func TestResolveDatabasePasswordAllowsExplicitLegacyEnvName(t *testing.T) {
	resolver := DatabasePasswordResolver{
		LookupEnv: func(key string) (string, bool) {
			if key == "PGPASSWORD" {
				return "explicit-pg-password", true
			}
			return "", false
		},
	}

	got, err := resolver.Resolve(context.Background(), config.DatabaseConfig{PasswordEnv: "PGPASSWORD"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != "explicit-pg-password" {
		t.Fatalf("password = %q, want explicitly named PGPASSWORD value", got)
	}
}

func TestResolveDatabasePasswordSecretKeyIgnoresAmbientEnv(t *testing.T) {
	resolver := DatabasePasswordResolver{
		Credentials: mapCredentialStore{"postgres_password": "file-secret"},
		LookupEnv: func(key string) (string, bool) {
			if key == "POSTGRES_PASSWORD" {
				return "env-shadow", true
			}
			return "", false
		},
	}

	got, err := resolver.Resolve(context.Background(), config.DatabaseConfig{PasswordSecretKey: "postgres_password"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != "file-secret" {
		t.Fatalf("password = %q, want file-backed secret value", got)
	}
}

func TestResolveDatabasePasswordReadsPasswordFile(t *testing.T) {
	resolver := DatabasePasswordResolver{
		ReadFile: func(path string) ([]byte, error) {
			if path != "/run/secrets/db-password" {
				t.Fatalf("read path = %q, want declared password file", path)
			}
			return []byte("file-password\n"), nil
		},
	}

	got, err := resolver.Resolve(context.Background(), config.DatabaseConfig{PasswordFile: "/run/secrets/db-password"})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != "file-password" {
		t.Fatalf("password = %q, want newline-trimmed file password", got)
	}
}
