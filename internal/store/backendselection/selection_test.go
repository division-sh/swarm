package backendselection

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveBackendSourcePrecedence(t *testing.T) {
	repo := t.TempDir()
	tests := []struct {
		name string
		in   Input
		want Backend
		src  Source
	}{
		{
			name: "flag beats environment and config",
			in: Input{
				RepoRoot:       repo,
				FlagBackend:    "sqlite",
				FlagBackendSet: true,
				EnvBackend:     "postgres",
				EnvBackendSet:  true,
				ConfigBackend:  "postgres",
			},
			want: BackendSQLite,
			src:  SourceFlag,
		},
		{
			name: "environment beats config",
			in: Input{
				RepoRoot:      repo,
				EnvBackend:    "sqlite",
				EnvBackendSet: true,
				ConfigBackend: "postgres",
			},
			want: BackendSQLite,
			src:  SourceEnvironment,
		},
		{
			name: "runtime config beats rollout default",
			in: Input{
				RepoRoot:      repo,
				ConfigBackend: "sqlite",
			},
			want: BackendSQLite,
			src:  SourceRuntimeConfig,
		},
		{
			name: "rollout default remains postgres until SQLite runtime support lands",
			in: Input{
				RepoRoot: repo,
			},
			want: BackendPostgres,
			src:  SourceRolloutDefault,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Resolve(tt.in)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if got.Backend != tt.want || got.BackendSource != tt.src {
				t.Fatalf("selection = backend %q source %q, want %q from %q", got.Backend, got.BackendSource, tt.want, tt.src)
			}
		})
	}
}

func TestResolveRejectsInvalidSelectedBackend(t *testing.T) {
	_, err := Resolve(Input{EnvBackend: "mysql", EnvBackendSet: true})
	if err == nil {
		t.Fatal("Resolve unexpectedly accepted invalid backend")
	}
	if !strings.Contains(err.Error(), "unsupported store backend") || !strings.Contains(err.Error(), "postgres, sqlite") {
		t.Fatalf("error = %v, want supported backend guidance", err)
	}
}

func TestResolveIgnoresInvalidLowerPriorityConfigBackend(t *testing.T) {
	got, err := Resolve(Input{
		FlagBackend:    "postgres",
		FlagBackendSet: true,
		ConfigBackend:  "mysql",
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Backend != BackendPostgres || got.BackendSource != SourceFlag {
		t.Fatalf("selection = %#v, want flag-selected postgres", got)
	}
}

func TestResolveSQLitePathSources(t *testing.T) {
	repo := t.TempDir()
	tests := []struct {
		name string
		in   Input
		want string
		src  Source
	}{
		{
			name: "env path beats config",
			in: Input{
				RepoRoot:         repo,
				FlagBackend:      "sqlite",
				FlagBackendSet:   true,
				EnvSQLitePath:    "env/dev.db",
				EnvSQLitePathSet: true,
				ConfigSQLitePath: "config/dev.db",
			},
			want: filepath.Join(repo, "env", "dev.db"),
			src:  SourceEnvironment,
		},
		{
			name: "config path beats default",
			in: Input{
				RepoRoot:         repo,
				FlagBackend:      "sqlite",
				FlagBackendSet:   true,
				ConfigSQLitePath: "config/dev.db",
			},
			want: filepath.Join(repo, "config", "dev.db"),
			src:  SourceRuntimeConfig,
		},
		{
			name: "default path is repo relative",
			in: Input{
				RepoRoot:       repo,
				FlagBackend:    "sqlite",
				FlagBackendSet: true,
			},
			want: filepath.Join(repo, ".swarm", "dev.db"),
			src:  SourceRolloutDefault,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Resolve(tt.in)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if got.SQLitePath != tt.want || got.SQLitePathSource != tt.src {
				t.Fatalf("sqlite path = %q source %q, want %q from %q", got.SQLitePath, got.SQLitePathSource, tt.want, tt.src)
			}
		})
	}
}

func TestResolveRejectsEmptyExplicitSQLitePath(t *testing.T) {
	_, err := Resolve(Input{
		FlagBackend:      "sqlite",
		FlagBackendSet:   true,
		EnvSQLitePath:    " ",
		EnvSQLitePathSet: true,
	})
	if err == nil {
		t.Fatal("Resolve unexpectedly accepted empty explicit SQLite path")
	}
	if !strings.Contains(err.Error(), "sqlite path from environment must be non-empty") {
		t.Fatalf("error = %v, want empty path guidance", err)
	}
}

func TestSQLiteUnsupportedRuntimeErrorNamesTrackedTail(t *testing.T) {
	err := SQLiteUnsupportedRuntimeError(filepath.Join("tmp", "dev.db"))
	if err == nil {
		t.Fatal("SQLiteUnsupportedRuntimeError returned nil")
	}
	for _, want := range []string{"sqlite", "#1085-#1088", "tmp"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err.Error(), want)
		}
	}
}
