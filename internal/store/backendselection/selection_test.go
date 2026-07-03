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
				RepoRoot:                repo,
				FlagBackend:             "sqlite",
				FlagBackendSet:          true,
				EnvBackend:              "postgres",
				EnvBackendSet:           true,
				ConfigBackend:           "postgres",
				DefaultSQLitePath:       filepath.Join(repo, "default.db"),
				DefaultSQLitePathSource: SourceRolloutDefault,
			},
			want: BackendSQLite,
			src:  SourceFlag,
		},
		{
			name: "environment beats config",
			in: Input{
				RepoRoot:                repo,
				EnvBackend:              "sqlite",
				EnvBackendSet:           true,
				ConfigBackend:           "postgres",
				DefaultSQLitePath:       filepath.Join(repo, "default.db"),
				DefaultSQLitePathSource: SourceRolloutDefault,
			},
			want: BackendSQLite,
			src:  SourceEnvironment,
		},
		{
			name: "runtime config beats rollout default",
			in: Input{
				RepoRoot:                repo,
				ConfigBackend:           "sqlite",
				DefaultSQLitePath:       filepath.Join(repo, "default.db"),
				DefaultSQLitePathSource: SourceRolloutDefault,
			},
			want: BackendSQLite,
			src:  SourceRuntimeConfig,
		},
		{
			name: "rollout default is sqlite for local dev",
			in: Input{
				RepoRoot:                repo,
				DefaultSQLitePath:       filepath.Join(repo, "default.db"),
				DefaultSQLitePathSource: SourceRolloutDefault,
			},
			want: BackendSQLite,
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
				RepoRoot:                repo,
				FlagBackend:             "sqlite",
				FlagBackendSet:          true,
				EnvSQLitePath:           "env/dev.db",
				EnvSQLitePathSet:        true,
				ConfigSQLitePath:        "config/dev.db",
				DefaultSQLitePath:       filepath.Join(repo, "default.db"),
				DefaultSQLitePathSource: SourceRolloutDefault,
			},
			want: filepath.Join(repo, "env", "dev.db"),
			src:  SourceEnvironment,
		},
		{
			name: "config path beats default",
			in: Input{
				RepoRoot:                repo,
				FlagBackend:             "sqlite",
				FlagBackendSet:          true,
				ConfigSQLitePath:        "config/dev.db",
				DefaultSQLitePath:       filepath.Join(repo, "default.db"),
				DefaultSQLitePathSource: SourceRolloutDefault,
			},
			want: filepath.Join(repo, "config", "dev.db"),
			src:  SourceRuntimeConfig,
		},
		{
			name: "default path is caller supplied",
			in: Input{
				RepoRoot:                repo,
				FlagBackend:             "sqlite",
				FlagBackendSet:          true,
				DefaultSQLitePath:       filepath.Join(repo, "local", "dev.db"),
				DefaultSQLitePathSource: SourceProjectDefault,
			},
			want: filepath.Join(repo, "local", "dev.db"),
			src:  SourceProjectDefault,
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
