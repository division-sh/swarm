package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestRuntimeConfigRecoveryDefaultsWithoutExecutionSelector(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		recovery     bool
	}{
		{"omitted", "", true},
		{"false", "runtime:\n  recovery_on_startup: false\n", false},
		{"true", "runtime:\n  recovery_on_startup: true\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "swarm.yaml")
			source := tc.source + "llm:\n  backend: anthropic\n  session:\n    lock_ttl: 10s\n    rotate_after_turns: 40\n    rotate_on_parse_failures: 3\n"
			if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
				t.Fatal(err)
			}
			loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: path})
			if err != nil {
				t.Fatal(err)
			}
			if loaded.Path != path || loaded.Source != unifiedConfigSourceExplicit || !loaded.validated || loaded.Config == nil || loaded.Config.Runtime.RecoveryOnStartup != tc.recovery {
				t.Fatalf("selected recovery configuration = %#v, want recovery=%v from %s", loaded, tc.recovery, path)
			}
		})
	}
}

func TestRuntimeConfigExcludedFieldsRefuseBeforeOverrides(t *testing.T) {
	// Keep every component-source case on the surviving file-admission entrance.
	for _, fieldPath := range []string{
		"sharding",
		"runtime.max_concurrent_agents",
		"runtime.event_poll_interval",
		"database.password",
		"llm.claude_cli.retries",
		"llm.claude_cli.no_session_persistence",
		"llm.claude_cli.use_tmux",
	} {
		parts := strings.Split(fieldPath, ".")
		key := parts[len(parts)-1]
		for _, literal := range []string{"null", "''", "{}", "[]", "1", "false"} {
			for _, merged := range []bool{false, true} {
				source := key + ": " + literal
				if merged {
					source = "<<: {" + source + "}"
				}
				for i := len(parts) - 2; i >= 0; i-- {
					source = parts[i] + ": {" + source + "}"
				}
				t.Run(fmt.Sprintf("%s/%s/merged=%t", fieldPath, literal, merged), func(t *testing.T) {
					root := t.TempDir()
					path := filepath.Join(root, "swarm.yaml")
					if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
						t.Fatal(err)
					}
					for _, override := range []string{"", "openai_responses"} {
						loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: path, BackendOverride: override})
						blockers := unifiedConfigBlockers(loaded.Diagnostics)
						if err == nil || loaded.validated || len(blockers) != 1 {
							t.Fatalf("override=%q: excluded source admitted or misdiagnosed: result=%#v error=%v", override, loaded, err)
						}
						diagnostic := blockers[0]
						key := fieldPath
						passwordSourceRefusal := fieldPath == "database.password" && (literal == "''" || literal == "{}" || literal == "[]")
						if passwordSourceRefusal {
							key = `$["database"]["password"]`
						}
						if diagnostic.Key != key || diagnostic.Path != path {
							t.Fatalf("override=%q: excluded-field diagnostic = %#v", override, diagnostic)
						}
						// Preserve the CLI's Split/secret distinction, while the
						// component decoder retains its typed vocabulary oracle.
						if passwordSourceRefusal {
							if diagnostic.Kind != unifiedConfigDiagnosticParseFailed || diagnostic.Line == 0 || diagnostic.Column == 0 || !strings.Contains(diagnostic.Message, `field "password" is not supported`) || !strings.Contains(diagnostic.Message, "Valid fields:") || slices.Contains(diagnostic.ValidOptions, "password") || !slices.Contains(diagnostic.ValidOptions, "password_env") {
								t.Fatalf("override=%q: located source refusal = %#v", override, diagnostic)
							}
						} else if fieldPath == "database.password" {
							if diagnostic.Kind != unifiedConfigDiagnosticValidationFailed || !strings.Contains(diagnostic.Message, "unsupported plaintext secret material") || diagnostic.Remediation != "declare a file, secret key, or explicit env delegation field instead" {
								t.Fatalf("override=%q: plaintext-secret refusal = %#v", override, diagnostic)
							}
						} else if diagnostic.Kind != unifiedConfigDiagnosticSplitUnsupported || !strings.Contains(diagnostic.Remediation, "tracked split:") || !strings.Contains(diagnostic.Remediation, "no supported replacement") {
							t.Fatalf("override=%q: Split refusal = %#v", override, diagnostic)
						}
						if !strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), diagnostic.Remediation) || strings.Contains(err.Error(), "RETIRED") {
							t.Fatalf("override=%q: not explicit source rejection: %v", override, err)
						}
					}
				})
			}
		}
	}
}

func TestRuntimeConfigUnknownKeyDiagnosticNamesFileVocabularyAndSuggestion(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "swarm.yaml")
	if err := os.WriteFile(path, []byte("runtime:\n  recovery_on_startupp: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: path})
	blockers := unifiedConfigBlockers(loaded.Diagnostics)
	if err == nil || loaded.validated || len(blockers) != 1 {
		t.Fatalf("unknown key admitted or misdiagnosed: result=%#v error=%v", loaded, err)
	}
	diagnostic := blockers[0]
	if diagnostic.Path != path || diagnostic.Line != 2 || diagnostic.Column == 0 || !strings.Contains(diagnostic.Remediation, "recovery_on_startup") || !slices.Contains(diagnostic.ValidOptions, "recovery_on_startup") {
		t.Fatalf("error=%v diagnostic=%#v", err, diagnostic)
	}
	if !strings.Contains(err.Error(), "Valid fields:") || !strings.Contains(err.Error(), path) {
		t.Fatal(err)
	}
}

func TestRuntimeConfigAdmitsSourceBeforeOverrides(t *testing.T) {
	const prefix = "llm:\n  backend: anthropic\n  session:\n    lock_ttl: 10s\n    rotate_after_turns: 40\n    rotate_on_parse_failures: 3\n"
	for _, row := range []struct{ source, key string }{
		{"totally_unknown: true", "totally_unknown"},
		{"<<: {totally_unknown: null}", "totally_unknown"},
		{"database: {<<: {totally_unknown: true}}", "totally_unknown"},
		{"connection: {api_serve: localhost}", "api_serve"},
	} {
		t.Run(row.source, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "swarm.yaml")
			if err := os.WriteFile(path, []byte(prefix+row.source+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			for _, override := range []string{"", "openai_responses"} {
				loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: path, BackendOverride: override})
				blockers := unifiedConfigBlockers(loaded.Diagnostics)
				if err == nil || loaded.validated || len(blockers) != 1 {
					t.Fatalf("override=%q: invalid source admitted or misdiagnosed: result=%#v error=%v", override, loaded, err)
				}
				diagnostic := blockers[0]
				if diagnostic.Path != path || !strings.Contains(diagnostic.Message, row.key) || len(diagnostic.ValidOptions) == 0 || diagnostic.Line == 0 || diagnostic.Column == 0 {
					t.Fatalf("override=%q: diagnostic=%#v, error=%v", override, diagnostic, err)
				}
			}
		})
	}
}
