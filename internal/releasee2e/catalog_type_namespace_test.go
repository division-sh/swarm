package releasee2e

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCatalogTypeNamespacePublicAdmissionBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s required", goldenPostgresEnv)
	}
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	scalar := "scalars:\n  Shared: text\n"
	enum := "enums:\n  Shared: {values: [open], default: open}\n"
	named := "types:\n  Shared: {value: integer}\n"
	missing := "types:\n  Shared: {value: Missing}\n"
	cycle := "types:\n  Shared: {next: Shared}\n"
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			base := filepath.Join(root, backend)
			store := goldenSQLiteStore(base)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			config := filepath.Join(base, "swarm.yaml")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			token := filepath.Join(base, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := goldenProcessEnv(t, base, store.passwordEnv, 0)
			makeSource := func(t *testing.T, rootTypes, flowTypes string) string {
				t.Helper()
				source := filepath.Join(t.TempDir(), "contracts")
				copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "tests/tier8-boot-verification/test-boot-success"), source)
				writeReleaseFile(t, filepath.Join(source, "types.yaml"), rootTypes)
				writeReleaseFile(t, filepath.Join(source, "child/schema.yaml"), "name: child\nstages: {done: {final: true}}\n")
				if flowTypes != "" {
					writeReleaseFile(t, filepath.Join(source, "child/types.yaml"), flowTypes)
				}
				return source
			}
			for _, cut := range []struct{ name, root, child, first, second string }{
				{"scalar_named_local", scalar + missing, "", `scalar alias "Shared"`, `named type "Shared"`},
				{"enum_named_local", enum + cycle, "", `enum "Shared"`, `named type "Shared"`},
				{"scalar_named_composed", scalar, missing, `scalar alias "Shared"`, `named type "Shared"`},
				{"enum_named_composed", enum, cycle, `enum "Shared"`, `named type "Shared"`},
				{"named_scalar_composed", named, scalar, `named type "Shared"`, `scalar alias "Shared"`},
				{"named_enum_composed", named, enum, `named type "Shared"`, `enum "Shared"`},
				{"builtin_scalar", "scalars:\n  text: integer\n", "", `built-in scalar "text"`, `scalar alias "text"`},
				{"builtin_enum", "enums:\n  uuid: {values: [open], default: open}\n", "", `built-in scalar "uuid"`, `enum "uuid"`},
				{"builtin_named", "types:\n  integer: {value: text}\n", "", `built-in scalar "integer"`, `named type "integer"`},
			} {
				t.Run(cut.name, func(t *testing.T) {
					source := makeSource(t, cut.root, cut.child)
					for _, command := range []string{"verify", "serve"} {
						args := []string{command, source, "--config", config}
						if command == "verify" {
							args = append(args, "--portable", "--json")
						}
						refused := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, args...)
						message := refused.output
						if command == "verify" {
							var report struct {
								Errors []struct {
									CheckID  string `json:"check_id"`
									Severity string `json:"severity"`
									Message  string `json:"message"`
								} `json:"errors"`
							}
							if err := json.Unmarshal([]byte(refused.output), &report); err != nil {
								t.Fatalf("verify JSON: %v\n%s", err, refused.output)
							}
							if len(report.Errors) != 1 || report.Errors[0].CheckID != "source_loading" || report.Errors[0].Severity != "hard_invalidity" {
								t.Fatalf("wrong public admission refusal: %s", refused.output)
							}
							message = report.Errors[0].Message
						}
						if refused.err == nil {
							t.Fatalf("%s admitted name collision: %s", command, refused.output)
						}
						for _, want := range []string{"TYPE-NAME-COLLISION", cut.first, cut.second, "types.yaml:2:3", "Rename one"} {
							if !strings.Contains(message, want) {
								t.Fatalf("%s refusal omitted %q: %s", command, want, refused.output)
							}
						}
						if cut.child != "" && !strings.Contains(message, "child/types.yaml:2:3") {
							t.Fatalf("%s lost composed declaration source: %s", command, refused.output)
						}
						if command == "serve" {
							listener, err := releaseAPIListener(refused.output)
							if err != nil || listener != "" {
								t.Fatalf("serve published a listener before source admission: %v\n%s", err, refused.output)
							}
						}
					}
				})
			}
			for _, override := range []struct{ name, root, child string }{
				{"scalar_override", scalar, "scalars:\n  Shared: integer\n"},
				{"enum_override", enum, "enums:\n  Shared: {values: [child], default: child}\n"},
				{"named_override", named, "types:\n  Shared: {child_value: text}\n"},
			} {
				t.Run(override.name, func(t *testing.T) {
					source := makeSource(t, override.root, override.child)
					result := runReleaseCommand(t, goldenStartupTimeout, base, env, "", binary, "verify", "--portable", "--json", source, "--config", config)
					if result.err != nil {
						t.Fatalf("valid same-category override verify: %v\n%s", result.err, result.output)
					}
					process := startReleaseServe(t, releaseProcessSpec{BinaryPath: binary, WorkingDir: base, Source: source, ConfigPath: config, Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env})
					ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
					defer cancel()
					if err := process.waitReady(ctx); err != nil {
						t.Fatalf("valid same-category override boot: %v\n%s", err, process.output.String())
					}
					if err := process.stopAndWait(10 * time.Second); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}
