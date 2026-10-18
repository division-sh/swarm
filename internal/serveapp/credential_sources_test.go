package serveapp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

// Invariant: both-store public boot names each actual required source without
// disclosing values or conflating tool environment and channel file custody.
func TestServeRequiredCredentialSourceReadbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, verbose := range []bool{false, true} {
				t.Run(map[bool]string{false: "concise", true: "verbose"}[verbose], func(t *testing.T) {
					isolateCLIAPIConfigEnv(t)
					const key = "telegram_bot_token"
					const fileValue = "private-file-value-2729"
					const envValue = "private-environment-value-2729"
					const receipt = "private-admission-receipt-2729"
					path := filepath.Join(t.TempDir(), "per-instance-credentials.json")
					t.Setenv("SWARM_CREDENTIALS_FILE", path)
					t.Setenv("TELEGRAM_BOT_TOKEN", envValue)
					file, err := runtimecredentials.NewFileStore(path)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := file.AdmitWithReceipt(context.Background(), key, fileValue, receipt); err != nil {
						t.Fatal(err)
					}
					if err := file.Set(context.Background(), "unrelated_do_not_list", "unrelated-private-value"); err != nil {
						t.Fatal(err)
					}
					seal, err := runtimecredentials.SealCurrentValue(context.Background(), file, key)
					if err != nil {
						t.Fatal(err)
					}
					before, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					root := t.TempDir()
					if err := os.CopyFS(root, os.DirFS(filepath.Join(repoRootForTest(), "tests/tier8-boot-verification/test-boot-success"))); err != nil {
						t.Fatal(err)
					}
					writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), `credential_probe:
  description: Credential provenance boot fixture; never dispatched.
  handler_type: http
  credentials: [telegram_bot_token]
  http: {method: GET, url: "http://127.0.0.1:1/not-called"}
  output_schema: {type: object, properties: {ok: {type: boolean}}}
  response_success: {kind: http_status_2xx}
`)
					opts, start := clockDeploymentHarness(t, backend, root)
					opts.Verbose = verbose
					opts.SwarmDir = t.TempDir()
					config, err := os.ReadFile(opts.ConfigPath)
					if err != nil {
						t.Fatal(err)
					}
					config = append(config, []byte(`
channels:
  bindings:
    source-proof:
      pack: provider.telegram.hitl_channel
      destination: "-100123"
`)...)
					if err := os.WriteFile(opts.ConfigPath, config, 0o600); err != nil {
						t.Fatal(err)
					}
					process, _ := start()
					if code := process.stop(); code != 0 {
						t.Fatalf("serve stop=%d\n%s", code, process.outputString())
					}
					text := process.outputString()
					var lines []string
					for _, line := range strings.Split(text, "\n") {
						if strings.HasPrefix(line, "credential ") {
							lines = append(lines, line)
						}
					}
					want := "credential telegram_bot_token · source env:TELEGRAM_BOT_TOKEN\ncredential telegram_bot_token · source file:" + path
					if strings.Join(lines, "\n") != want {
						t.Fatalf("credential source golden mismatch\ngot: %s\nwant: %s\nboot:\n%s", strings.Join(lines, "\n"), want, text)
					}
					for _, forbidden := range []string{fileValue, envValue, receipt, seal.Seal.String(), "unrelated_do_not_list", "unrelated-private-value"} {
						if strings.Contains(text, forbidden) {
							t.Fatal("serve disclosed credential bytes or an unrelated store key")
						}
					}
					after, err := os.ReadFile(path)
					if err != nil || string(after) != string(before) {
						t.Fatalf("readback mutated credential storage: %v", err)
					}
				})
			}
		})
	}
}
