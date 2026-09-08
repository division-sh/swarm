package serveapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestServedRootScenarioCLISetupUsesRuntimeCoordinates(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := canonicalrouting.CopyScenarioRootSetup(t)
			aliasScenario := `name: reject root alias
setup:
  entities:
    - as: widget
      type: widget
      current_state: waiting
      fields: {score: 5}
steps:
  - publish: widget.scored
    target: widget
    payload: {delta: 7}
`
			if err := os.WriteFile(filepath.Join(root, "tests", "root-alias.yaml"), []byte(aliasScenario), 0o644); err != nil {
				t.Fatal(err)
			}
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, root)
			configPath := writeServeRuntimeTestConfig(t)
			for _, tc := range []struct {
				label     string
				wantError string
			}{
				{label: "tests/root-setup.yaml"},
				{label: "tests/root-alias.yaml", wantError: "resolves to root entity"},
			} {
				var stdout, stderr bytes.Buffer
				code := executeScenarioInOwnedLifecycle(t, repoRootForTest(), []string{
					"test", root, tc.label, "--config", configPath,
					"--timeout", "20s", "--poll-interval", "10ms",
				}, rt.Endpoint, &stdout, &stderr)
				if tc.wantError == "" && code != 0 {
					t.Fatalf("%s: code=%d stderr=%s stdout=%s", tc.label, code, stderr.String(), stdout.String())
				}
				if tc.wantError != "" && (code == 0 || !strings.Contains(stderr.String(), tc.wantError)) {
					t.Fatalf("%s: code=%d stderr=%s, want %q", tc.label, code, stderr.String(), tc.wantError)
				}
			}
			var invalid int
			if err := rt.DB.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM entity_state WHERE flow_instance <> CAST(run_id AS TEXT)`).Scan(&invalid); err != nil {
				t.Fatal(err)
			}
			if invalid != 0 {
				t.Fatalf("seeded %d noncanonical root entity coordinates", invalid)
			}
			var published int
			if err := rt.DB.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events WHERE event_name = 'widget.scored'`).Scan(&published); err != nil {
				t.Fatal(err)
			}
			if published != 1 {
				t.Fatalf("published %d root events; the alias rejection must precede publication", published)
			}
		})
	}
}
