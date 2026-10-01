package releasee2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const fullLifecycleFixtureSource = "standing_telegram"

func TestFullLifecycleJourneyMatrixIsClosed(t *testing.T) {
	want := map[string]fullLifecycleJourney{
		"J1-sqlite-graceful":                  {name: "J1-sqlite-graceful", backend: "sqlite", kind: fullLifecycleGraceful},
		"J2-postgres-graceful":                {name: "J2-postgres-graceful", backend: "postgres", kind: fullLifecycleGraceful},
		"J3-sqlite-crash-intrinsic-recover":   {name: "J3-sqlite-crash-intrinsic-recover", backend: "sqlite", kind: fullLifecycleCrashIntrinsic},
		"J4-postgres-crash-intrinsic-recover": {name: "J4-postgres-crash-intrinsic-recover", backend: "postgres", kind: fullLifecycleCrashIntrinsic},
		"J5-sqlite-dev-fresh":                 {name: "J5-sqlite-dev-fresh", backend: "sqlite", kind: fullLifecycleDevFresh},
	}
	if len(fullLifecycleJourneys) != len(want) {
		t.Fatalf("full lifecycle journey count = %d, want %d", len(fullLifecycleJourneys), len(want))
	}
	seen := make(map[string]bool, len(fullLifecycleJourneys))
	for _, journey := range fullLifecycleJourneys {
		if seen[journey.name] {
			t.Fatalf("duplicate full lifecycle journey %s", journey.name)
		}
		seen[journey.name] = true
		if expected, ok := want[journey.name]; !ok || journey != expected {
			t.Fatalf("unsupported full lifecycle journey %#v", journey)
		}
	}
}

func fullLifecycleFixtureRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(releaseE2ERepoRoot(t), "internal", "releasee2e", "testdata", "full_lifecycle")
}

func fullLifecycleExecutableSource(t *testing.T) string {
	t.Helper()
	return filepath.Join(fullLifecycleFixtureRoot(t), fullLifecycleFixtureSource)
}

func mutateFullLifecycleFixtureWithoutExactConnectorResponse(t *testing.T, contracts string) {
	t.Helper()
	manifestPath := filepath.Join(contracts, "telegram-chat", "schema.yaml")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read lifecycle bot manifest: %v", err)
	}
	const connectorImport = "  connector_packs:\n    - provider: telegram\n      tool: telegram.send_message\n"
	if !strings.Contains(string(raw), connectorImport) {
		t.Fatalf("lifecycle bot manifest has no exact Telegram connector import:\n%s", raw)
	}
	writeReleaseFile(t, manifestPath, strings.Replace(string(raw), connectorImport, "", 1))
	writeReleaseFile(t, filepath.Join(contracts, "telegram-chat", "tools.yaml"), `telegram.send_message:
  description: Deliberately unmocked transport for source-admission proof.
  handler_type: http
  effect_class: non_idempotent_write
  credentials: [telegram_bot_token]
  http:
    method: POST
    url: https://unreachable.invalid/bot{{credentials.telegram_bot_token}}/send
  input_schema:
    type: object
    properties:
      chat_id: {type: string}
      text: {type: string}
    required: [chat_id, text]
  response_success: {kind: http_status_2xx}
`)
}
