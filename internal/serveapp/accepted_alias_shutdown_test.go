package serveapp

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/credentials"
)

// Keep the accepted-publication/immediate-shutdown path from the A9 control;
// neither delivery quiescence nor a sleep may stand in for the shutdown join.
func TestAcceptedAliasPublicationJoinedShutdownBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			credentialPath := filepath.Join(t.TempDir(), "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			file, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"telegram.telegram_bot_token", "telegram.webhook_signing_secret"} {
				if err := file.Set(context.Background(), key, "alias-secret-"+key); err != nil {
					t.Fatal(err)
				}
			}
			root := writeStandingTelegramServeFixture(t, "")
			disableChannelOnboardingBusinessConsumers(t, root)
			_, start := clockDeploymentHarness(t, backend, root)
			process, served := start()
			requireServedLandingRun(t, servedTestProcessRuntime(t, process), ".")
			status, receipt := postProviderAliasUpdate(t, strings.TrimSuffix(served.Endpoint, "/v1/rpc"), filepath.Base(root), "alias-secret-telegram.webhook_signing_secret", `{"update_id":9801,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"incumbent"}}`)
			if status != http.StatusAccepted {
				t.Fatalf("authenticated publication: status=%d receipt=%s", status, receipt)
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("joined shutdown=%d\n%s", code, process.outputString())
			}
		})
	}
}
