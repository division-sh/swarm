package serveapp

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
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
			for _, key := range []string{"telegram_bot_token", "webhook_signing.alpha"} {
				if err := file.Set(context.Background(), key, "alias-secret-"+key); err != nil {
					t.Fatal(err)
				}
			}
			root := canonicalrouting.CopyStandingRootTreePublic(t)
			for flow, alias := range map[string]string{".": "shared", "beta": "other"} {
				path := filepath.Join(root, flow, "schema.yaml")
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				updated := string(body)
				if flow == "." {
					updated = strings.Replace(updated, "name: standing-root-tree\n", "name: shop\n", 1)
					updated = strings.Replace(updated, "alias: alpha\n", "alias: "+alias+"\n", 1)
				} else {
					updated = strings.Replace(updated, "alias: beta\n", "alias: "+alias+"\n", 1)
				}
				writeWorkflowValidationFixtureFile(t, path, updated)
			}
			_, start := clockDeploymentHarness(t, backend, root)
			process, served := start()
			statuses, err := servedTestProcessRuntime(t, process).Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 || statuses[0].FlowPath != "." {
				t.Fatalf("enabled root/dormant sibling: statuses=%+v err=%v", statuses, err)
			}
			status, receipt := postProviderAliasUpdate(t, strings.TrimSuffix(served.Endpoint, "/v1/rpc"), "shared", "alias-secret-webhook_signing.alpha", `{"update_id":9801,"message":{"message_id":7,"from":{"id":41},"chat":{"id":42,"type":"private"},"text":"incumbent"}}`)
			if status != http.StatusAccepted {
				t.Fatalf("authenticated publication: status=%d receipt=%s", status, receipt)
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("joined shutdown=%d\n%s", code, process.outputString())
			}
		})
	}
}
