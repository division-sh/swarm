//go:build linux || darwin

package serveapp

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/tools"
)

func TestWhatsAppAuthoredReplyFixtureUsesInstalledPrivateConnector(t *testing.T) {
	repo := repoRootForTest()
	module, _, err := cliapp.NewSwarmWorkflowModule(repo, filepath.Join(repo, "internal/serveapp/testdata/whatsapp-session-reply"), filepath.Join(repo, "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	tool, found := module.SemanticSource().ToolEntries()["whatsapp.send_text"]
	native, installed := tool.InProcess()
	if !found || !installed || native != contracts.ToolInProcessWhatsAppSendText || tool.AgentExposable() || tool.Effect() != contracts.ActivityEffectClassNonIdempotentWrite {
		t.Fatal("authored fixture did not consume its exact private connector")
	}
	if warnings, err := tools.ValidateToolImplementations(module.SemanticSource()); err != nil || len(warnings) != 0 {
		t.Fatal("declaration-first native activity admission failed", warnings, err)
	}
	readAccount := contracts.MustToolSchemaEntry(contracts.WithToolHandler(contracts.ToolHandlerInProcess),
		contracts.WithToolCategory(contracts.ToolCategoryProviderRegistration.String()), contracts.WithToolEffect(contracts.ActivityEffectClassReadOnly),
		contracts.WithToolSchemas(contracts.MustToolInputSchema(contracts.ToolSchemaObject), contracts.MustToolInputSchema(contracts.ToolSchemaObject)),
		contracts.WithToolInProcessTarget(contracts.ToolInProcessWhatsAppReadAccount))
	if _, err := tools.ValidateToolImplementations(semanticview.Wrap(&contracts.WorkflowContractBundle{Tools: map[string]contracts.ToolSchemaEntry{"whatsapp.read_account": readAccount}})); err == nil {
		t.Fatal("session-write qualification admitted read-account execution")
	}
}

func requireServedNativeAuthoredCustomerReply(t *testing.T, endpoint string, peer *serveNativeProtocolPeer) {
	t.Helper()
	const text = "A genuine customer message for the authored activity"
	customer, customerLID := peer.customer(text, "SERVED_AUTHORED_CUSTOMER")
	deadline := time.After(5 * time.Second)
	for {
		select {
		case sent := <-peer.sent:
			if sent.To.ToNonAD() != customer && sent.To.ToNonAD() != customerLID {
				if sent.Body.GetConversation() == text {
					t.Fatal("authored business reply was sent to the operator mailbox")
				}
				continue
			}
			if sent.Body.GetConversation() != text || sent.ID == "" {
				t.Fatal("authored activity changed the admitted customer input", sent.ID, sent.Body)
			}
			return
		case <-deadline:
			logServedNativeActivityFailure(t, endpoint)
			t.Fatal("enabled authored native activity did not produce a genuine encrypted customer reply")
		}
	}
}

func logServedNativeActivityFailure(t *testing.T, endpoint string) {
	t.Helper()
	var runs struct {
		Runs []operatorread.RunHeader `json:"runs"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"limit": 10}, &runs)
	for _, run := range runs.Runs {
		for _, method := range []string{"event.list", "runtime.logs"} {
			response := requestServedJSONRPC(t, endpoint, method, map[string]any{"filter": map[string]any{"run_id": run.RunID}, "limit": 50})
			t.Logf("authored native %s run=%s error=%v result=%s", method, run.RunID, response.Error, response.Result)
		}
	}
}
