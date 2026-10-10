//go:build linux || darwin

package serveapp

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
}

func requireServedNativeAuthoredCustomerReply(t *testing.T, peer *serveNativeProtocolPeer) {
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
			t.Fatal("enabled authored native activity did not produce a genuine encrypted customer reply")
		}
	}
}
