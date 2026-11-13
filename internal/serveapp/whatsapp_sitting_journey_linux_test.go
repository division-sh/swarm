package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/tests/support/testutil"
	"go.mau.fi/whatsmeow/types"
)

// One public-command journey, with only the external WhatsApp service and the
// human's QR/terminal actions doubled. No operator inbox, agent or seeded device.
func TestWhatsAppPublicSittingJourneyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			terminal := sittingTerminal(t)
			peer := newServeNativeProtocolPeer(t)
			source := t.TempDir()
			copyReleaseFixtureTree(t, filepath.Join(repoRootForTest(), "packs/providers/whatsapp"), filepath.Join(source, "whatsapp"))
			writeStandingCandidateFile(t, filepath.Join(source, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
			writeStandingCandidateFile(t, filepath.Join(source, "schema.yaml"), `stages:
  active:
    gate:
      decision: retire_service
      outcomes:
        retire:
          advances_to: done
  done:
    final: true
inputs: [inbound.whatsapp.raw, inbound.whatsapp.message]
outputs: [inbound.whatsapp.message]
`)
			const account = "+15551234567"
			writeStandingCandidateFile(t, filepath.Join(source, "policy.yaml"), "operator_account: '"+account+"'\n")
			writeStandingCandidateFile(t, filepath.Join(source, "nodes.yaml"), `echo:
  inbound.whatsapp.message:
    activity:
      id: live_echo
      tool: whatsapp.send_text
      input:
        account: policy.operator_account
        destination: payload.conversation_reference
        text: payload.text
`)
			directory := t.TempDir()
			if err := os.Chmod(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			var config string
			if backend == "sqlite" {
				config = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend,
					filepath.Join(t.TempDir(), "sitting.sqlite"), channelOnboardingHostWorkspaceFields())
			} else {
				config = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
			}
			content, err := os.ReadFile(config)
			if err != nil {
				t.Fatal(err)
			}
			writeStandingCandidateFile(t, config, string(content)+fmt.Sprintf("\npaths:\n  swarm_dir: %q\nserve:\n  api_listen_addr: 127.0.0.1:0\n  mcp_listen_addr: 127.0.0.1:0\n", directory))
			t.Chdir(filepath.Dir(config))

			qr := make(chan channelonboarding.Result, 1)
			factory := func(ctx context.Context, options cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
				session, err := OpenChannelRegistration(ctx, options)
				if err != nil {
					return nil, err
				}
				return firstConnectPairingObserver{ChannelRegistrationSession: session, principal: options.Principal,
					observe: func(result channelonboarding.Result, _ bool) {
						if result.Pairing != nil && result.Pairing.Code != "" {
							select {
							case qr <- result:
							default:
							}
						}
					}}, nil
			}
			registration := fixtureRegistration(factory)
			command := func(args ...string) string {
				t.Helper()
				var out, diagnostic bytes.Buffer
				if code := cliapp.Execute(t.Context(), append([]string{"--config", config}, args...), &out, &diagnostic, nil, nil, registration); code != 0 {
					t.Fatalf("public %v exit=%d: %s\n%s", args, code, &out, &diagnostic)
				}
				return out.String()
			}
			if out := command("verify", source, "--portable"); !strings.Contains(out, "portable structural checks passed") {
				t.Fatal("sitting source did not pass portable verification", out)
			}
			t.Log("step=portable_verify PASS")

			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			out, diagnostic := &lockedBuffer{}, &lockedBuffer{}
			prompts := make(chan string, 2)
			writer := &sittingCLIWriter{Writer: out, qrRendered: make(chan struct{}), pause: func(string, string) error { return nil }}
			done := make(chan int, 1)
			joined := false
			defer func() {
				cancel()
				if !joined {
					<-done
				}
			}()
			go func() {
				done <- cliapp.Execute(ctx, []string{"--config", config, "channel", "connect", "whatsapp"}, writer,
					sittingPromptWriter{Writer: diagnostic, prompts: prompts}, nil, nil, registration)
			}()
			var pairing channelonboarding.Result
			select {
			case pairing = <-qr:
			case code := <-done:
				joined = true
				t.Fatal("connect exited before QR", code, diagnostic.String())
			case <-ctx.Done():
				t.Fatal("connect did not offer a QR", diagnostic.String())
			}
			select {
			case <-writer.qrRendered:
			case <-ctx.Done():
				t.Fatal("public connect did not render its QR", diagnostic.String())
			}
			if !strings.Contains(out.String(), "██") || pairing.Pairing.Paired || pairing.Operation.AccountConfirmed {
				t.Fatal("visible QR did not precede pairing and account approval")
			}
			t.Log("step=visible_qr PASS")
			peer.pair(pairing.Pairing.Code)
			select {
			case prompt := <-prompts:
				if prompt != "Is this the right account? [y/N] " || !strings.Contains(out.String(), "Linked to "+account) {
					t.Fatal("confirmation did not preview the exact TEST account", prompt, out.String())
				}
			case <-ctx.Done():
				t.Fatal("account confirmation prompt missing", diagnostic.String())
			}
			if _, err := terminal.WriteString("y\n"); err != nil {
				t.Fatal(err)
			}
			select {
			case code := <-done:
				joined = true
				if code != 0 || !strings.Contains(out.String(), "Registered whatsapp account "+account+".") {
					t.Fatal("public account confirmation did not join successfully", code, out.String(), diagnostic.String())
				}
			case <-ctx.Done():
				t.Fatal("confirmed connect did not join", diagnostic.String())
			}
			t.Log("step=interactive_account_confirmation PASS")

			process := startRuntimeTestProcessWithRunner(t, source, cliapp.ServeOptions{}, func(ctx context.Context, _ string, opts cliapp.ServeOptions) int {
				return cliapp.Execute(ctx, []string{"--config", config, "serve", source}, opts.Output, opts.ErrorOutput, Run, nil, registration)
			})
			process.waitForReadyLine()
			peer.awaitPostLogin()
			t.Log("step=retained_serve_ready PASS")

			var listing struct {
				Accounts []channelonboarding.Result `json:"accounts"`
			}
			if err := json.Unmarshal([]byte(command("channel", "list", "--json")), &listing); err != nil || len(listing.Accounts) != 1 {
				t.Fatal("public account list did not identify one TEST account", err)
			}
			registered := listing.Accounts[0].Operation
			if registered.AccountReference != account || registered.OperationID != pairing.Operation.OperationID ||
				!registered.AccountConfirmed || registered.IdentityOperationID != "" {
				t.Fatal("rehearsal replaced the approved account or enrolled an operator")
			}
			if status := command("channel", "status", "whatsapp", account); !strings.Contains(status, account) {
				t.Fatal("public status omitted the exact TEST account", status)
			}
			select {
			case <-peer.sent:
				t.Fatal("no-HITL rehearsal emitted a message before customer input")
			default:
			}
			const text = "R130 WA live echo 01"
			customer := types.NewJID("15551234571", types.DefaultUserServer)
			lid := types.NewJID("100000000005", types.HiddenUserServer)
			sender, chat, chatLID := peer.senderFrom(customer, lid)
			peer.sendEncryptedFrame(sender.EncryptText(t, text, "R130_SITTING_SINGLE_INPUT"))
			select {
			case reply := <-peer.sent:
				if reply.ID == "" || reply.Body.GetConversation() != text || reply.To.ToNonAD() != chat && reply.To.ToNonAD() != chatLID {
					t.Fatal("public journey did not echo the exact input to its sender", reply)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("mock customer did not receive its exact reply", process.outputString())
			}
			t.Log("step=inbound_exact_reply PASS")
			if logout := command("channel", "logout", "whatsapp", account); !strings.Contains(logout, "Logged out whatsapp account "+account+".") {
				t.Fatal("public deployment logout did not unlink the exact account", logout)
			}
			select {
			case <-peer.logouts:
			case <-time.After(5 * time.Second):
				t.Fatal("deployment logout did not reach the mocked TEST device")
			}
			t.Log("step=deployment_logout PASS")
			if code := process.stop(); code != 0 {
				t.Fatal("public serve did not join after logout", code)
			}
			select {
			case duplicate := <-peer.sent:
				t.Fatal("sitting produced more than one reply", duplicate.ID)
			default:
			}
			t.Log("step=single_reply_and_joined_shutdown PASS")
		})
	}
}
