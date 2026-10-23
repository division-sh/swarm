//go:build linux || darwin

package serveapp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/serveapp"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

type registrationProviderRead func(*http.Request) (*http.Response, error)

func (f registrationProviderRead) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

// Registration needs only a selected deployment, not an admitted flow/listener.
func TestDeploymentChannelRegistrationWithoutRuntimeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := t.TempDir()
			t.Chdir(root)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(root, "credentials.json"))
			config := fmt.Sprintf("store:\n  backend: sqlite\n  sqlite:\n    path: %q\n", filepath.Join(root, "store.sqlite"))
			if backend == "postgres" {
				p, err := testpostgres.ParseConnectionParameters(testutil.StartEmptyPostgresDSN(t))
				if err != nil {
					t.Fatal(err)
				}
				t.Setenv("PGPASSWORD", p.Password)
				config = fmt.Sprintf("store:\n  backend: postgres\ndatabase:\n  host: %q\n  port: %d\n  name: %q\n  user: %q\n  password_env: PGPASSWORD\n  sslmode: %q\n", p.Host, p.Port, p.Database, p.User, p.SSLMode)
			}
			path := filepath.Join(root, "deployment.yaml")
			if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			input, err := os.CreateTemp(root, "token-input")
			if err != nil {
				t.Fatal(err)
			}
			defer input.Close()
			if _, err := input.WriteString("420079:test-credential\n"); err != nil {
				t.Fatal(err)
			}
			if _, err := input.Seek(0, 0); err != nil {
				t.Fatal(err)
			}
			original := os.Stdin
			os.Stdin = input
			defer func() { os.Stdin = original }()
			reads := 0
			botID := 420079
			polls := 0
			confirmations := 0
			webhook := false
			webhookClaims := 0
			factory := func(ctx context.Context, opts cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
				var native cliapp.ChannelRegistrationSession
				const callback = "https://hooks.example.test/webhooks/chat/telegram?swarm_callback_generation=current"
				opts.CurrentWebhook = func(_ context.Context, received, account string) bool {
					return webhook && received == callback && account == "420080"
				}
				opts.HTTP = &http.Client{Transport: registrationProviderRead(func(request *http.Request) (*http.Response, error) {
					if request.URL.Host != "api.telegram.org" {
						t.Fatalf("registration attempted an unexpected provider operation: %s", request.URL.Host)
					}
					var body string
					switch {
					case strings.HasSuffix(request.URL.Path, "/getMe"):
						reads++
						body = fmt.Sprintf(`{"ok":true,"result":{"id":%d}}`, botID)
					case strings.HasSuffix(request.URL.Path, "/getWebhookInfo"):
						body = `{"ok":true,"result":{"url":""}}`
						if webhook {
							operations, err := opts.Identities.ListOperatorChannelOperations(ctx, opts.Principal.ID)
							if err != nil {
								t.Fatal(err)
							}
							plan, err := packfixture.TriggerCatalog(t).CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "chat", Provider: "telegram", SigningSecret: "telegram.webhook"})
							if err != nil {
								t.Fatal(err)
							}
							parents, err := opts.Operations.ListChannelOnboardingOperations(ctx)
							if err != nil {
								t.Fatal(err)
							}
							for _, identity := range operations {
								if identity.State != operatorchannel.StateAwaitingClaim {
									continue
								}
								for _, parent := range parents {
									if parent.OperationID != identity.OnboardingOperationID {
										continue
									}
									deliver := func(text string) providertriggers.Delivery {
										raw := fmt.Sprintf(`{"update_id":987,"message":{"message_id":987,"from":{"id":12346},"chat":{"id":12346,"type":"private"},"text":%q}}`, text)
										var payload any
										if err := json.Unmarshal([]byte(raw), &payload); err != nil {
											t.Fatal(err)
										}
										delivery, err := plan.Accept(providertriggers.Request{Provider: "telegram", Method: http.MethodPost, Body: []byte(raw), Payload: payload,
											Headers: http.Header{"X-Telegram-Bot-Api-Secret-Token": {"payload-secret"}}, Target: providertriggers.Target{WebhookSecret: "payload-secret"}})
										if err != nil {
											t.Fatal(err)
										}
										return delivery
									}
									receiving := channelonboarding.AccountReference{Provider: "telegram", Account: "420080"}
									current := func() bool { return true }
									credential := func(admission channelonboarding.CredentialAdmission) bool {
										value, current, err := opts.Credentials.ObserveValueMatchingSeal(ctx, credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal})
										return err == nil && current && value.CredentialValue() == "420080:other-credential"
									}
									ordinary := func() (providertriggers.Delivery, error) { return deliver("ordinary customer message"), nil }
									if consumed, err := native.ConsumeWebhookClaim(ctx, receiving, credential, ordinary, current); err != nil || consumed {
										t.Fatal("ordinary traffic became a deployment ceremony", consumed, err)
									}
									stale := func(channelonboarding.CredentialAdmission) bool { return false }
									if consumed, err := native.ConsumeWebhookClaim(ctx, receiving, stale, ordinary, current); err != nil || consumed {
										t.Fatal("pending operator credentials blocked ordinary customer traffic", consumed, err)
									}
									exact := deliver(identity.Challenge)
									admit := func() (providertriggers.Delivery, error) { return exact, nil }
									if consumed, err := native.ConsumeWebhookClaim(ctx, receiving, stale, admit, current); err == nil || consumed {
										t.Fatal("stale pending credential admitted an operator claim", consumed, err)
									}
									foreign := channelonboarding.AccountReference{Provider: "telegram", Account: "420079"}
									if consumed, err := native.ConsumeWebhookClaim(ctx, foreign, credential, admit, current); err != nil || consumed {
										t.Fatal("another receiving account claimed the pending operator", consumed, err)
									}
									if consumed, err := native.ConsumeWebhookClaim(ctx, receiving, credential, admit, func() bool { return false }); err == nil || consumed {
										t.Fatal("stale receiving evidence claimed an operator", consumed, err)
									}
									for range 2 {
										if consumed, err := native.ConsumeWebhookClaim(ctx, receiving, credential, admit, current); err != nil || !consumed {
											t.Fatal("current pending webhook claim or exact replay refused", consumed, err)
										}
										webhookClaims++
									}
								}
							}
							body = fmt.Sprintf(`{"ok":true,"result":{"url":%q}}`, callback)
						}
					case strings.HasSuffix(request.URL.Path, "/getUpdates"):
						if webhook {
							t.Fatal("holder started a competing Telegram poll receiver")
						}
						if request.URL.Query().Get("limit") == "1" {
							if request.URL.Query().Get("offset") != "3" {
								t.Fatal("confirmation acknowledged beyond the exact matched challenge")
							}
							operations, err := opts.Identities.ListOperatorChannelOperations(ctx, opts.Principal.ID)
							if err != nil {
								t.Fatal(err)
							}
							settled := false
							for _, op := range operations {
								if op.State != operatorchannel.StateAwaitingConfirmation {
									continue
								}
								receipt, found, err := opts.Identities.LoadOperatorChannelClaimReceipt(ctx, operatorchannel.SessionClaimReceiptID(op.OnboardingOperationID, "2"))
								if err != nil || !found || receipt.OperationID != op.OperationID || receipt.NativeCaptureFingerprint == "" {
									t.Fatal("confirmation preceded durable native claim settlement", err)
								}
								settled = true
							}
							if !settled {
								t.Fatal("confirmation preceded the durable claimant preview")
							}
							confirmations++
							if confirmations == 1 {
								return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"ok":false}`))}, nil
							}
							// Later updates returned by this read must not be acknowledged.
							body = `{"ok":true,"result":[{"update_id":50}]}`
							break
						}
						polls++
						text := "ordinary customer message"
						if polls > 1 {
							operations, err := opts.Identities.ListOperatorChannelOperations(ctx, opts.Principal.ID)
							if err != nil {
								t.Fatal(err)
							}
							for _, op := range operations {
								if op.State == operatorchannel.StateAwaitingClaim {
									text = op.Challenge
								}
							}
						}
						body = fmt.Sprintf(`{"ok":true,"result":[{"update_id":%d,"message":{"message_id":%d,"from":{"id":12345},"chat":{"id":12345,"type":"private"},"text":%q}}]}`, polls, polls, text)
					default:
						t.Fatalf("registration attempted a provider mutation or unrelated read")
					}
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}},
						Body: io.NopCloser(strings.NewReader(body))}, nil
				})}
				var err error
				native, err = serveapp.OpenChannelRegistration(ctx, opts)
				return native, err
			}
			var out, errOut bytes.Buffer
			code := cliapp.Execute(context.Background(), []string{"--config", path, "channel", "connect", "telegram", "--yes"}, &out, &errOut,
				func(context.Context, cliapp.InvocationRoot, cliapp.ServeOptions) int {
					t.Fatal("registration started serve")
					return 1
				}, nil, factory)
			if code != 0 || reads == 0 || !strings.Contains(out.String(), "Registered telegram account 420079.") {
				t.Fatalf("public registration exit=%d reads=%d out=%s err=%s", code, reads, out.String(), errOut.String())
			}
			if strings.Contains(out.String()+errOut.String(), "test-credential") || strings.Contains(out.String(), "READY") {
				t.Fatal("registration disclosed a token or claimed runtime readiness")
			}
			out.Reset()
			errOut.Reset()
			code = cliapp.Execute(context.Background(), []string{"--config", path, "channel", "connect", "telegram", "420079", "--yes"}, &out, &errOut, nil, nil, factory)
			if code != 0 || !strings.Contains(out.String(), "Reverified telegram account 420079.") {
				t.Fatalf("retained registration exit=%d out=%s err=%s", code, out.String(), errOut.String())
			}
			botID++
			out.Reset()
			errOut.Reset()
			code = cliapp.Execute(context.Background(), []string{"--config", path, "channel", "connect", "telegram", "420079", "--yes"}, &out, &errOut, nil, nil, factory)
			if code == 0 || strings.Contains(out.String(), "Reverified") {
				t.Fatal("reverify accepted a different provider account")
			}
			out.Reset()
			errOut.Reset()
			code = cliapp.Execute(context.Background(), []string{"--config", path, "channel", "list"}, &out, &errOut, nil, nil, factory)
			if code != 0 || strings.Count(out.String(), "telegram 420079") != 1 || strings.Contains(out.String(), "420080") {
				t.Fatalf("deployment readback exit=%d out=%s err=%s", code, out.String(), errOut.String())
			}
			config += "channels:\n  hitl:\n    provider: telegram\n    account: \"420079\"\n"
			if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			botID = 420079
			out.Reset()
			errOut.Reset()
			code = cliapp.Execute(context.Background(), []string{"--config", path, "channel", "connect", "telegram", "420079", "--yes"}, &out, &errOut, nil, nil, factory)
			if code == 0 || confirmations != 1 || polls != 2 || !strings.Contains(out.String(), "Send the code SWARM-") {
				t.Fatalf("failed confirmation was not retained after settlement: exit=%d polls=%d confirmations=%d out=%s err=%s", code, polls, confirmations, out.String(), errOut.String())
			}
			out.Reset()
			errOut.Reset()
			code = cliapp.Execute(context.Background(), []string{"--config", path, "channel", "connect", "telegram", "420079", "--yes"}, &out, &errOut, nil, nil, factory)
			if code != 0 || polls != 2 || confirmations != 2 || !strings.Contains(out.String(), "Claimed by 12*45 in a direct conversation.") ||
				!strings.Contains(out.String(), "Reverified telegram account 420079.") {
				t.Fatalf("pre-serve HITL exit=%d polls=%d out=%s err=%s", code, polls, out.String(), errOut.String())
			}
			out.Reset()
			errOut.Reset()
			code = cliapp.Execute(context.Background(), []string{"--config", path, "channel", "list", "--json"}, &out, &errOut, nil, nil, factory)
			var readback struct {
				Accounts []channelonboarding.Result `json:"accounts"`
			}
			if code != 0 || json.Unmarshal(out.Bytes(), &readback) != nil || len(readback.Accounts) != 1 {
				t.Fatalf("HITL registration readback exit=%d err=%s", code, errOut.String())
			}
			registered := readback.Accounts[0]
			if registered.IdentityOperation == nil || registered.IdentityOperation.State != operatorchannel.StateBound || registered.Binding == nil ||
				registered.Operation.IdentityOperationID != registered.IdentityOperation.OperationID || registered.Binding.OperationID != registered.IdentityOperation.OperationID ||
				registered.Operation.ActivationRevision != 0 || registered.Readiness != nil {
				t.Fatal("operator registration lacks its exact bound identity or acquired runtime authority")
			}
			if err := input.Truncate(0); err != nil {
				t.Fatal(err)
			}
			if _, err := input.Seek(0, 0); err != nil {
				t.Fatal(err)
			}
			if _, err := input.WriteString("420080:other-credential\n"); err != nil {
				t.Fatal(err)
			}
			if _, err := input.Seek(0, 0); err != nil {
				t.Fatal(err)
			}
			botID = 420080
			out.Reset()
			errOut.Reset()
			code = cliapp.Execute(context.Background(), []string{"--config", path, "channel", "connect", "telegram", "--yes"}, &out, &errOut, nil, nil, factory)
			if code != 0 || !strings.Contains(out.String(), "Registered telegram account 420080.") || strings.Contains(out.String(), "Send the code") {
				t.Fatalf("second account exit=%d out=%s err=%s", code, out.String(), errOut.String())
			}
			webhook = true
			config = strings.Replace(config, `"420079"`, `"420080"`, 1)
			if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
				t.Fatal(err)
			}
			out.Reset()
			errOut.Reset()
			code = cliapp.Execute(context.Background(), []string{"--config", path, "channel", "connect", "telegram", "420080", "--yes"}, &out, &errOut, nil, nil, factory)
			if code != 0 || webhookClaims != 2 || polls != 2 || !strings.Contains(out.String(), "Reverified telegram account 420080.") {
				t.Fatalf("existing-holder claim exit=%d claims=%d polls=%d out=%s err=%s", code, webhookClaims, polls, out.String(), errOut.String())
			}
			out.Reset()
			errOut.Reset()
			code = cliapp.Execute(context.Background(), []string{"--config", path, "channel", "logout", "telegram", "420079"}, &out, &errOut, nil, nil, factory)
			if code != 0 || !strings.Contains(out.String(), "Logged out telegram account 420079.") {
				t.Fatalf("exact logout exit=%d out=%s err=%s", code, out.String(), errOut.String())
			}
			out.Reset()
			errOut.Reset()
			code = cliapp.Execute(context.Background(), []string{"--config", path, "channel", "list"}, &out, &errOut, nil, nil, factory)
			if code != 0 || strings.Contains(out.String(), "420079") || strings.Count(out.String(), "telegram 420080") != 1 {
				t.Fatalf("isolated logout readback exit=%d out=%s err=%s", code, out.String(), errOut.String())
			}
			out.Reset()
			errOut.Reset()
			code = cliapp.Execute(context.Background(), []string{"--config", path, "channel", "connect", "telegram", "420080", "--yes"}, &out, &errOut, nil, nil, factory)
			if code != 0 || !strings.Contains(out.String(), "Reverified telegram account 420080.") {
				t.Fatalf("other account lost its credentials exit=%d out=%s err=%s", code, out.String(), errOut.String())
			}
		})
	}
}
