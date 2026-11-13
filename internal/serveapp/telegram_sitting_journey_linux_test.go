package serveapp

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/tests/support/testutil"
	"golang.org/x/sys/unix"
)

type telegramSittingPromptWriter struct {
	io.Writer
	prompts chan string
}

func (w telegramSittingPromptWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if err == nil && (string(p) == "Secret value: " || string(p) == "Confirm secret value: " || string(p) == "Is this the right account? [y/N] ") {
		w.prompts <- string(p)
	}
	return n, err
}

// Only the remote provider, DNS/trust and human terminal actions are doubled.
// The native HTTPS listener, enrollment, activity and logout use shipping owners.
func TestTelegramPublicSittingJourneyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			terminal := sittingTerminal(t)
			provider := &channelOnboardingTelegramProvider{}
			const hostname = "hooks.sitting.test"
			invocation := t.TempDir()
			key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			certificate := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{hostname, "api.telegram.org"},
				NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
				KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
				IsCA: true, BasicConstraintsValid: true}
			der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
			if err != nil {
				t.Fatal(err)
			}
			keyDER, err := x509.MarshalECPrivateKey(key)
			if err != nil {
				t.Fatal(err)
			}
			certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
			keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
			for name, data := range map[string][]byte{"cert.pem": certPEM, "key.pem": keyPEM} {
				if err := os.WriteFile(filepath.Join(invocation, name), data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			pair, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				t.Fatal(err)
			}
			telegram := httptest.NewUnstartedServer(provider)
			telegram.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
			telegram.StartTLS()
			defer telegram.Close()
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(certPEM) {
				t.Fatal("synthetic certificate was not admitted by test trust")
			}
			base := http.DefaultTransport
			transport := base.(*http.Transport).Clone()
			transport.Proxy = nil
			transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
			transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				switch address {
				case hostname + ":443":
					address = "127.0.0.1:443"
				case "api.telegram.org:443":
					address = telegram.Listener.Addr().String()
				default:
					host, _, err := net.SplitHostPort(address)
					if err != nil || !net.ParseIP(host).IsLoopback() {
						return nil, fmt.Errorf("sitting rehearsal refuses external dial %q", address)
					}
				}
				return (&net.Dialer{}).DialContext(ctx, network, address)
			}
			http.DefaultTransport = transport
			t.Cleanup(func() { http.DefaultTransport = base; transport.CloseIdleConnections() })
			source := t.TempDir()
			copyReleaseFixtureTree(t, filepath.Join(repoRootForTest(), "packs/providers/telegram"), filepath.Join(source, "telegram"))
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
inputs: [inbound.telegram.text_message]
`)
			const account = "420079"
			writeStandingCandidateFile(t, filepath.Join(source, "policy.yaml"), "operator_account: '"+account+"'\n")
			writeStandingCandidateFile(t, filepath.Join(source, "nodes.yaml"), `echo:
  inbound.telegram.text_message:
    activity:
      id: live_echo
      tool: telegram.send_message
      input:
        account: policy.operator_account
        chat_id: payload.conversation_reference
        text: payload.text
`)
			var config string
			if backend == "sqlite" {
				config = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, filepath.Join(t.TempDir(), "sitting.sqlite"), channelOnboardingHostWorkspaceFields())
			} else {
				config = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
			}
			content, err := os.ReadFile(config)
			if err != nil {
				t.Fatal(err)
			}
			retainedConfig, ok := strings.CutPrefix(string(content), "runtime:\n  recovery_on_startup: false\n")
			if !ok {
				t.Fatal("runtime config helper no longer has the expected recovery override")
			}
			token := make([]byte, 32)
			if _, err := rand.Read(token); err != nil {
				t.Fatal(err)
			}
			tokenFile := filepath.Join(t.TempDir(), "api.token")
			if err := os.WriteFile(tokenFile, []byte(fmt.Sprintf("%x\n", token)), 0o600); err != nil {
				t.Fatal(err)
			}
			api := "127.0.0.1:" + freeDoctorTCPPort(t)
			writeStandingCandidateFile(t, config, retainedConfig+fmt.Sprintf("\npaths:\n  swarm_dir: %q\nconnection:\n  api_server: http://%s\n  api_token_file: %q\nserve:\n  api_listen_addr: %s\n  api_token_file: %q\n  mcp_listen_addr: 127.0.0.1:0\n  public_webhook:\n    hostname: %s\n    certificate_file: cert.pem\n    private_key_file: key.pem\n", t.TempDir(), api, tokenFile, api, tokenFile, hostname))
			t.Chdir(invocation)
			registration := ChannelRegistrations()
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
			prompts := make(chan string, 3)
			done := make(chan int, 1)
			joined := false
			defer func() {
				cancel()
				if !joined {
					_ = terminal.Close()
					<-done
				}
			}()
			go func() {
				done <- cliapp.Execute(ctx, []string{"--config", config, "channel", "connect", "telegram"}, out,
					telegramSittingPromptWriter{Writer: diagnostic, prompts: prompts}, nil, nil, registration)
			}()
			const secret = "synthetic-sitting-telegram-token"
			for _, want := range []string{"Secret value: ", "Confirm secret value: ", "Is this the right account? [y/N] "} {
				select {
				case prompt := <-prompts:
					if prompt != want {
						t.Fatalf("public prompt %q, want %q", prompt, want)
					}
				case code := <-done:
					joined = true
					t.Fatal("connect exited before its interactive prompt", code, diagnostic.String())
				case <-ctx.Done():
					t.Fatal("connect did not present its interactive prompt", diagnostic.String())
				}
				input := secret
				if strings.HasPrefix(want, "Is this") {
					if !strings.Contains(out.String(), "Linked to "+account) {
						t.Fatal("confirmation omitted exact TEST bot", out.String())
					}
					input = "y"
				} else {
					for {
						state, err := unix.IoctlGetTermios(int(os.Stdin.Fd()), unix.TCGETS)
						if err != nil || ctx.Err() != nil {
							t.Fatal("terminal did not enter hidden input", err, ctx.Err())
						}
						if state.Lflag&unix.ECHO == 0 {
							break
						}
						runtime.Gosched()
					}
				}
				if _, err := terminal.WriteString(input + "\n"); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case code := <-done:
				joined = true
				if code != 0 || !strings.Contains(out.String(), "Registered telegram account "+account+".") || strings.Contains(out.String()+diagnostic.String(), secret) {
					t.Fatal("public hidden-token/account confirmation did not join safely", code, diagnostic.String())
				}
			case <-ctx.Done():
				t.Fatal("confirmed connect did not join", diagnostic.String())
			}
			t.Log("step=hidden_token_and_interactive_account_confirmation PASS")

			process := startRuntimeTestProcessWithRunner(t, source, cliapp.ServeOptions{}, func(ctx context.Context, _ string, opts cliapp.ServeOptions) int {
				return cliapp.Execute(ctx, []string{"--config", config, "serve", source}, opts.Output, opts.ErrorOutput,
					Run, nil, registration)
			})
			process.waitForReadyLine()
			callback, signing, _ := provider.Registration()
			if !strings.HasPrefix(callback, "https://"+hostname+"/webhooks/") || signing == "" {
				t.Fatal("native HTTPS callback was not registered")
			}
			t.Log("step=retained_native_https_serve_ready PASS")
			var listing struct {
				Accounts []channelonboarding.Result `json:"accounts"`
			}
			if err := json.Unmarshal([]byte(command("channel", "list", "--json")), &listing); err != nil || len(listing.Accounts) != 1 || listing.Accounts[0].Operation.AccountReference != account || !listing.Accounts[0].Operation.AccountConfirmed || listing.Accounts[0].Operation.IdentityOperationID != "" {
				t.Fatal("public list did not retain one exact bot without operator enrollment", err)
			}
			if status := command("channel", "status", "telegram", account); !strings.Contains(status, account) {
				t.Fatal("public status omitted exact bot", status)
			}
			_, count := provider.OnboardingCounts()
			if count != 0 {
				t.Fatal("no-agent/no-HITL rehearsal sent output before customer input", count)
			}
			const text = "R130 TG live echo 01"
			body := `{"update_id":13001,"message":{"message_id":1,"from":{"id":1001,"is_bot":false,"first_name":"TEST sender"},"chat":{"id":1001,"type":"private"},"text":"` + text + `"}}`
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, callback, strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Telegram-Bot-Api-Secret-Token", signing)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal("mock provider could not deliver through verified native HTTPS", err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusAccepted {
				t.Fatal("native HTTPS inbound refused", response.StatusCode, process.outputString())
			}
			delivery := waitChannelOnboardingDelivery(t, provider, 0)
			if fmt.Sprint(delivery["chat_id"]) != "1001" || delivery["text"] != text {
				t.Fatal("wrong reply destination or text", delivery)
			}
			t.Log("step=https_inbound_exact_reply PASS")
			if logout := command("channel", "logout", "telegram", account); !strings.Contains(logout, "Logged out telegram account "+account+".") {
				t.Fatal("public deployment logout failed", logout)
			}
			t.Log("step=deployment_logout PASS")
			if code := process.stop(); code != 0 {
				t.Fatal("public serve did not join after logout", code)
			}
			_, count = provider.OnboardingCounts()
			if count != 1 {
				t.Fatal("sitting did not produce exactly one reply", count)
			}
			t.Log("step=single_reply_and_joined_shutdown PASS")
		})
	}
}
