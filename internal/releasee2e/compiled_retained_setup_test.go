package releasee2e

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/tests/support/testutil/telegramapi"
	"github.com/division-sh/swarm/tests/support/testutil/testprocess"
)

func writeJoinedRetainedSource(t *testing.T, root string) string {
	t.Helper()
	source := filepath.Join(root, "contracts")
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), numericScatterSource), source)
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/full_lifecycle/standing_telegram/telegram"), filepath.Join(source, "telegram"))
	writeReleaseFile(t, filepath.Join(source, "schema.yaml"), `stages: {done: {final: true}}
schedules:
  continuity-clock: {every: 24h, emit: continuity.tick}
inputs: [item.registered, control.seed, control.review, control.finish, work.requested, retry.requested, dead.requested]
outputs: [continuity.tick]
connect:
  - {event: item.registered, from: ., to: registry, resolution: select-or-create}
  - {event: control.seed, from: ., to: control, resolution: select-or-create}
  - {event: control.review, from: ., to: control}
  - {event: control.finish, from: ., to: control}
  - {event: work.requested, from: ., to: control}
  - {event: retry.requested, from: ., to: control}
  - {event: dead.requested, from: ., to: control}
`)
	rootEvents := filepath.Join(source, "events.yaml")
	raw, err := os.ReadFile(rootEvents)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"control.seed", "control.review", "control.finish", "work.requested", "retry.requested", "dead.requested"} {
		raw = append(raw, []byte(name+":\n  _key: key\n  key: text\n")...)
		if name == "work.requested" {
			raw = append(raw, []byte("  note: text\n")...)
		}
	}
	writeReleaseFile(t, rootEvents, string(raw)+"continuity.tick:\n")
	for path, body := range map[string]string{
		"control/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
		"control/schema.yaml": `instance: key
stages:
  waiting:
    timers:
      - {after: 2160h, advances_to: done}
  review:
    gate:
      decision: continuity
      outcomes:
        approve: {advances_to: done}
        reject: {advances_to: done}
  done: {final: true}
inputs: [control.seed, control.review, control.finish, work.requested, retry.requested, dead.requested]
`,
		"control/state.yaml":  "key: text\n",
		"control/policy.yaml": "handler_retry_base_seconds: 3600\n",
		"control/nodes.yaml": `owner:
  control.seed:
    data_accumulation:
      writes: [{source_field: key, target_field: key}]
  control.review: {advances_to: review}
  control.finish: {advances_to: done}
`,
		"control/agents.yaml": `worker:
  role: continuity_worker
  intent: prompts/worker.md
  model: memory
  memory: true
  tools: []
  subscriptions: [work.requested]
retry-worker:
  role: retry_worker
  intent: prompts/retry-worker.md
  model: retry
  tools: []
  subscriptions: [retry.requested]
dead-worker:
  role: dead_worker
  intent: prompts/dead-worker.md
  model: dead
  tools: []
  subscriptions: [dead.requested]
`,
		"control/prompts/worker.md":       "Remember the synthetic marker supplied in the first event. Recall it on later turns.\n",
		"control/prompts/retry-worker.md": "Handle the synthetic retry request.\n",
		"control/prompts/dead-worker.md":  "Handle the synthetic refused request.\n",
	} {
		writeReleaseFile(t, filepath.Join(source, path), body)
	}
	return source
}

// Ordinary TLS trust and CONNECT proxy configuration reach the existing double
// from unmodified child binaries. Only the synthetic Telegram endpoint is routed.
func joinedTelegramTransport(t *testing.T, root string, provider *telegramapi.Double) []string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "synthetic Telegram"},
		DNSNames: []string{"api.telegram.org"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewUnstartedServer(provider)
	upstream.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12}
	upstream.StartTLS()
	t.Cleanup(upstream.Close)
	ca := filepath.Join(root, "synthetic-telegram-ca.pem")
	writeReleaseFile(t, ca, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
	var mu sync.Mutex
	connections := map[net.Conn]bool{}
	var joined sync.WaitGroup
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != "api.telegram.org:443" {
			http.Error(w, "fixture proxy refuses non-Telegram traffic", http.StatusForbidden)
			return
		}
		remote, err := net.Dial("tcp", upstream.Listener.Addr().String())
		if err != nil {
			http.Error(w, "fixture endpoint unavailable", http.StatusBadGateway)
			return
		}
		client, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			remote.Close()
			return
		}
		mu.Lock()
		connections[client], connections[remote] = true, true
		mu.Unlock()
		joined.Add(1)
		defer joined.Done()
		defer func() {
			client.Close()
			remote.Close()
			mu.Lock()
			delete(connections, client)
			delete(connections, remote)
			mu.Unlock()
		}()
		if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		if err := buffered.Flush(); err != nil {
			return
		}
		copied := make(chan struct{})
		go func() { io.Copy(remote, buffered); remote.Close(); close(copied) }()
		io.Copy(client, remote)
		client.Close()
		<-copied
	}))
	t.Cleanup(func() {
		proxy.Close()
		mu.Lock()
		for connection := range connections {
			connection.Close()
		}
		mu.Unlock()
		joined.Wait()
	})
	return []string{"HTTPS_PROXY=" + proxy.URL, "HTTP_PROXY=" + proxy.URL, "NO_PROXY=127.0.0.1,localhost", "SSL_CERT_FILE=" + ca}
}

func joinedRegisterTelegram(t *testing.T, s *joinedRetainedScenario) {
	t.Helper()
	output := &releaseProcessOutput{secrets: []string{"joined-bot-token"}}
	cmd := exec.CommandContext(s.ctx, s.binary, "--config", s.spec.ConfigPath, "channel", "connect", "telegram", "--yes",
		"--api-server", s.process.apiBase, "--api-token-file", "api-token", "--no-color")
	cmd.Dir, cmd.Env = s.spec.WorkingDir, s.spec.Env
	cmd.Stdin, cmd.Stdout, cmd.Stderr = strings.NewReader("joined-bot-token\n"), output, output
	if err := testprocess.Start(cmd); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	complete := false
	defer func() {
		if !complete {
			cmd.Process.Kill()
			<-done
		}
	}()
	code := regexp.MustCompile(`Send the code ([^ ]+) to @SwarmTestBot before `)
	if err := pollReleaseCondition(s.ctx, 10*time.Millisecond, func() (bool, error) {
		if match := code.FindStringSubmatch(output.String()); len(match) == 2 {
			s.telegram.QueueUpdate("joined-bot-token", map[string]any{"update_id": 1, "message": map[string]any{
				"message_id": 1, "date": time.Now().Unix(), "text": match[1], "from": map[string]any{"id": 73001, "is_bot": false, "first_name": "Synthetic"},
				"chat": map[string]any{"id": 73001, "type": "private"}}})
			return true, nil
		}
		select {
		case err := <-done:
			complete = true
			return false, fmt.Errorf("connect ended before authenticated claim: %v\n%s", err, output.String())
		default:
			return false, nil
		}
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		complete = true
		if err != nil {
			t.Fatalf("public deployment registration: %v\n%s", err, output.String())
		}
	case <-s.ctx.Done():
		t.Fatal("public deployment registration did not complete")
	}
	joinedChannelReadback(t, s)
}

func joinedChannelReadback(t *testing.T, s *joinedRetainedScenario) string {
	t.Helper()
	result := runReleaseCommand(t, goldenStartupTimeout, s.spec.WorkingDir, s.spec.Env, "", s.binary,
		"--config", s.spec.ConfigPath, "channel", "list", "--json", "--api-server", s.process.apiBase, "--api-token-file", "api-token")
	var page struct {
		Accounts []struct {
			Operation struct {
				OperationID      string `json:"operation_id"`
				AccountReference string `json:"operator_account"`
				AccountConfirmed bool   `json:"account_confirmed"`
				Phase            string `json:"phase"`
			} `json:"operation"`
			Binding map[string]any `json:"binding"`
		} `json:"accounts"`
	}
	if err := json.Unmarshal([]byte(result.output), &page); result.err != nil || err != nil || len(page.Accounts) != 1 {
		t.Fatalf("registered-channel public readback: command=%v decode=%v\n%s", result.err, err, result.output)
	}
	account := page.Accounts[0]
	if account.Operation.OperationID == "" || account.Operation.AccountReference != "420079" || !account.Operation.AccountConfirmed || account.Operation.Phase != "succeeded" || account.Binding == nil {
		t.Fatalf("registered-channel identity/binding absent: %s", result.output)
	}
	return joinedJSON(account)
}
