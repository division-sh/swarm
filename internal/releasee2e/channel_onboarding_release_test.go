package releasee2e

import (
	"context"
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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

const (
	releaseChannelAPIToken   = "release-channel-api-token"
	releaseChannelCredential = "release-channel-provider-credential"
	releaseChannelPublicHost = "hooks.release-channel.test"
)

var (
	releaseChannelChallengePattern = regexp.MustCompile(`SWARM-[A-Z2-7]{16}`)
	releaseChannelOperationPattern = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`)
)

func TestChannelOnboardingReleaseBinaryJourneys(t *testing.T) {
	releaseRoot := goldenReleaseRoot(t)
	binaryPath := buildReleaseBinary(t, releaseRoot)
	root := filepath.Join(releaseRoot, "channel-onboarding")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("create release channel project: %v", err)
	}
	contracts := writeReleaseChannelFixture(t, root)
	storePath := filepath.Join(root, "runtime.db")
	configPath := filepath.Join(root, "swarm.yaml")
	writeReleaseFile(t, configPath, releaseChannelRuntimeConfig(storePath))
	tokenFile := filepath.Join(root, "api-token")
	writeReleaseFile(t, tokenFile, releaseChannelAPIToken+"\n")

	provider := &releaseTelegramAPIDouble{}
	apiPort := freeReleaseTCPPort(t)
	mcpPort := freeReleaseTCPPort(t)
	publicPort := freeReleaseTCPPort(t)
	publicListen := fmt.Sprintf("127.0.0.1:%d", publicPort)
	providerEnv := startReleaseTelegramAPI(t, provider, root, publicListen)
	env := appendReleaseChannelEnv(goldenProcessEnv(t, root, "", 1), providerEnv...)
	start := func() *releaseServeProcess {
		process := startReleaseServe(t, releaseProcessSpec{
			BinaryPath: binaryPath, WorkingDir: root, ConfigPath: configPath, Contracts: contracts,
			Store: "sqlite", APIPort: apiPort, MCPPort: mcpPort,
			PublicWebhookBaseURL: "https://" + releaseChannelPublicHost, PublicWebhookListen: publicListen,
			TokenFile: tokenFile, Token: releaseChannelAPIToken, Env: env,
		})
		ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
		defer cancel()
		if err := process.waitReady(ctx); err != nil {
			t.Fatal(err)
		}
		return process
	}

	process := start()
	var connected releaseChannelRow
	var initialBundleHash string
	t.Run("RB-01_golden_path", func(t *testing.T) {
		initialBundleHash = releaseChannelServedBundleHash(t, process.rpc)
		command := startReleaseChannelCommand(t, binaryPath, root, env, releaseChannelCredential, configPath,
			"channel", "connect", "telegram", "--yes", "--api-server", process.apiBase, "--api-token-file", tokenFile)
		challenge := waitReleaseChannelChallenge(t, command)
		callbackURL, signingSecret := waitReleaseChannelRegistration(t, provider, command)
		publishReleaseTelegramClaim(t, callbackURL, publicListen, signingSecret, challenge)
		output := waitReleaseChannelCommand(t, command)
		assertReleaseChannelOutputSecretSafe(t, output, releaseChannelCredential, signingSecret)
		if !strings.Contains(output, "READY") {
			t.Fatalf("release connect output lacks READY:\n%s", output)
		}
		delivery := waitReleaseChannelDelivery(t, provider, 0)
		if delivery["text"] != "Swarm channel connected." {
			t.Fatalf("release connect confirmation = %#v", delivery)
		}
		rows := releaseChannelList(t, binaryPath, root, env, configPath, process.apiBase, tokenFile)
		connected = requireReleaseCurrentChannel(t, rows)
		if connected.Operation == nil || connected.Operation.Phase != "succeeded" || connected.Activation == nil || connected.Readiness == nil || !connected.Readiness.Ready {
			t.Fatalf("release connected readback = %#v", connected)
		}
		human := runReleaseChannelCommand(t, binaryPath, root, env, "", configPath,
			"channel", "list", "--api-server", process.apiBase, "--api-token-file", tokenFile)
		if human.err != nil || !strings.Contains(human.output, "READY") {
			t.Fatalf("release connected human list: %v\n%s", human.err, human.output)
		}
	})

	t.Run("RB-02_recovery_path", func(t *testing.T) {
		if err := process.stopAndWait(10 * time.Second); err != nil {
			t.Fatalf("graceful release serve stop before changed-source restart: %v\n%s", err, process.output.String())
		}
		bumpReleaseChannelSource(t, contracts)
		process = start()
		restartedBundleHash := releaseChannelServedBundleHash(t, process.rpc)
		if restartedBundleHash == initialBundleHash {
			t.Fatalf("changed-source process restart retained bundle hash %s\n%s", initialBundleHash, process.output.String())
		}
		retained := requireReleaseCurrentChannel(t, releaseChannelList(t, binaryPath, root, env, configPath, process.apiBase, tokenFile))
		if retained.Activation != nil || retained.Readiness != nil || retained.Recovery == nil || len(retained.Recovery.Commands) != 1 || retained.Recovery.Commands[0] != "swarm channel reconnect telegram" {
			t.Fatalf("release retained identity after changed-source restart %s -> %s = %#v\n%s", initialBundleHash, restartedBundleHash, retained, process.output.String())
		}
		beforeRegistrations, beforeDeliveries := provider.Counts()
		blocked := runReleaseChannelCommand(t, binaryPath, root, env, "", configPath,
			"channel", "reconnect", "telegram", "--yes", "--api-server", process.apiBase, "--api-token-file", tokenFile)
		operationID := releaseChannelOperationPattern.FindString(blocked.output)
		resumeCommand := "swarm channel resume " + operationID + " --credential-stdin"
		if blocked.err == nil || operationID == "" || !strings.Contains(blocked.output, "CHANNEL_CREDENTIAL_REQUIRED") || !strings.Contains(blocked.output, resumeCommand) {
			t.Fatalf("release credential-required result: %v\n%s", blocked.err, blocked.output)
		}
		blockedRows := releaseChannelList(t, binaryPath, root, env, configPath, process.apiBase, tokenFile)
		blockedRow := requireReleaseOperation(t, blockedRows, operationID)
		if blockedRow.Operation.Phase != "preparing" || blockedRow.Operation.Coordinate.RuntimeInstanceID == "" || blockedRow.Activation != nil || blockedRow.Recovery == nil ||
			len(blockedRow.Recovery.Commands) != 1 || blockedRow.Recovery.Commands[0] != resumeCommand {
			t.Fatalf("release blocked readback = %#v", blockedRow)
		}
		if registrations, deliveries := provider.Counts(); registrations != beforeRegistrations || deliveries != beforeDeliveries {
			t.Fatalf("release blocked operation changed provider effects: registrations=%d/%d deliveries=%d/%d", registrations, beforeRegistrations, deliveries, beforeDeliveries)
		}

		if err := process.stopAndWait(10 * time.Second); err != nil {
			t.Fatalf("graceful release serve stop: %v\n%s", err, process.output.String())
		}
		process = start()
		restartedRows := releaseChannelList(t, binaryPath, root, env, configPath, process.apiBase, tokenFile)
		restarted := requireReleaseOperation(t, restartedRows, operationID)
		if restarted.Operation.Phase != "preparing" || restarted.Operation.Coordinate.RuntimeInstanceID == blockedRow.Operation.Coordinate.RuntimeInstanceID || restarted.Recovery == nil ||
			len(restarted.Recovery.Commands) != 1 || restarted.Recovery.Commands[0] != resumeCommand {
			t.Fatalf("release restarted blocked readback = %#v, predecessor runtime=%s", restarted, blockedRow.Operation.Coordinate.RuntimeInstanceID)
		}
		if registrations, deliveries := provider.Counts(); registrations != beforeRegistrations || deliveries != beforeDeliveries {
			t.Fatalf("release restart replayed provider effects: registrations=%d/%d deliveries=%d/%d", registrations, beforeRegistrations, deliveries, beforeDeliveries)
		}

		resumeArgs := append(strings.Fields(resumeCommand)[1:], "--yes", "--api-server", process.apiBase, "--api-token-file", tokenFile)
		resumed := runReleaseChannelCommand(t, binaryPath, root, env, releaseChannelCredential, configPath, resumeArgs...)
		if resumed.err != nil || !strings.Contains(resumed.output, "READY") {
			t.Fatalf("release printed resume command failed: %v\n%s", resumed.err, resumed.output)
		}
		assertReleaseChannelOutputSecretSafe(t, resumed.output, releaseChannelCredential)
		delivery := waitReleaseChannelDelivery(t, provider, beforeDeliveries)
		if delivery["text"] != "Swarm channel connected." {
			t.Fatalf("release resumed confirmation = %#v", delivery)
		}
		ready := requireReleaseOperation(t, releaseChannelList(t, binaryPath, root, env, configPath, process.apiBase, tokenFile), operationID)
		if ready.Operation.Phase != "succeeded" || ready.Activation == nil || ready.Readiness == nil || !ready.Readiness.Ready {
			t.Fatalf("release resumed readback = %#v", ready)
		}
		if registrations, deliveries := provider.Counts(); registrations != beforeRegistrations+1 || deliveries != beforeDeliveries+1 {
			t.Fatalf("release resume effects registrations=%d deliveries=%d, want %d/%d", registrations, deliveries, beforeRegistrations+1, beforeDeliveries+1)
		}
	})

	if err := process.stopAndWait(10 * time.Second); err != nil {
		t.Fatalf("final release serve stop: %v\n%s", err, process.output.String())
	}
}

type releaseChannelRow struct {
	Identity struct {
		Interface struct {
			Selector string `json:"selector"`
		} `json:"interface"`
		Status string `json:"status"`
	} `json:"identity"`
	Operation *struct {
		OperationID string `json:"operation_id"`
		Phase       string `json:"phase"`
		Coordinate  struct {
			RuntimeInstanceID string `json:"runtime_instance_id"`
		} `json:"coordinate"`
	} `json:"operation"`
	Activation any `json:"activation"`
	Readiness  *struct {
		Ready bool `json:"ready"`
	} `json:"readiness"`
	Recovery *struct {
		Commands []string `json:"commands"`
	} `json:"recovery"`
}

type releaseChannelCommand struct {
	cmd    *exec.Cmd
	output *releaseProcessOutput
	done   chan error
}

func startReleaseChannelCommand(t *testing.T, binaryPath, root string, env []string, stdin, configPath string, args ...string) *releaseChannelCommand {
	t.Helper()
	fullArgs := append([]string{"--config", configPath}, args...)
	cmd := exec.Command(binaryPath, fullArgs...)
	cmd.Dir = root
	cmd.Env = env
	cmd.Stdin = strings.NewReader(stdin + "\n")
	output := &releaseProcessOutput{}
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start release channel command: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	return &releaseChannelCommand{cmd: cmd, output: output, done: done}
}

func waitReleaseChannelCommand(t *testing.T, command *releaseChannelCommand) string {
	t.Helper()
	select {
	case err := <-command.done:
		if err != nil {
			t.Fatalf("release channel command failed: %v\n%s", err, command.output.String())
		}
		return command.output.String()
	case <-time.After(30 * time.Second):
		_ = command.cmd.Process.Kill()
		t.Fatalf("release channel command timed out:\n%s", command.output.String())
		return ""
	}
}

func waitReleaseChannelChallenge(t *testing.T, command *releaseChannelCommand) string {
	t.Helper()
	return waitReleaseChannelValue(t, command, "challenge", func() string {
		return releaseChannelChallengePattern.FindString(command.output.String())
	})
}

func waitReleaseChannelRegistration(t *testing.T, provider *releaseTelegramAPIDouble, command *releaseChannelCommand) (string, string) {
	t.Helper()
	value := waitReleaseChannelValue(t, command, "registration", func() string {
		callbackURL, signingSecret, _ := provider.Registration()
		if callbackURL == "" || signingSecret == "" {
			return ""
		}
		return callbackURL + "\n" + signingSecret
	})
	callbackURL, signingSecret, _ := strings.Cut(value, "\n")
	return callbackURL, signingSecret
}

func waitReleaseChannelValue(t *testing.T, command *releaseChannelCommand, label string, read func() string) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if value := read(); value != "" {
			return value
		}
		select {
		case err := <-command.done:
			t.Fatalf("release channel command exited before %s: %v\n%s", label, err, command.output.String())
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for release channel %s:\n%s", label, command.output.String())
	return ""
}

func publishReleaseTelegramClaim(t *testing.T, callbackURL, publicListen, signingSecret, challenge string) {
	t.Helper()
	callback, err := url.Parse(callbackURL)
	if err != nil {
		t.Fatalf("parse release callback: %v", err)
	}
	callback.Scheme = "http"
	callback.Host = publicListen
	body, err := json.Marshal(map[string]any{
		"update_id": time.Now().UnixNano(),
		"message": map[string]any{
			"message_id": 1,
			"from":       map[string]any{"id": 7001, "username": "release_operator"},
			"chat":       map[string]any{"id": 1001, "type": "private"},
			"text":       challenge,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, callback.String(), strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", signingSecret)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("publish release Telegram claim: %v", err)
	}
	defer response.Body.Close()
	responseBody, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusAccepted || !strings.Contains(string(responseBody), `"operator_channel_claim_disposition":"consumed_by_binding"`) {
		t.Fatalf("release claim status/body = %d/%s", response.StatusCode, responseBody)
	}
}

func waitReleaseChannelDelivery(t *testing.T, provider *releaseTelegramAPIDouble, index int) map[string]any {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if delivery := provider.Delivery(index); delivery != nil {
			return delivery
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for release Telegram delivery %d", index)
	return nil
}

func runReleaseChannelCommand(t *testing.T, binaryPath, root string, env []string, stdin, configPath string, args ...string) releaseCommandResult {
	t.Helper()
	return runReleaseCommand(t, 30*time.Second, root, env, stdin, binaryPath, append([]string{"--config", configPath}, args...)...)
}

func releaseChannelList(t *testing.T, binaryPath, root string, env []string, configPath, apiBase, tokenFile string) []releaseChannelRow {
	t.Helper()
	result := runReleaseChannelCommand(t, binaryPath, root, env, "", configPath,
		"channel", "list", "--json", "--api-server", apiBase, "--api-token-file", tokenFile)
	if result.err != nil {
		t.Fatalf("release channel list: %v\n%s", result.err, result.output)
	}
	var envelope struct {
		Channels []releaseChannelRow `json:"channels"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(result.output)), &envelope); err != nil {
		t.Fatalf("decode release channel list: %v\n%s", err, result.output)
	}
	return envelope.Channels
}

func requireReleaseCurrentChannel(t *testing.T, rows []releaseChannelRow) releaseChannelRow {
	t.Helper()
	for _, row := range rows {
		if row.Identity.Status == "current" {
			return row
		}
	}
	t.Fatalf("release channel list has no current row: %#v", rows)
	return releaseChannelRow{}
}

func requireReleaseOperation(t *testing.T, rows []releaseChannelRow, operationID string) releaseChannelRow {
	t.Helper()
	for _, row := range rows {
		if row.Operation != nil && row.Operation.OperationID == operationID {
			return row
		}
	}
	t.Fatalf("release channel list has no operation %s: %#v", operationID, rows)
	return releaseChannelRow{}
}

func releaseChannelServedBundleHash(t *testing.T, rpc *releaseRPCClient) string {
	t.Helper()
	var health struct {
		Ready  bool `json:"ready"`
		Bundle struct {
			BundleHash string `json:"bundle_hash"`
		} `json:"bundle"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rpc.call(ctx, "health.check", map[string]any{}, &health); err != nil {
		t.Fatalf("release channel health.check: %v", err)
	}
	if !health.Ready || health.Bundle.BundleHash == "" {
		t.Fatalf("release channel health.check = %#v", health)
	}
	return health.Bundle.BundleHash
}

func assertReleaseChannelOutputSecretSafe(t *testing.T, output string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if secret != "" && strings.Contains(output, secret) {
			t.Fatalf("release channel output leaked secret:\n%s", output)
		}
	}
}

func writeReleaseChannelFixture(t *testing.T, root string) string {
	t.Helper()
	contracts := filepath.Join(root, "contracts")
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "examples", "integrations", "telegram-agent"), contracts)
	copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "internal", "releasee2e", "testdata", "channel_onboarding_release"), contracts)
	for _, relative := range []string{
		filepath.Join("bot", "flows", "telegram-chat", "agents.yaml"),
		filepath.Join("bot", "flows", "telegram-chat", "nodes.yaml"),
		filepath.Join("bot", "flows", "telegram-chat", "events.yaml"),
	} {
		if err := os.Remove(filepath.Join(contracts, relative)); err != nil {
			t.Fatalf("remove release channel consumer %s: %v", relative, err)
		}
	}
	return contracts
}

func releaseChannelRuntimeConfig(storePath string) string {
	return "runtime:\n" +
		"  execution_posture: live\n" +
		"  recovery_on_startup: true\n" +
		"workspace:\n" +
		"  backend: host\n" +
		"  allow_exec_on_host: true\n" +
		"store:\n" +
		"  backend: sqlite\n" +
		"  sqlite:\n" +
		"    path: " + fmt.Sprintf("%q", storePath) + "\n" +
		"llm:\n" +
		"  backend: claude_cli\n"
}

func bumpReleaseChannelSource(t *testing.T, contracts string) {
	t.Helper()
	path := filepath.Join(contracts, releaseChannelManifestName())
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read release channel package for changed-source restart: %v", err)
	}
	updated := strings.Replace(string(body), `version: "1.0.0"`, `version: "1.0.1"`, 1)
	if updated == string(body) {
		t.Fatal("release channel package has no exact version declaration to replace")
	}
	writeReleaseFile(t, path, updated)
}

func releaseChannelManifestName() string {
	return strings.Join([]string{"package", "yaml"}, ".")
}

func startReleaseTelegramAPI(t *testing.T, handler http.Handler, root, publicListen string) []string {
	t.Helper()
	certificate, trustPEM := releaseTelegramCertificate(t)
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	publicOrigin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		upstream := *request.URL
		upstream.Scheme = "http"
		upstream.Host = publicListen
		forward, err := http.NewRequestWithContext(request.Context(), request.Method, upstream.String(), request.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		forward.Header = request.Header.Clone()
		response, err := http.DefaultTransport.RoundTrip(forward)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		for key, values := range response.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	}))
	publicOrigin.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	publicOrigin.StartTLS()
	t.Cleanup(publicOrigin.Close)
	upstreams := map[string]string{
		"api.telegram.org:443":            strings.TrimPrefix(server.URL, "https://"),
		releaseChannelPublicHost + ":443": strings.TrimPrefix(publicOrigin.URL, "https://"),
	}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		upstream, admitted := upstreams[request.Host]
		if request.Method != http.MethodConnect || !admitted {
			http.Error(w, "release channel proxy rejected target", http.StatusForbidden)
			return
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "CONNECT hijacking unavailable", http.StatusInternalServerError)
			return
		}
		client, _, err := hijacker.Hijack()
		if err != nil {
			return
		}
		provider, err := net.Dial("tcp", upstream)
		if err != nil {
			_ = client.Close()
			return
		}
		_, _ = client.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
		go func() {
			_, _ = io.Copy(provider, client)
			_ = provider.Close()
		}()
		_, _ = io.Copy(client, provider)
		_ = client.Close()
	}))
	t.Cleanup(proxy.Close)
	trustPath := filepath.Join(root, "telegram-test-ca.pem")
	writeReleaseFile(t, trustPath, string(trustPEM))
	return []string{
		"HTTPS_PROXY=" + proxy.URL,
		"HTTP_PROXY=",
		"ALL_PROXY=",
		"NO_PROXY=127.0.0.1,localhost",
		"SSL_CERT_FILE=" + trustPath,
	}
}

func releaseTelegramCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	now := time.Now().UTC()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Swarm release Telegram test CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "api.telegram.org"},
		DNSNames: []string{"api.telegram.org", releaseChannelPublicHost}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, ca, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
}

func appendReleaseChannelEnv(base []string, overrides ...string) []string {
	keys := make(map[string]struct{}, len(overrides))
	for _, entry := range overrides {
		key, _, _ := strings.Cut(entry, "=")
		keys[key] = struct{}{}
	}
	result := make([]string, 0, len(base)+len(overrides))
	for _, entry := range base {
		key, _, _ := strings.Cut(entry, "=")
		if _, replaced := keys[key]; !replaced {
			result = append(result, entry)
		}
	}
	return append(result, overrides...)
}
