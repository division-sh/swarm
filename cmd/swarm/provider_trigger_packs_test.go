package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store"
	"gopkg.in/yaml.v3"
)

func TestProviderTriggerReleaseLayoutLoadsCompleteFilesystemInventory(t *testing.T) {
	releaseRoot := t.TempDir()
	binaryPath := filepath.Join(releaseRoot, "swarm")
	build := exec.Command("go", "build", "-o", binaryPath, "./cmd/swarm")
	build.Dir = repoRoot()
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build release-shaped swarm binary: %v\n%s", err, output)
	}
	copyProviderTriggerReleaseTree(t, filepath.Join(repoRoot(), "packs", "provider-triggers"), filepath.Join(releaseRoot, "packs", "provider-triggers"))
	copyProviderTriggerReleaseTree(t, filepath.Join(repoRoot(), "tests", "tier8-boot-verification", "test-boot-success"), filepath.Join(releaseRoot, "contracts"))
	platformSpecBody, err := os.ReadFile(filepath.Join(repoRoot(), "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := os.WriteFile(filepath.Join(releaseRoot, "platform-spec.yaml"), platformSpecBody, 0o644); err != nil {
		t.Fatalf("write release platform spec: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(releaseRoot, "data"), 0o755); err != nil {
		t.Fatalf("mkdir release data: %v", err)
	}
	sqlitePath := filepath.Join(releaseRoot, "runtime.db")
	seedReleaseProviderTriggerStore(t, platformSpecBody, sqlitePath)
	configPath := filepath.Join(releaseRoot, "platform-config.yaml")
	writeReleaseProviderTriggerConfig(t, configPath, sqlitePath)

	runDoctor := func() (string, error) {
		cmd := exec.Command(binaryPath,
			"doctor",
			"--config", configPath,
			"--contracts", filepath.Join(releaseRoot, "contracts"),
			"--platform-spec", filepath.Join(releaseRoot, "platform-spec.yaml"),
			"--data", filepath.Join(releaseRoot, "data"),
			"--workspace-backend", "host",
			"--api-listen-addr", "127.0.0.1:0",
			"--mcp-listen-addr", "127.0.0.1:0",
			"--json",
		)
		cmd.Dir = releaseRoot
		cmd.Env = append(os.Environ(), "SWARM_CONFIG=", "SWARM_CREDENTIALS_FILE=", "SWARM_MANAGED_CREDENTIALS_FILE=", "SWARM_TEST_POSTGRES_DSN=", "PGPASSWORD=", "CLAUDE_CODE_OAUTH_TOKEN=")
		output, err := cmd.CombinedOutput()
		return string(output), err
	}

	output, err := runDoctor()
	if err != nil {
		t.Fatalf("release-shaped doctor failed: %v\n%s", err, output)
	}
	for _, provider := range []string{"github", "intercom", "shopify", "slack", "stripe", "telegram", "twilio", "typeform"} {
		for _, want := range []string{"provider_trigger_pack_" + provider, "provenance=platform", filepath.Join("packs", "provider-triggers", provider)} {
			if !strings.Contains(output, want) {
				t.Fatalf("release doctor missing %q for %s:\n%s", want, provider, output)
			}
		}
	}
	serveReleaseProviderTriggerRequests(t, binaryPath, releaseRoot, configPath)

	if err := os.RemoveAll(filepath.Join(releaseRoot, "packs", "provider-triggers", "stripe")); err != nil {
		t.Fatalf("remove declared Stripe pack: %v", err)
	}
	output, err = runDoctor()
	if err == nil {
		t.Fatalf("release doctor accepted missing declared Stripe pack:\n%s", output)
	}
	for _, want := range []string{"provider_trigger_pack_load_failed", "stripe", "pack.yaml"} {
		if !strings.Contains(output, want) {
			t.Fatalf("missing-pack release failure lacks %q:\n%s", want, output)
		}
	}
}

func TestProviderTriggerPlatformDirsAreElevated(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	writeRuntimeConfigText(t, filepath.Join(repo, "swarm.yaml"), strings.Join([]string{
		"provider_triggers:",
		"  packs:",
		"    platform_dirs:",
		"      - ./packs/provider-triggers/github",
	}, "\n")+"\n")

	_, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{RepoRoot: repo})
	if err == nil {
		t.Fatal("project platform_dirs passed elevated trust admission")
	}
	for _, want := range []string{"provider_triggers.packs.platform_dirs", "not allowed in project_config", "move this key"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("platform_dirs trust error = %q, want containing %q", err, want)
		}
	}
}

func TestProviderTriggerPackDirsResolveFromEffectiveDeclaringLayers(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	projectExternal := filepath.Join(repo, "project-external")
	copyProviderTriggerPackFixture(t, "stripe", projectExternal, true)
	localDir := filepath.Join(repo, ".swarm")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatalf("mkdir local config dir: %v", err)
	}
	localPlatform := filepath.Join(localDir, "platform-github")
	copyProviderTriggerPackFixture(t, "github", localPlatform, false)

	writeRuntimeConfigText(t, filepath.Join(repo, "swarm.yaml"), strings.Join([]string{
		"provider_triggers:",
		"  packs:",
		"    external_dirs:",
		"      - ./project-external",
	}, "\n")+"\n")
	writeRuntimeConfigText(t, filepath.Join(localDir, "swarm.yaml"), strings.Join([]string{
		"provider_triggers:",
		"  packs:",
		"    platform_dirs:",
		"      - ./platform-github",
	}, "\n")+"\n")
	explicitDir := t.TempDir()
	explicitPath := filepath.Join(explicitDir, "explicit.yaml")
	writeRuntimeConfigText(t, explicitPath, "runtime:\n  recovery_on_startup: false\n")

	cfgResult, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicitPath})
	if err != nil {
		t.Fatalf("load layered config: %v", err)
	}
	loaded, err := loadConfiguredProviderTriggerPacks(repo, cfgResult)
	if err != nil {
		t.Fatalf("load configured provider trigger packs: %v", err)
	}
	if len(loaded.PlatformDirs) != 1 || loaded.PlatformDirs[0] != localPlatform {
		t.Fatalf("platform dirs = %v, want declaring local layer path %s", loaded.PlatformDirs, localPlatform)
	}
	if len(loaded.ExternalDirs) != 1 || loaded.ExternalDirs[0] != projectExternal {
		t.Fatalf("external dirs = %v, want declaring project layer path %s", loaded.ExternalDirs, projectExternal)
	}
	if got := cfgResult.KeyOrigins["provider_triggers.packs.platform_dirs"]; got.Path != filepath.Join(localDir, "swarm.yaml") || got.Layer != unifiedLayerLocalOperator {
		t.Fatalf("platform key origin = %+v", got)
	}
	if got := cfgResult.KeyOrigins["provider_triggers.packs.external_dirs"]; got.Path != filepath.Join(repo, "swarm.yaml") || got.Layer != unifiedLayerProject {
		t.Fatalf("external key origin = %+v", got)
	}
}

func copyProviderTriggerPackFixture(t *testing.T, provider, target string, external bool) {
	t.Helper()
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("mkdir pack fixture: %v", err)
	}
	source := filepath.Join(repoRoot(), "packs", "provider-triggers", provider)
	for _, name := range []string{"pack.yaml", "trigger.yaml"} {
		body, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			t.Fatalf("read %s fixture: %v", name, err)
		}
		if external && name == "pack.yaml" {
			body = []byte(strings.Replace(string(body), "source: platform", "source: external", 1))
		}
		if err := os.WriteFile(filepath.Join(target, name), body, 0o644); err != nil {
			t.Fatalf("write %s fixture: %v", name, err)
		}
	}
}

func writeReleaseProviderTriggerConfig(t *testing.T, path, sqlitePath string) {
	t.Helper()
	lines := []string{
		"store:",
		"  backend: sqlite",
		"  sqlite:",
		"    path: " + strconv.Quote(sqlitePath),
		"provider_triggers:",
		"  packs:",
		"    platform_dirs:",
	}
	for _, provider := range []string{"github", "intercom", "shopify", "slack", "stripe", "telegram", "twilio", "typeform"} {
		lines = append(lines, "      - packs/provider-triggers/"+provider)
	}
	writeRuntimeConfigText(t, path, strings.Join(lines, "\n")+"\n")
}

func seedReleaseProviderTriggerStore(t *testing.T, platformSpecBody []byte, sqlitePath string) {
	t.Helper()
	var platformSpec runtimecontracts.PlatformSpecDocument
	if err := yaml.Unmarshal(platformSpecBody, &platformSpec); err != nil {
		t.Fatalf("parse release platform spec: %v", err)
	}
	plans, err := store.GeneratePlatformTableDDLs(platformSpec)
	if err != nil {
		t.Fatalf("generate release platform tables: %v", err)
	}
	sqliteStore, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("create release SQLite store: %v", err)
	}
	ctx := context.Background()
	if err := sqliteStore.EnsureSchemaTables(ctx, plans); err != nil {
		_ = sqliteStore.Close()
		t.Fatalf("initialize release SQLite store: %v", err)
	}
	seedProviderTriggerSmokeRuntime(t, runtimecorrelation.WithRunID(ctx, "76000000-0000-0000-0000-000000000001"), sqliteStore,
		"76000000-0000-0000-0000-000000000001", "76000000-0000-0000-0000-000000000002", "release-stripe", "stripe-customer", "stripe", "stripe-release-secret", "release-stripe-agent")
	seedProviderTriggerSmokeRuntime(t, runtimecorrelation.WithRunID(ctx, "76000000-0000-0000-0000-000000000003"), sqliteStore,
		"76000000-0000-0000-0000-000000000003", "76000000-0000-0000-0000-000000000004", "release-slack", "slack-customer", "slack", "slack-release-secret", "release-slack-agent")
	if err := sqliteStore.Close(); err != nil {
		t.Fatalf("close seeded release SQLite store: %v", err)
	}
}

func serveReleaseProviderTriggerRequests(t *testing.T, binaryPath, releaseRoot, configPath string) {
	t.Helper()
	apiAddr := reserveReleaseTestAddress(t)
	mcpAddr := reserveReleaseTestAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, binaryPath,
		"serve",
		"--config", configPath,
		"--contracts", filepath.Join(releaseRoot, "contracts"),
		"--platform-spec", filepath.Join(releaseRoot, "platform-spec.yaml"),
		"--data", filepath.Join(releaseRoot, "data"),
		"--workspace-backend", "host",
		"--store", "sqlite",
		"--api-listen-addr", apiAddr,
		"--mcp-listen-addr", mcpAddr,
		"--require-bundle-match=false",
		"--shutdown-grace", "1s",
	)
	cmd.Dir = releaseRoot
	cmd.Env = append(os.Environ(), "SWARM_CONFIG=", "SWARM_CREDENTIALS_FILE=", "SWARM_MANAGED_CREDENTIALS_FILE=", "SWARM_TEST_POSTGRES_DSN=", "PGPASSWORD=", "CLAUDE_CODE_OAUTH_TOKEN=")
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start release-shaped serve: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
		}
	}()

	baseURL := "http://" + apiAddr
	deadline := time.Now().Add(20 * time.Second)
	for {
		response, err := http.Get(baseURL + "/readyz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case err := <-done:
			t.Fatalf("release-shaped serve exited before readiness: %v\n%s", err, output.String())
		default:
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("release-shaped serve did not become ready:\n%s", output.String())
		}
		time.Sleep(100 * time.Millisecond)
	}

	now := time.Now().UTC()
	stripeBody := []byte(`{"id":"evt_release_1","type":"payment_intent.succeeded","data":{"object":{"id":"pi_release"}}}`)
	stripeTimestamp := strconv.FormatInt(now.Unix(), 10)
	stripeRequest, err := http.NewRequest(http.MethodPost, baseURL+"/webhooks/stripe-customer/stripe", bytes.NewReader(stripeBody))
	if err != nil {
		t.Fatalf("build Stripe release request: %v", err)
	}
	stripeRequest.Header.Set("Content-Type", "application/json")
	stripeRequest.Header.Set("Stripe-Signature", "t="+stripeTimestamp+",v1="+releaseHMACSHA256Hex("stripe-release-secret", []byte(stripeTimestamp+"."+string(stripeBody))))
	assertReleaseProviderResponse(t, stripeRequest, http.StatusAccepted, "accepted")

	slackTimestamp := strconv.FormatInt(now.Unix(), 10)
	slackBody := []byte(`{"type":"event_callback","event_id":"EvRelease1","event":{"type":"message","text":"hello"}}`)
	slackRequest, err := http.NewRequest(http.MethodPost, baseURL+"/webhooks/slack-customer/slack", bytes.NewReader(slackBody))
	if err != nil {
		t.Fatalf("build Slack release request: %v", err)
	}
	slackRequest.Header.Set("Content-Type", "application/json")
	slackRequest.Header.Set("X-Slack-Request-Timestamp", slackTimestamp)
	slackRequest.Header.Set("X-Slack-Signature", "v0="+releaseHMACSHA256Hex("slack-release-secret", []byte("v0:"+slackTimestamp+":"+string(slackBody))))
	assertReleaseProviderResponse(t, slackRequest, http.StatusAccepted, "accepted")

	duplicateRequest, err := http.NewRequest(http.MethodPost, baseURL+"/webhooks/slack-customer/slack", bytes.NewReader(slackBody))
	if err != nil {
		t.Fatalf("build duplicate Slack release request: %v", err)
	}
	duplicateRequest.Header = slackRequest.Header.Clone()
	assertReleaseProviderResponse(t, duplicateRequest, http.StatusOK, "duplicate")

	challengeBody := []byte(`{"type":"url_verification","challenge":"release-challenge"}`)
	challengeRequest, err := http.NewRequest(http.MethodPost, baseURL+"/webhooks/slack-customer/slack", bytes.NewReader(challengeBody))
	if err != nil {
		t.Fatalf("build Slack challenge release request: %v", err)
	}
	challengeRequest.Header.Set("Content-Type", "application/json")
	challengeRequest.Header.Set("X-Slack-Request-Timestamp", slackTimestamp)
	challengeRequest.Header.Set("X-Slack-Signature", "v0="+releaseHMACSHA256Hex("slack-release-secret", []byte("v0:"+slackTimestamp+":"+string(challengeBody))))
	assertReleaseProviderResponse(t, challengeRequest, http.StatusOK, "release-challenge")
}

func reserveReleaseTestAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve release listener: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close reserved release listener: %v", err)
	}
	return address
}

func releaseHMACSHA256Hex(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func assertReleaseProviderResponse(t *testing.T, request *http.Request, wantStatus int, wantBody string) {
	t.Helper()
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("send release provider request: %v", err)
	}
	defer response.Body.Close()
	responseBody := new(bytes.Buffer)
	if _, err := responseBody.ReadFrom(response.Body); err != nil {
		t.Fatalf("read release provider response: %v", err)
	}
	if response.StatusCode != wantStatus || !strings.Contains(responseBody.String(), wantBody) {
		t.Fatalf("release provider response = %d %q, want %d containing %q", response.StatusCode, responseBody.String(), wantStatus, wantBody)
	}
}

func copyProviderTriggerReleaseTree(t *testing.T, source, target string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o755)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, body, 0o644)
	})
	if err != nil {
		t.Fatalf("copy release tree %s: %v", source, err)
	}
}
