package serveapp

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/yamlsource"
)

func freeDoctorTCPPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate test TCP port: %v", err)
	}
	defer listener.Close()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split test TCP address %q: %v", listener.Addr(), err)
	}
	return port
}

const (
	serveAPITokenFileFlagSource            = "--api-token-file"
	defaultWorkspaceDataSourceSource       = "project_default"
	defaultWorkspaceDataSourceRelativePath = ".swarm/data"
)

func isolateCLIAPIConfigEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SWARM_CONFIG", "")
	t.Setenv("SWARM_API_SERVER", "")
	t.Setenv("SWARM_API_TOKEN", "")
	t.Setenv("SWARM_API_TOKEN_FILE", "")
	t.Setenv("SWARM_API_LISTEN_ADDR", "")
	t.Setenv("SWARM_MCP_LISTEN_ADDR", "")
	t.Setenv("SWARM_CONTRACTS_PATH", "")
	t.Setenv("SWARM_CONTRACTS_DIR", "")
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())
}

func setDoctorProviderSecret(t *testing.T, key, value string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "provider-credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", path)
	store, err := runtimecredentials.NewFileStore(path)
	if err != nil {
		t.Fatalf("create provider credential store: %v", err)
	}
	if err := store.Set(context.Background(), key, value); err != nil {
		t.Fatalf("set provider credential: %v", err)
	}
}

func testProviderTriggerPackDirs(t *testing.T) []string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve provider trigger test fixture source path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "packs", "provider-triggers"))
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read provider trigger pack root: %v", err)
	}
	dirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, filepath.Join(root, entry.Name()))
		}
	}
	sort.Strings(dirs)
	return dirs
}

func withTestProviderTriggerPlatformInventory(t *testing.T, configText string) string {
	t.Helper()
	if strings.Contains(configText, "\nprovider_triggers:") || strings.HasPrefix(configText, "provider_triggers:") {
		t.Fatalf("test runtime config already declares provider_triggers; compose the intended inventory explicitly")
	}
	lines := []string{"provider_triggers:", "  packs:", "    platform_dirs:"}
	for _, dir := range testProviderTriggerPackDirs(t) {
		lines = append(lines, fmt.Sprintf("      - %q", dir))
	}
	return strings.TrimRight(configText, "\n") + "\n" + strings.Join(lines, "\n") + "\n"
}

func writeTestVerifyRuntimeConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "verify-runtime.yaml")
	if err := os.WriteFile(path, []byte(withTestProviderTriggerPlatformInventory(t, "llm:\n  backend: anthropic\n")), 0o644); err != nil {
		t.Fatalf("write verify runtime config: %v", err)
	}
	return path
}
func writeDoctorClaudeHostConfig(t *testing.T, dockerBin string) string {
	t.Helper()
	path := writeDoctorClaudeConfig(t, dockerBin)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read doctor config: %v", err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(raw), "workspace:\n", "workspace:\n  backend: host\n", 1)), 0o644); err != nil {
		t.Fatalf("write host doctor config: %v", err)
	}
	return path
}

func emptyProviderTriggerCatalog(t *testing.T) *providertriggers.CatalogSnapshot {
	t.Helper()
	catalog, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatalf("create empty provider trigger catalog: %v", err)
	}
	return catalog
}

func testWorkspaceBackendConfig(backend string) *config.Config {
	return &config.Config{LLM: config.LLMConfig{Backend: backend}}
}

func writeCLIAPIConfigFile(t *testing.T, values map[string]string) string {
	t.Helper()
	var body strings.Builder
	for _, section := range []struct {
		name string
		keys map[string]string
	}{
		{name: "connection", keys: map[string]string{"api_server": "api_server", "api_token_file": "api_token_file"}},
		{name: "paths", keys: map[string]string{"swarm_dir": "swarm_dir", "contracts_path": "contracts_path", "platform_spec_path": "platform_spec_path"}},
		{name: "serve", keys: map[string]string{"serve_api_listen_addr": "api_listen_addr", "serve_mcp_listen_addr": "mcp_listen_addr", "serve_api_token_file": "api_token_file"}},
	} {
		lines := []string{}
		for key, yamlName := range section.keys {
			if value, ok := values[key]; ok {
				lines = append(lines, fmt.Sprintf("  %s: %q", yamlName, value))
			}
		}
		if len(lines) > 0 {
			sort.Strings(lines)
			body.WriteString(section.name + ":\n" + strings.Join(lines, "\n") + "\n")
		}
	}
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(path, []byte(withTestProviderTriggerPlatformInventory(t, body.String())), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	return path
}

func decodeAuthoritativeYAMLFileForTest(t testing.TB, path string, target any) {
	t.Helper()
	source, err := yamlsource.LoadFile(path)
	if err != nil {
		t.Fatalf("read authoritative YAML %s: %v", path, err)
	}
	if err := source.Decode(target); err != nil {
		t.Fatalf("decode authoritative YAML %s: %v", path, err)
	}
}

func decodeAuthoritativeYAMLBytesForTest(t testing.TB, raw []byte, target any) {
	t.Helper()
	source, err := yamlsource.Load(raw)
	if err != nil {
		t.Fatalf("parse authoritative YAML: %v", err)
	}
	if err := source.Decode(target); err != nil {
		t.Fatalf("decode authoritative YAML: %v", err)
	}
}

func copyProviderTriggerPackFixture(t *testing.T, provider, target string, external bool) {
	t.Helper()
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("mkdir pack fixture: %v", err)
	}
	source := filepath.Join(cliapp.RepoRoot(), "packs", "provider-triggers", provider)
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

func testProviderTriggerCatalog(t *testing.T) *providertriggers.CatalogSnapshot {
	t.Helper()
	catalog, _, err := providertriggers.NewCatalogSnapshotFromPackDirs("0.7.0", testProviderTriggerPackDirs(t), nil)
	if err != nil {
		t.Fatalf("load provider trigger catalog: %v", err)
	}
	return catalog
}
