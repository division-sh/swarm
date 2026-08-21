package cliapp

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func testProviderTriggerCatalog(t *testing.T) *providertriggers.CatalogSnapshot {
	t.Helper()
	return packfixture.TriggerCatalog(t)
}

func emptyProviderTriggerCatalog(t *testing.T) *providertriggers.CatalogSnapshot {
	t.Helper()
	registry, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatalf("create empty provider trigger registry: %v", err)
	}
	return registry
}

func withTestProviderTriggerPlatformInventory(t *testing.T, configText string) string {
	t.Helper()
	return strings.TrimRight(configText, "\n") + "\n"
}

func writeTestVerifyRuntimeConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "verify-runtime.yaml")
	writeRuntimeConfigText(t, path, withTestProviderTriggerPlatformInventory(t, "runtime:\n  execution_posture: live\nllm:\n  backend: anthropic\n"))
	return path
}
