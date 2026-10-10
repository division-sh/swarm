package cliapp

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/testutil"
)

func withTestProviderTriggerPlatformInventory(t *testing.T, configText string) string {
	t.Helper()
	return strings.TrimRight(configText, "\n") + "\n"
}

func writeTestVerifyRuntimeConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "verify-runtime.yaml")
	writeRuntimeConfigText(t, path, withTestProviderTriggerPlatformInventory(t, "llm:\n  backend: anthropic\n"+testutil.EphemeralServeListenerConfig()))
	return path
}
