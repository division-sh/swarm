package cliapp

import (
	"path/filepath"
	"strings"
	"testing"
)

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
