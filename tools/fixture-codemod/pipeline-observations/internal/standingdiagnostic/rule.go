package standingdiagnostic

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

const Predecessor = "67f4b617d76f23b291502f00f8aecc2edad3c4f6"
const Path = "tools/fixture-codemod/pipeline-observations/family145-standing-diagnostic-preimages.json"
const Digest = "5d58067e927054768fcd278e82a4fb6d0f9ec60b0cf0f9c507cfb30e8d4cac72"

type Pin struct{ File, Function, Before, After, SHA256 string }
type Endpoint struct{ File, Function, After string }
type Manifest struct {
	Pins      []Pin
	Endpoints []Endpoint
}
type Rule struct{ File, Function, Backend string }

const standing = "internal/serveapp/standing_ingress_supported_surface_test.go"
const memory = "internal/serveapp/agent_memory_supported_surface_test.go"

var Rules = []Rule{
	{standing, "standingPostgresDiagnostics", "postgres"},
	{standing, "standingSQLiteDiagnostics", "sqlite"},
	{standing, "requireStandingTelegramCalls", ""},
	{standing, "TestStandingIngressSupportedSurfaceSQLiteRestartPreservesAuthorityAndReplies", ""},
	{standing, "TestStandingIngressSupportedSurfacePostgresRestartPreservesAuthorityAndReplies", ""},
	{standing, "TestStandingRestartMixedHealthyAndTerminalProcessParity", ""},
	{standing, "standingWebhookDiagnostics", ""},
	{standing, "sendStandingTelegramUpdate", ""},
	{memory, "standingMemoryStoreDiagnostics", ""},
	{memory, "runStandingTelegramMemorySupportedSurface", ""},
	{memory, "TestCanonicalTelegramAgentSupportedSurfaceSQLitePostgres", ""},
	{memory, "TestCanonicalTelegramAgentLiveSelectionPreservesAuthoredDoubles", ""},
	{memory, "sendStandingTelegramUpdatePublication", ""},
	{memory, "sendStandingTelegramPublication", ""},
}

var Endpoints = []Endpoint{
	{"internal/serveapp/standing_diagnostic_reader_test.go", "standingStoreDiagnostics", ""},
	{"internal/serveapp/standing_diagnostic_reader_test.go", "renderStandingStoreDiagnostics", ""},
	{"internal/store/internal/backend/eventrecord/standing_diagnostic_observation.go", "ReadStandingEventNameDiagnosticsTx", ""},
	{"internal/store/internal/backend/eventrecord/standing_diagnostic_observation.go", "ReadStandingRuntimeLogDiagnosticsTx", ""},
	{"internal/store/internal/backend/delivery/read_projections.go", "ReadStandingDeliveryDiagnosticsTx", ""},
	{"internal/store/internal/runtimepersistence/test_event_support.go", "ReadStandingEventNameDiagnosticsForTest", ""},
	{"internal/store/internal/runtimepersistence/test_event_support.go", "ReadStandingRuntimeLogDiagnosticsForTest", ""},
	{"internal/store/internal/runtimepersistence/test_standing_diagnostic_observation.go", "ReadStandingDeliveryDiagnosticsForTest", ""},
	{"internal/store/storetest/standing_diagnostic_observation.go", "ReadStandingEventNameDiagnostics", ""},
	{"internal/store/storetest/standing_diagnostic_observation.go", "ReadStandingRuntimeLogDiagnostics", ""},
	{"internal/store/storetest/standing_diagnostic_observation.go", "ReadStandingDeliveryDiagnostics", ""},
}

func Decode(raw []byte) (Manifest, error) {
	var out Manifest
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return out, fmt.Errorf("standing diagnostic whole-chain manifest changed")
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	if len(out.Pins) != 14 || len(out.Endpoints) != 11 {
		return out, fmt.Errorf("standing diagnostic declaration inventory changed")
	}
	return out, nil
}

func Transform(rule Rule, before string) (string, error) {
	if rule.Backend == "" {
		return before, nil
	}
	argument := "path"
	if rule.Backend == "postgres" {
		argument = "dsn"
	} else if rule.Backend != "sqlite" {
		return "", fmt.Errorf("unsupported diagnostic predecessor")
	}
	return requestkey.Normalize(fmt.Sprintf("func %s(%s string) string { return standingStoreDiagnostics(%q, %s) }", rule.Function, argument, rule.Backend, argument))
}

func Previous(manifest Manifest, file, function, current string) string {
	for _, pin := range manifest.Pins {
		if pin.File == file && pin.Function == function && current == pin.After {
			return pin.Before
		}
	}
	return current
}
