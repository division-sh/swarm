package releasee2e

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestReleaseE2EPackageStaysAtPublicProcessBoundary(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get release E2E package directory: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read release E2E package directory: %v", err)
	}
	files := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" {
			continue
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			t.Fatalf("release E2E package contains production Go source %s", entry.Name())
		}
		path := filepath.Join(dir, entry.Name())
		parsed, err := parser.ParseFile(files, path, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, imported := range parsed.Imports {
			name, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatalf("decode import in %s: %v", entry.Name(), err)
			}
			alias := ""
			if imported.Name != nil {
				alias = imported.Name.Name
			}
			if !releaseE2EImportAllowed(entry.Name(), name, alias) {
				t.Fatalf("release E2E source %s imports Swarm implementation package %q", entry.Name(), name)
			}
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		if strings.Contains(string(raw), "//go:"+"linkname") {
			t.Fatalf("release E2E source %s uses go:linkname", entry.Name())
		}
		for _, forbidden := range []string{
			"ManagedProvider" + "PreflightAuthority",
			"NewClaude" + "CLIRuntime",
			"Serve" + "Options",
			"TestRuntime" + "ReadyHook",
			"cliapp." + "Execute",
			"serveapp." + "Run",
		} {
			if strings.Contains(string(raw), forbidden) {
				t.Fatalf("release E2E source %s names forbidden in-process seam %q", entry.Name(), forbidden)
			}
		}
	}
}

func releaseE2EImplementationImport(file, name string) bool {
	return !releaseE2EImportAllowed(file, name, "")
}

func TestReleaseE2ECapacityFixtureImportBoundary(t *testing.T) {
	const fixture = "github.com/division-sh/swarm/tests/support/testpostgres"
	for _, file := range []string{"golden_agent_workload_test.go", "postgres_capacity_admission_test.go"} {
		if releaseE2EImplementationImport(file, fixture) {
			t.Fatalf("shared fixture owner refused in %s", file)
		}
		for _, forbidden := range []string{fixture + "/other", fixture + "other", "github.com/division-sh/swarm/internal/serveapp", "github.com/division-sh/swarm/internal/runtime/contracts"} {
			if !releaseE2EImplementationImport(file, forbidden) {
				t.Fatalf("capacity exception allowed %q", forbidden)
			}
		}
	}
	if !releaseE2EImplementationImport("another_test.go", fixture) {
		t.Fatal("capacity exception escaped its two fixture files")
	}
}

func releaseE2EImportAllowed(file, name, alias string) bool {
	if !strings.Contains(name, "github.com/division-sh/swarm") {
		return true
	}
	// Process lifetime support grants no in-process product execution.
	if name == "github.com/division-sh/swarm/tests/support/testutil/testprocess" && alias == "" {
		switch file {
		case "process_harness_test.go", "docker_helper_test.go", "lifecycle_test.go", "run_observer_overflow_test.go", "workspace_gateway_docker_test.go":
			return true
		}
	}
	// Concrete HTTP doubles contain no in-process Swarm execution owners.
	if file == "provider_connector_journey_test.go" && name == "github.com/division-sh/swarm/tests/support/testutil/connectorapi" && alias == "" {
		return true
	}
	if file == "external_integrations_journey_test.go" && alias == "" &&
		(name == "github.com/division-sh/swarm/tests/support/testutil/connectorapi" || name == "github.com/division-sh/swarm/tests/support/testutil/llmapi" || name == "github.com/division-sh/swarm/tests/support/testutil/mcpapi") {
		return true
	}
	if file == "provider_stop_handoff_test.go" && alias == "" &&
		(name == "github.com/division-sh/swarm/tests/support/testutil/llmapi" || name == "github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting" ||
			name == "github.com/division-sh/swarm/internal/store/storetest") {
		return true
	}
	if name == "github.com/division-sh/swarm/tests/support/testpostgres" && alias == "" &&
		(file == "golden_agent_workload_test.go" || file == "postgres_capacity_admission_test.go") {
		return true
	}
	// DSN-only sandbox allocation is fixture setup, not in-process execution.
	if name == "github.com/division-sh/swarm/tests/support/testutil" && alias == "" && file == "golden_agent_workload_test.go" {
		return true
	}
	// Compilation and qualification support is not in-process product execution.
	return name == "github.com/division-sh/swarm/internal/testplanning" && alias == "" &&
		(file == "process_harness_test.go" || file == "golden_agent_workload_test.go")
}

func TestReleaseE2EImportBoundaryRejectsRuntimeAndUnscopedPlanning(t *testing.T) {
	for _, tc := range []struct {
		file, name, alias string
		allowed           bool
	}{
		{"process_harness_test.go", "github.com/division-sh/swarm/tests/support/testutil/testprocess", "", true},
		{"docker_helper_test.go", "github.com/division-sh/swarm/tests/support/testutil/testprocess", "", true},
		{"lifecycle_test.go", "github.com/division-sh/swarm/tests/support/testutil/testprocess", "", true},
		{"run_observer_overflow_test.go", "github.com/division-sh/swarm/tests/support/testutil/testprocess", "", true},
		{"workspace_gateway_docker_test.go", "github.com/division-sh/swarm/tests/support/testutil/testprocess", "", true},
		{"other_test.go", "github.com/division-sh/swarm/tests/support/testutil/testprocess", "", false},
		{"process_harness_test.go", "github.com/division-sh/swarm/tests/support/testutil/testprocess", ".", false},
		{"process_harness_test.go", "github.com/division-sh/swarm/tests/support/testutil/testprocess", "other", false},
		{"process_harness_test.go", "github.com/division-sh/swarm/tests/support/testutil/testprocess/subpackage", "", false},
		{"process_harness_test.go", "github.com/division-sh/swarm/tests/support/testutil/testprocessother", "", false},
		{"process_harness_test.go", "github.com/division-sh/swarm/internal/testplanning", "", true},
		{"golden_agent_workload_test.go", "github.com/division-sh/swarm/internal/testplanning", "", true},
		{"other_test.go", "github.com/division-sh/swarm/internal/testplanning", "", false},
		{"process_harness_test.go", "github.com/division-sh/swarm/internal/testplanning", ".", false},
		{"process_harness_test.go", "github.com/division-sh/swarm/internal/testplanning", "other", false},
		{"process_harness_test.go", "github.com/division-sh/swarm/internal/testplanning/subpackage", "", false},
		{"process_harness_test.go", "github.com/division-sh/swarm/internal/runtime", "", false},
		{"process_harness_test.go", "github.com/division-sh/swarm/internal/store", "", false},
		{"process_harness_test.go", "github.com/division-sh/swarm/internal/serveapp", "", false},
		{"golden_agent_workload_test.go", "github.com/division-sh/swarm/internal/runtime/llm", "", false},
		{"golden_agent_workload_test.go", "github.com/division-sh/swarm/internal/runtime/bootverify", "", false},
		{"golden_agent_workload_test.go", "github.com/division-sh/swarm/internal/runtime/bootverify", "bootverify", false},
		{"golden_agent_workload_test.go", "github.com/division-sh/swarm/internal/runtime/bootverify", ".", false},
		{"golden_agent_workload_test.go", "github.com/division-sh/swarm/internal/cliapp", "", false},
		{"golden_agent_workload_test.go", "github.com/division-sh/swarm/tests/support/testutil", "", true},
		{"other_test.go", "github.com/division-sh/swarm/tests/support/testutil", "", false},
		{"golden_agent_workload_test.go", "github.com/division-sh/swarm/tests/support/testutil", "fixture", false},
		{"other_test.go", "net/http", "", true},
		{"provider_connector_journey_test.go", "github.com/division-sh/swarm/tests/support/testutil/connectorapi", "", true},
		{"other_test.go", "github.com/division-sh/swarm/tests/support/testutil/connectorapi", "", false},
		{"provider_connector_journey_test.go", "github.com/division-sh/swarm/tests/support/testutil/connectorapi", ".", false},
		{"provider_connector_journey_test.go", "github.com/division-sh/swarm/tests/support/testutil/connectorapi", "other", false},
		{"provider_connector_journey_test.go", "github.com/division-sh/swarm/tests/support/testutil/connectorapi/other", "", false},
		{"provider_connector_journey_test.go", "github.com/division-sh/swarm/tests/support/testutil/connectorapiother", "", false},
		{"external_integrations_journey_test.go", "github.com/division-sh/swarm/tests/support/testutil/connectorapi", "", true},
		{"external_integrations_journey_test.go", "github.com/division-sh/swarm/tests/support/testutil/llmapi", "", true},
		{"external_integrations_journey_test.go", "github.com/division-sh/swarm/tests/support/testutil/mcpapi", "", true},
		{"external_integrations_journey_test.go", "github.com/division-sh/swarm/tests/support/testutil/mcpapi", ".", false},
		{"external_integrations_journey_test.go", "github.com/division-sh/swarm/tests/support/testutil/mcpapi", "fixture", false},
		{"external_integrations_journey_test.go", "github.com/division-sh/swarm/tests/support/testutil/mcpapi/other", "", false},
		{"external_integrations_journey_test.go", "github.com/division-sh/swarm/tests/support/testutil/mcpapiother", "", false},
		{"other_test.go", "github.com/division-sh/swarm/tests/support/testutil/mcpapi", "", false},
		{"provider_stop_handoff_test.go", "github.com/division-sh/swarm/tests/support/testutil/llmapi", "", true},
		{"provider_stop_handoff_test.go", "github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting", "", true},
		{"provider_stop_handoff_test.go", "github.com/division-sh/swarm/internal/store/storetest", "", true},
		{"other_test.go", "github.com/division-sh/swarm/internal/store/storetest", "", false},
		{"provider_stop_handoff_test.go", "github.com/division-sh/swarm/internal/store/storetest", "fixture", false},
		{"other_test.go", "github.com/division-sh/swarm/tests/support/testutil/llmapi", "", false},
		{"provider_stop_handoff_test.go", "github.com/division-sh/swarm/internal/operatorread", "", false},
		{"provider_stop_handoff_test.go", "github.com/division-sh/swarm/tests/support/testutil/llmapi", ".", false},
		{"provider_stop_handoff_test.go", "github.com/division-sh/swarm/tests/support/testutil/llmapi/other", "", false},
	} {
		t.Run(tc.file+"/"+tc.name+"/"+tc.alias, func(t *testing.T) {
			if got := releaseE2EImportAllowed(tc.file, tc.name, tc.alias); got != tc.allowed {
				t.Fatalf("import admission=%t, want %t", got, tc.allowed)
			}
		})
	}
}
