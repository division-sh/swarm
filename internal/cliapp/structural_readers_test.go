package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestStructuralReadersShareAdmittedSource(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	// A directory is not a credential document. Any accidental credential
	// construction/read must fail instead of making the test pass without keys.
	t.Setenv("SWARM_CREDENTIALS_FILE", t.TempDir())
	t.Setenv("SWARM_MANAGED_CREDENTIALS_FILE", t.TempDir())
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	for _, command := range [][]string{{"verify"}, {"describe"}, {"describe", "--graph"}, {"describe", "routes"}} {
		t.Run(strings.Join(command, " "), func(t *testing.T) {
			args := append(append([]string(nil), command...), root, "--json")
			var out, errOut bytes.Buffer
			if code := executeRootCommandWithOptions(context.Background(), RepoRoot(), args, &out, &errOut, defaultRootCommandOptions()); code != 0 {
				t.Fatalf("%v exit %d: %s\n%s", args, code, out.String(), errOut.String())
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(out.Bytes(), &fields); err != nil {
				t.Fatal(err)
			}
			for _, retired := range []string{"workspace_backend", "capability_subjects"} {
				if _, exists := fields[retired]; exists {
					t.Fatalf("structural command reports %s: %s", retired, out.String())
				}
			}
			if command[len(command)-1] != "routes" {
				if string(fields["validation_scope"]) != `"structural"` || string(fields["live_readiness"]) != `"not_evaluated"` {
					t.Fatalf("missing structural scope: %s", out.String())
				}
			}
			if command[0] == "verify" {
				if string(fields["ok"]) != "true" || string(fields["production_valid"]) != "true" {
					t.Fatalf("admitted source rejected: %s", out.String())
				}
				return
			}
			if command[len(command)-1] == "routes" {
				return
			}
			var view authoringview.View
			if err := json.Unmarshal(out.Bytes(), &view); err != nil {
				t.Fatal(err)
			}
			for _, finding := range view.Diagnostics {
				if finding.Severity == "hard_invalidity" {
					t.Fatalf("admitted provider source has false invalidity: %#v", finding)
				}
			}
			if len(view.RoutingTopology.RootInputSources) != 1 {
				t.Fatalf("missing standing ingress: %#v", view.RoutingTopology)
			}
		})
	}
}

func TestStructuralReadersRetainPostBootInvalidity(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "false")
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	root := canonicalrouting.CopyVerifyLintEvidence(t, true)
	configPath := writeTestVerifyRuntimeConfig(t)
	for _, command := range [][]string{{"verify"}, {"describe"}, {"describe", "--graph"}} {
		t.Run(strings.Join(command, " "), func(t *testing.T) {
			args := append(append([]string(nil), command...), root, "--config", configPath, "--json")
			var out, errOut bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), RepoRoot(), args, &out, &errOut, defaultRootCommandOptions())
			var findings []struct {
				CheckID  string `json:"check_id"`
				Severity string `json:"severity"`
				Message  string `json:"message"`
			}
			var result map[string]json.RawMessage
			if err := json.Unmarshal(out.Bytes(), &result); err != nil {
				t.Fatalf("decode %v: %v; exit=%d out=%s stderr=%s", args, err, code, out.String(), errOut.String())
			}
			field := "diagnostics"
			if command[0] == "verify" {
				field = "errors"
				if code == 0 {
					t.Fatal("verify accepted missing emit schema")
				}
			} else if code != 0 {
				t.Fatalf("describe failed to render invalid source: %d %s", code, errOut.String())
			}
			if err := json.Unmarshal(result[field], &findings); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, finding := range findings {
				if finding.CheckID == "workflow_contract_validation" && finding.Severity == "hard_invalidity" && strings.Contains(finding.Message, "emit schema strict mode enabled") {
					found = true
				}
			}
			if !found {
				t.Fatalf("lost canonical post-boot invalidity: %s", out.String())
			}
		})
	}
}
