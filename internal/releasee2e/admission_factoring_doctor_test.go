package releasee2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type factoringDoctorReport struct {
	OK                 bool              `json:"ok"`
	CapabilitySubjects []json.RawMessage `json:"capability_subjects"`
	PackInventory      struct {
		BaseMode        string `json:"base_mode"`
		BaseDigest      string `json:"base_digest"`
		EffectiveDigest string `json:"effective_digest"`
		Packs           []struct {
			ID, Type, Version, Source string
			ManifestHash              string `json:"manifest_hash"`
		}
	} `json:"pack_inventory"`
	Findings        []struct{ Code, Message, Status, Severity string } `json:"findings"`
	SchemaInventory json.RawMessage                                    `json:"schema_inventory"`
}

func TestAdmissionFactoringDoctorReadback(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	// Captured through compiled public doctor at pre-extraction49abcf245.
	// This oracle imports no in-process admission or renderer implementation.
	raw, err := os.ReadFile(filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/doctor-installed.golden"))
	if err != nil {
		t.Fatal(err)
	}
	var want factoringDoctorReport
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if len(want.CapabilitySubjects) != 8 || len(want.PackInventory.Packs) != 14 || len(want.Findings) != 8 {
		t.Fatal("incomplete pinned installed inventory oracle")
	}
	for _, hostile := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid-tree", true: "hostile-tree"}[hostile], func(t *testing.T) {
			cwd := goldenReleaseRoot(t)
			config, env := clockReleaseConfig(t, cwd, "sqlite")
			body := "name: valid-tree\n"
			if hostile {
				body = "not: valid: source\n"
			}
			writeReleaseFile(t, filepath.Join(cwd, "schema.yaml"), body)
			args := []string{"doctor", "--backend", "claude_cli", "--workspace-backend", "host", "--config", config, "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:0", "--schema-inventory"}
			result := runReleaseCommand(t, goldenStartupTimeout, cwd, env, "", binary, append(args, "--json")...)
			var report factoringDoctorReport
			if err := json.Unmarshal([]byte(result.output), &report); result.err != nil || err != nil {
				t.Fatalf("compiled doctor JSON: %v %v\n%s", result.err, err, result.output)
			}
			assertFactoringDoctorInstalledReadback(t, report, want)
			text := runReleaseCommand(t, goldenStartupTimeout, cwd, env, "", binary, args...)
			if text.err != nil {
				t.Fatalf("compiled doctor text: %v\n%s", text.err, text.output)
			}
			for _, finding := range want.Findings {
				if strings.Count(text.output, finding.Message) != 1 {
					t.Fatalf("missing/duplicate typed subject text %s\n%s", finding.Code, text.output)
				}
			}
			for _, entry := range want.PackInventory.Packs {
				if !strings.Contains(text.output, entry.ID+" type="+entry.Type) || !strings.Contains(text.output, "source="+entry.Source+" manifest="+entry.ManifestHash) {
					t.Fatalf("missing pack provenance %s\n%s", entry.ID, text.output)
				}
			}
			if !strings.Contains(text.output, "schema inventory:") {
				t.Fatal("schema inventory option lost")
			}
			t.Run("dropped-readback-negative", func(t *testing.T) {
				if len(want.CapabilitySubjects) == 0 {
					t.Fatal("negative control has no inventory obligation")
				}
				// Exercise the same oracle without changing production: dropping
				// final readback must not be mistaken for an empty valid inventory.
				lost := report
				lost.CapabilitySubjects = nil
				if factoringDoctorInstalledReadbackMatches(lost, want) {
					t.Fatal("oracle admitted dropped final inventory")
				}
			})
		})
	}
	t.Run("invalid-env-config", func(t *testing.T) {
		cwd := goldenReleaseRoot(t)
		config, env := clockReleaseConfig(t, cwd, "sqlite")
		env = append(env, "SWARM_TOOL_GATEWAY_TOKEN=stale-parent-token")
		args := []string{"doctor", "--config", config, "--workspace-backend", "host", "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:0", "--schema-inventory"}
		for _, asJSON := range []bool{true, false} {
			t.Run(map[bool]string{true: "json", false: "text"}[asJSON], func(t *testing.T) {
				commandArgs := append([]string(nil), args...)
				if asJSON {
					commandArgs = append(commandArgs, "--json")
				}
				result := runReleaseCommand(t, goldenStartupTimeout, cwd, env, "", binary, commandArgs...)
				exit, exited := result.err.(*exec.ExitError)
				if !exited || exit.ExitCode() != 3 || strings.Contains(result.output, "stale-parent-token") {
					t.Fatalf("compiled doctor env blocker lost exit/redaction: %v\n%s", result.err, result.output)
				}
				if !asJSON {
					if !strings.Contains(result.output, "/config_load_failed @ doctor:") || strings.Contains(result.output, "/api_listener @ doctor:") {
						t.Fatalf("invalid config bypassed the earlier admission gate\n%s", result.output)
					}
					return
				}
				var report factoringDoctorReport
				if err := json.Unmarshal([]byte(result.output), &report); err != nil || report.OK {
					t.Fatalf("compiled doctor accepted env blocker: %v\n%s", err, result.output)
				}
				found := 0
				for _, finding := range report.Findings {
					if finding.Code == "config_load_failed" && finding.Status == "failed" && finding.Severity == "blocker" {
						found++
					}
				}
				if found != 1 || len(report.CapabilitySubjects) != 0 || len(report.PackInventory.Packs) != 0 {
					t.Fatalf("invalid config bypassed the earlier admission gate: blockers=%d\n%s", found, result.output)
				}
			})
		}
	})
	t.Run("failure-gates", func(t *testing.T) {
		for _, tc := range []struct{ name, suffix, code string }{
			{"invalid-pack", "platform:\n  packs:\n    platform_dirs: [missing-pack-directory]\n", "platform_pack_load_failed"},
			// #1801/6196686932: the override refuses at source admission, before spec readback.
			{"invalid-admission", "paths:\n  platform_spec_path: invalid-spec.yaml\n", "config_load_failed"},
			{"invalid-workspace", "workspace:\n  backend: invalid\n", "workspace_backend_invalid"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cwd := goldenReleaseRoot(t)
				env := goldenProcessEnv(t, cwd, "", 0)
				config := filepath.Join(cwd, "doctor.yaml")
				writeReleaseFile(t, config, "llm:\n  backend: claude_cli\n"+tc.suffix)
				writeReleaseFile(t, filepath.Join(cwd, "invalid-spec.yaml"), "platform:\n  version: 99.0.0\n")
				result := runReleaseCommand(t, goldenStartupTimeout, cwd, env, "", binary, "doctor", "--config", config, "--json", "--api-listen-addr", "127.0.0.1:0", "--mcp-listen-addr", "127.0.0.1:0")
				var report factoringDoctorReport
				exit, exited := result.err.(*exec.ExitError)
				if err := json.Unmarshal([]byte(result.output), &report); err != nil || !exited || exit.ExitCode() != 3 || report.OK {
					t.Fatalf("invalid doctor admission: %v %v\n%s", result.err, err, result.output)
				}
				found := 0
				var refusal string
				for _, finding := range report.Findings {
					if finding.Code == tc.code && finding.Status == "failed" && finding.Severity == "blocker" {
						found++
						refusal = finding.Message
					}
				}
				if found != 1 {
					t.Fatalf("typed blocker %s count=%d\n%s", tc.code, found, result.output)
				}
				if tc.name == "invalid-admission" {
					want := fmt.Sprintf("swarm.yaml config blockers:\n`paths.platform_spec_path` at %s:2:3 is not a config key; the platform spec is embedded in `swarm`.", config)
					if refusal != want {
						t.Fatalf("config retirement lost exact located teaching; want %q\n%s", want, result.output)
					}
					if len(report.CapabilitySubjects) != 0 || len(report.PackInventory.Packs) != 0 {
						t.Fatalf("retired spec override reached installed readback\n%s", result.output)
					}
				}
			})
		}
	})
}

func factoringDoctorInstalledReadbackMatches(report, want factoringDoctorReport) bool {
	wantBody, err := json.Marshal(want.CapabilitySubjects)
	if err != nil {
		return false
	}
	gotBody, err := json.Marshal(report.CapabilitySubjects)
	return err == nil && report.OK && string(gotBody) == string(wantBody) && reflect.DeepEqual(report.PackInventory, want.PackInventory) && len(report.SchemaInventory) > 0
}

func assertFactoringDoctorInstalledReadback(t *testing.T, report, want factoringDoctorReport) {
	t.Helper()
	if !factoringDoctorInstalledReadbackMatches(report, want) {
		t.Fatalf("installed doctor readback does not match admitted inventory: subjects=%d want=%d packs=%d", len(report.CapabilitySubjects), len(want.CapabilitySubjects), len(report.PackInventory.Packs))
	}
	seen := map[string]bool{}
	for _, finding := range report.Findings {
		if seen[finding.Code] {
			t.Fatalf("duplicate diagnostic %s", finding.Code)
		}
		seen[finding.Code] = true
		if strings.Contains(finding.Code, "contract_source") || strings.Contains(finding.Code, "agent_free_source") {
			t.Fatalf("doctor loaded invocation source: %#v", finding)
		}
	}
	for _, raw := range report.CapabilitySubjects {
		var subject struct {
			Applicability, Status string
			TriggerAdmission      json.RawMessage `json:"trigger_admission"`
		}
		if err := json.Unmarshal(raw, &subject); err != nil {
			t.Fatal(err)
		}
		if subject.Applicability != "installed" || subject.Status != "AVAILABLE" || len(subject.TriggerAdmission) != 0 {
			t.Fatalf("doctor acquired effective authority: %#v", subject)
		}
	}
}
