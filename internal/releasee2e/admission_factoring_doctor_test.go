package releasee2e

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/yamlsource"
)

type factoringDoctorReport struct {
	OK                 bool            `json:"ok"`
	CapabilitySubjects []packs.Subject `json:"capability_subjects"`
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
	version, err := platform.PlatformVersion()
	if err != nil {
		t.Fatal(err)
	}
	base, err := packartifact.LoadEmbeddedPlatformPackInventory(version)
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := packartifact.NewEffectivePackInventory(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := yamlsource.LoadFile(filepath.Join(releaseE2ERepoRoot(t), "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.AdmitPlatformSpecValue(snapshot.Document("platform-spec.yaml").Root())
	if err != nil {
		t.Fatal(err)
	}
	projection, err := packadmission.Admit(inventory, spec)
	if err != nil {
		t.Fatal(err)
	}
	want, err := projection.ProviderTriggers.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatal(err)
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
			assertFactoringDoctorInstalledReadback(t, report, want, inventory)
			text := runReleaseCommand(t, goldenStartupTimeout, cwd, env, "", binary, args...)
			if text.err != nil {
				t.Fatalf("compiled doctor text: %v\n%s", text.err, text.output)
			}
			for _, subject := range want {
				if strings.Count(text.output, packs.RenderSubject(subject, false)) != 1 {
					t.Fatalf("missing/duplicate typed subject text %s\n%s", subject.ID, text.output)
				}
			}
			for _, entry := range inventory.Entries() {
				if !strings.Contains(text.output, entry.ID()+" type="+string(entry.Type())) || !strings.Contains(text.output, "source="+entry.Source()+" manifest="+entry.ManifestHash()) {
					t.Fatalf("missing pack provenance %s\n%s", entry.ID(), text.output)
				}
			}
			if !strings.Contains(text.output, "schema inventory:") {
				t.Fatal("schema inventory option lost")
			}
			t.Run("dropped-readback-negative", func(t *testing.T) {
				if reflect.DeepEqual(want, []packs.Subject(nil)) {
					t.Fatal("negative control has no inventory obligation")
				}
				// Exercise the same oracle without changing production: dropping
				// final readback must not be mistaken for an empty valid inventory.
				lost := report
				lost.CapabilitySubjects = nil
				if factoringDoctorInstalledReadbackMatches(lost, want, inventory) {
					t.Fatal("oracle admitted dropped final inventory")
				}
			})
		})
	}
	t.Run("failure-gates", func(t *testing.T) {
		for _, tc := range []struct{ name, suffix, code string }{
			{"invalid-pack", "platform:\n  packs:\n    platform_dirs: [missing-pack-directory]\n", "platform_pack_load_failed"},
			{"invalid-admission", "paths:\n  platform_spec_path: invalid-spec.yaml\n", "platform_pack_load_failed"},
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
				for _, finding := range report.Findings {
					if finding.Code == tc.code && finding.Status == "failed" && finding.Severity == "blocker" {
						found++
					}
				}
				if found != 1 {
					t.Fatalf("typed blocker %s count=%d\n%s", tc.code, found, result.output)
				}
			})
		}
	})
}

func factoringDoctorInstalledReadbackMatches(report factoringDoctorReport, want []packs.Subject, inventory *packartifact.EffectivePackInventory) bool {
	wantBody, err := json.Marshal(want)
	if err != nil {
		return false
	}
	gotBody, err := json.Marshal(report.CapabilitySubjects)
	return err == nil && report.OK && string(gotBody) == string(wantBody) && len(report.PackInventory.Packs) == len(inventory.Entries()) && len(report.SchemaInventory) > 0 && report.PackInventory.BaseDigest == inventory.BaseDigest() && report.PackInventory.EffectiveDigest == inventory.Digest()
}

func assertFactoringDoctorInstalledReadback(t *testing.T, report factoringDoctorReport, want []packs.Subject, inventory *packartifact.EffectivePackInventory) {
	t.Helper()
	if !factoringDoctorInstalledReadbackMatches(report, want, inventory) {
		t.Fatalf("installed doctor readback does not match admitted inventory: subjects=%d want=%d packs=%d", len(report.CapabilitySubjects), len(want), len(report.PackInventory.Packs))
	}
	for index, entry := range inventory.Entries() {
		got := report.PackInventory.Packs[index]
		if got.ID != entry.ID() || got.Source != entry.Source() || got.ManifestHash != entry.ManifestHash() {
			t.Fatalf("pack identity/provenance changed: %#v", got)
		}
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
	for _, subject := range report.CapabilitySubjects {
		if subject.Applicability != "installed" || subject.Status != packs.StatusAvailable || subject.TriggerAdmission != nil {
			t.Fatalf("doctor acquired effective authority: %#v", subject)
		}
	}
}
