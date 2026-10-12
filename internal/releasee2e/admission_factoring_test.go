package releasee2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestAdmissionFactoringCompiledStructuralSurfaces(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	config, env := clockReleaseConfig(t, root, "sqlite")
	source := canonicalrouting.CopyStandingRootTreePublic(t)
	for _, args := range [][]string{
		{"verify", "--portable", "--json"}, {"verify", "--json"},
		{"describe"}, {"describe", "--json"}, {"describe", "--quiet"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, append(args, source, "--config", config)...)
			if result.err != nil {
				t.Fatalf("compiled source reader: %v\n%s", result.err, result.output)
			}
			for _, retired := range []string{"capability_subjects", "production_valid", "workspace_backend"} {
				if strings.Contains(result.output, `"`+retired+`"`) {
					t.Fatalf("structural read reports %s: %s", retired, result.output)
				}
			}
			if strings.Contains(strings.Join(args, " "), "--json") {
				// Public JSON names are deliberately asserted separately from text output.
				var fields map[string]json.RawMessage
				if err := json.Unmarshal([]byte(result.output), &fields); err != nil {
					t.Fatal(err)
				}
				if string(fields["live_readiness"]) != `"not_evaluated"` {
					t.Fatalf("invented live readiness: %s", result.output)
				}
				if args[0] == "verify" && len(args) == 2 {
					// First-use store absence is valid but must remain explicit.
					if string(fields["validation_scope"]) != `"deployment"` || string(fields["admission_complete"]) != "false" || !strings.Contains(string(fields["observations"]), `"selected_store_access"`) || !strings.Contains(string(fields["observations"]), `"startup_process_possession"`) {
						t.Fatalf("lost fresh-store deployment admission: %s", result.output)
					}
				}
			}
		})
	}
	// The authored source gate stays intact: describe/verify must not bypass
	// malformed recursive definitions simply to produce a projection.
	writeReleaseFile(t, filepath.Join(source, "schema.yaml"), "name: broken\ncontext:\n  malformed: {type: array}\n")
	result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "verify", source, "--portable", "--json", "--config", config)
	if result.err == nil {
		t.Fatalf("compiled verify accepted malformed schema: %s", result.output)
	}
}

func TestFactoringCapabilitySubjectAggregateBothStores(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveFactoringCompiledStandingSubjects(t, binary, backend, false) })
	}
}

func TestAdmissionFactoringCompiledCapabilitySurfacesBothStores(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	t.Run("source-free-doctor", func(t *testing.T) {
		// Source-free doctor receives hostile source bytes. No provider launch or
		// paid credential probe is provisioned or credited.
		writeReleaseFile(t, filepath.Join(root, "schema.yaml"), "not: valid: source\n")
		_, env := clockReleaseConfig(t, root, "sqlite")
		result := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "doctor", "--backend", "claude_cli", "--workspace-backend", "host", "--json")
		if result.err != nil {
			exit, ok := result.err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 1 {
				t.Fatalf("doctor command failed outside prerequisites: %v\n%s", result.err, result.output)
			}
		}
		var report struct {
			OK                 bool            `json:"ok"`
			CapabilitySubjects []packs.Subject `json:"capability_subjects"`
			PackInventory      json.RawMessage `json:"pack_inventory"`
		}
		if err := json.Unmarshal([]byte(result.output), &report); err != nil {
			t.Fatal(err)
		}
		if len(report.CapabilitySubjects) == 0 {
			t.Fatalf("doctor omitted installed inventory: ok=%t capability_subjects=%d pack_inventory=%s", report.OK, len(report.CapabilitySubjects), report.PackInventory)
		}
		for _, subject := range report.CapabilitySubjects {
			if subject.Kind == packs.SubjectProviderTrigger && (subject.Applicability != "installed" || subject.Status != packs.StatusAvailable || subject.TriggerAdmission != nil) {
				t.Fatalf("source-free doctor invented target authority: %#v", subject)
			}
		}
	})
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveFactoringCompiledStandingSubjects(t, binary, backend, true) })
	}
}

func proveFactoringCompiledStandingSubjects(t *testing.T, binary, backend string, provision bool) {
	t.Helper()
	root := goldenReleaseRoot(t)
	config, env := clockReleaseConfig(t, root, backend)
	source := canonicalrouting.CopyStandingRootTreePublic(t)
	writeReleaseFile(t, filepath.Join(root, "api-token"), goldenAPIToken+"\n")
	spec := releaseProcessSpec{BinaryPath: binary, WorkingDir: root, Source: source, ConfigPath: config, Store: backend, TokenFile: "api-token", Token: goldenAPIToken, Env: env}
	start := func(wantRuns int) *releaseServeProcess {
		t.Helper()
		p := startReleaseServe(t, spec)
		ctx, cancel := context.WithTimeout(t.Context(), goldenStartupTimeout)
		defer cancel()
		if err := p.waitReady(ctx); err != nil {
			t.Fatal(err)
		}
		var inventory struct {
			Runs []struct {
				Origin struct {
					Kind string `json:"kind"`
				} `json:"origin"`
			} `json:"runs"`
			NextCursor string `json:"next_cursor"`
		}
		if err := p.rpc.call(ctx, "run.list", map[string]any{}, &inventory); err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, run := range inventory.Runs {
			if run.Origin.Kind == "standing_generation" {
				count++
			}
		}
		if inventory.NextCursor != "" || count != wantRuns {
			t.Fatalf("exact standing inventory: count=%d want=%d cursor=%s\n%s", count, wantRuns, inventory.NextCursor, p.output.String())
		}
		return p
	}
	p := start(0)
	for _, flow := range []string{".", "beta"} {
		if !strings.Contains(p.output.String(), "DORMANT ingress "+flow+"/telegram") {
			t.Fatalf("lost distinct dormant declaration %s\n%s", flow, p.output.String())
		}
	}
	if !strings.Contains(p.output.String(), "NOT READY") {
		t.Fatal("dormant readback manufactured readiness")
	}
	if provision {
		for _, alias := range []string{"alpha", "beta"} {
			result := runReleaseCommand(t, goldenStartupTimeout, root, env, "supplied-signing-value\n", binary, "secrets", "set", "webhook_signing."+alias)
			if result.err != nil {
				t.Fatalf("compiled secret provision: %v\n%s", result.err, result.output)
			}
			response, err := http.Post(p.apiBase+"/webhooks/"+alias+"/telegram", "application/json", strings.NewReader(`{"update_id":1}`))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusNotFound {
				t.Fatalf("credential insertion hot-activated %s: HTTP %d", alias, response.StatusCode)
			}
		}
	}
	if err := p.stopAndWait(goldenShutdownGrace); err != nil {
		t.Fatalf("public shutdown: %v\n%s", err, p.output.String())
	}
	if provision {
		p = start(2)
		for _, flow := range []string{".", "beta"} {
			if strings.Contains(p.output.String(), "DORMANT ingress "+flow+"/telegram") {
				t.Fatalf("restart did not evaluate provisioned target %s\n%s", flow, p.output.String())
			}
		}
		if !strings.Contains(p.output.String(), "READY") {
			t.Fatal("provisioned restart omitted readiness")
		}
		if strings.Contains(p.output.String(), "supplied-signing-value") {
			t.Fatal("readback leaked secret bytes")
		}
		if err := p.stopAndWait(goldenShutdownGrace); err != nil {
			t.Fatalf("provisioned shutdown: %v\n%s", err, p.output.String())
		}
	}
}

func TestAdmissionFactoringRegisteredToolExecution(t *testing.T) {
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	env := goldenProcessEnv(t, root, "", 0)
	assertGoldenProcessHasNoExternalExecutables(t, env)
	source := filepath.Join(releaseE2ERepoRoot(t), "internal/releasee2e/testdata/workspace_mcp")
	result := runReleaseCommand(t, time.Minute, root, env, "", binary, "test", source, "tests/transport.yaml", "--timeout", "20s", "--poll-interval", "25ms")
	if result.err != nil || !strings.Contains(result.output, "swarm test ok: scenarios=1") {
		t.Fatalf("compiled MockOnly host worker / real MCP / read / emit settlement: %v\n%s", result.err, result.output)
	}
	t.Log("public host MockOnly/private SQLite; both-store durable output/journal credit is separately named, not a deployment-config override")
}
