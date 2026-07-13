package main

import (
	"bytes"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"gopkg.in/yaml.v3"
)

func TestServeLifecyclePresenterConciseReadinessUsesTypedFacts(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Dev: true, Output: &out})
	presenter.boot(1, "process_start", "ok", "")
	presenter.boot(7, "recovery_decision", "clean_start", "no_active_run")
	presenter.recordStore(storebackend.Selection{
		Backend:          storebackend.BackendSQLite,
		BackendSource:    storebackend.SourceRolloutDefault,
		SQLitePath:       "/tmp/project/.swarm/stores/dev.db",
		SQLitePathSource: storebackend.SourceProjectDefault,
	})
	presenter.recordWorkspace("bundle-a", workspaceBackendSelection{Backend: workspace.BackendDocker, Reasons: []workspaceCapabilityReason{{Kind: workspaceReasonClaudeCLI, AgentID: "phrase-completer"}}})
	presenter.readyPresentation(serveLifecycleReadyFacts{
		ProjectName: "tg-chat",
		BundleCount: 1,
		FlowCount:   2,
		AgentCount:  1,
		ToolCount:   15,
		APIListener: "127.0.0.1:8081",
		MCPListener: "127.0.0.1:8082",
		ReadyAfter:  871 * time.Millisecond,
		Standing: []serveLifecycleIngressFact{
			{Provider: "telegram", URL: "http://127.0.0.1:8081/webhooks/ingress/telegram", SigningSecret: "webhook_signing.telegram", SigningBound: true, BundleHash: "bundle-b"},
			{Provider: "github", URL: "http://127.0.0.1:8081/webhooks/ingress/github", SigningSecret: "webhook_signing.github", SigningBound: false, BundleHash: "bundle-a"},
		},
	})
	presenter.shutdown(nil)
	presenter.finish()

	text := out.String()
	for _, want := range []string{
		"swarm serve --dev · tg-chat",
		"ready · 2 flows · 1 agent · 15 tools",
		"store                      sqlite · /tmp/project/.swarm/stores/dev.db",
		"workspace                  docker · agent \"phrase-completer\" runs in a container",
		"recovery                   clean start",
		"listeners                  api 127.0.0.1:8081 · mcp 127.0.0.1:8082",
		"ready in 871ms",
		"github webhook",
		"webhook_signing.github unbound",
		"telegram webhook",
		"webhook_signing.telegram bound",
		"shutdown · complete",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("concise lifecycle output missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "[1/22]") || strings.Contains(text, "\x1b[") {
		t.Fatalf("concise non-TTY output contains verbose or terminal decoration:\n%q", text)
	}
	if strings.Contains(text, "bundle-a") || strings.Contains(text, "bundle-b") {
		t.Fatalf("single-bundle concise output exposed internal bundle labels:\n%s", text)
	}
}

func TestServeLifecyclePresenterVerbosePreservesExactlyCanonicalBootSequence(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Verbose: true, Output: &out})
	names := []string{
		"process_start",
		"config_load",
		"db_connection",
		"bundle_load",
		"startup_ownership_lease",
		"recovery_snapshot_inspection",
		"recovery_decision",
		"pipeline_maintenance",
		"system_nodes_start",
		"schedule_restoration",
		"manager_recovery_if_enabled",
		"outbox_sweeper",
		"static_agents_bootstrap",
		"flow_required_agents",
		"workspace_validation_and_system_containers",
		"mcp_tool_validation",
		"manager_event_loop_start",
		"boot_self_check_optional",
		"platform_boot_event_published",
		"http_listener_bind",
		"health_endpoints_respond",
		"ready",
	}
	for i, name := range names {
		presenter.boot(i+1, name, "ok", "")
	}
	presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "project", APIListener: "127.0.0.1:8081", MCPListener: "127.0.0.1:8082"})

	rows := parseServeBootProgressRows(t, out.String())
	if len(rows) != runtime.BootProgressTotalSteps {
		t.Fatalf("verbose rows = %d, want %d\n%s", len(rows), runtime.BootProgressTotalSteps, out.String())
	}
	for i, row := range rows {
		if row.Step != i+1 || row.Total != runtime.BootProgressTotalSteps || row.Name != names[i] {
			t.Fatalf("row %d = %#v, want step/name %d/%s", i, row, i+1, names[i])
		}
	}
	for _, forbidden := range []string{"events(", "runs(", "table(count)"} {
		if strings.Contains(out.String(), forbidden) {
			t.Fatalf("verbose serve retained schema inventory %q:\n%s", forbidden, out.String())
		}
	}
}

func TestServeLifecyclePresenterFailureNeverPrintsReadiness(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Output: &out})
	presenter.fail(20, "http_listener_bind", errors.New("address already in use"))
	presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "must-not-render"})
	presenter.shutdown(nil)
	presenter.finish()

	text := out.String()
	if !strings.Contains(text, "serve failed · http listener bind · address already in use") {
		t.Fatalf("failure output = %q", text)
	}
	for _, forbidden := range []string{"\n  ready in ", "must-not-render", "shutdown · complete"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("failure output contains %q:\n%s", forbidden, text)
		}
	}
}

func TestServeLifecyclePresenterUnhandledDiagnosticFallsBackToGenericFailure(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Dev: true, Output: &out})
	presenter.failWithDiagnostic(5, "runtime_context", errors.New("construct runtime graph"), func(io.Writer) bool {
		return false
	})
	presenter.finish()

	text := out.String()
	if !strings.Contains(text, "serve failed · runtime context · construct runtime graph") {
		t.Fatalf("unhandled diagnostic dropped generic failure:\n%s", text)
	}
	if strings.Contains(text, "ready in") {
		t.Fatalf("unhandled diagnostic rendered readiness:\n%s", text)
	}
}

func TestServeLifecyclePresenterRendersZeroIngressAndShutdownFailure(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Output: &out})
	presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "empty", ReadyAfter: time.Millisecond})
	presenter.shutdown(errors.New("runtime drain timed out; dev container cleanup failed"))
	presenter.finish()

	text := out.String()
	for _, want := range []string{"standing ingress           none configured", "shutdown · failed · runtime drain timed out; dev container cleanup failed"} {
		if !strings.Contains(text, want) {
			t.Fatalf("output missing %q:\n%s", want, text)
		}
	}
}

func TestServeLifecyclePresenterDoesNotContradictRuntimeFailureWithCleanShutdown(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Output: &out})
	presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "project"})
	presenter.runtimeFailure("api server", errors.New("accept failed"))
	presenter.shutdown(nil)
	presenter.finish()

	text := out.String()
	if !strings.Contains(text, "runtime failed · api server · accept failed") {
		t.Fatalf("runtime failure missing:\n%s", text)
	}
	if strings.Contains(text, "shutdown · complete") {
		t.Fatalf("runtime failure was contradicted by clean shutdown:\n%s", text)
	}
}

func TestServeLifecyclePresenterPreReadyRuntimeFailureSuppressesCommit(t *testing.T) {
	var stdout, stderr bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Output: &stdout, ErrorOutput: &stderr})
	presenter.runtimeFailure("api server", errors.New("accept failed before readiness"))
	published := false
	if presenter.commitReady(serveLifecycleReadyFacts{ProjectName: "must-not-render"}, func() { published = true }) {
		t.Fatal("readiness committed after a pre-ready runtime failure")
	}
	presenter.finish()

	if published {
		t.Fatal("public readiness was published after a pre-ready runtime failure")
	}
	if stdout.Len() != 0 {
		t.Fatalf("pre-ready runtime failure rendered readiness: %q", stdout.String())
	}
	if text := stderr.String(); !strings.Contains(text, "serve failed · api server · accept failed before readiness") || strings.Contains(text, "must-not-render") {
		t.Fatalf("pre-ready runtime failure output is not one startup disposition:\n%s", text)
	}
}

func TestServeLifecyclePresenterUsesAuthorSafeMultiContextWorkspaceProjection(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Output: &out})
	presenter.recordWorkspace("review 1.0.0", workspaceBackendSelection{Backend: workspace.BackendDocker})
	presenter.recordWorkspace("notifications 2.0.0", workspaceBackendSelection{Backend: workspace.BackendDocker})
	presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "2 persisted bundles", BundleCount: 2})

	text := out.String()
	if strings.Count(text, "workspace                  docker · agent work runs in a container") != 1 {
		t.Fatalf("identical workspace decisions were not aggregated:\n%s", text)
	}
	for _, forbidden := range []string{"bundle-v1:sha256:", "sha256:", "fingerprint"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("multi-context projection exposed %q:\n%s", forbidden, text)
		}
	}
}

func TestServeLifecyclePresenterProjectsRecoveryOutcomesWithoutBookkeeping(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Output: &out})
	presenter.recordAbandonedWork(1, 2, 3)
	presenter.recordRecoveredWork(1, 2, 3, 4, 5, 6)
	presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "project"})

	text := out.String()
	for _, want := range []string{"active work cleared for a clean start", "unfinished work restored"} {
		if !strings.Contains(text, want) {
			t.Fatalf("recovery projection missing %q:\n%s", want, text)
		}
	}
	for _, forbidden := range []string{"deliveries", "sessions", "timers", "containers", "pipeline receipts"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("recovery projection exposed bookkeeping %q:\n%s", forbidden, text)
		}
	}
}

func TestServeLifecyclePresenterFailureUsesDiagnosticChannel(t *testing.T) {
	var stdout, stderr bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Output: &stdout, ErrorOutput: &stderr})
	presenter.fail(20, "http_listener_bind", errors.New("address already in use"))
	presenter.finish()

	if stdout.Len() != 0 {
		t.Fatalf("failure contaminated stdout: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "ERROR: serve failed · http listener bind · address already in use") {
		t.Fatalf("stderr missing canonical failure:\n%s", stderr.String())
	}
}

func TestServeLifecyclePresenterJoinsEarlyFailureAndCleanupOnce(t *testing.T) {
	var stdout, stderr bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Output: &stdout, ErrorOutput: &stderr})
	presenter.fail(4, "bundle_load", errors.New("bundle validation failed"))
	presenter.cleanupFailure("bundle source cleanup", errors.New("remove bundle tempdir"))
	presenter.cleanupFailure("store shutdown", errors.New("close journal"))
	presenter.finish()

	text := stderr.String()
	if strings.Count(text, "ERROR:") != 1 || strings.Count(text, "serve failed") != 1 {
		t.Fatalf("failure rendered more than one terminal disposition:\n%s", text)
	}
	for _, want := range []string{"bundle validation failed", "bundle source cleanup: remove bundle tempdir", "store shutdown: close journal"} {
		if !strings.Contains(text, want) {
			t.Fatalf("joined failure missing %q:\n%s", want, text)
		}
	}
	if stdout.Len() != 0 {
		t.Fatalf("joined failure contaminated stdout: %q", stdout.String())
	}
}

func TestServeLifecyclePresenterVerboseFailureCoalescesCanonicalSequence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Verbose: true, Output: &stdout, ErrorOutput: &stderr})
	presenter.boot(1, "process_start", "ok", "")
	presenter.boot(18, "boot_self_check_optional", "ok", "subscribed")
	presenter.boot(19, "platform_boot_event_published", "ok", "event-id")
	presenter.boot(18, "boot_self_check_optional", "FAILED", "self-check timed out")
	presenter.finish()

	if stdout.Len() != 0 {
		t.Fatalf("failed verbose startup contaminated stdout: %q", stdout.String())
	}
	rows := parseServeBootProgressRows(t, stderr.String())
	if len(rows) != 2 || rows[0].Step != 1 || rows[1].Step != 18 || rows[1].Name != "boot_self_check_optional" {
		t.Fatalf("coalesced rows = %#v, want canonical steps 1 and 18\n%s", rows, stderr.String())
	}
	if strings.Contains(stderr.String(), "platform_boot_event_published") || strings.Count(stderr.String(), "boot_self_check_optional") != 1 {
		t.Fatalf("verbose failure retained a later or duplicate step:\n%s", stderr.String())
	}
}

func TestServeLifecyclePresenterPreservesWarningsOnLateStartupFailure(t *testing.T) {
	var stdout, stderr bytes.Buffer
	presenter := newServeLifecyclePresenter(serveOptions{Output: &stdout, ErrorOutput: &stderr})
	report := runtimebootverify.Report{}
	report.Add(runtimebootverify.Finding{
		CheckID:     "unsigned_webhook",
		Severity:    runtimebootverify.SeveritySemanticDriftWarn,
		Location:    "telegram",
		Message:     "telegram accepts unsigned webhooks",
		Remediation: "configure webhook signing",
	})
	presenter.recordBootWarnings(report)
	presenter.fail(22, "ready", errors.New("start runtime contexts"))
	presenter.finish()

	text := stderr.String()
	if strings.Count(text, "[WARN]") != 1 || !strings.Contains(text, "telegram accepts unsigned webhooks") || !strings.Contains(text, "serve failed · ready") {
		t.Fatalf("late failure did not retain exactly one warning and terminal failure:\n%s", text)
	}
	if stdout.Len() != 0 {
		t.Fatalf("late failure contaminated stdout: %q", stdout.String())
	}
}

func TestServeLifecyclePresenterUsesResolvedStoreSelectionVerbatim(t *testing.T) {
	tests := []struct {
		name      string
		selection storebackend.Selection
		want      string
		forbidden string
	}{
		{
			name:      "project local",
			selection: storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: "/project/.swarm/stores/dev.db", SQLitePathSource: storebackend.SourceProjectDefault},
			want:      "sqlite · /project/.swarm/stores/dev.db",
		},
		{
			name:      "borrowed project key",
			selection: storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: "/home/user/.swarm/stores/projects/tg-chat-4eaae51056bb/dev.db", SQLitePathSource: storebackend.SourceSwarmDirDefault},
			want:      "sqlite · /home/user/.swarm/stores/projects/tg-chat-4eaae51056bb/dev.db",
		},
		{
			name:      "configured sqlite",
			selection: storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePath: "/var/lib/swarm/custom.db", SQLitePathSource: storebackend.SourceRuntimeConfig},
			want:      "sqlite · /var/lib/swarm/custom.db",
		},
		{
			name:      "postgres",
			selection: storebackend.Selection{Backend: storebackend.BackendPostgres, BackendSource: storebackend.SourceRuntimeConfig},
			want:      "postgres · path not applicable",
			forbidden: ".db",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			presenter := newServeLifecyclePresenter(serveOptions{Output: &out})
			presenter.recordStore(test.selection)
			presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "project"})
			if !strings.Contains(out.String(), test.want) {
				t.Fatalf("store output missing %q:\n%s", test.want, out.String())
			}
			if test.forbidden != "" && strings.Contains(out.String(), test.forbidden) {
				t.Fatalf("store output contains reconstructed path marker %q:\n%s", test.forbidden, out.String())
			}
		})
	}
}

func TestServeLifecycleOwnerRejectsParallelTerminalWriters(t *testing.T) {
	path := filepath.Join(repoRoot(), "cmd", "swarm", "main.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	guarded := map[string]bool{
		"runServeRuntime":                           true,
		"serveReadyStandingIngress":                 true,
		"serveLifecycleSourceCounts":                true,
		"serveLifecycleProjectName":                 true,
		"enforceServeBundleMatchAdmission":          true,
		"enforceServeBundleMatchAdmissionForHashes": true,
		"serveHTTPServer":                           true,
		"shutdownHTTPServer":                        true,
	}
	for _, declaration := range parsed.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !guarded[fn.Name.Name] {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			owner, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			if owner.Name == "log" || owner.Name == "slog" || (owner.Name == "fmt" && strings.HasPrefix(selector.Sel.Name, "Fprint")) {
				t.Errorf("%s bypasses serveLifecyclePresenter through %s.%s", fn.Name.Name, owner.Name, selector.Sel.Name)
			}
			return true
		})
	}

	for _, retired := range []string{"serveBootReporter", "logWorkspaceBackendDecision", "logReadySummary", "logReadyStandingIngress", "logBootWarnings"} {
		for _, declaration := range parsed.Decls {
			if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == retired {
				t.Errorf("retired parallel presentation owner %s remains live", retired)
			}
		}
	}

	presenterSource, err := os.ReadFile(filepath.Join(repoRoot(), "cmd", "swarm", "serve_lifecycle_presentation.go"))
	if err != nil {
		t.Fatalf("read serve lifecycle presenter: %v", err)
	}
	for _, forbidden := range []string{"workspaceBackendDecisionDetail(", "packs.RenderSubject("} {
		if strings.Contains(string(presenterSource), forbidden) {
			t.Errorf("serve lifecycle presenter delegates author-facing semantics through %s", forbidden)
		}
	}
}

func TestPlatformSpecOwnsServeLifecycleAndDoctorSchemaPresentation(t *testing.T) {
	var spec struct {
		CLISpecification struct {
			CommandCatalog struct {
				Serve struct {
					Boot struct {
						CanonicalOwner     string `yaml:"canonical_presentation_owner"`
						Rule               string `yaml:"rule"`
						OutputBoundary     string `yaml:"output_boundary"`
						ProjectionBoundary string `yaml:"projection_boundary"`
						ReadinessBoundary  string `yaml:"readiness_commit_boundary"`
						SchemaDetail       string `yaml:"schema_inventory_detail"`
						BootProgress       struct {
							CanonicalNameOwner string `yaml:"canonical_name_owner"`
							RenderingRule      string `yaml:"rendering_rule"`
						} `yaml:"boot_progress_sequence"`
					} `yaml:"boot_observability"`
				} `yaml:"serve"`
				Doctor struct {
					Command         string `yaml:"command"`
					SchemaInventory struct {
						CanonicalOwner string `yaml:"canonical_owner"`
						SourceOwner    string `yaml:"source_owner"`
						Behavior       string `yaml:"behavior"`
					} `yaml:"schema_inventory"`
				} `yaml:"doctor"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	boot := spec.CLISpecification.CommandCatalog.Serve.Boot
	for _, want := range []string{"serveLifecyclePresenter"} {
		if !strings.Contains(boot.CanonicalOwner, want) {
			t.Fatalf("boot canonical owner missing %q: %s", want, boot.CanonicalOwner)
		}
	}
	for _, want := range []string{"Default `swarm serve`", "`swarm serve --dev`", "does not imply verbose", "22-step"} {
		if !strings.Contains(boot.Rule, want) {
			t.Fatalf("boot rule missing %q: %s", want, boot.Rule)
		}
	}
	for _, want := range []string{"one writer", "Direct log", "Local foreground `swarm run start`", "buffered failure replay", "stdout", "stderr", "one joined terminal disposition"} {
		if !strings.Contains(boot.OutputBoundary, want) {
			t.Fatalf("boot output boundary missing %q: %s", want, boot.OutputBoundary)
		}
	}
	for _, want := range []string{"typed store", "Diagnostic detail renderers", "docker · agent", "clean start", "bound/unbound", "workflow name/version", "active work cleared", "never context labels"} {
		if !strings.Contains(boot.ProjectionBoundary, want) {
			t.Fatalf("boot projection boundary missing %q: %s", want, boot.ProjectionBoundary)
		}
	}
	for _, want := range []string{"one commit point", "standing-ingress", "`/readyz`", "`health.check`", "local foreground startup", "server/runtime failure before"} {
		if !strings.Contains(boot.ReadinessBoundary, want) {
			t.Fatalf("boot readiness boundary missing %q: %s", want, boot.ReadinessBoundary)
		}
	}
	for _, want := range []string{"coalesces repeated updates", "numeric order", "stderr", "canonical step name"} {
		if !strings.Contains(boot.BootProgress.RenderingRule, want) {
			t.Fatalf("boot rendering rule missing %q: %s", want, boot.BootProgress.RenderingRule)
		}
	}
	if !strings.Contains(boot.BootProgress.CanonicalNameOwner, "runtime.CanonicalBootProgressName") {
		t.Fatalf("boot canonical name owner is incomplete: %s", boot.BootProgress.CanonicalNameOwner)
	}
	for _, want := range []string{"Default, dev, explicit verbose", "MUST NOT render per-table", "doctor.schema_inventory"} {
		if !strings.Contains(boot.SchemaDetail, want) {
			t.Fatalf("schema disposition missing %q: %s", want, boot.SchemaDetail)
		}
	}
	doctor := spec.CLISpecification.CommandCatalog.Doctor
	if !strings.Contains(doctor.Command, "--schema-inventory") || !strings.Contains(doctor.SchemaInventory.CanonicalOwner, "doctor.schema_inventory") || !strings.Contains(doctor.SchemaInventory.SourceOwner, "stateStoreSchemaPlans") {
		t.Fatalf("doctor schema inventory ownership is incomplete: %#v", doctor)
	}
	for _, want := range []string{"without starting runtime", "without", "database state", "JSON adds a typed schema_inventory object"} {
		if !strings.Contains(doctor.SchemaInventory.Behavior, want) {
			t.Fatalf("doctor schema inventory behavior missing %q: %s", want, doctor.SchemaInventory.Behavior)
		}
	}
}
