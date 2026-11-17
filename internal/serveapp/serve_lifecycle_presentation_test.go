package serveapp

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestServeLifecyclePresenterMasksProofReuseFallbackPresentation(t *testing.T) {
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{})
	presenter.recordOperatorChannelProofReuse(operatorchannel.Binding{
		Interface:          operatorchannel.InterfaceIdentity{ChannelPackID: "provider.telegram.hitl_channel"},
		ExternalAccountRef: "123456789",
		ConversationScope:  operatorchannel.ConversationScopeDirect,
	})
	if len(presenter.operatorWarnings) != 1 {
		t.Fatalf("operator warnings = %#v", presenter.operatorWarnings)
	}
	warning := presenter.operatorWarnings[0]
	if strings.Contains(warning, "123456789") || !strings.Contains(warning, operatorchannel.MaskPresentation("123456789")) {
		t.Fatalf("proof reuse warning did not use masked claimant fallback: %q", warning)
	}
}

func TestServeLifecyclePresenterConciseReadinessUsesTypedFacts(t *testing.T) {
	var out bytes.Buffer
	accountRows := []channelonboarding.AccountStatus{
		{Provider: "whatsapp", Account: "+15550100", State: "live", Holders: []channelonboarding.AccountHolder{{Flow: "frontdesk"}}},
		{Provider: "whatsapp", Account: "+15550200", State: "refused", Reason: "registration_not_current"},
		{Provider: "whatsapp", Account: "+15550300", State: "refused", Health: "error", Cause: "connection reset", Reason: "competing_holders"},
	}
	reads := 0
	facts, _ := installedChannelFactsFor("whatsapp")
	observed, err := serveReadyDeploymentAccounts(t.Context(), []channelonboarding.Operation{
		(channelonboarding.Operation{DeploymentRegistration: true, Provider: "whatsapp", AccountReference: "+15550200", AccountConfirmed: true, Phase: channelonboarding.PhaseSucceeded}).WithProviderFacts(facts),
		(channelonboarding.Operation{DeploymentRegistration: true, Provider: "whatsapp", AccountReference: "+15550100", AccountConfirmed: true, Phase: channelonboarding.PhaseSucceeded}).WithProviderFacts(facts),
		(channelonboarding.Operation{DeploymentRegistration: true, Provider: "whatsapp", AccountReference: "+15550300", AccountConfirmed: true, Phase: channelonboarding.PhaseSucceeded}).WithProviderFacts(facts),
	}, func(_ context.Context, request apiv1.Request) (any, error) {
		reads++
		if request.Method != "channel.list" || request.Params["provider"] != "whatsapp" {
			t.Fatalf("account readiness bypassed the existing status read: %+v", request)
		}
		if _, exists := request.Params["holder"]; exists {
			t.Fatalf("account readiness sent caller holder evidence: %+v", request)
		}
		for _, row := range accountRows {
			if row.Account == request.Params["account"] {
				return row, nil
			}
		}
		return nil, errors.New("unknown account")
	})
	if err != nil || reads != 3 || !reflect.DeepEqual(observed, accountRows) {
		t.Fatalf("per-account readiness rows=%+v reads=%d err=%v", observed, reads, err)
	}
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Dev: true, Output: &out})
	presenter.boot(1, "process_start", "ok", "")
	presenter.boot(7, "recovery_decision", "clean_start", "no_active_run")
	presenter.recordStore(storebackend.Selection{
		Backend:          storebackend.BackendSQLite,
		BackendSource:    storebackend.SourceRolloutDefault,
		SQLitePath:       "/tmp/project/.swarm/stores/dev.db",
		SQLitePathSource: storebackend.SourceProjectDefault,
	})
	presenter.recordWorkspace("bundle-a", cliapp.WorkspaceBackendSelection{Backend: workspace.BackendDocker, Reasons: []cliapp.WorkspaceCapabilityReason{{Kind: cliapp.WorkspaceReasonClaudeCLI, AgentID: "phrase-completer"}}})
	recorder, ok := any(presenter).(interface {
		recordCredentialSources([]runtimecredentials.SourceFact)
	})
	if !ok {
		t.Fatal("serve presenter omits required credential source facts")
	}
	recorder.recordCredentialSources([]runtimecredentials.SourceFact{
		{Key: "telegram_bot_token", Reference: "file:/tmp/deployment/credentials.json"},
		{Key: "telegram_bot_token", Reference: "env:TELEGRAM_BOT_TOKEN"},
		{Key: "telegram_bot_token", Reference: "env:TELEGRAM_BOT_TOKEN"},
		{Key: "missing", Reference: ""},
	})
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
			{Provider: "telegram", URL: "http://127.0.0.1:8081/webhooks/ingress/telegram", BundleHash: "bundle-b", Subject: testServeIngressSubject("bundle-b", "ingress", "telegram", "webhook_signing.telegram", packs.RequirementStatusBound)},
			{Provider: "github", URL: "http://127.0.0.1:8081/webhooks/ingress/github", BundleHash: "bundle-a", Subject: testServeIngressSubject("bundle-a", "ingress", "github", "webhook_signing.github", packs.RequirementStatusUnbound)},
		},
		Accounts: observed,
	})
	presenter.shutdown(nil)
	presenter.finish()

	text := out.String()
	for _, want := range []string{
		"swarm serve --dev · tg-chat",
		"ready · 2 flows · 1 agent · 15 tools",
		"store                      sqlite · /tmp/project/.swarm/stores/dev.db · scratch; replaced by --dev",
		"workspace                  docker · agent \"phrase-completer\" runs in a container",
		"recovery                   clean start",
		"listeners                  api 127.0.0.1:8081 · mcp 127.0.0.1:8082",
		"ready in 871ms",
		"github webhook",
		"webhook_signing.github unbound",
		"telegram webhook",
		"WhatsApp +15550100 (frontdesk): connected",
		"WhatsApp +15550200: refused: registration_not_current",
		"WhatsApp +15550300: error: connection reset · competing_holders",
		"webhook_signing.telegram bound",
		"shutdown · complete",
		"credential telegram_bot_token · source file:/tmp/deployment/credentials.json",
		"credential telegram_bot_token · source env:TELEGRAM_BOT_TOKEN",
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
	if strings.Count(text, "source env:TELEGRAM_BOT_TOKEN") != 1 || strings.Contains(text, "credential missing") {
		t.Fatalf("credential facts were key-deduplicated, repeated, or fabricated:\n%s", text)
	}
	for _, state := range []string{"none", "stopped", "refused", "live"} {
		for _, scope := range []string{"exact", "base", "provider", "bundle", "flow", "unlinked", "recovery", "credentials", "invalid", "http"} {
			t.Run(state+"/"+scope, func(t *testing.T) {
				bundle, enabled := "bundle-v2:sha256:"+strings.Repeat("a", 64), false
				subject := packs.Subject{
					ID: "ingress:" + bundle + ":frontdesk:whatsapp", Kind: packs.SubjectProviderTrigger,
					Provider: "whatsapp", Source: "trigger_pack_binding", Applicability: "effective", Status: packs.StatusNotReady,
					TriggerAdmission: &packs.TriggerAdmission{
						Transport: packs.ChannelTransportSession, BindingEnabled: &enabled, BindingBlockReason: "session_admission_required",
						BundleHash: bundle, FlowPath: "frontdesk", Alias: "frontdesk", CatalogGeneration: strings.Repeat("b", 64),
						PolicySource: "verified_pack", RequestAuthentication: "SESSION_ACCOUNT", Event: "inbound.whatsapp.raw",
						Pack: &packs.TriggerPackIdentity{ID: "provider.whatsapp", Version: "1.0.0", ManifestHash: "sha256:" + strings.Repeat("c", 64), Provenance: "external"},
					},
					Capabilities: []packs.Capability{{Code: packs.CapabilityReceiveSessionEvents, Target: "whatsapp"}},
				}
				account := channelonboarding.AccountStatus{Provider: "whatsapp", Account: "+15550100", Linked: true, State: state,
					Holders: []channelonboarding.AccountHolder{{BundleHash: bundle, Flow: "frontdesk"}}}
				registrations := 0
				switch scope {
				case "base":
					subject.TriggerAdmission.BindingBlockReason = ""
				case "provider":
					account.Provider = "telegram"
				case "bundle":
					account.Holders[0].BundleHash = "other-bundle"
				case "flow":
					account.Holders[0].Flow = "other-flow"
				case "unlinked":
					account.Linked = false
				case "recovery":
					subject.TriggerAdmission.BindingBlockReason = "recovery_required"
					subject.TriggerAdmission.RecoveryOperationID, subject.TriggerAdmission.RecoveryCommand = "recovery-operation", "swarm channel connect whatsapp"
				case "credentials":
					subject.TriggerAdmission.BindingBlockReason = "credentials_absent"
				case "invalid":
					subject.TriggerAdmission.RequestAuthentication = "HMAC_SHA256"
				case "http":
					registrations = 1
				}
				accounts := []channelonboarding.AccountStatus{account}
				if state == "none" {
					accounts = nil
				} else if state == "refused" {
					accounts[0].Reason = "registration_not_current"
				}
				var output bytes.Buffer
				p := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &output})
				p.recordNoConnectedChannels(registrations, accounts)
				p.readyPresentation(serveLifecycleReadyFacts{Accounts: accounts, Standing: []serveLifecycleIngressFact{
					{Provider: "whatsapp", BundleHash: bundle, URL: "in-process session", Subject: subject},
				}})
				text := output.String()
				wantWarning := registrations == 0 && (state != "live" || scope == "unlinked")
				wantCapability := state != "live" || scope != "exact" && scope != "base" && scope != "http"
				if strings.Contains(text, "No channels connected yet") != wantWarning || strings.Contains(text, "  capability ") != wantCapability {
					t.Fatalf("warning=%t capability=%t expected:\n%s", wantWarning, wantCapability, text)
				}
				if scope == "recovery" && !strings.Contains(text, "recovery required") || scope == "invalid" && !strings.Contains(text, "INVALID") {
					t.Fatalf("independent declaration refusal was hidden:\n%s", text)
				}
			})
		}
	}
}

func TestServeLifecyclePresenterStylesOnlyWaitingToken(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Dev: true, Output: &out})
	presenter.waiting = func(value string) string { return "\x1b[33m" + value + "\x1b[0m" }
	if err := presenter.writeFeedReady(); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), "ready — \x1b[33mwaiting\x1b[0m for events\n"; got != want {
		t.Fatalf("feed-ready palette = %q, want %q", got, want)
	}
}

func TestServeLifecyclePresenterVerbosePreservesExactlyCanonicalBootSequence(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Verbose: true, Output: &out})
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
		"manager_recovery_if_enabled",
		"schedule_restoration",
		"static_agents_bootstrap",
		"flow_required_agents",
		"workspace_validation_and_system_containers",
		"mcp_tool_validation",
		"manager_event_loop_start",
		"outbox_sweeper",
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
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &out})
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
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Dev: true, Output: &out})
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
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &out})
	presenter.recordStore(storebackend.Selection{Backend: storebackend.BackendPostgres})
	presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "empty", ReadyAfter: time.Millisecond})
	presenter.shutdown(errors.New("runtime drain timed out; dev container cleanup failed"))
	presenter.finish()

	text := out.String()
	for _, want := range []string{"standing ingress           none configured", "shutdown · failed · runtime drain timed out; dev container cleanup failed", "postgres · path not applicable · retained"} {
		if !strings.Contains(text, want) {
			t.Fatalf("output missing %q:\n%s", want, text)
		}
	}
}

func TestServeLifecyclePresenterDoesNotContradictRuntimeFailureWithCleanShutdown(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &out})
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
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &stdout, ErrorOutput: &stderr})
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

func TestServeLifecyclePresenterPublishesAdmissionBeforeVisibleReadiness(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Verbose: true, Output: &out})
	presenter.boot(runtime.BootProgressTotalSteps, "ready", "ok", "")
	published := false
	if !presenter.commitReady(serveLifecycleReadyFacts{ProjectName: "project"}, func() {
		published = true
		if out.Len() != 0 {
			t.Fatalf("visible readiness preceded public admission: %q", out.String())
		}
	}) {
		t.Fatal("readiness commit was rejected")
	}
	if !published {
		t.Fatal("public readiness was not published")
	}
	if !strings.Contains(out.String(), "[22/22]") {
		t.Fatalf("visible readiness was not rendered after admission: %q", out.String())
	}
}

func TestServeLifecyclePresenterUsesAuthorSafeMultiContextWorkspaceProjection(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &out})
	presenter.recordWorkspace("review 1.0.0", cliapp.WorkspaceBackendSelection{Backend: workspace.BackendDocker})
	presenter.recordWorkspace("notifications 2.0.0", cliapp.WorkspaceBackendSelection{Backend: workspace.BackendDocker})
	presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "2 persisted bundles", BundleCount: 2})

	text := out.String()
	if strings.Count(text, "workspace                  docker · agent work runs in a container") != 1 {
		t.Fatalf("identical workspace decisions were not aggregated:\n%s", text)
	}
	for _, forbidden := range []string{"bundle-v2:sha256:", "sha256:", "fingerprint"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("multi-context projection exposed %q:\n%s", forbidden, text)
		}
	}
}

func TestServeLifecyclePresenterProjectsRecoveryOutcomesWithoutBookkeeping(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &out})
	presenter.recordClosedUnavailableWork()
	presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "project"})

	text := out.String()
	for _, want := range []string{"unfinished work could not be resumed and was closed"} {
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

func TestServeFailureRendersAcknowledgedRecoveryAction(t *testing.T) {
	for _, diagnostic := range []bool{false, true} {
		var out bytes.Buffer
		presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &out})
		presenter.recordClosedUnavailableWork()
		if diagnostic {
			presenter.failWithDiagnostic(5, "runtime_context", errors.New("startup failed"), func(w io.Writer) bool {
				_, _ = io.WriteString(w, "startup diagnostic\n")
				return true
			})
		} else {
			presenter.fail(5, "runtime_context", errors.New("startup failed"))
		}
		presenter.finish()
		text := out.String()
		if !strings.Contains(text, "recovery action") || !strings.Contains(text, "unfinished work could not be resumed and was closed") {
			t.Fatalf("diagnostic=%t failure dropped recovery action:\n%s", diagnostic, text)
		}
		if strings.Contains(text, "ready in") {
			t.Fatalf("diagnostic=%t failure rendered readiness:\n%s", diagnostic, text)
		}
	}
}

func TestServeLifecyclePresenterDoesNotTreatRecoveryAdmissionAsRestoration(t *testing.T) {
	var out bytes.Buffer
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &out})
	presenter.boot(7, "recovery_decision", "allowed", "recovery_enabled_with_persisted_work")
	presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "project"})

	text := out.String()
	if !strings.Contains(text, "recovery                   persisted work recovery enabled") {
		t.Fatalf("recovery admission projection missing:\n%s", text)
	}
	if strings.Contains(text, "restored") {
		t.Fatalf("recovery admission was presented as a terminal restoration:\n%s", text)
	}
}

func TestServeLifecyclePresenterFailureUsesDiagnosticChannel(t *testing.T) {
	var stdout, stderr bytes.Buffer
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &stdout, ErrorOutput: &stderr})
	presenter.fail(20, "http_listener_bind", errors.New("address already in use"))
	presenter.finish()

	if stdout.Len() != 0 {
		t.Fatalf("failure contaminated stdout: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "ERROR: serve failed · http listener bind · address already in use") {
		t.Fatalf("stderr missing canonical failure:\n%s", stderr.String())
	}
}

func TestServeOwnershipTranscriptUsesOnlyHumanProjectLanguage(t *testing.T) {
	t.Run("no metadata-only recovery teaching", func(t *testing.T) {
		var out bytes.Buffer
		presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &out})
		presenter.readyPresentation(serveLifecycleReadyFacts{ProjectName: "project"})
		if strings.Contains(out.String(), "Recovered previous session") {
			t.Fatalf("serving presentation inferred old process recovery:\n%s", out.String())
		}
	})

	for _, test := range []struct {
		name   string
		result runtimestartupownership.TerminalResult
		want   string
	}{
		{
			name: "exact successor",
			result: runtimestartupownership.TerminalResult{
				Cause: runtimestartupownership.TerminalOwnershipSuperseded, SuccessorAuthorityID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
			},
			want: "This serve lost ownership to a newer serve and is shutting down.\n",
		},
		{
			name:   "unprovable ownership",
			result: runtimestartupownership.TerminalResult{Cause: runtimestartupownership.TerminalOwnershipUnprovable},
			want:   "This serve can no longer verify project ownership and is shutting down.\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &stdout, ErrorOutput: &stderr})
			presenter.ownershipLost(test.result)
			presenter.finish()
			if stderr.String() != test.want || stdout.Len() != 0 {
				t.Fatalf("ownership transcript = stdout:%q stderr:%q, want stderr %q", stdout.String(), stderr.String(), test.want)
			}
			for _, forbidden := range []string{"lease", "grant", "generation", "authority", "fencing"} {
				if strings.Contains(strings.ToLower(stderr.String()), forbidden) {
					t.Fatalf("ownership transcript exposed %q: %s", forbidden, stderr.String())
				}
			}
		})
	}
}

func TestServeLifecyclePresenterJoinsEarlyFailureAndCleanupOnce(t *testing.T) {
	for _, name := range []string{"bundle_load", "startup_recovery"} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &stdout, ErrorOutput: &stderr})
			presenter.recordStartupContext(runtimeCompositionRequest{
				Repo: "/project's $work", Config: cliapp.RuntimeConfigLoadResult{Path: "/project/config's.yaml"},
				SwarmDir:       cliapp.CLISwarmDirResolution{Path: "/state"},
				StoreSelection: storebackend.Selection{Backend: storebackend.BackendSQLite, SQLitePathSource: storebackend.SourceSwarmDirDefault},
			})
			presenter.fail(4, name, errors.New("bundle validation failed"))
			presenter.cleanupFailure("bundle source cleanup", errors.New("remove bundle tempdir"))
			presenter.cleanupFailure("store shutdown", errors.New("close journal"))
			presenter.finish()

			text := stderr.String()
			heading := "serve failed"
			if name == "startup_recovery" {
				heading = "startup recovery refused"
			}
			if strings.Count(text, "ERROR:") != 1 || strings.Count(text, heading) != 1 {
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
			if name == "startup_recovery" {
				if !strings.Contains(text, "cd '/project'\\''s $work'") || !strings.Contains(text, "doctor --config '/project/config'\\''s.yaml'") || strings.Contains(text, "store status") {
					t.Fatalf("recovery diagnostics changed selectors/quoting or invented a borrowed-store selector:\n%s", text)
				}
			} else if strings.Contains(text, "doctor --config") {
				t.Fatalf("unrelated failure was labelled recovery:\n%s", text)
			}
		})
	}
}

func TestServeLifecyclePresenterVerboseFailureCoalescesCanonicalSequence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Verbose: true, Output: &stdout, ErrorOutput: &stderr})
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
	presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &stdout, ErrorOutput: &stderr})
	report := runtimebootverify.Report{}
	report.Add(runtimebootverify.Finding{
		CheckID:     "inbound_unsigned_webhook",
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
			presenter := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &out})
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
	path := filepath.Join(repoRootForTest(), "cmd", "swarm", "main.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	guarded := map[string]bool{
		"Run":                                        true,
		"serveReadyStandingIngress":                  true,
		"serveLifecycleSourceCounts":                 true,
		"serveLifecycleProjectName":                  true,
		"enforceServePinnedBundleAdmissionForHashes": true,
		"serveHTTPServer":                            true,
		"shutdownHTTPServer":                         true,
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

	presenterSource, err := os.ReadFile(filepath.Join(repoRootForTest(), "internal", "serveapp", "serve_lifecycle_presentation.go"))
	if err != nil {
		t.Fatalf("read serve lifecycle presenter: %v", err)
	}
	for _, forbidden := range []string{"workspaceBackendDecisionDetail("} {
		if strings.Contains(string(presenterSource), forbidden) {
			t.Errorf("serve lifecycle presenter delegates author-facing semantics through %s", forbidden)
		}
	}
}

func testServeIngressSubject(bundleHash, alias, provider, secret, status string) packs.Subject {
	return packs.Subject{
		ID: "ingress:" + bundleHash + ":" + alias + ":" + provider, Kind: packs.SubjectProviderTrigger,
		Provider: provider, Source: "raw_declaration", Applicability: "effective",
		TriggerAdmission: &packs.TriggerAdmission{
			Transport:  packs.ChannelTransportWebhook,
			BundleHash: bundleHash, FlowPath: alias, Alias: alias, CatalogGeneration: "catalog-generation",
			PolicySource: "raw_declaration", RequestAuthentication: "HMAC_SHA256", Event: "inbound." + provider,
		},
		Requirements: []packs.Requirement{packs.RequirementWithStatus(packs.RequirementSecret, secret, packs.RequirementScopeTarget, status, "credential_store")},
		Capabilities: []packs.Capability{{Code: packs.CapabilityReceiveHTTPSRoute, Target: "/webhooks/" + alias + "/" + provider}},
	}
}
