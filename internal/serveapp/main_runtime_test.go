package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/preservationcleanup"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type delayedRunStatusAgent struct {
	id            string
	subscriptions []events.EventType
	started       chan struct{}
	release       chan struct{}
}

type servedEventPublishBlockingLLMRuntime struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (r servedEventPublishBlockingLLMRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.AnthropicAPIProviderContract()
}

type servedSessionCleanupProofLLMRuntime struct {
	store   *store.PostgresStore
	started chan<- string
	release <-chan struct{}
}

func (r servedSessionCleanupProofLLMRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.AnthropicAPIProviderContract()
}

type servedLiveAgentProofLLMRuntime struct {
	calls             *atomic.Int32
	directiveFailures bool
}

func (servedLiveAgentProofLLMRuntime) ProviderContract() runtimellm.ProviderContract {
	return runtimellm.AnthropicAPIProviderContract()
}

type servedDirectivePersistenceFaults struct {
	mu          sync.Mutex
	afterCommit bool
	remaining   int
}

type servedPostgresDirectiveFaultStore struct {
	*store.PostgresStore
	faults *servedDirectivePersistenceFaults
}

type servedSQLiteDirectiveFaultStore struct {
	*store.SQLiteRuntimeStore
	faults *servedDirectivePersistenceFaults
}

func managedRuntimeAdmissionContextForTest(t testing.TB, ctx context.Context) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		"cmd-swarm-test-authority",
		1,
		"",
		"cmd-swarm-test-actors",
		"cmd-swarm-test-bundle",
		nil,
	)
	if err != nil {
		t.Fatalf("managedexecution.New: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}

func TestValidateServeAPIAuthBindingDefaultTokenLoopbackBoundary(t *testing.T) {
	defaultAuth := apiv1.AuthTokenResolution{Tokens: []string{apiv1.DefaultLoopbackAPIToken}, Source: apiv1.AuthTokenSourceBuiltInLoopbackToken}
	explicitAuth := apiv1.AuthTokenResolution{Tokens: []string{"operator-token"}, Source: apiv1.AuthTokenSource(serveAPITokenFileFlagSource), Explicit: true, TokenFile: filepath.Join(t.TempDir(), "token")}
	tests := []struct {
		name    string
		addr    string
		auth    apiv1.AuthTokenResolution
		wantErr string
	}{
		{name: "default token allowed on ipv4 loopback", addr: "127.0.0.1:8081", auth: defaultAuth},
		{name: "default token allowed on ipv6 loopback", addr: "[::1]:8081", auth: defaultAuth},
		{name: "default token rejects localhost", addr: "localhost:8081", auth: defaultAuth, wantErr: "non-loopback API bind localhost:8081 requires --api-token-file or config serve.api_token_file"},
		{name: "default token rejects wildcard", addr: "0.0.0.0:8081", auth: defaultAuth, wantErr: "non-loopback API bind 0.0.0.0:8081 requires --api-token-file or config serve.api_token_file"},
		{name: "default token rejects routable", addr: "192.0.2.10:8081", auth: defaultAuth, wantErr: "non-loopback API bind 192.0.2.10:8081 requires --api-token-file or config serve.api_token_file"},
		{name: "explicit token allows wildcard", addr: "0.0.0.0:8081", auth: explicitAuth},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateServeAPIAuthBinding(tc.addr, tc.auth)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateServeAPIAuthBinding: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestPlatformSpecWorkspaceDataSourceAuthorityPromoted(t *testing.T) {
	var spec struct {
		WorkspaceModel struct {
			DataSourceAuthority struct {
				PromotedBy                     string   `yaml:"promoted_by"`
				ImplementationStatus           string   `yaml:"implementation_status"`
				CanonicalOwner                 string   `yaml:"canonical_owner"`
				CLIFlag                        string   `yaml:"cli_flag"`
				ConfigKey                      string   `yaml:"config_key"`
				RetiredEnvVar                  string   `yaml:"retired_env_var"`
				SourceOrder                    []string `yaml:"source_order"`
				DefaultBehavior                string   `yaml:"default_behavior"`
				FailureBehavior                string   `yaml:"failure_behavior"`
				VolumesFromConflictRule        string   `yaml:"volumes_from_conflict_rule"`
				ReadPolicy                     string   `yaml:"read_policy"`
				RetiredNonAuthoritativeSources []string `yaml:"retired_non_authoritative_sources"`
				SplitScope                     []string `yaml:"split_scope"`
			} `yaml:"data_source_authority"`
		} `yaml:"workspace_model"`
		CLISpecification struct {
			CommandCatalog struct {
				Serve struct {
					WorkspaceDataSourceAuthority struct {
						PromotedBy string   `yaml:"promoted_by"`
						Owner      string   `yaml:"owner"`
						Flag       string   `yaml:"flag"`
						ConfigKey  string   `yaml:"config_key"`
						RetiredEnv string   `yaml:"retired_env_var"`
						Consumers  []string `yaml:"consumers"`
					} `yaml:"workspace_data_source_authority"`
				} `yaml:"serve"`
				Run struct {
					Flags []string `yaml:"flags"`
					Modes struct {
						ForegroundLocalStart struct {
							Invocation string `yaml:"invocation"`
							Behavior   string `yaml:"behavior"`
						} `yaml:"foreground_local_start"`
					} `yaml:"modes"`
				} `yaml:"run"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(cliapp.RepoRoot(), defaultPlatformSpecPath), &spec)
	authority := spec.WorkspaceModel.DataSourceAuthority
	if !strings.Contains(authority.PromotedBy, "#1139") || !strings.Contains(authority.PromotedBy, "#1223") || strings.TrimSpace(authority.ImplementationStatus) != "implemented" {
		t.Fatalf("workspace data source authority status = promoted_by:%q implementation_status:%q", authority.PromotedBy, authority.ImplementationStatus)
	}
	if !strings.Contains(authority.CanonicalOwner, "workspace_model.data_source_authority") {
		t.Fatalf("workspace data source canonical owner = %q", authority.CanonicalOwner)
	}
	if authority.CLIFlag != "--data" || authority.ConfigKey != "workspace.data_source" || authority.RetiredEnvVar != "SWARM_WORKSPACE_DATA_SOURCE" {
		t.Fatalf("workspace data source selectors = %#v", authority)
	}
	for _, want := range []string{"--data", "workspace.data_source", defaultWorkspaceDataSourceSource} {
		if !stringSliceContains(authority.SourceOrder, want) {
			t.Fatalf("workspace data source order missing %q: %#v", want, authority.SourceOrder)
		}
	}
	for _, want := range []string{defaultWorkspaceDataSourceRelativePath, "0755", "repo-local `data/`", "fail closed", "SWARM_WORKSPACE_VOLUMES_FROM", "read-only", "opaque"} {
		if !strings.Contains(authority.DefaultBehavior+authority.FailureBehavior+authority.VolumesFromConflictRule+authority.ReadPolicy+strings.Join(authority.RetiredNonAuthoritativeSources, "\n"), want) {
			t.Fatalf("workspace data source spec missing %q:\n%#v", want, authority)
		}
	}
	for _, want := range []string{"#1137", "#1138", "#1214", "workspace-init", "SWARM_WORKSPACE_DATA_MOUNT"} {
		if !joinedContains(authority.SplitScope, want) {
			t.Fatalf("workspace data source split scope missing %q: %#v", want, authority.SplitScope)
		}
	}
	command := spec.CLISpecification.CommandCatalog.Serve.WorkspaceDataSourceAuthority
	if !strings.Contains(command.PromotedBy, "#1139") || !strings.Contains(command.PromotedBy, "#1223") || command.Owner != "workspace_model.data_source_authority" || command.Flag != "--data <path>" {
		t.Fatalf("serve command data authority = %#v", command)
	}
	for _, want := range []string{"serve boot", "local foreground `swarm run start`", "Builder project reload", "selected-contract run-fork"} {
		if !joinedContains(command.Consumers, want) {
			t.Fatalf("serve command data authority consumers missing %q: %#v", want, command.Consumers)
		}
	}
	run := spec.CLISpecification.CommandCatalog.Run
	if !stringSliceContains(run.Flags, "--data <path>") {
		t.Fatalf("run command flags missing --data <path>: %#v", run.Flags)
	}
	if !strings.Contains(run.Modes.ForegroundLocalStart.Invocation, "--data <path>") || !strings.Contains(run.Modes.ForegroundLocalStart.Behavior, "workspace_model.data_source_authority") || !strings.Contains(run.Modes.ForegroundLocalStart.Behavior, "--connect") || !strings.Contains(run.Modes.ForegroundLocalStart.Behavior, "--reattach") {
		t.Fatalf("run local foreground data authority missing from spec: %#v", run.Modes.ForegroundLocalStart)
	}
}

func TestCloseServeRuntimeDevCleanupRunsAfterShutdownAndJoinsErrors(t *testing.T) {
	shutdownErr := fmt.Errorf("shutdown timed out")
	cleanupErr := fmt.Errorf("cleanup failed")
	var order []string
	supervisor := &runtimeProjectSupervisor{
		currentRT: &runtimepkg.Runtime{},
	}
	supervisor.shutdownRuntime = func(context.Context, *runtimepkg.Runtime, runtimepkg.ShutdownOptions) error {
		order = append(order, "shutdown")
		return shutdownErr
	}
	workspaces := serveRuntimeWorkspaceStub{
		cleanup: func(context.Context) (runtimedestructivereset.ContainerResetResult, error) {
			order = append(order, "cleanup")
			return runtimedestructivereset.ContainerResetResult{}, cleanupErr
		},
	}

	err := closeServeRuntime(context.Background(), supervisor, cliapp.ServeOptions{
		Dev:           true,
		ShutdownGrace: runtimepkg.DefaultShutdownGrace,
	}, workspaces)
	if err == nil || !strings.Contains(err.Error(), shutdownErr.Error()) || !strings.Contains(err.Error(), cleanupErr.Error()) {
		t.Fatalf("closeServeRuntime err = %v, want joined shutdown and cleanup errors", err)
	}
	if got := strings.Join(order, ","); got != "shutdown,cleanup" {
		t.Fatalf("order = %s, want shutdown,cleanup", got)
	}
	if got := supervisor.CurrentRuntime(); got != nil {
		t.Fatalf("CurrentRuntime after close = %p, want nil", got)
	}
}

func TestServeLifecyclePresenterProjectsBootFactsByMode(t *testing.T) {
	var quiet bytes.Buffer
	concise := newServeLifecyclePresenter(cliapp.ServeOptions{Output: &quiet})
	concise.boot(1, "process_start", "ok", "")
	if quiet.Len() != 0 {
		t.Fatalf("concise presenter emitted before readiness = %q", quiet.String())
	}

	var verbose bytes.Buffer
	verbosePresenter := newServeLifecyclePresenter(cliapp.ServeOptions{Verbose: true, Output: &verbose})
	verbosePresenter.boot(1, "process_start", "ok", "contracts=contracts")
	if verbose.Len() != 0 {
		t.Fatalf("verbose presenter emitted uncommitted startup facts = %q", verbose.String())
	}
	verbosePresenter.fail(2, "config_load", errors.New("invalid config"))
	verbosePresenter.finish()
	out := verbose.String()
	for _, want := range []string{"[1/22]", "process_start", "ok", "contracts=contracts", "[2/22]", "config_load", "FAILED"} {
		if !strings.Contains(out, want) {
			t.Fatalf("verbose output missing %q:\n%s", want, out)
		}
	}
}

func TestCLI_ServeLifecycleRoutesDiagnosticsToStderr(t *testing.T) {
	tests := []struct {
		name           string
		run            func(*serveLifecyclePresenter)
		code           int
		wantStderr     string
		wantStdout     string
		startupFailure bool
	}{
		{
			name: "generic startup failure", code: 1, wantStderr: "ERROR: serve failed · config load · missing config", startupFailure: true,
			run: func(p *serveLifecyclePresenter) { p.fail(2, "config_load", errors.New("missing config")) },
		},
		{
			name: "specialized startup failure", code: 3, wantStderr: "[BLOCKER] workspace_prerequisite", startupFailure: true,
			run: func(p *serveLifecyclePresenter) {
				p.failWithDiagnostic(5, "runtime_context", errors.New("workspace unavailable"), func(out io.Writer) bool {
					fmt.Fprintln(out, "[BLOCKER] workspace_prerequisite: workspace unavailable")
					return true
				})
			},
		},
		{
			name: "listener failure", code: 3, wantStderr: "ERROR: serve failed · http listener bind · address already in use", startupFailure: true,
			run: func(p *serveLifecyclePresenter) {
				p.fail(20, "http_listener_bind", errors.New("address already in use"))
			},
		},
		{
			name: "boot warning", code: 0, wantStderr: "WARNING: using the built-in development API token", wantStdout: "ready in",
			run: func(p *serveLifecyclePresenter) {
				p.recordDefaultAPITokenWarning()
				p.readyPresentation(serveLifecycleReadyFacts{ProjectName: "project"})
				p.shutdown(nil)
			},
		},
		{
			name: "runtime failure", code: 1, wantStderr: "ERROR: runtime failed · api server · accept failed", wantStdout: "ready in",
			run: func(p *serveLifecyclePresenter) {
				p.readyPresentation(serveLifecycleReadyFacts{ProjectName: "project"})
				p.runtimeFailure("api server", errors.New("accept failed"))
				p.shutdown(nil)
			},
		},
		{
			name: "failed shutdown", code: 1, wantStderr: "ERROR: shutdown · failed · drain timed out", wantStdout: "ready in",
			run: func(p *serveLifecyclePresenter) {
				p.readyPresentation(serveLifecycleReadyFacts{ProjectName: "project"})
				p.shutdown(errors.New("drain timed out"))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runServe := func(_ context.Context, _ string, serveOpts cliapp.ServeOptions) int {
				presenter := newServeLifecyclePresenter(serveOpts)
				test.run(presenter)
				presenter.finish()
				return test.code
			}
			var stdout, stderr bytes.Buffer
			code := cliapp.Execute(context.Background(), t.TempDir(), []string{"serve"}, &stdout, &stderr, runServe)
			if code != test.code {
				t.Fatalf("code = %d, want %d\nstdout=%s\nstderr=%s", code, test.code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), test.wantStderr) {
				t.Fatalf("stderr missing %q:\n%s", test.wantStderr, stderr.String())
			}
			if test.wantStdout != "" && !strings.Contains(stdout.String(), test.wantStdout) {
				t.Fatalf("stdout missing %q:\n%s", test.wantStdout, stdout.String())
			}
			if test.startupFailure && stdout.Len() != 0 {
				t.Fatalf("startup failure contaminated stdout: %q", stdout.String())
			}
		})
	}
}

func TestRunServeRuntimeJoinsEarlyStartupAndStoreCleanupFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	mock.ExpectClose().WillReturnError(errors.New("close journal"))

	oldBuildStores := buildStoresForServe
	buildStoresForServe = func(context.Context, storebackend.Selection, *config.Config) (storeBundle, error) {
		return storeBundle{SQLDB: db}, nil
	}
	t.Cleanup(func() { buildStoresForServe = oldBuildStores })

	var stdout, stderr bytes.Buffer
	code := Run(context.Background(), cliapp.RepoRoot(), cliapp.ServeOptions{
		ConfigPath:         writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", filepath.Join(t.TempDir(), "unused.sqlite"), nil),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "sqlite",
		StoreModeSet:       true,
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		RequireBundleMatch: false,
		TestBeforeReadinessCommit: func() error {
			return errors.New("startup failed before readiness commit")
		},
		Output:      &stdout,
		ErrorOutput: &stderr,
	})
	if code == 0 {
		t.Fatalf("Run code = 0, want startup failure\nstdout=%s\nstderr=%s", stdout.String(), stderr.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("close expectation: %v", err)
	}
	text := stderr.String()
	if strings.Count(text, "ERROR:") != 1 || !strings.Contains(text, "close journal") {
		t.Fatalf("startup and store cleanup did not produce one joined terminal failure:\n%s", text)
	}
	if stdout.Len() != 0 {
		t.Fatalf("early startup failure contaminated stdout: %q", stdout.String())
	}
}

func TestServeBundleMatchAdmissionRejectsActiveAvailabilityConflicts(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	bootFingerprint := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	persistedMissingRunID := uuid.NewString()
	legacyRunID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES ($1::uuid, 'running', 'bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222', 'persisted', now())
	`, persistedMissingRunID); err != nil {
		t.Fatalf("seed active persisted missing run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_source, bundle_fingerprint, started_at)
		VALUES ($1::uuid, 'paused', 'legacy', $2, now())
	`, legacyRunID, bootFingerprint); err != nil {
		t.Fatalf("seed active legacy run: %v", err)
	}

	err := enforceServeBundleMatchAdmission(ctx, pg, bootFingerprint, true, "")
	if err == nil {
		t.Fatal("enforceServeBundleMatchAdmission error = nil, want availability conflict")
	}
	got := err.Error()
	for _, want := range []string{
		"active run bundle availability conflict",
		persistedMissingRunID,
		"BUNDLE_DATA_INTEGRITY_ERROR",
		"persisted_missing_bundle_row",
		legacyRunID,
		"BUNDLE_UNAVAILABLE",
		"cause=legacy",
		"legacy_bundle_fingerprint=" + bootFingerprint,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("admission error = %q, want detail %q", got, want)
		}
	}

	pinnedHash := "bundle-v1:sha256:3333333333333333333333333333333333333333333333333333333333333333"
	err = enforceServeBundleMatchAdmission(ctx, pg, pinnedHash, false, pinnedHash)
	if err == nil || !strings.Contains(err.Error(), "active run bundle availability conflict") {
		t.Fatalf("DB-loaded disabled legacy admission error = %v, want active availability conflict", err)
	}
}

func TestServeBundleMatchAdmissionAllowsPersistedPresentAndDisabled(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	bootFingerprint := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	persistedHash := "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
	missingHash := "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"

	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: serve-test', '{}'::jsonb)
	`, persistedHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES
			($1::uuid, 'running', $2, 'persisted', now()),
			($3::uuid, 'completed', $4, 'persisted', now())
	`, uuid.NewString(), persistedHash, uuid.NewString(), missingHash); err != nil {
		t.Fatalf("seed persisted-present and completed-missing runs: %v", err)
	}
	if err := enforceServeBundleMatchAdmission(ctx, pg, bootFingerprint, true, ""); err != nil {
		t.Fatalf("enforceServeBundleMatchAdmission persisted-present/completed-missing: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES ($1::uuid, 'running', $2, 'persisted', now())
	`, uuid.NewString(), missingHash); err != nil {
		t.Fatalf("seed disabled persisted-missing run: %v", err)
	}
	if err := enforceServeBundleMatchAdmission(ctx, pg, bootFingerprint, false, ""); err != nil {
		t.Fatalf("enforceServeBundleMatchAdmission disabled: %v", err)
	}
}

func TestServeBundleMatchAdmissionRejectsDifferentPersistedActiveRunInDBLoadedMode(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	pinnedHash := "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
	otherHash := "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"
	otherRunID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES
			($1, 'name: pinned', '{}'::jsonb),
			($2, 'name: other', '{}'::jsonb)
	`, pinnedHash, otherHash); err != nil {
		t.Fatalf("seed bundle rows: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES
			($1::uuid, 'running', $2, 'persisted', now()),
			($3::uuid, 'paused', $4, 'persisted', now()),
			($5::uuid, 'completed', $4, 'persisted', now())
	`, uuid.NewString(), pinnedHash, otherRunID, otherHash, uuid.NewString()); err != nil {
		t.Fatalf("seed active persisted runs: %v", err)
	}

	err := enforceServeBundleMatchAdmission(ctx, pg, pinnedHash, true, pinnedHash)
	if err == nil {
		t.Fatal("enforceServeBundleMatchAdmission error = nil, want pinned bundle_hash conflict")
	}
	got := err.Error()
	for _, want := range []string{
		"active run pinned bundle_hash conflict",
		pinnedHash,
		otherRunID,
		otherHash,
		"bundle_source=persisted",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("admission error = %q, want detail %q", got, want)
		}
	}

	err = enforceServeBundleMatchAdmission(ctx, pg, pinnedHash, false, pinnedHash)
	if err == nil || !strings.Contains(err.Error(), "active run pinned bundle_hash conflict") {
		t.Fatalf("disabled legacy admission error = %v, want DB-loaded pinned conflict", err)
	}
}

func TestLoadServeRuntimeBundleFromCatalogLoadsPersistedRuntimeSource(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	if _, err := pg.BindSchemaCapabilities(ctx); err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}
	bundle := loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier12-runtime-tools", "test-flow-data-access"))
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	if _, err := pg.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		ParsedJSON:  projection.ParsedJSON,
		DataBlob:    projection.DataBlob,
		Metadata:    projection.Metadata,
	}); err != nil {
		t.Fatalf("UpsertBundleCatalog: %v", err)
	}
	runningPlatformSpecPath := runtimecontracts.DefaultPlatformSpecFile(cliapp.RepoRoot())
	if _, err := loadServeRuntimeBundleFromCatalog(ctx, cliapp.RepoRoot(), storeBundle{}, projection.BundleHash, runningPlatformSpecPath); err == nil || !strings.Contains(err.Error(), "requires selected bundle catalog store") {
		t.Fatalf("loadServeRuntimeBundleFromCatalog without selected catalog err = %v, want selected-owner failure", err)
	}

	stores := selectedPostgresStoreBundle(pg, &config.Config{})
	if stores.InboundStore == nil || stores.runtimeStores().InboundStore == nil {
		t.Fatal("selected Postgres store bundle missing InboundStore for served webhook ingress")
	}
	loaded, err := loadServeRuntimeBundleFromCatalog(ctx, cliapp.RepoRoot(), stores, projection.BundleHash, runningPlatformSpecPath)
	if err != nil {
		t.Fatalf("loadServeRuntimeBundleFromCatalog: %v", err)
	}
	defer func() {
		if loaded.cleanup != nil {
			if err := loaded.cleanup(); err != nil {
				t.Fatalf("cleanup DB-loaded runtime source: %v", err)
			}
		}
	}()

	if !loaded.dbLoaded {
		t.Fatal("dbLoaded = false, want true")
	}
	if loaded.serveIdentityDetail() != projection.BundleHash {
		t.Fatalf("serve identity = %q, want bundle hash %q", loaded.serveIdentityDetail(), projection.BundleHash)
	}
	authorLabel := serveRuntimeBundleAuthorLabel(loaded)
	if authorLabel == "" || strings.Contains(authorLabel, projection.BundleHash) || strings.Contains(authorLabel, loaded.bootIdentity.Fingerprint) {
		t.Fatalf("author label = %q, want workflow name/version without bundle identity", authorLabel)
	}
	if projectName := serveLifecycleProjectName(cliapp.LocalRuntimeStateResolution{}, []serveRuntimeBundle{loaded}); projectName != authorLabel {
		t.Fatalf("no-root project name = %q, want author label %q", projectName, authorLabel)
	}
	if loaded.bundleSourceFact.BundleHash != projection.BundleHash || loaded.bundleSourceFact.BundleSource != storerunlifecycle.BundleSourcePersisted {
		t.Fatalf("bundle source fact = %#v, want persisted %s", loaded.bundleSourceFact, projection.BundleHash)
	}
	if strings.Contains(loaded.contractsRoot, filepath.Join("tests", "tier12-runtime-tools", "test-flow-data-access")) {
		t.Fatalf("DB-loaded contracts root leaked local fixture path: %s", loaded.contractsRoot)
	}
	if _, err := os.Stat(filepath.Join(loaded.contractsRoot, "flows", "support", "data", "exclusions.yaml")); err != nil {
		t.Fatalf("DB-loaded source missing reconstructed data file: %v", err)
	}
	prepared, err := prepareLoadedServeBundleSource(ctx, stores, loaded, false)
	if err != nil {
		t.Fatalf("prepareLoadedServeBundleSource: %v", err)
	}
	if prepared.BundleHash != projection.BundleHash || prepared.BundleSource != storerunlifecycle.BundleSourcePersisted {
		t.Fatalf("prepared source fact = %#v, want persisted %s", prepared, projection.BundleHash)
	}
	if _, err := prepareLoadedServeBundleSource(ctx, stores, loaded, true); err == nil || !strings.Contains(err.Error(), "--bundle-hash is mutually exclusive with --dev") {
		t.Fatalf("prepareLoadedServeBundleSource dev error = %v", err)
	}
}

func TestRunServeRuntimeDBLoadedUsesEmbeddedSpecBeforeCatalogRead(t *testing.T) {
	_, _, pg := installServeRuntimePostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	bundle := loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-success"))
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	if _, err := pg.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		ParsedJSON:  projection.ParsedJSON,
		DataBlob:    projection.DataBlob,
		Metadata:    projection.Metadata,
	}); err != nil {
		t.Fatalf("UpsertBundleCatalog: %v", err)
	}

	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		BundleHash:         projection.BundleHash,
		PlatformSpecPath:   filepath.Join(t.TempDir(), "missing-platform-spec.yaml"),
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
	if strings.Contains(serve.outputString(), "missing-platform-spec.yaml") || strings.Contains(serve.outputString(), "read platform spec") {
		t.Fatalf("DB-loaded serve used missing local platform spec before catalog read:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeDBLoadedExecutesExplicitHostRefusal(t *testing.T) {
	_, _, pg := installServeRuntimePostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	bundleHash := seedServeRuntimeBundleCatalog(t, ctx, pg, doctorAgentContractsPath)
	var out lockedBuffer
	code := Run(ctx, cliapp.RepoRoot(), cliapp.ServeOptions{
		ConfigPath:         writeDoctorClaudeHostConfig(t, ""),
		BundleHash:         bundleHash,
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Output:             &out,
	})
	if code != cliapp.CLIExitRuntime {
		t.Fatalf("DB-loaded serve code = %d, want %d\n%s", code, cliapp.CLIExitRuntime, out.String())
	}
	assertClaudeHostRefusal(t, out.String())
}

func TestRunServeRuntimeDBLoadedExecutesDockerManagerRecovery(t *testing.T) {
	const dockerBin = "/opt/db-loaded-docker"
	var daemonProbes atomic.Int32
	contractsRoot := writeServeRuntimeNativeBashFixture(t)
	_, _, pg := installServeRuntimePostgresTestStoresWithWorkspaceFactory(t, func(mountSources cliapp.WorkspaceMountSources) cliapp.ServeWorkspaceLifecycle {
		manager := workspace.NewDockerManager(nil)
		cfg := workspace.DefaultDockerConfig()
		cfg.DockerBin = dockerBin
		cfg.WorkspaceImage = "db-loaded-workspace:test"
		cfg.SharedDataSource = mountSources.DataSource
		cfg.ContractsSource = contractsRoot
		manager.SetConfig(cfg)
		manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
			if len(args) > 0 && args[0] == "version" {
				daemonProbes.Add(1)
				return "", fmt.Errorf("daemon offline")
			}
			return "", nil
		})
		return manager
	})
	ctx := context.Background()
	bundleHash := seedServeRuntimeBundleCatalogRoot(t, ctx, pg, contractsRoot)
	var out lockedBuffer
	code := Run(ctx, cliapp.RepoRoot(), cliapp.ServeOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		BundleHash:         bundleHash,
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Output:             &out,
	})
	if code == 0 {
		t.Fatalf("DB-loaded serve code = 0, want Docker prerequisite failure\n%s", out.String())
	}
	if daemonProbes.Load() == 0 {
		t.Fatal("DB-loaded runtime did not execute DockerManager daemon probe")
	}
	for _, want := range []string{dockerBin, "Start the Docker daemon, then verify with `" + dockerBin + " info`"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("DB-loaded runtime output missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunServeRuntimeDBLoadedRunForkSupportedSurfaceExecutesAndStampsPersistedIdentity(t *testing.T) {
	_, db, pg := installServeRuntimePostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	bundle := loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-success"))
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	if _, err := pg.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		ParsedJSON:  projection.ParsedJSON,
		DataBlob:    projection.DataBlob,
		Metadata:    projection.Metadata,
	}); err != nil {
		t.Fatalf("UpsertBundleCatalog: %v", err)
	}

	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		BundleHash:         projection.BundleHash,
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		StoreModeSet:       true,
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
	})
	serve.waitForReadyLine()
	apiAddr := serveRuntimeAPIListenerFromOutput(t, serve.outputString())

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700000340, 0).UTC()
	seedRunForkSelectedExecutionSourceEvent(t, db, sourceRunID, entityID, sourceEventID, "task.requested", "complete-task", "pending", "Serve DB Loaded Entity", "serve-db-loaded-test", at)
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = $2,
		    bundle_source = $3
		WHERE run_id = $1::uuid
	`, sourceRunID, projection.BundleHash, storerunlifecycle.BundleSourcePersisted); err != nil {
		t.Fatalf("stamp source run bundle identity: %v", err)
	}

	body := fmt.Sprintf(
		`{"jsonrpc":"2.0","id":"fork","method":"run.fork","params":{"source_run_id":%q,"fork_event_id":%q,"confirm_source_freeze":true,"idempotency_key":"db-loaded-serve-fork"}}`,
		sourceRunID,
		sourceEventID,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+apiAddr+"/v1/rpc", strings.NewReader(body))
	if err != nil {
		t.Fatalf("build run.fork request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/rpc run.fork: %v\nserve output:\n%s", err, serve.outputString())
	}
	defer resp.Body.Close()
	var rpc struct {
		Result apiv1.RunForkExecutionResult `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Data    any    `json:"data,omitempty"`
		} `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpc); err != nil {
		t.Fatalf("decode run.fork response: %v", err)
	}
	if resp.StatusCode != http.StatusOK || rpc.Error != nil {
		t.Fatalf("run.fork status=%d error=%#v result=%#v\nserve output:\n%s", resp.StatusCode, rpc.Error, rpc.Result, serve.outputString())
	}
	if rpc.Result.SourceRunID != sourceRunID || rpc.Result.BundleHash != projection.BundleHash || rpc.Result.ExecutedEventCount != 1 {
		t.Fatalf("run.fork result = %#v, want source=%s bundle_hash=%s executed=1", rpc.Result, sourceRunID, projection.BundleHash)
	}
	if !rpc.Result.SourceFrozen || rpc.Result.SourceRunStatus != store.RunForkSourceFrozenStatus {
		t.Fatalf("run.fork source outcome = %#v, want frozen/forked", rpc.Result)
	}
	if rpc.Result.ForkRunID == "" || rpc.Result.ForkEventID != sourceEventID {
		t.Fatalf("run.fork fork identity = %#v, want fork run and source fork event %s", rpc.Result, sourceEventID)
	}

	var forkBundleHash, forkBundleSource, forkBundleFingerprint string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(bundle_hash, ''), COALESCE(bundle_source, ''), COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, rpc.Result.ForkRunID).Scan(&forkBundleHash, &forkBundleSource, &forkBundleFingerprint); err != nil {
		t.Fatalf("load fork run bundle identity: %v", err)
	}
	if forkBundleHash != projection.BundleHash || forkBundleSource != storerunlifecycle.BundleSourcePersisted || forkBundleFingerprint != "" {
		t.Fatalf("fork bundle identity = hash:%q source:%q fingerprint:%q, want persisted %s without legacy fingerprint", forkBundleHash, forkBundleSource, forkBundleFingerprint, projection.BundleHash)
	}
	var lineageRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_run_id = $2::uuid
		  AND source_event_id = $3::uuid
	`, rpc.Result.ForkRunID, sourceRunID, sourceEventID).Scan(&lineageRows); err != nil {
		t.Fatalf("count selected-contract execution lineage: %v", err)
	}
	if lineageRows != 1 {
		t.Fatalf("selected-contract execution lineage rows = %d, want 1", lineageRows)
	}

	advancedSourceRunID := uuid.NewString()
	advancedEntityID := uuid.NewString()
	advancedSourceEventID := uuid.NewString()
	advancedAfterEventID := uuid.NewString()
	advancedAt := at.Add(10 * time.Second)
	seedRunForkSelectedExecutionSourceEvent(t, db, advancedSourceRunID, advancedEntityID, advancedSourceEventID, "task.requested", "complete-task", "pending", "Serve Advanced Entity", "serve-advanced-test", advancedAt)
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = $2,
		    bundle_source = $3
		WHERE run_id = $1::uuid
	`, advancedSourceRunID, projection.BundleHash, storerunlifecycle.BundleSourcePersisted); err != nil {
		t.Fatalf("stamp advanced source run bundle identity: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			execution_mode, run_id, event_id, event_name, entity_id, flow_instance,
			scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ('live', $1::uuid, $2::uuid, 'source.after', $3::uuid, 'flow-a/1',
			'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, advancedSourceRunID, advancedAfterEventID, advancedEntityID, advancedAt.Add(time.Second)); err != nil {
		t.Fatalf("seed post-fork source event: %v", err)
	}
	captureRunForkCLIRevision(t, db, advancedSourceRunID, runforkrevision.FamilyEvents)

	stdout, stderr, code := runServedCLICommand(t, "http://"+apiAddr+"/v1/rpc", []string{
		"run", "fork", advancedSourceRunID,
		"--at-event", advancedSourceEventID,
		"--confirm-source-freeze",
		"--idempotency-key", "db-loaded-serve-advanced-fork",
		"--json",
	})
	if code != 0 {
		t.Fatalf("advanced swarm run fork code=%d stderr=%s stdout=%s\nserve output:\n%s", code, stderr, stdout, serve.outputString())
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("advanced swarm run fork stderr=%q, want empty", stderr)
	}
	var advancedResult apiv1.RunForkExecutionResult
	if err := json.Unmarshal([]byte(stdout), &advancedResult); err != nil {
		t.Fatalf("decode advanced swarm run fork json: %v\n%s", err, stdout)
	}
	if advancedResult.SourceRunID != advancedSourceRunID || advancedResult.SourceFrozen || advancedResult.SourceRunStatus != "running" || advancedResult.ForkRunStatus != store.RunForkActivatedStatus {
		t.Fatalf("advanced run.fork result = %#v, want preserved running source and activated fork", advancedResult)
	}
	var advancedSourceStatus, advancedContinuedAs string
	if err := db.QueryRowContext(ctx, `
		SELECT status, COALESCE(continued_as_run_id::text, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, advancedSourceRunID).Scan(&advancedSourceStatus, &advancedContinuedAs); err != nil {
		t.Fatalf("load advanced source outcome: %v", err)
	}
	if advancedSourceStatus != "running" || advancedContinuedAs != "" {
		t.Fatalf("advanced source status/continued_as = %q/%q, want running with no freeze pointer", advancedSourceStatus, advancedContinuedAs)
	}

	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
}

func TestRunServeRuntimeJoinFailureReachesAPIAndCLI(t *testing.T) {
	endpoint, db, bundleHash, _ := startServedJoinProofRuntime(t)
	initial := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name":      "order.started",
		"bundle_hash":     bundleHash,
		"payload":         map[string]any{"expected": []any{"a"}, "dispatch_id": "dispatch-1"},
		"idempotency_key": "join-failure-run-" + uuid.NewString(),
	})
	if !initial.NewRunCreated || initial.RunID == "" || initial.EventID == "" {
		t.Fatalf("join failure initial run = %#v", initial)
	}
	waitServedEventPublishDeliveryStatusCountForRun(t, db, "postgres", initial.RunID, initial.EventID, "node", "starter", "delivered", 1)
	waitServedEventPublishReceiptOutcomeCount(t, db, "postgres", initial.EventID, "platform", "pipeline", "success", 1)
	entityID := servedJoinEntityID(t, db, initial.RunID)

	arrival := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name": "item.completed", "run_id": initial.RunID, "source_event_id": initial.EventID,
		"payload":         map[string]any{"dispatch_id": "dispatch-1", "member_id": "a", "result": map[string]any{"ok": true}},
		"idempotency_key": "join-failure-arrival-" + uuid.NewString(),
	})
	waitServedEventPublishDeliveryStatusCountForRun(t, db, "postgres", initial.RunID, arrival.EventID, "node", "join-node", "dead_letter", 1)
	waitServedEventPublishReceiptOutcomeCount(t, db, "postgres", arrival.EventID, "node", "join-node", "dead_letter", 1)

	type failureReadback struct {
		EntityID   string `json:"entity_id"`
		Deliveries []struct {
			SubscriberType string                    `json:"subscriber_type"`
			SubscriberID   string                    `json:"subscriber_id"`
			Status         string                    `json:"status"`
			Failure        *runtimefailures.Envelope `json:"failure"`
		} `json:"deliveries"`
		DeadLetters []struct {
			Failure runtimefailures.Envelope `json:"failure"`
		} `json:"dead_letters"`
	}
	var event failureReadback
	requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": arrival.EventID}, &event)
	if event.EntityID != entityID || len(event.Deliveries) == 0 || len(event.DeadLetters) == 0 {
		t.Fatalf("join event.get evidence = %#v", event)
	}
	found := false
	for _, delivery := range event.Deliveries {
		if delivery.SubscriberType == "node" && delivery.SubscriberID == "join-node" && delivery.Status == "dead_letter" &&
			delivery.Failure != nil && delivery.Failure.Class == runtimefailures.ClassEarlyArrival && delivery.Failure.Detail.Code == "join_not_armed" {
			found = true
		}
	}
	if !found || event.DeadLetters[0].Failure.Class != runtimefailures.ClassEarlyArrival || event.DeadLetters[0].Failure.Detail.Code != "join_not_armed" {
		t.Fatalf("join event.get typed failure = %#v", event)
	}
	stdout, stderr, code := runServedCLICommand(t, endpoint, []string{"event", "view", arrival.EventID})
	if code != 0 || strings.TrimSpace(stderr) != "" {
		t.Fatalf("join event view code=%d stderr=%s stdout=%s", code, stderr, stdout)
	}
	for _, want := range []string{"subscriber=node/join-node", "status=dead letter", "failure=platform.early_arrival/join_not_armed", "dead_letters:"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("join event view missing %q:\n%s", want, stdout)
		}
	}
}

func TestRunServeRuntimeJoinForkReplayPreservesActivationAndTimer(t *testing.T) {
	endpoint, db, bundleHash, rt := startServedJoinProofRuntime(t)
	initial := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name": "order.started", "bundle_hash": bundleHash,
		"payload":         map[string]any{"expected": []any{"a", "b"}, "dispatch_id": "dispatch-1"},
		"idempotency_key": "join-fork-run-" + uuid.NewString(),
	})
	waitServedEventPublishDeliveryStatusCountForRun(t, db, "postgres", initial.RunID, initial.EventID, "node", "starter", "delivered", 1)
	entityID := servedJoinEntityID(t, db, initial.RunID)
	dispatched := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name": "order.dispatched", "run_id": initial.RunID, "source_event_id": initial.EventID,
		"payload": map[string]any{}, "idempotency_key": "join-fork-dispatch-" + uuid.NewString(),
	})
	waitServedEventPublishDeliveryStatusCountForRun(t, db, "postgres", initial.RunID, dispatched.EventID, "node", "dispatcher", "delivered", 1)
	waitServedEventPublishReceiptOutcomeCount(t, db, "postgres", dispatched.EventID, "platform", "pipeline", "success", 1)
	arrival := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name": "item.completed", "run_id": initial.RunID, "source_event_id": dispatched.EventID,
		"payload":         map[string]any{"dispatch_id": "dispatch-1", "member_id": "a", "result": map[string]any{"ok": true}},
		"idempotency_key": "join-fork-arrival-" + uuid.NewString(),
	})
	waitServedEventPublishDeliveryStatusCountForRun(t, db, "postgres", initial.RunID, arrival.EventID, "node", "join-node", "delivered", 1)
	waitServedEventPublishReceiptOutcomeCount(t, db, "postgres", arrival.EventID, "platform", "pipeline", "success", 1)
	waitServedJoinSourceTimer(t, db, initial.RunID)
	waitCtx, cancelWait := context.WithTimeout(context.Background(), servedProofPollDeadline)
	defer cancelWait()
	if err := rt.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("wait for join source quiescence before fork frontier: %v", err)
	}
	waitServedRunDeliveryQuiescence(t, db, initial.RunID)
	forkEventID := seedServedJoinForkFrontier(t, db, initial.RunID, entityID, arrival.EventID)
	if _, err := (&store.PostgresStore{DB: db}).PlanRunFork(context.Background(), store.RunForkPlanRequest{
		SourceRunID: initial.RunID,
		At:          forkEventID,
	}); err != nil {
		t.Fatalf("plan served join fork frontier: %v", err)
	}

	var fork apiv1.RunForkExecutionResult
	requireServedJSONRPCResult(t, endpoint, "run.fork", map[string]any{
		"source_run_id": initial.RunID, "fork_event_id": forkEventID, "confirm_source_freeze": true, "idempotency_key": "join-fork-" + uuid.NewString(),
	}, &fork)
	if fork.ForkRunID == "" || fork.SourceRunID != initial.RunID || fork.ExecutedEventCount != 1 {
		t.Fatalf("join run.fork result = %#v", fork)
	}
	forkCtx := runtimecorrelation.WithRunID(context.Background(), fork.ForkRunID)
	instance, ok, err := runtimepipeline.NewWorkflowInstanceStore(db).Load(forkCtx, entityID)
	if err != nil || !ok {
		t.Fatalf("load fork join instance = %#v, %v, %v", instance, ok, err)
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	activation, ok, err := joinruntime.Load(carrier.StateBuckets, "join-node", joinruntime.ActivationKey("awaiting", "awaiting", "dispatch-1"))
	if err != nil || !ok || activation.Status != joinruntime.StatusOpen || activation.Completed() != 1 || activation.Expected() != 2 {
		t.Fatalf("fork join activation = %#v, %v, %v", activation, ok, err)
	}
	if output, ok := activation.Outputs["a"]; !ok || output.Hash == "" {
		t.Fatalf("fork join output = %#v", activation.Outputs)
	}
	var fireEvent string
	var firePayload []byte
	var reconstructed int
	if err := db.QueryRowContext(context.Background(), `
		SELECT fire_event, fire_payload, COUNT(*) OVER ()
		FROM timers
		WHERE run_id = $1::uuid
		  AND source_timer_id IS NOT NULL
		  AND forked_from_run_id = $2::uuid
		  AND forked_from_event_id = $3::uuid
		  AND reconstruction_owner = $4
		  AND status = 'active'
	`, fork.ForkRunID, initial.RunID, forkEventID, store.RunForkHistoricalReplayTimerReconstructionOwner).Scan(&fireEvent, &firePayload, &reconstructed); err != nil {
		t.Fatalf("load reconstructed join timer: %v", err)
	}
	var timerPayload map[string]any
	if err := json.Unmarshal(firePayload, &timerPayload); err != nil {
		t.Fatalf("decode reconstructed join timer payload: %v", err)
	}
	handle, ok := timeridentity.ParseTimerHandle(timerPayload)
	if reconstructed != 1 || fireEvent != "platform.join_timeout" || !ok || handle.Kind != timeridentity.TimerHandleJoinTimeout ||
		handle.Join.Stage != "awaiting" || handle.Join.JoinID != "awaiting" {
		t.Fatalf("fork join timer = event:%q count:%d handle:%#v parsed:%v", fireEvent, reconstructed, handle, ok)
	}
}

func waitServedJoinSourceTimer(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	var count int
	for time.Now().Before(deadline) {
		if err := db.QueryRowContext(context.Background(), `
			SELECT COUNT(*)
			FROM timers
			WHERE run_id = $1::uuid
			  AND fire_event = 'platform.join_timeout'
			  AND status = 'active'
		`, runID).Scan(&count); err != nil {
			t.Fatalf("load served join source timer: %v", err)
		}
		if count == 1 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("served join source timers for run %s = %d, want 1\n%s", runID, count, servedEventPublishDebugSummary(t, db, "postgres", runID))
}

func waitServedRunDeliveryQuiescence(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	stable := 0
	for time.Now().Before(deadline) {
		var active int
		if err := db.QueryRowContext(context.Background(), `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE run_id = $1::uuid
			  AND status IN ('pending', 'in_progress')
		`, runID).Scan(&active); err != nil {
			t.Fatalf("count active served run deliveries: %v", err)
		}
		if active == 0 {
			stable++
			if stable == 4 {
				return
			}
		} else {
			stable = 0
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("served run %s deliveries did not remain quiescent\n%s", runID, servedEventPublishDebugSummary(t, db, "postgres", runID))
}

func TestRunServeRuntimeDBLoadedRunForkCrossBundleTargetExecutesAndStampsTargetIdentity(t *testing.T) {

	_, db, pg := installServeRuntimePostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	sourceBundle := loadWorkflowValidationFixtureBundle(t, filepath.Join("tests", "tier8-boot-verification", "test-boot-success"))
	sourceProjection, err := runtimecontracts.BuildBundleCatalogProjection(sourceBundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection(source): %v", err)
	}
	targetRoot := canonicalrouting.CopyRunForkTarget(t)
	targetBundle := loadWorkflowValidationBundleAt(t, targetRoot)
	targetProjection, err := runtimecontracts.BuildBundleCatalogProjection(targetBundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection(target): %v", err)
	}
	for label, projection := range map[string]runtimecontracts.BundleCatalogProjection{
		"source": sourceProjection,
		"target": targetProjection,
	} {
		if _, err := pg.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
			BundleHash:  projection.BundleHash,
			ContentYAML: projection.ContentYAML,
			ParsedJSON:  projection.ParsedJSON,
			DataBlob:    projection.DataBlob,
			Metadata:    projection.Metadata,
		}); err != nil {
			t.Fatalf("UpsertBundleCatalog(%s): %v", label, err)
		}
	}
	if sourceProjection.BundleHash == targetProjection.BundleHash {
		t.Fatalf("source and target projections unexpectedly share hash %s", sourceProjection.BundleHash)
	}

	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		BundleHash:         sourceProjection.BundleHash,
		BundleHashes:       []string{targetProjection.BundleHash},
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		StoreModeSet:       true,
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
	})
	serve.waitForReadyLine()
	apiAddr := serveRuntimeAPIListenerFromOutput(t, serve.outputString())

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700000345, 0).UTC()
	seedRunForkSelectedExecutionSourceEvent(t, db, sourceRunID, entityID, sourceEventID, "task.requested", "complete-task", "pending", "Serve Cross Bundle Entity", "serve-cross-bundle-test", at)
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = $2,
		    bundle_source = $3
		WHERE run_id = $1::uuid
	`, sourceRunID, sourceProjection.BundleHash, storerunlifecycle.BundleSourcePersisted); err != nil {
		t.Fatalf("stamp source run bundle identity: %v", err)
	}

	var stdout, stderr bytes.Buffer
	code := cliapp.Execute(ctx, t.TempDir(), []string{
		"run", "fork", sourceRunID,
		"--bundle-hash", targetProjection.BundleHash,
		"--at-event", sourceEventID,
		"--confirm-source-freeze",
		"--idempotency-key", "db-loaded-cross-bundle-serve-fork",
		"--json",
		"--api-server", "http://" + apiAddr,
	}, &stdout, &stderr, nil)
	if code != 0 {
		t.Fatalf("swarm fork code=%d stderr=%s stdout=%s\nserve output:\n%s", code, stderr.String(), stdout.String(), serve.outputString())
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("swarm fork stderr=%q, want empty", stderr.String())
	}
	var rpcResult apiv1.RunForkExecutionResult
	if err := json.Unmarshal(stdout.Bytes(), &rpcResult); err != nil {
		t.Fatalf("decode swarm fork json: %v\n%s", err, stdout.String())
	}
	if rpcResult.SourceRunID != sourceRunID || rpcResult.BundleHash != targetProjection.BundleHash || rpcResult.ExecutedEventCount != 1 {
		t.Fatalf("run.fork result = %#v, want source=%s target=%s executed=1", rpcResult, sourceRunID, targetProjection.BundleHash)
	}

	var forkBundleHash, forkBundleSource, forkBundleFingerprint string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(bundle_hash, ''), COALESCE(bundle_source, ''), COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, rpcResult.ForkRunID).Scan(&forkBundleHash, &forkBundleSource, &forkBundleFingerprint); err != nil {
		t.Fatalf("load fork run bundle identity: %v", err)
	}
	if forkBundleHash != targetProjection.BundleHash || forkBundleSource != storerunlifecycle.BundleSourcePersisted || forkBundleFingerprint != "" {
		t.Fatalf("fork bundle identity = hash:%q source:%q fingerprint:%q, want persisted target %s without legacy fingerprint", forkBundleHash, forkBundleSource, forkBundleFingerprint, targetProjection.BundleHash)
	}
	var mode, selectedHash, contractsRoot string
	if err := db.QueryRowContext(ctx, `
		SELECT mode, COALESCE(bundle_hash, ''), COALESCE(contracts_root, '')
		FROM run_fork_selected_contract_bindings
		WHERE fork_run_id = $1::uuid
	`, rpcResult.ForkRunID).Scan(&mode, &selectedHash, &contractsRoot); err != nil {
		t.Fatalf("load selected-contract binding: %v", err)
	}
	if mode != store.RunForkContractSelectionModeBundleHash || selectedHash != targetProjection.BundleHash || contractsRoot != "" {
		t.Fatalf("selected-contract binding = mode:%q hash:%q root:%q, want target bundle_hash selection", mode, selectedHash, contractsRoot)
	}
	var routeMode, routeHash string
	if err := db.QueryRowContext(ctx, `
		SELECT mode, COALESCE(bundle_hash, '')
		FROM run_fork_selected_contract_route_recoveries
		WHERE fork_run_id = $1::uuid
	`, rpcResult.ForkRunID).Scan(&routeMode, &routeHash); err != nil {
		t.Fatalf("load selected-contract route recovery: %v", err)
	}
	if routeMode != store.RunForkContractSelectionModeBundleHash || routeHash != targetProjection.BundleHash {
		t.Fatalf("route recovery = mode:%q hash:%q, want target bundle_hash selection", routeMode, routeHash)
	}
	var forkEventID string
	if err := db.QueryRowContext(ctx, `
		SELECT fork_event_id::text
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid AND source_event_id = $2::uuid
	`, rpcResult.ForkRunID, sourceEventID).Scan(&forkEventID); err != nil {
		t.Fatalf("load selected-contract execution lineage: %v", err)
	}
	var targetSubscriberDeliveries, sourceSubscriberDeliveries int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid AND event_id = $2::uuid AND subscriber_id = 'test-node'
	`, rpcResult.ForkRunID, forkEventID).Scan(&targetSubscriberDeliveries); err != nil {
		t.Fatalf("count target subscriber deliveries: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid AND event_id = $2::uuid AND subscriber_id = 'complete-task'
	`, rpcResult.ForkRunID, forkEventID).Scan(&sourceSubscriberDeliveries); err != nil {
		t.Fatalf("count source subscriber deliveries: %v", err)
	}
	if targetSubscriberDeliveries == 0 || sourceSubscriberDeliveries != 0 {
		t.Fatalf("fork delivery recipients target=%d source=%d, want target bundle route only", targetSubscriberDeliveries, sourceSubscriberDeliveries)
	}

	if code := serve.stop(); code != 0 {
		t.Fatalf("Run exit code = %d\noutput:\n%s", code, serve.outputString())
	}
}

func TestRunServeRuntimeEventPublishRunIDFollowUpServedPathDefaultSQLite(t *testing.T) {
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	contractsPath := writeServedEventPublishFollowUpFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	oldBuildStores := buildStoresForServe
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
	})
	var servedDB *sql.DB
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		stores, err := oldBuildStores(ctx, selection, cfg)
		if err == nil {
			servedDB = stores.SQLDB
		}
		return stores, err
	}
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath:              writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestLifecycleProbe:      probe,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})

	if servedDB == nil {
		t.Fatal("served sqlite SQLDB is required for event.publish proof")
	}
	runServedEventPublishFollowUpProof(t, endpoint, servedDB, "sqlite", bundleHash, probe)
}

func TestRunServeRuntimeEventPublishRunIDFollowUpServedPathPostgres(t *testing.T) {
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	contractsPath := writeServedEventPublishFollowUpFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		StoreMode:               "postgres",
		StoreModeSet:            true,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		Verbose:                 true,
		TestLifecycleProbe:      probe,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})

	runServedEventPublishFollowUpProof(t, endpoint, db, "postgres", bundleHash, probe)
}

func TestRunServeRuntimeEventPublishTargetRouteServedPathDefaultSQLite(t *testing.T) {
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	contractsPath := writeServedEventPublishTargetRouteFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	oldBuildStores := buildStoresForServe
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
	})
	var servedDB *sql.DB
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		stores, err := oldBuildStores(ctx, selection, cfg)
		if err == nil {
			servedDB = stores.SQLDB
		}
		return stores, err
	}
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath:              writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestLifecycleProbe:      probe,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})

	if servedDB == nil {
		t.Fatal("served sqlite SQLDB is required for event.publish target-route proof")
	}
	runServedEventPublishTargetRouteProof(t, endpoint, servedDB, "sqlite", bundleHash, probe)
}

func TestRunServeRuntimeEventPublishTargetRouteServedPathPostgres(t *testing.T) {
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	contractsPath := writeServedEventPublishTargetRouteFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		StoreMode:               "postgres",
		StoreModeSet:            true,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		Verbose:                 true,
		TestLifecycleProbe:      probe,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})

	runServedEventPublishTargetRouteProof(t, endpoint, db, "postgres", bundleHash, probe)
}

func TestRunServeRuntimeEventPublishExistingRunActiveLoadServedPathDefaultSQLite(t *testing.T) {
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	contractsPath := writeServedEventPublishActiveLoadFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	agentStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	oldBuildStores := buildStoresForServe
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
	})
	var servedDB *sql.DB
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		stores, err := oldBuildStores(ctx, selection, cfg)
		if err == nil {
			servedDB = stores.SQLDB
		}
		return stores, err
	}
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath:              writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestLifecycleProbe:      probe,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		TestLLMRuntime:          servedEventPublishBlockingLLMRuntime{started: agentStarted, release: release},
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	if servedDB == nil {
		t.Fatal("served sqlite SQLDB is required for active-load event.publish proof")
	}
	runServedEventPublishActiveLoadProof(t, endpoint, servedDB, "sqlite", bundleHash, probe, agentStarted, release, &releaseOnce)
}

func TestRunServeRuntimeEventPublishExistingRunActiveLoadServedPathPostgres(t *testing.T) {
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	contractsPath := writeServedEventPublishActiveLoadFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	agentStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		StoreMode:               "postgres",
		StoreModeSet:            true,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		Verbose:                 true,
		TestLifecycleProbe:      probe,
		TestLLMRuntime:          servedEventPublishBlockingLLMRuntime{started: agentStarted, release: release},
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	runServedEventPublishActiveLoadProof(t, endpoint, db, "postgres", bundleHash, probe, agentStarted, release, &releaseOnce)
}

func TestRunServeRuntimeBundleDeleteForceQuiescesSessionWriterBeforeCleanupPostgres(t *testing.T) {
	proof := startServedSessionCleanupProof(t)
	var result struct {
		OK      bool   `json:"ok"`
		Status  string `json:"status"`
		Deleted bool   `json:"deleted"`
	}
	runServedSessionCleanupMutation(t, proof, "bundle.delete", map[string]any{
		"bundle_hash": proof.BundleHash, "force": true, "idempotency_key": "issue-1927-bundle-force-" + uuid.NewString(),
	}, &result)
	if !result.OK || result.Status != "completed" || !result.Deleted {
		t.Fatalf("served bundle.delete result = %#v", result)
	}
	assertServedSessionCleanupQuiesced(t, proof)
}

func TestRunServeRuntimeNukeQuiescesSessionWriterBeforeCleanupPostgres(t *testing.T) {
	proof := startServedSessionCleanupProof(t)
	var result struct {
		OK             bool   `json:"ok"`
		Status         string `json:"status"`
		IncludeBundles bool   `json:"include_bundles"`
	}
	runServedSessionCleanupMutation(t, proof, "runtime.nuke", map[string]any{
		"include_bundles": false, "idempotency_key": "issue-1927-runtime-nuke-" + uuid.NewString(),
	}, &result)
	if !result.OK || result.Status != "completed" || result.IncludeBundles {
		t.Fatalf("served runtime.nuke result = %#v", result)
	}
	assertServedSessionCleanupQuiesced(t, proof)
}

type servedSessionCleanupProof struct {
	Endpoint   string
	DB         *sql.DB
	BundleHash string
	RunID      string
	SessionID  string
	Contexts   *runtimepkg.RuntimeContextManager
	Release    func()
}

func startServedSessionCleanupProof(t *testing.T) servedSessionCleanupProof {
	t.Helper()
	_, db, pg := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	contractsPath := writeServedSessionCleanupFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
	started := make(chan string, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	contextsReady := make(chan *runtimepkg.RuntimeContextManager, 1)
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath: writeServeRuntimeTestConfig(t), ContractsPath: contractsPath, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: "postgres", StoreModeSet: true, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, Verbose: true, TestLifecycleProbe: probe,
		TestLLMRuntime:          servedSessionCleanupProofLLMRuntime{store: pg, started: started, release: release},
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		TestRuntimeContextsReadyHook: func(contexts *runtimepkg.RuntimeContextManager) {
			contextsReady <- contexts
		},
	})
	releaseWriter := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseWriter)
	var contexts *runtimepkg.RuntimeContextManager
	select {
	case contexts = <-contextsReady:
	case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
		t.Fatal("timed out waiting for served runtime context manager")
	}
	initial := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name": "item.received", "bundle_hash": bundleHash,
		"payload": map[string]any{"item_id": "cleanup"}, "idempotency_key": "issue-1927-cleanup-initial-" + uuid.NewString(),
	})
	waitForServedEventPublishNodeDeliveryLifecycle(t, db, "postgres", initial.RunID, initial.EventID, probe)
	hold := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name": "hold/item.agent_hold", "run_id": initial.RunID, "source_event_id": initial.EventID,
		"payload": map[string]any{"note": "hold session writer"}, "idempotency_key": "issue-1927-cleanup-hold-" + uuid.NewString(),
	})
	var sessionID string
	select {
	case sessionID = <-started:
	case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
		t.Fatalf("timed out waiting for lifecycle-authorized session writer\n%s", servedEventPublishDebugSummary(t, db, "postgres", initial.RunID))
	}
	waitServedEventPublishDeliveryStatusCountForRun(t, db, "postgres", initial.RunID, hold.EventID, "agent", "load-agent", "in_progress", 1)
	var sessionRunID, status string
	if err := db.QueryRowContext(context.Background(), `
		SELECT COALESCE(run_id::text, ''), status
		FROM agent_sessions
		WHERE session_id = $1::uuid
	`, sessionID).Scan(&sessionRunID, &status); err != nil {
		t.Fatalf("load in-flight served session: %v", err)
	}
	if sessionRunID != initial.RunID || status != "active" {
		t.Fatalf("in-flight served session = run:%q status:%q, want %s/active", sessionRunID, status, initial.RunID)
	}
	return servedSessionCleanupProof{
		Endpoint: endpoint, DB: db, BundleHash: bundleHash, RunID: initial.RunID,
		SessionID: sessionID, Contexts: contexts, Release: releaseWriter,
	}
}

func runServedSessionCleanupMutation(t *testing.T, proof servedSessionCleanupProof, method string, params map[string]any, out any) {
	t.Helper()
	response := make(chan servedJSONRPCEnvelope, 1)
	go func() {
		response <- requestServedJSONRPC(t, proof.Endpoint, method, params)
	}()
	deadline := time.Now().Add(servedEventPublishLifecycleProbeWaitTimeout)
	for time.Now().Before(deadline) {
		lookup := proof.Contexts.LookupBundleHashStatus(proof.BundleHash)
		if lookup.State == runtimepkg.RuntimeContextStateUnloaded {
			proof.Release()
			select {
			case envelope := <-response:
				if envelope.Error != nil {
					t.Fatalf("%s error = %#v", method, envelope.Error)
				}
				if err := json.Unmarshal(envelope.Result, out); err != nil {
					t.Fatalf("decode %s result: %v\n%s", method, err, string(envelope.Result))
				}
				return
			case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
				t.Fatalf("timed out waiting for %s after runtime admission closed", method)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	proof.Release()
	t.Fatalf("timed out waiting for %s to close runtime admission", method)
}

func assertServedSessionCleanupQuiesced(t *testing.T, proof servedSessionCleanupProof) {
	t.Helper()
	var current int
	if err := proof.DB.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM agent_sessions
		WHERE session_id = $1::uuid
		  AND status IN ('active', 'suspended')
	`, proof.SessionID).Scan(&current); err != nil {
		t.Fatalf("count current served cleanup sessions: %v", err)
	}
	if current != 0 {
		t.Fatalf("current served cleanup sessions after shutdown barrier = %d, want 0", current)
	}
	var late int
	if err := proof.DB.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM agent_sessions
		WHERE agent_id = 'load-agent'
		  AND status IN ('active', 'suspended')
	`).Scan(&late); err != nil {
		t.Fatalf("count late served cleanup sessions: %v", err)
	}
	if late != 0 {
		t.Fatalf("late served cleanup sessions = %d, want 0 after successful runtime shutdown", late)
	}
}

func TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathDefaultSQLite(t *testing.T) {
	runServedDynamicAutoEmitBackendProof(t, servedparity.BackendDefaultSQLite)
}

func TestRunServeRuntimeEventPublishDynamicAutoEmitServedPathPostgres(t *testing.T) {
	runServedDynamicAutoEmitBackendProof(t, servedparity.BackendExplicitPostgres)
}

func TestServedParityHarnessEventPublishDynamicAutoEmitLifecycle(t *testing.T) {
	scenario := servedparity.MustScenario(servedparity.ScenarioEventPublishDynamicAutoEmitLifecycle)
	servedparity.Run(t, scenario, runServedDynamicAutoEmitBackendProof)
}

func TestCreateMintedCarryProjectionReachesHandlerFromPublicIngressOnBothBackends(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.TemplateCreateMintedKey)
	scenario := servedparity.MustScenario(servedparity.ScenarioEventPublishDynamicAutoEmitLifecycle)
	servedparity.Run(t, scenario, runServedCreateCarryProjectionBackendProof)
}

func TestServedParityHarnessLiveAgentEventReplayLifecycle(t *testing.T) {
	scenarios := []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioEventReplayLiveAgentLifecycle),
		servedparity.MustScenario(servedparity.ScenarioAgentReplayLiveAgentLifecycle),
	}
	servedparity.RunScenarioGroup(t, scenarios, runServedLiveAgentEventReplayBackendProof)
}

func TestServedParityHarnessRunControlLifecycle(t *testing.T) {
	scenarios := []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioRunPauseControlLifecycle),
		servedparity.MustScenario(servedparity.ScenarioRunContinueControlLifecycle),
		servedparity.MustScenario(servedparity.ScenarioRunStopControlLifecycle),
	}
	servedparity.RunScenarioGroup(t, scenarios, runServedRunControlBackendProof)
}

func TestServedParityHarnessLiveAgentReplayBacklogLifecycle(t *testing.T) {
	scenarios := []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioAgentReplayBacklogLiveAgentLifecycle),
	}
	servedparity.RunScenarioGroup(t, scenarios, runServedLiveAgentReplayBacklogBackendProof)
}

func TestServedParityHarnessAgentRestartLifecycle(t *testing.T) {
	scenario := servedparity.MustScenario(servedparity.ScenarioAgentRestartLifecycle)
	servedparity.Run(t, scenario, runServedAgentRestartBackendProof)
}

func TestServedParityHarnessAgentDirectiveOutcomeLifecycle(t *testing.T) {
	scenario := servedparity.MustScenario(servedparity.ScenarioAgentDirectiveOutcomeLifecycle)
	servedparity.Run(t, scenario, runServedAgentDirectiveBackendProof)
}

func TestServedParityHarnessRuntimeIngressControlLifecycle(t *testing.T) {
	scenarios := []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioRuntimePauseIngressLifecycle),
		servedparity.MustScenario(servedparity.ScenarioRuntimeResumeIngressLifecycle),
	}
	servedparity.RunScenarioGroup(t, scenarios, runServedRuntimeIngressControlBackendProof)
}

func TestServedParityHarnessMailboxDecisionLifecycle(t *testing.T) {
	scenarios := []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioMailboxNoticeAcknowledgmentLifecycle),
		servedparity.MustScenario(servedparity.ScenarioMailboxBeginInputLifecycle),
		servedparity.MustScenario(servedparity.ScenarioMailboxCancelInputLifecycle),
		servedparity.MustScenario(servedparity.ScenarioMailboxDecisionCardLifecycle),
		servedparity.MustScenario(servedparity.ScenarioMailboxDeferDecisionLifecycle),
	}
	servedparity.RunScenarioGroup(t, scenarios, runServedMailboxDecisionBackendProof)
}

func TestServedParityHarnessTestSetupEntitiesLifecycle(t *testing.T) {
	scenario := servedparity.MustScenario(servedparity.ScenarioTestSetupEntitiesLifecycle)
	servedparity.Run(t, scenario, runServedTestSetupEntitiesBackendProof)
}

func TestServedParityHarnessConversationForkLifecycle(t *testing.T) {
	scenarios := []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioConversationForkLifecycle),
		servedparity.MustScenario(servedparity.ScenarioConversationForkChatLifecycle),
		servedparity.MustScenario(servedparity.ScenarioConversationForkDeleteLifecycle),
	}
	servedparity.RunScenarioGroup(t, scenarios, runServedConversationForkBackendProof)
}

func TestRunServeRuntimeSQLiteOptionalMutatorsFailClosed(t *testing.T) {
	rt := startServedControlProofRuntime(t, servedparity.BackendDefaultSQLite)
	cases := []struct {
		method string
		params map[string]any
	}{
		{
			method: "bundle.register",
			params: map[string]any{
				"content_yaml":    "api_version: swarm.bundle.register.v1\nfiles: []\n",
				"idempotency_key": "issue-1386-sqlite-bundle-register",
			},
		},
		{
			method: "bundle.delete",
			params: map[string]any{
				"bundle_hash":     rt.BundleHash,
				"force":           true,
				"idempotency_key": "issue-1386-sqlite-bundle-delete",
			},
		},
		{
			method: "run.fork",
			params: map[string]any{
				"source_run_id":   uuid.NewString(),
				"fork_event_id":   uuid.NewString(),
				"idempotency_key": "issue-1386-sqlite-run-fork",
			},
		},
		{
			method: "runtime.nuke",
			params: map[string]any{
				"dry_run":         true,
				"idempotency_key": "issue-1386-sqlite-runtime-nuke",
			},
		},
	}
	for _, tc := range cases {
		t.Run(strings.ReplaceAll(tc.method, ".", "_"), func(t *testing.T) {
			errResp := requireServedJSONRPCError(t, rt.Endpoint, tc.method, tc.params)
			if errResp.Data["code"] != apiv1.MethodUnavailableCode {
				t.Fatalf("%s SQLite fail-closed data = %#v, want %s", tc.method, errResp.Data, apiv1.MethodUnavailableCode)
			}
		})
	}
}

type servedControlProofRuntime struct {
	Endpoint   string
	DB         *sql.DB
	Backend    string
	BundleHash string
	Probe      *lifecycletest.Probe
	Runtime    *runtimepkg.Runtime
}

type servedConversationForkProofRuntime struct {
	servedControlProofRuntime
	Credentials *runtimecredentials.FileStore
	LLMRequests *atomic.Int32
}

func startServedLiveAgentProofRuntime(t *testing.T, backend servedparity.Backend) servedControlProofRuntime {
	return startServedLiveAgentProofRuntimeWithLLM(t, backend, servedLiveAgentProofLLMRuntime{})
}

func startServedLiveAgentProofRuntimeWithLLM(t *testing.T, backend servedparity.Backend, llm servedLiveAgentProofLLMRuntime) servedControlProofRuntime {
	return startServedLiveAgentProofRuntimeWithLLMAndDirectiveFaults(t, backend, llm, nil)
}

func startServedLiveAgentProofRuntimeWithLLMAndDirectiveFaults(t *testing.T, backend servedparity.Backend, llm servedLiveAgentProofLLMRuntime, faults *servedDirectivePersistenceFaults) servedControlProofRuntime {
	t.Helper()
	switch backend {
	case servedparity.BackendDefaultSQLite:
		unsetStoreSelectorEnv(t)
		stubServeRuntimeWorkspaceLifecycle(t)
		sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
		contractsPath := writeServedLiveAgentFixture(t)
		bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
		probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
		oldBuildStores := buildStoresForServe
		t.Cleanup(func() {
			buildStoresForServe = oldBuildStores
		})
		var servedDB *sql.DB
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			stores, err := oldBuildStores(ctx, selection, cfg)
			if err == nil {
				servedDB = stores.SQLDB
				stores = wrapServedDirectiveFaultStore(t, stores, faults)
			}
			return stores, err
		}
		endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
			ConfigPath:              writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
			ContractsPath:           contractsPath,
			PlatformSpecPath:        defaultPlatformSpecPath,
			APIListenAddr:           "127.0.0.1:0",
			MCPListenAddr:           "127.0.0.1:0",
			SelfCheck:               true,
			RequireBundleMatch:      false,
			NoRequireBundleMatch:    true,
			Verbose:                 true,
			TestLifecycleProbe:      probe,
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
			TestLLMRuntime:          llm,
		})
		if servedDB == nil {
			t.Fatal("served sqlite SQLDB is required for live-agent served parity proof")
		}
		return servedControlProofRuntime{Endpoint: endpoint, DB: servedDB, Backend: "sqlite", BundleHash: bundleHash, Probe: probe}
	case servedparity.BackendExplicitPostgres:
		_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
			return serveRuntimeWorkspaceStub{}
		})
		if faults != nil {
			oldBuildStores := buildStoresForServe
			t.Cleanup(func() { buildStoresForServe = oldBuildStores })
			buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
				stores, err := oldBuildStores(ctx, selection, cfg)
				if err == nil {
					stores = wrapServedDirectiveFaultStore(t, stores, faults)
				}
				return stores, err
			}
		}
		contractsPath := writeServedLiveAgentFixture(t)
		bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
		probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
		endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
			ConfigPath:              writeServeRuntimeTestConfig(t),
			ContractsPath:           contractsPath,
			PlatformSpecPath:        defaultPlatformSpecPath,
			StoreMode:               "postgres",
			StoreModeSet:            true,
			APIListenAddr:           "127.0.0.1:0",
			MCPListenAddr:           "127.0.0.1:0",
			SelfCheck:               true,
			RequireBundleMatch:      false,
			Verbose:                 true,
			TestLifecycleProbe:      probe,
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
			TestLLMRuntime:          llm,
		})
		return servedControlProofRuntime{Endpoint: endpoint, DB: db, Backend: "postgres", BundleHash: bundleHash, Probe: probe}
	default:
		t.Fatalf("unknown live-agent served parity backend %q", backend)
		return servedControlProofRuntime{}
	}
}

func wrapServedDirectiveFaultStore(t *testing.T, stores storeBundle, faults *servedDirectivePersistenceFaults) storeBundle {
	t.Helper()
	if faults == nil {
		return stores
	}
	switch eventStore := stores.EventStore.(type) {
	case *store.PostgresStore:
		stores.EventStore = &servedPostgresDirectiveFaultStore{PostgresStore: eventStore, faults: faults}
	case *store.SQLiteRuntimeStore:
		stores.EventStore = &servedSQLiteDirectiveFaultStore{SQLiteRuntimeStore: eventStore, faults: faults}
	default:
		t.Fatalf("unsupported served directive event store %T", stores.EventStore)
	}
	return stores
}

func startServedControlProofRuntime(t *testing.T, backend servedparity.Backend) servedControlProofRuntime {
	return startServedControlProofRuntimeWithFixture(t, backend, writeServedEventPublishFollowUpFixture)
}

func startServedControlProofRuntimeWithFixture(t *testing.T, backend servedparity.Backend, fixture func(*testing.T) string) servedControlProofRuntime {
	t.Helper()
	switch backend {
	case servedparity.BackendDefaultSQLite:
		unsetStoreSelectorEnv(t)
		stubServeRuntimeWorkspaceLifecycle(t)
		sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
		contractsPath := fixture(t)
		bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
		probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
		oldBuildStores := buildStoresForServe
		t.Cleanup(func() {
			buildStoresForServe = oldBuildStores
		})
		var servedDB *sql.DB
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			stores, err := oldBuildStores(ctx, selection, cfg)
			if err == nil {
				servedDB = stores.SQLDB
			}
			return stores, err
		}
		endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
			ConfigPath:              writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
			ContractsPath:           contractsPath,
			PlatformSpecPath:        defaultPlatformSpecPath,
			APIListenAddr:           "127.0.0.1:0",
			MCPListenAddr:           "127.0.0.1:0",
			SelfCheck:               true,
			RequireBundleMatch:      false,
			NoRequireBundleMatch:    true,
			Verbose:                 true,
			TestLifecycleProbe:      probe,
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		})
		if servedDB == nil {
			t.Fatal("served sqlite SQLDB is required for control served parity proof")
		}
		return servedControlProofRuntime{Endpoint: endpoint, DB: servedDB, Backend: "sqlite", BundleHash: bundleHash, Probe: probe}
	case servedparity.BackendExplicitPostgres:
		_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
			return serveRuntimeWorkspaceStub{}
		})
		contractsPath := fixture(t)
		bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
		probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
		endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
			ConfigPath:              writeServeRuntimeTestConfig(t),
			ContractsPath:           contractsPath,
			PlatformSpecPath:        defaultPlatformSpecPath,
			StoreMode:               "postgres",
			StoreModeSet:            true,
			APIListenAddr:           "127.0.0.1:0",
			MCPListenAddr:           "127.0.0.1:0",
			SelfCheck:               true,
			RequireBundleMatch:      false,
			Verbose:                 true,
			TestLifecycleProbe:      probe,
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		})
		return servedControlProofRuntime{Endpoint: endpoint, DB: db, Backend: "postgres", BundleHash: bundleHash, Probe: probe}
	default:
		t.Fatalf("unknown served control backend %q", backend)
		return servedControlProofRuntime{}
	}
}

func runServedRunControlBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	rt := startServedControlProofRuntime(t, backend)
	runServedRunControlLifecycleProof(t, rt)
}

func runServedLiveAgentEventReplayBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	rt := startServedLiveAgentProofRuntime(t, backend)
	runServedLiveAgentEventReplayLifecycleProof(t, rt)
}

func runServedLiveAgentReplayBacklogBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	rt := startServedLiveAgentProofRuntime(t, backend)
	runServedLiveAgentReplayBacklogLifecycleProof(t, rt)
}

func runServedAgentRestartBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	rt := startServedLiveAgentProofRuntime(t, backend)
	runServedAgentRestartLifecycleProof(t, rt)
}

func runServedAgentDirectiveBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	var effects atomic.Int32
	faults := &servedDirectivePersistenceFaults{}
	rt := startServedLiveAgentProofRuntimeWithLLMAndDirectiveFaults(t, backend, servedLiveAgentProofLLMRuntime{calls: &effects, directiveFailures: true}, faults)
	runServedAgentDirectiveOutcomeLifecycleProof(t, rt, &effects, faults)
}

func runServedRuntimeIngressControlBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	rt := startServedControlProofRuntimeWithFixture(t, backend, writeServedExternalEventFixture)
	runServedRuntimeIngressControlLifecycleProof(t, rt)
}

func runServedMailboxDecisionBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	rt := startServedControlProofRuntime(t, backend)
	runServedMailboxDecisionLifecycleProof(t, rt)
}

func runServedTestSetupEntitiesBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	rt := startServedTestSetupEntitiesProofRuntime(t, backend)
	runServedTestSetupEntitiesLifecycleProof(t, rt)
}

func runServedConversationForkBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	rt := startServedConversationForkProofRuntime(t, backend)
	runServedConversationForkLifecycleProof(t, rt)
}

func startServedConversationForkProofRuntime(t *testing.T, backend servedparity.Backend) servedConversationForkProofRuntime {
	t.Helper()
	requests := &atomic.Int32{}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var body struct {
			Messages []struct {
				Role string `json:"role"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		hasToolResult := false
		for _, message := range body.Messages {
			if message.Role == "tool" {
				hasToolResult = true
				break
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if hasToolResult {
			_, _ = w.Write([]byte(`{"model":"gpt-compatible","choices":[{"message":{"role":"assistant","content":"snapshot inspected; requested event remained sandboxed"}}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}`))
			return
		}
		_, _ = w.Write([]byte(`{"model":"gpt-compatible","choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"snapshot-read","type":"function","function":{"name":"fork_snapshot_read_entities","arguments":"{}"}},{"id":"event-stub","type":"function","function":{"name":"emit_event","arguments":"{\"event_name\":\"forkchat.note\"}"}}]}}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`))
	}))
	t.Cleanup(provider.Close)

	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	credentials, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("create forkchat proof credential store: %v", err)
	}
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)

	start := func(configPath string, opts cliapp.ServeOptions) servedControlProofRuntime {
		oldBuildStores := buildStoresForServe
		t.Cleanup(func() { buildStoresForServe = oldBuildStores })
		var servedDB *sql.DB
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			stores, err := oldBuildStores(ctx, selection, cfg)
			if err == nil {
				servedDB = stores.SQLDB
			}
			return stores, err
		}
		opts.ConfigPath = configPath
		contractsPath := writeServedExternalEventFixture(t)
		opts.ContractsPath = contractsPath
		opts.PlatformSpecPath = defaultPlatformSpecPath
		opts.APIListenAddr = "127.0.0.1:0"
		opts.MCPListenAddr = "127.0.0.1:0"
		opts.SelfCheck = true
		opts.RequireBundleMatch = false
		opts.NoRequireBundleMatch = true
		opts.Verbose = true
		opts.TestLLMRuntime = runtimellm.NoopRuntime{}
		endpoint, rt := startServedEventPublishFollowUpRuntime(t, opts)
		if servedDB == nil {
			t.Fatal("served conversation fork SQLDB is required")
		}
		return servedControlProofRuntime{Endpoint: endpoint, DB: servedDB, BundleHash: servedEventPublishFixtureBundleHash(t, contractsPath), Runtime: rt}
	}

	var rt servedControlProofRuntime
	switch backend {
	case servedparity.BackendDefaultSQLite:
		unsetStoreSelectorEnv(t)
		stubServeRuntimeWorkspaceLifecycle(t)
		sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
		configPath := writeServedConversationForkConfig(t, storebackend.BackendSQLite.String(), sqlitePath, provider.URL)
		rt = start(configPath, cliapp.ServeOptions{})
		rt.Backend = "sqlite"
	case servedparity.BackendExplicitPostgres:
		_, _, _ = installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
		configPath := writeServedConversationForkConfig(t, storebackend.BackendPostgres.String(), "", provider.URL)
		rt = start(configPath, cliapp.ServeOptions{StoreMode: "postgres", StoreModeSet: true})
		rt.Backend = "postgres"
	default:
		t.Fatalf("unknown conversation fork served parity backend %q", backend)
	}
	return servedConversationForkProofRuntime{servedControlProofRuntime: rt, Credentials: credentials, LLMRequests: requests}
}

func writeServedConversationForkConfig(t *testing.T, backend, sqlitePath, providerURL string) string {
	t.Helper()
	lines := []string{
		"runtime:",
		"  recovery_on_startup: false",
		"workspace:",
		"  data_source: " + t.TempDir(),
		"store:",
		"  backend: " + backend,
	}
	if sqlitePath != "" {
		lines = append(lines, "  sqlite:", "    path: "+sqlitePath)
	}
	lines = append(lines,
		"llm:",
		"  backend: openai_compatible",
		"  openai_compatible:",
		"    base_url: "+providerURL,
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	)
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	text := withTestProviderTriggerPlatformInventory(t, strings.Join(lines, "\n")+"\n")
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatalf("write conversation fork served config: %v", err)
	}
	return path
}

type servedConversationForkSource struct {
	RunID     string
	AgentID   string
	SessionID string
	Turn1ID   string
	Turn2ID   string
	Event1ID  string
	Event2ID  string
	EntityID  string
	Turn1At   time.Time
	Turn2At   time.Time
}

func runServedConversationForkLifecycleProof(t *testing.T, rt servedConversationForkProofRuntime) {
	t.Helper()
	initial := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name":      "external.observed",
		"bundle_hash":     rt.BundleHash,
		"payload":         map[string]any{},
		"idempotency_key": "issue-1997-" + rt.Backend + "-source-run",
	})
	if !initial.NewRunCreated || initial.RunID == "" {
		t.Fatalf("%s conversation fork source run = %#v", rt.Backend, initial)
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), servedProofPollDeadline)
	defer cancelWait()
	if err := rt.Runtime.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("wait for %s conversation fork source quiescence: %v", rt.Backend, err)
	}
	fixture := seedServedConversationForkSource(t, rt.DB, rt.Backend, initial.RunID)
	keyPrefix := "issue-1997-" + rt.Backend + "-" + fixture.SessionID

	create := func(selector map[string]any, key string) struct {
		Fork                store.OperatorConversationForkSession `json:"fork"`
		IdempotencyReplayed bool                                  `json:"idempotency_replayed"`
	} {
		params := map[string]any{"source_session_id": fixture.SessionID, "fork_point": selector}
		if key != "" {
			params["idempotency_key"] = key
		}
		var result struct {
			Fork                store.OperatorConversationForkSession `json:"fork"`
			IdempotencyReplayed bool                                  `json:"idempotency_replayed"`
		}
		requireServedJSONRPCResult(t, rt.Endpoint, "conversation.fork", params, &result)
		return result
	}

	missingKey := keyPrefix + "-missing-turn"
	missingTurnID := fixture.Turn1ID[:12]
	missing := requireServedJSONRPCError(t, rt.Endpoint, "conversation.fork", map[string]any{
		"source_session_id": fixture.SessionID,
		"fork_point":        map[string]any{"kind": "turn", "turn_id": missingTurnID},
		"idempotency_key":   missingKey,
	})
	if missing.Data["code"] != apiv1.TurnNotFoundCode {
		t.Fatalf("%s prefix-shaped exact turn error = %#v", rt.Backend, missing.Data)
	}
	if got := servedConversationForkRequestArtifactCounts(t, rt.DB, rt.Backend, fixture.SessionID, missingKey); got != ([4]int{}) {
		t.Fatalf("%s prefix-shaped exact turn persisted request artifacts = %#v, want none", rt.Backend, got)
	}

	turnKey := keyPrefix + "-turn"
	turnFork := create(map[string]any{"kind": "turn", "turn_id": fixture.Turn1ID}, turnKey)
	if turnFork.Fork.SourceRunID != fixture.RunID || turnFork.Fork.SourceAgentID != fixture.AgentID || turnFork.Fork.ForkPoint.TurnID != fixture.Turn1ID || turnFork.Fork.State != "active" {
		t.Fatalf("%s turn fork = %#v", rt.Backend, turnFork)
	}
	if got := turnFork.Fork.ExpiresAt.Sub(turnFork.Fork.CreatedAt); got != store.ConversationForkLifecycleTTL {
		t.Fatalf("%s fork TTL = %s, want %s", rt.Backend, got, store.ConversationForkLifecycleTTL)
	}
	turnReplay := create(map[string]any{"kind": "turn", "turn_id": fixture.Turn1ID}, turnKey)
	if !turnReplay.IdempotencyReplayed || turnReplay.Fork.ForkID != turnFork.Fork.ForkID {
		t.Fatalf("%s turn fork replay = %#v", rt.Backend, turnReplay)
	}
	conflict := requireServedJSONRPCError(t, rt.Endpoint, "conversation.fork", map[string]any{
		"source_session_id": fixture.SessionID,
		"fork_point":        map[string]any{"kind": "turn", "turn_id": fixture.Turn2ID},
		"idempotency_key":   turnKey,
	})
	if conflict.Data["code"] != apiv1.IdempotencyConflictCode {
		t.Fatalf("%s fork conflict = %#v", rt.Backend, conflict.Data)
	}

	eventFork := create(map[string]any{"kind": "event", "event_id": fixture.Event2ID}, keyPrefix+"-event")
	if eventFork.Fork.ForkPoint.TurnIndex != 2 || eventFork.Fork.ForkPoint.TurnID != fixture.Turn2ID || eventFork.Fork.ForkPoint.EventID != fixture.Event2ID {
		t.Fatalf("%s event fork point = %#v", rt.Backend, eventFork.Fork.ForkPoint)
	}
	at := fixture.Turn1At.Add(30 * time.Second)
	timeFork := create(map[string]any{"kind": "time", "at": at.Format(time.RFC3339Nano)}, "")
	keylessDuplicate := create(map[string]any{"kind": "time", "at": at.Format(time.RFC3339Nano)}, "")
	if timeFork.Fork.ForkPoint.TurnID != fixture.Turn1ID || keylessDuplicate.Fork.ForkID == timeFork.Fork.ForkID {
		t.Fatalf("%s keyless time forks = first:%#v second:%#v", rt.Backend, timeFork, keylessDuplicate)
	}

	var page1 store.ConversationForkListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.fork_list", map[string]any{"source_session_id": fixture.SessionID, "limit": 2}, &page1)
	if len(page1.Forks) != 2 || page1.NextCursor == "" {
		t.Fatalf("%s fork list page1 = %#v", rt.Backend, page1)
	}
	var page2 store.ConversationForkListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.fork_list", map[string]any{"source_session_id": fixture.SessionID, "limit": 2, "cursor": page1.NextCursor}, &page2)
	if len(page2.Forks) != 2 || page2.NextCursor != "" {
		t.Fatalf("%s fork list page2 = %#v", rt.Backend, page2)
	}

	prelaunch := requireServedJSONRPCError(t, rt.Endpoint, "conversation.fork_chat", map[string]any{
		"fork_id":         turnFork.Fork.ForkID,
		"message":         "reject before provider launch",
		"idempotency_key": keyPrefix + "-prelaunch",
	})
	if prelaunch == nil || rt.LLMRequests.Load() != 0 {
		t.Fatalf("%s prelaunch rejection = %#v requests=%d, want error before HTTP launch", rt.Backend, prelaunch, rt.LLMRequests.Load())
	}
	requireServedConversationForkRowCount(t, rt.DB, rt.Backend, "conversation_fork_snapshots", turnFork.Fork.ForkID, 1)
	requireServedConversationForkRowCount(t, rt.DB, rt.Backend, "conversation_fork_turns", turnFork.Fork.ForkID, 1)
	requireServedConversationForkTurnState(t, rt.DB, rt.Backend, turnFork.Fork.ForkID, 1, "failed")
	if err := rt.Credentials.Set(context.Background(), "OPENAI_COMPATIBLE_API_KEY", "forkchat-proof-key"); err != nil {
		t.Fatalf("set forkchat proof credential: %v", err)
	}

	countsBefore := servedConversationForkLiveCounts(t, rt.DB, rt.Backend, fixture.RunID)
	chatKey := keyPrefix + "-chat"
	chatParams := map[string]any{"fork_id": turnFork.Fork.ForkID, "message": "inspect snapshot and try emit_event", "idempotency_key": chatKey}
	var chat store.ConversationForkChatResult
	chatResponse := requestServedJSONRPC(t, rt.Endpoint, "conversation.fork_chat", chatParams)
	if chatResponse.Error != nil {
		t.Fatalf("conversation.fork_chat error = %#v\n%s", chatResponse.Error, servedConversationForkTurnDebug(t, rt.DB, rt.Backend, turnFork.Fork.ForkID))
	}
	if err := json.Unmarshal(chatResponse.Result, &chat); err != nil {
		t.Fatalf("decode conversation.fork_chat result: %v\n%s", err, string(chatResponse.Result))
	}
	if chat.IdempotencyReplayed || chat.Turn.TurnIndex != 2 || chat.Snapshot.SourceTurn.TurnID != fixture.Turn1ID {
		t.Fatalf("%s fork chat = %#v", rt.Backend, chat)
	}
	if len(chat.Snapshot.EntitySnapshot) != 1 || chat.Snapshot.EntitySnapshot[0].CurrentState != "draft" || chat.Snapshot.EntitySnapshot[0].Fields["name"] != "Before" {
		t.Fatalf("%s immutable fork snapshot = %#v", rt.Backend, chat.Snapshot.EntitySnapshot)
	}
	if rt.LLMRequests.Load() != 2 {
		t.Fatalf("%s fork chat provider requests = %d, want tool round plus answer", rt.Backend, rt.LLMRequests.Load())
	}
	var sawSnapshotRead, sawEventStub bool
	for _, call := range chat.Turn.ToolCalls {
		var result map[string]any
		if err := json.Unmarshal(call.Result, &result); err != nil {
			t.Fatalf("decode %s fork tool result: %v", rt.Backend, err)
		}
		switch call.Name {
		case "fork_snapshot_read_entities":
			sawSnapshotRead = result["status"] == "read_from_snapshot" && result["snapshot_owner"] == store.ConversationForkChatSnapshotOwner
		case "emit_event":
			sawEventStub = result["status"] == "stubbed" && result["live_mutation"] == false
		}
	}
	if !sawSnapshotRead || !sawEventStub {
		t.Fatalf("%s fork chat tool calls = %#v", rt.Backend, chat.Turn.ToolCalls)
	}
	requireServedConversationForkRowCount(t, rt.DB, rt.Backend, "conversation_fork_snapshots", turnFork.Fork.ForkID, 1)
	if after := servedConversationForkLiveCounts(t, rt.DB, rt.Backend, fixture.RunID); after != countsBefore {
		t.Fatalf("%s fork chat live counts changed from %#v to %#v", rt.Backend, countsBefore, after)
	}
	var chatReplay store.ConversationForkChatResult
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.fork_chat", chatParams, &chatReplay)
	if !chatReplay.IdempotencyReplayed || chatReplay.Turn.TurnID != chat.Turn.TurnID || rt.LLMRequests.Load() != 2 {
		t.Fatalf("%s fork chat replay = %#v requests=%d", rt.Backend, chatReplay, rt.LLMRequests.Load())
	}
	chatConflict := requireServedJSONRPCError(t, rt.Endpoint, "conversation.fork_chat", map[string]any{"fork_id": turnFork.Fork.ForkID, "message": "different", "idempotency_key": chatKey})
	if chatConflict.Data["code"] != apiv1.IdempotencyConflictCode {
		t.Fatalf("%s fork chat conflict = %#v", rt.Backend, chatConflict.Data)
	}

	var viewed store.OperatorConversationForkSession
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.fork_view", map[string]any{"fork_id": turnFork.Fork.ForkID}, &viewed)
	if len(viewed.Turns) != 1 || viewed.Turns[0].TurnID != chat.Turn.TurnID {
		t.Fatalf("%s fork view = %#v", rt.Backend, viewed)
	}

	setServedConversationForkExpiry(t, rt.DB, rt.Backend, eventFork.Fork.ForkID, time.Now().UTC().Add(-time.Minute))
	var expired store.OperatorConversationForkSession
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.fork_view", map[string]any{"fork_id": eventFork.Fork.ForkID}, &expired)
	if expired.State != "expired" {
		t.Fatalf("%s expired fork state = %q", rt.Backend, expired.State)
	}

	deleteKey := keyPrefix + "-delete"
	var deleted struct {
		OK                  bool   `json:"ok"`
		ForkID              string `json:"fork_id"`
		Deleted             bool   `json:"deleted"`
		AlreadyDeleted      bool   `json:"already_deleted"`
		IdempotencyReplayed bool   `json:"idempotency_replayed"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.fork_delete", map[string]any{"fork_id": turnFork.Fork.ForkID, "idempotency_key": deleteKey}, &deleted)
	if !deleted.OK || !deleted.Deleted || deleted.AlreadyDeleted || deleted.IdempotencyReplayed {
		t.Fatalf("%s fork delete = %#v", rt.Backend, deleted)
	}
	var deleteReplay = deleted
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.fork_delete", map[string]any{"fork_id": turnFork.Fork.ForkID, "idempotency_key": deleteKey}, &deleteReplay)
	if !deleteReplay.IdempotencyReplayed || !deleteReplay.Deleted {
		t.Fatalf("%s fork delete replay = %#v", rt.Backend, deleteReplay)
	}
	var alreadyDeleted = deleted
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.fork_delete", map[string]any{"fork_id": turnFork.Fork.ForkID, "idempotency_key": keyPrefix + "-delete-again"}, &alreadyDeleted)
	if alreadyDeleted.Deleted || !alreadyDeleted.AlreadyDeleted || alreadyDeleted.IdempotencyReplayed {
		t.Fatalf("%s new-key fork delete = %#v", rt.Backend, alreadyDeleted)
	}
	chatDeleted := requireServedJSONRPCError(t, rt.Endpoint, "conversation.fork_chat", map[string]any{"fork_id": turnFork.Fork.ForkID, "message": "after delete"})
	if chatDeleted.Code == 0 {
		t.Fatalf("%s deleted fork chat unexpectedly succeeded", rt.Backend)
	}
	var active store.ConversationForkListResult
	requireServedJSONRPCResult(t, rt.Endpoint, "conversation.fork_list", map[string]any{"source_session_id": fixture.SessionID, "limit": 20}, &active)
	for _, item := range active.Forks {
		if item.ForkID == turnFork.Fork.ForkID || item.ForkID == eventFork.Fork.ForkID {
			t.Fatalf("%s inactive fork survived active list: %#v", rt.Backend, active.Forks)
		}
	}

	for _, scenarioID := range []string{servedparity.ScenarioConversationForkLifecycle, servedparity.ScenarioConversationForkChatLifecycle, servedparity.ScenarioConversationForkDeleteLifecycle} {
		requireServedParitySettlementPostconditions(t, rt.Endpoint, rt.DB, rt.Backend, fixture.RunID, servedparity.MustScenario(scenarioID))
	}
}

func seedServedConversationForkSource(t *testing.T, db *sql.DB, backend, runID string) servedConversationForkSource {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	fixture := servedConversationForkSource{
		RunID: runID, AgentID: "fork-source-agent", SessionID: uuid.NewString(),
		Turn1ID: uuid.NewString(), Turn2ID: uuid.NewString(), Event1ID: uuid.NewString(), Event2ID: uuid.NewString(), EntityID: uuid.NewString(),
		Turn1At: now.Add(-2 * time.Minute), Turn2At: now.Add(-time.Minute),
	}
	ctx := context.Background()
	capabilityIDs := make([]string, 0, 2)
	var capabilityStore managedcapabilities.Persistence
	switch backend {
	case "postgres":
		capabilityStore = &store.PostgresStore{DB: db}
	case "sqlite":
		capabilityStore = &store.SQLiteRuntimeStore{SQLiteSchemaStore: &store.SQLiteSchemaStore{DB: db}}
	default:
		t.Fatalf("unknown conversation fork proof backend %q", backend)
	}
	for i, turnID := range []string{fixture.Turn1ID, fixture.Turn2ID} {
		surface, err := managedcapabilities.New(managedcapabilities.Plan{
			ActorID: fixture.AgentID, RuntimeMode: "session", Provider: "served-fork-source", Transport: "api",
			ProviderContract: "served-conversation-fork-source-v1",
			Authority: managedcapabilities.Authority{
				Kind: managedcapabilities.AuthorityProviderTurn, ID: turnID,
				ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: "served-conversation-fork-source",
				RunID: fixture.RunID, SessionID: fixture.SessionID, TurnOrdinal: i + 1,
			},
			CreatedAt: []time.Time{fixture.Turn1At, fixture.Turn2At}[i],
		})
		if err != nil {
			t.Fatalf("build %s conversation fork source capability: %v", backend, err)
		}
		if err := capabilityStore.SaveManagedCapabilitySurface(ctx, surface); err != nil {
			t.Fatalf("seed %s conversation fork source capability: %v", backend, err)
		}
		capabilityIDs = append(capabilityIDs, surface.ID)
	}
	var statements []struct {
		query string
		args  []any
	}
	switch backend {
	case "postgres":
		statements = []struct {
			query string
			args  []any
		}{
			{`INSERT INTO agents (agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source, runtime_descriptor) VALUES ($1, 'fork-source', 'researcher', 'regular', 'openai_compatible', TRUE, 'authored', '{"type":"researcher","model":"regular","resolved_model":"gpt-compatible","resolved_llm_provider":"openai_compatible","resolved_llm_transport":"api","execution_mode":"live"}'::jsonb)`, []any{fixture.AgentID}},
			{`INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status, created_at, updated_at) VALUES ($1::uuid, $2::uuid, $3, 'fork-source', TRUE, 'authored', 'active', $4, $4)`, []any{fixture.SessionID, fixture.RunID, fixture.AgentID, now.Add(-3 * time.Minute)}},
			{`INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, trigger_event_id, trigger_event_type, capability_surface_id, parse_ok, execution_mode, created_at) VALUES ($1::uuid,$2::uuid,$3,$4::uuid,'fork-source',TRUE,'authored',$5::uuid,'task.ready',$6::uuid,true,'live',$7),($8::uuid,$2::uuid,$3,$4::uuid,'fork-source',TRUE,'authored',$9::uuid,'task.done',$10::uuid,true,'live',$11)`, []any{fixture.Turn1ID, fixture.RunID, fixture.AgentID, fixture.SessionID, fixture.Event1ID, capabilityIDs[0], fixture.Turn1At, fixture.Turn2ID, fixture.Event2ID, capabilityIDs[1], fixture.Turn2At}},
			{`INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at) VALUES ($1::uuid,$2::uuid,'flow/forkchat','default','after','{}'::jsonb,'{"name":"After"}'::jsonb,'{}'::jsonb,2,$3,$3,$3)`, []any{fixture.RunID, fixture.EntityID, fixture.Turn1At.Add(10 * time.Second)}},
			{`INSERT INTO entity_mutations (run_id, entity_id, field, old_value, new_value, writer_type, writer_id, created_at) VALUES ($1::uuid,$2::uuid,'current_state',NULL,'"draft"'::jsonb,'platform','test',$3),($1::uuid,$2::uuid,'name',NULL,'"Before"'::jsonb,'platform','test',$3),($1::uuid,$2::uuid,'current_state','"draft"'::jsonb,'"after"'::jsonb,'platform','test',$4)`, []any{fixture.RunID, fixture.EntityID, fixture.Turn1At.Add(-30 * time.Second), fixture.Turn1At.Add(10 * time.Second)}},
		}
	case "sqlite":
		statements = []struct {
			query string
			args  []any
		}{
			{`INSERT INTO agents (agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source, runtime_descriptor) VALUES (?, 'fork-source', 'researcher', 'regular', 'openai_compatible', 1, 'authored', '{"type":"researcher","model":"regular","resolved_model":"gpt-compatible","resolved_llm_provider":"openai_compatible","resolved_llm_transport":"api","execution_mode":"live"}')`, []any{fixture.AgentID}},
			{`INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status, created_at, updated_at) VALUES (?, ?, ?, 'fork-source', 1, 'authored', 'active', ?, ?)`, []any{fixture.SessionID, fixture.RunID, fixture.AgentID, now.Add(-3 * time.Minute), now.Add(-3 * time.Minute)}},
			{`INSERT INTO agent_turns (turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, trigger_event_id, trigger_event_type, capability_surface_id, parse_ok, execution_mode, created_at) VALUES (?,?,?,?,'fork-source',1,'authored',?,'task.ready',?,true,'live',?),(?,?,?,?,'fork-source',1,'authored',?,'task.done',?,true,'live',?)`, []any{fixture.Turn1ID, fixture.RunID, fixture.AgentID, fixture.SessionID, fixture.Event1ID, capabilityIDs[0], fixture.Turn1At, fixture.Turn2ID, fixture.RunID, fixture.AgentID, fixture.SessionID, fixture.Event2ID, capabilityIDs[1], fixture.Turn2At}},
			{`INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at) VALUES (?,?,'flow/forkchat','default','after','{}','{"name":"After"}','{}',2,?,?,?)`, []any{fixture.RunID, fixture.EntityID, fixture.Turn1At.Add(10 * time.Second), fixture.Turn1At.Add(10 * time.Second), fixture.Turn1At.Add(10 * time.Second)}},
			{`INSERT INTO entity_mutations (run_id, entity_id, field, old_value, new_value, writer_type, writer_id, created_at) VALUES (?,?,'current_state',NULL,'"draft"','platform','test',?),(?,?,'name',NULL,'"Before"','platform','test',?),(?,?,'current_state','"draft"','"after"','platform','test',?)`, []any{fixture.RunID, fixture.EntityID, fixture.Turn1At.Add(-30 * time.Second), fixture.RunID, fixture.EntityID, fixture.Turn1At.Add(-30 * time.Second), fixture.RunID, fixture.EntityID, fixture.Turn1At.Add(10 * time.Second)}},
		}
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatalf("seed %s conversation fork source: %v\n%s", backend, err, statement.query)
		}
	}
	return fixture
}

type servedConversationForkCounts struct {
	Runs      int
	Events    int
	Mailbox   int
	Mutations int
}

func servedConversationForkLiveCounts(t *testing.T, db *sql.DB, backend, runID string) servedConversationForkCounts {
	t.Helper()
	ctx := context.Background()
	count := func(query string, args ...any) int {
		var value int
		if err := db.QueryRowContext(ctx, query, args...).Scan(&value); err != nil {
			t.Fatalf("%s conversation fork count: %v", backend, err)
		}
		return value
	}
	if backend == "postgres" {
		return servedConversationForkCounts{Runs: count(`SELECT COUNT(*) FROM runs`), Events: count(`SELECT COUNT(*) FROM events`), Mailbox: count(`SELECT COUNT(*) FROM mailbox`), Mutations: count(`SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid`, runID)}
	}
	return servedConversationForkCounts{Runs: count(`SELECT COUNT(*) FROM runs`), Events: count(`SELECT COUNT(*) FROM events`), Mailbox: count(`SELECT COUNT(*) FROM mailbox`), Mutations: count(`SELECT COUNT(*) FROM entity_mutations WHERE run_id = ?`, runID)}
}

func servedConversationForkRequestArtifactCounts(t *testing.T, db *sql.DB, backend, sessionID, idempotencyKey string) [4]int {
	t.Helper()
	queries := []string{
		`SELECT COUNT(*) FROM api_idempotency WHERE method = 'conversation.fork' AND idempotency_key = ?`,
		`SELECT COUNT(*) FROM conversation_forks WHERE source_session_id = ?`,
		`SELECT COUNT(*) FROM conversation_fork_snapshots`,
		`SELECT COUNT(*) FROM conversation_fork_turns`,
	}
	args := []any{idempotencyKey, sessionID, nil, nil}
	if backend == "postgres" {
		queries[0] = `SELECT COUNT(*) FROM api_idempotency WHERE method = 'conversation.fork' AND idempotency_key = $1`
		queries[1] = `SELECT COUNT(*) FROM conversation_forks WHERE source_session_id = $1::uuid`
	}
	var counts [4]int
	for i, query := range queries {
		var queryArgs []any
		if args[i] != nil {
			queryArgs = []any{args[i]}
		}
		if err := db.QueryRowContext(context.Background(), query, queryArgs...).Scan(&counts[i]); err != nil {
			t.Fatalf("%s count conversation fork request artifact %d: %v", backend, i, err)
		}
	}
	return counts
}

func requireServedConversationForkRowCount(t *testing.T, db *sql.DB, backend, table, forkID string, want int) {
	t.Helper()
	placeholder := "?"
	if backend == "postgres" {
		placeholder = "$1::uuid"
	}
	var got int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM `+table+` WHERE fork_id = `+placeholder, forkID).Scan(&got); err != nil {
		t.Fatalf("%s count %s for fork %s: %v", backend, table, forkID, err)
	}
	if got != want {
		t.Fatalf("%s %s rows for fork %s = %d, want %d", backend, table, forkID, got, want)
	}
}

func requireServedConversationForkTurnState(t *testing.T, db *sql.DB, backend, forkID string, turnIndex int, want string) {
	t.Helper()
	query := `SELECT state FROM conversation_fork_turns WHERE fork_id=? AND turn_index=?`
	if backend == "postgres" {
		query = `SELECT state FROM conversation_fork_turns WHERE fork_id=$1::uuid AND turn_index=$2`
	}
	var got string
	if err := db.QueryRowContext(context.Background(), query, forkID, turnIndex).Scan(&got); err != nil {
		t.Fatalf("%s load conversation fork turn state: %v", backend, err)
	}
	if got != want {
		t.Fatalf("%s conversation fork turn %d state = %q, want %q", backend, turnIndex, got, want)
	}
}

func servedConversationForkTurnDebug(t *testing.T, db *sql.DB, backend, forkID string) string {
	t.Helper()
	query := `SELECT turn_index, state, COALESCE(CAST(failure AS TEXT), '') FROM conversation_fork_turns WHERE fork_id=? ORDER BY turn_index`
	if backend == "postgres" {
		query = `SELECT turn_index, state, COALESCE(failure::text, '') FROM conversation_fork_turns WHERE fork_id=$1::uuid ORDER BY turn_index`
	}
	rows, err := db.QueryContext(context.Background(), query, forkID)
	if err != nil {
		return fmt.Sprintf("conversation_fork_turns: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var turnIndex int
		var state, failure string
		if err := rows.Scan(&turnIndex, &state, &failure); err != nil {
			return fmt.Sprintf("conversation_fork_turns scan: %v", err)
		}
		out = append(out, fmt.Sprintf("turn=%d state=%s failure=%s", turnIndex, state, failure))
	}
	if err := rows.Err(); err != nil {
		return fmt.Sprintf("conversation_fork_turns rows: %v", err)
	}
	return "conversation_fork_turns: " + strings.Join(out, "; ")
}

func setServedConversationForkExpiry(t *testing.T, db *sql.DB, backend, forkID string, expiresAt time.Time) {
	t.Helper()
	var err error
	if backend == "postgres" {
		_, err = db.ExecContext(context.Background(), `UPDATE conversation_forks SET expires_at = $1 WHERE fork_id = $2::uuid`, expiresAt, forkID)
	} else {
		_, err = db.ExecContext(context.Background(), `UPDATE conversation_forks SET expires_at = ? WHERE fork_id = ?`, expiresAt, forkID)
	}
	if err != nil {
		t.Fatalf("%s expire conversation fork %s: %v", backend, forkID, err)
	}
}

func startServedTestSetupEntitiesProofRuntime(t *testing.T, backend servedparity.Backend) servedControlProofRuntime {
	t.Helper()
	switch backend {
	case servedparity.BackendDefaultSQLite:
		unsetStoreSelectorEnv(t)
		stubServeRuntimeWorkspaceLifecycle(t)
		sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
		contractsPath := writeServedTestSetupFixture(t)
		bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
		oldBuildStores := buildStoresForServe
		t.Cleanup(func() {
			buildStoresForServe = oldBuildStores
		})
		var servedDB *sql.DB
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			stores, err := oldBuildStores(ctx, selection, cfg)
			if err == nil {
				servedDB = stores.SQLDB
			}
			return stores, err
		}
		endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
			ConfigPath:              writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
			ContractsPath:           contractsPath,
			PlatformSpecPath:        defaultPlatformSpecPath,
			APIListenAddr:           "127.0.0.1:0",
			MCPListenAddr:           "127.0.0.1:0",
			SelfCheck:               true,
			RequireBundleMatch:      false,
			NoRequireBundleMatch:    true,
			Verbose:                 true,
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		})
		if servedDB == nil {
			t.Fatal("served sqlite SQLDB is required for test.setup_entities served parity proof")
		}
		return servedControlProofRuntime{Endpoint: endpoint, DB: servedDB, Backend: "sqlite", BundleHash: bundleHash}
	case servedparity.BackendExplicitPostgres:
		_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
			return serveRuntimeWorkspaceStub{}
		})
		contractsPath := writeServedTestSetupFixture(t)
		bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
		endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
			ConfigPath:              writeServeRuntimeTestConfig(t),
			ContractsPath:           contractsPath,
			PlatformSpecPath:        defaultPlatformSpecPath,
			StoreMode:               "postgres",
			StoreModeSet:            true,
			APIListenAddr:           "127.0.0.1:0",
			MCPListenAddr:           "127.0.0.1:0",
			SelfCheck:               true,
			RequireBundleMatch:      false,
			Verbose:                 true,
			TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		})
		return servedControlProofRuntime{Endpoint: endpoint, DB: db, Backend: "postgres", BundleHash: bundleHash}
	default:
		t.Fatalf("unknown served test.setup_entities backend %q", backend)
		return servedControlProofRuntime{}
	}
}

func runServedRunControlLifecycleProof(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	runID, initialEventID, entityID := createServedControlWaitingRun(t, rt, "run-control-release-"+uuid.NewString())
	keyPrefix := "issue-1864-" + rt.Backend + "-" + runID

	pauseKey := keyPrefix + "-run-pause"
	requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{
		"run_id":          runID,
		"idempotency_key": pauseKey,
	})
	requireServedRunControlState(t, rt.DB, rt.Backend, runID, "paused", "paused")
	requireServedRunStatus(t, rt.Endpoint, runID, "paused")
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "run.pause", pauseKey, 1)
	requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{
		"run_id":          runID,
		"idempotency_key": pauseKey,
	})
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "run.pause", pauseKey, 1)

	queued := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name":      "item.processed",
		"run_id":          runID,
		"source_event_id": initialEventID,
		"payload":         map[string]any{"item_id": "review"},
		"idempotency_key": keyPrefix + "-run-queued",
	})
	if queued.RunID != runID || queued.SourceEventID != initialEventID || queued.NewRunCreated || queued.EventID == "" {
		t.Fatalf("%s queued event.publish result = %#v, want existing paused run", rt.Backend, queued)
	}
	waitServedEventPublishDeliveryStatusCountForRun(t, rt.DB, rt.Backend, runID, queued.EventID, "node", "item-observer", "pending", 1)
	requireNoServedDeliveryStatusDuring(t, rt.DB, rt.Backend, queued.EventID, "node", "item-observer", "delivered", 250*time.Millisecond)

	continueKey := keyPrefix + "-run-continue"
	requireServedOKJSONRPC(t, rt.Endpoint, "run.continue", map[string]any{
		"run_id":          runID,
		"idempotency_key": continueKey,
	})
	requireServedRunControlState(t, rt.DB, rt.Backend, runID, "running", "running", "completed")
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "run.continue", continueKey, 1)
	requireServedOKJSONRPC(t, rt.Endpoint, "run.continue", map[string]any{
		"run_id":          runID,
		"idempotency_key": continueKey,
	})
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "run.continue", continueKey, 1)
	waitServedEventPublishDeliveryStatusCountForRun(t, rt.DB, rt.Backend, runID, queued.EventID, "node", "item-observer", "delivered", 1)
	requireServedEventPublishEntityState(t, rt.DB, rt.Backend, runID, entityID, "done")
	requireServedRunStatus(t, rt.Endpoint, runID, "completed")
	requireServedParitySettlementPostconditions(t, rt.Endpoint, rt.DB, rt.Backend, runID, servedparity.MustScenario(servedparity.ScenarioRunContinueControlLifecycle))

	stopRunID, pendingEventID, stopEntityID, stopCardID := seedServedRunControlPendingRunWithAgentDelivery(t, rt)
	stopKey := "issue-1864-" + rt.Backend + "-" + stopRunID + "-run-stop"
	requireServedOKJSONRPC(t, rt.Endpoint, "run.stop", map[string]any{
		"run_id":          stopRunID,
		"idempotency_key": stopKey,
	})
	requireServedRunControlState(t, rt.DB, rt.Backend, stopRunID, "stopped", "cancelled")
	requireServedStoppedPendingDelivery(t, rt.DB, rt.Backend, pendingEventID, "agent-pending")
	requireServedTerminalDecisionCardStateChangeOnly(t, rt, stopRunID, stopEntityID, stopCardID)
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "run.stop", stopKey, 1)
	requireServedOKJSONRPC(t, rt.Endpoint, "run.stop", map[string]any{
		"run_id":          stopRunID,
		"idempotency_key": stopKey,
	})
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "run.stop", stopKey, 1)
	requireServedTerminalDecisionCardStateChangeOnly(t, rt, stopRunID, stopEntityID, stopCardID)
	requireServedRunStatus(t, rt.Endpoint, stopRunID, "cancelled")
	requireServedParitySettlementPostconditions(t, rt.Endpoint, rt.DB, rt.Backend, stopRunID, servedparity.MustScenario(servedparity.ScenarioRunStopControlLifecycle))
}

func runServedRuntimeIngressControlLifecycleProof(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	keyPrefix := "issue-1864-" + rt.Backend + "-" + uuid.NewString()
	pauseKey := keyPrefix + "-runtime-pause"
	requireServedOKJSONRPC(t, rt.Endpoint, "runtime.pause", map[string]any{
		"idempotency_key": pauseKey,
	})
	requireServedRuntimeIngressState(t, rt.DB, rt.Backend, "paused", "platform.paused")
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "runtime.pause", pauseKey, 1)
	requireServedOKJSONRPC(t, rt.Endpoint, "runtime.pause", map[string]any{
		"idempotency_key": pauseKey,
	})
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "runtime.pause", pauseKey, 1)
	requireServedEventNameCount(t, rt.DB, rt.Backend, "platform.paused", 1)
	duplicatePause := requireServedJSONRPCError(t, rt.Endpoint, "runtime.pause", map[string]any{})
	if duplicatePause.Data["code"] != apiv1.RuntimeAlreadyPausedCode {
		t.Fatalf("%s duplicate runtime.pause data = %#v, want %s", rt.Backend, duplicatePause.Data, apiv1.RuntimeAlreadyPausedCode)
	}

	queued := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name":      "external.observed",
		"bundle_hash":     rt.BundleHash,
		"payload":         map[string]any{},
		"idempotency_key": keyPrefix + "-runtime-queued",
	})
	if !queued.NewRunCreated || queued.RunID == "" || queued.EventID == "" {
		t.Fatalf("%s runtime-paused event.publish result = %#v, want new queued run", rt.Backend, queued)
	}
	requireNoServedReceiptOutcomeDuring(t, rt.DB, rt.Backend, queued.EventID, "platform", "pipeline", "success", 250*time.Millisecond)

	resumeKey := keyPrefix + "-runtime-resume"
	requireServedOKJSONRPC(t, rt.Endpoint, "runtime.resume", map[string]any{
		"idempotency_key": resumeKey,
	})
	requireServedRuntimeIngressState(t, rt.DB, rt.Backend, "running", "platform.resumed")
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "runtime.resume", resumeKey, 1)
	requireServedOKJSONRPC(t, rt.Endpoint, "runtime.resume", map[string]any{
		"idempotency_key": resumeKey,
	})
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "runtime.resume", resumeKey, 1)
	requireServedEventNameCount(t, rt.DB, rt.Backend, "platform.resumed", 1)
	duplicateResume := requireServedJSONRPCError(t, rt.Endpoint, "runtime.resume", map[string]any{})
	if duplicateResume.Data["code"] != apiv1.RuntimeNotPausedCode {
		t.Fatalf("%s duplicate runtime.resume data = %#v, want %s", rt.Backend, duplicateResume.Data, apiv1.RuntimeNotPausedCode)
	}

	waitServedEventPublishReceiptOutcomeCount(t, rt.DB, rt.Backend, queued.EventID, "platform", "pipeline", "success", 1)
	requireServedRunStatus(t, rt.Endpoint, queued.RunID, "running")
	requireServedParitySettlementPostconditions(t, rt.Endpoint, rt.DB, rt.Backend, queued.RunID, servedparity.MustScenario(servedparity.ScenarioRuntimeResumeIngressLifecycle))
}

type servedDecisionCardFixture struct {
	RunID, EntityID, CardID, ContentHash, NoticeID string
	Workflow                                       *runtimepipeline.WorkflowInstanceStore
}

func runServedMailboxDecisionLifecycleProof(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	fixture := seedServedDecisionCardFixture(t, rt)
	var listed map[string]any
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.list", map[string]any{"run_id": fixture.RunID, "status": "pending"}, &listed)
	items, ok := listed["items"].([]any)
	if !ok || len(items) < 2 {
		t.Fatalf("%s mailbox.list items = %#v, want notice and decision card", rt.Backend, listed["items"])
	}

	var detail map[string]any
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": fixture.CardID}, &detail)
	if detail["kind"] != decisioncard.KindDecisionCard {
		t.Fatalf("%s mailbox.get kind = %#v", rt.Backend, detail)
	}

	var draft map[string]any
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.begin_input", map[string]any{
		"card_id": fixture.CardID, "verdict": "reject", "observed_content_hash": fixture.ContentHash,
		"idempotency_key": "begin-" + fixture.CardID,
	}, &draft)
	draftID := strings.TrimSpace(fmt.Sprint(draft["input_draft_id"]))
	if draftID == "" || draft["status"] != decisioncard.DraftStatusActive {
		t.Fatalf("%s begin_input = %#v", rt.Backend, draft)
	}
	var cancelled map[string]any
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.cancel_input", map[string]any{
		"card_id": fixture.CardID, "input_draft_id": draftID, "idempotency_key": "cancel-" + fixture.CardID,
	}, &cancelled)
	if cancelled["status"] != decisioncard.DraftStatusCancelled {
		t.Fatalf("%s cancel_input = %#v", rt.Backend, cancelled)
	}

	deferUntil := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	var deferred map[string]any
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.defer", map[string]any{
		"card_id": fixture.CardID, "until": deferUntil.Format(time.RFC3339Nano), "idempotency_key": "defer-" + fixture.CardID,
	}, &deferred)
	if deferred["status"] != decisioncard.StatusPending {
		t.Fatalf("%s mailbox.defer = %#v", rt.Backend, deferred)
	}

	var decided map[string]any
	decideParams := map[string]any{
		"card_id": fixture.CardID, "verdict": "approve", "observed_content_hash": fixture.ContentHash,
		"fields":          map[string]any{"score": int64(9007199254740991)},
		"idempotency_key": "decide-" + fixture.CardID,
	}
	unsafeRaw := fmt.Sprintf(`{"jsonrpc":"2.0","id":"unsafe-decision-number","method":"mailbox.decide","params":{"card_id":%q,"verdict":"approve","observed_content_hash":%q,"fields":{"score":9007199254740992},"idempotency_key":%q}}`, fixture.CardID, fixture.ContentHash, "unsafe-decide-"+fixture.CardID)
	unsafe := requestServedRawJSONRPC(t, rt.Endpoint, unsafeRaw)
	if unsafe.Error == nil || unsafe.Error.Code != -32600 {
		t.Fatalf("%s unsafe mailbox.decide = %#v, want invalid request", rt.Backend, unsafe)
	}
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "mailbox.decide", "unsafe-decide-"+fixture.CardID, 0)
	var stillPending map[string]any
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": fixture.CardID}, &stillPending)
	if card := servedAnyMap(t, stillPending["decision_card"]); card["status"] != decisioncard.StatusPending {
		t.Fatalf("%s card after unsafe decision = %#v", rt.Backend, card)
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", decideParams, &decided)
	if decided["status"] != decisioncard.StatusDecided || strings.TrimSpace(fmt.Sprint(decided["decision_event_id"])) == "" {
		t.Fatalf("%s mailbox.decide = %#v", rt.Backend, decided)
	}
	var replay map[string]any
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.decide", decideParams, &replay)
	if replay["idempotency_replayed"] != true || replay["decision_event_id"] != decided["decision_event_id"] {
		t.Fatalf("%s mailbox.decide replay = %#v, original %#v", rt.Backend, replay, decided)
	}
	conflictingParams := map[string]any{}
	for key, value := range decideParams {
		conflictingParams[key] = value
	}
	conflictingParams["fields"] = map[string]any{"score": int64(9007199254740990)}
	conflict := requireServedJSONRPCError(t, rt.Endpoint, "mailbox.decide", conflictingParams)
	if conflict.Data["code"] != apiv1.IdempotencyConflictCode {
		t.Fatalf("%s numeric decision conflict = %#v, want %s", rt.Backend, conflict.Data, apiv1.IdempotencyConflictCode)
	}
	var decidedDetail map[string]any
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": fixture.CardID}, &decidedDetail)
	decidedCard := servedAnyMap(t, decidedDetail["decision_card"])
	if fields := servedAnyMap(t, decidedCard["fields"]); !jsonScalarInt(fields["score"], 9007199254740991) {
		t.Fatalf("%s persisted decision fields = %#v", rt.Backend, fields)
	}
	requireServedDecisionEventSafeInteger(t, rt.DB, rt.Backend, strings.TrimSpace(fmt.Sprint(decided["decision_event_id"])))

	deadline := time.Now().Add(5 * time.Second)
	for {
		instance, ok, err := fixture.Workflow.Load(runtimecorrelation.WithRunID(context.Background(), fixture.RunID), fixture.EntityID)
		if err == nil && ok && instance.CurrentState == "done" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s decision route did not reach done: instance=%#v ok=%v err=%v", rt.Backend, instance, ok, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	requireServedMailboxSubscription(t, rt.Endpoint, fixture.CardID)
	var acknowledged map[string]any
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.acknowledge", map[string]any{
		"mailbox_id": fixture.NoticeID, "idempotency_key": "ack-" + fixture.NoticeID,
	}, &acknowledged)
	if acknowledged["kind"] != decisioncard.KindNotice || acknowledged["ok"] != true {
		t.Fatalf("%s mailbox.acknowledge = %#v", rt.Backend, acknowledged)
	}
	for _, scenarioID := range []string{
		servedparity.ScenarioMailboxNoticeAcknowledgmentLifecycle,
		servedparity.ScenarioMailboxBeginInputLifecycle,
		servedparity.ScenarioMailboxCancelInputLifecycle,
		servedparity.ScenarioMailboxDecisionCardLifecycle,
		servedparity.ScenarioMailboxDeferDecisionLifecycle,
	} {
		requireServedParitySettlementPostconditions(t, rt.Endpoint, rt.DB, rt.Backend, fixture.RunID, servedparity.MustScenario(scenarioID))
	}
}

func requireServedDecisionEventSafeInteger(t *testing.T, db *sql.DB, backend, eventID string) {
	t.Helper()
	var eventName, payload string
	query := `SELECT event_name, payload FROM events WHERE event_id = ?`
	args := []any{eventID}
	if backend == "postgres" {
		query = `SELECT event_name, payload::text FROM events WHERE event_id = $1::uuid`
	}
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&eventName, &payload); err != nil {
		t.Fatalf("%s load decision event %s: %v", backend, eventID, err)
	}
	if eventName != "mailbox.card_decided" || !strings.Contains(payload, "9007199254740991") {
		t.Fatalf("%s decision event = %s %s, want exact safe integer", backend, eventName, payload)
	}
	var decoded map[string]any
	if err := canonicaljson.DecodeInto([]byte(payload), &decoded); err != nil {
		t.Fatalf("%s decode decision event payload: %v", backend, err)
	}
	fields := servedAnyMap(t, decoded["fields"])
	if !jsonScalarInt(fields["score"], 9007199254740991) {
		t.Fatalf("%s decision event fields = %#v", backend, fields)
	}
}

func servedAnyMap(t *testing.T, value any) map[string]any {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("value = %#v, want object", value)
	}
	return object
}

func requireServedMailboxSubscription(t *testing.T, endpoint, cardID string) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(strings.TrimSuffix(endpoint, "/v1/rpc"), "http") + "/v1/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer " + apiv1.DefaultLoopbackAPIToken}})
	if err != nil {
		t.Fatalf("dial served mailbox subscription: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": "mailbox-subscribe-proof", "method": "mailbox.subscribe", "params": map[string]any{"cursor": 0}}); err != nil {
		t.Fatalf("write mailbox subscription: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var response struct {
		Result struct {
			SubscriptionID string `json:"subscription_id"`
		} `json:"result"`
		Error *servedJSONRPCError `json:"error"`
	}
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatalf("read mailbox subscription response: %v", err)
	}
	if response.Error != nil || strings.TrimSpace(response.Result.SubscriptionID) == "" {
		t.Fatalf("mailbox subscription response = %#v", response)
	}
	var notification struct {
		Method string `json:"method"`
		Params struct {
			Subscription string         `json:"subscription"`
			Result       map[string]any `json:"result"`
		} `json:"params"`
	}
	if err := conn.ReadJSON(&notification); err != nil {
		t.Fatalf("read mailbox subscription notification: %v", err)
	}
	if notification.Method != "rpc.subscription" || notification.Params.Subscription != response.Result.SubscriptionID || notification.Params.Result["card_id"] != cardID {
		t.Fatalf("mailbox subscription notification = %#v", notification)
	}
}

func runServedTestSetupEntitiesLifecycleProof(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	keyPrefix := "issue-1386-" + rt.Backend + "-test-setup"
	initial := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name":      "widget.started",
		"bundle_hash":     rt.BundleHash,
		"payload":         map[string]any{"seed": true},
		"idempotency_key": keyPrefix + "-run",
	})
	if !initial.NewRunCreated || initial.RunID == "" || initial.EventID == "" {
		t.Fatalf("%s setup trigger event.publish result = %#v, want new run", rt.Backend, initial)
	}
	runID := initial.RunID
	entityID := uuid.NewString()
	key := keyPrefix + "-entities"
	params := map[string]any{
		"bundle_hash":     rt.BundleHash,
		"run_id":          runID,
		"idempotency_key": key,
		"entities": []any{
			map[string]any{
				"alias":         "subject",
				"entity_id":     entityID,
				"entity_type":   "widget",
				"current_state": "waiting",
				"fields":        map[string]any{"score": 5},
			},
		},
	}
	var result struct {
		RunID    string `json:"run_id"`
		Entities []struct {
			Alias        string `json:"alias"`
			EntityID     string `json:"entity_id"`
			FlowInstance string `json:"flow_instance"`
			EntityType   string `json:"entity_type"`
			CurrentState string `json:"current_state"`
		} `json:"entities"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "test.setup_entities", params, &result)
	if result.RunID != runID || len(result.Entities) != 1 {
		t.Fatalf("%s test.setup_entities result = %#v, want run %s and one entity", rt.Backend, result, runID)
	}
	entity := result.Entities[0]
	if entity.Alias != "subject" || entity.EntityID != entityID || entity.FlowInstance != "" || entity.EntityType != "widget" || entity.CurrentState != "waiting" {
		t.Fatalf("%s test.setup_entities entity result = %#v", rt.Backend, entity)
	}
	requireServedTestSetupPersistence(t, rt.DB, rt.Backend, runID, entityID, rt.BundleHash)
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "test.setup_entities", key, 1)

	var replay struct {
		RunID    string `json:"run_id"`
		Entities []struct {
			EntityID string `json:"entity_id"`
		} `json:"entities"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "test.setup_entities", params, &replay)
	if replay.RunID != runID || len(replay.Entities) != 1 || replay.Entities[0].EntityID != entityID {
		t.Fatalf("%s test.setup_entities replay = %#v, want original run/entity", rt.Backend, replay)
	}
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "test.setup_entities", key, 1)
	requireServedTestSetupPersistence(t, rt.DB, rt.Backend, runID, entityID, rt.BundleHash)
	requireServedParitySettlementPostconditions(t, rt.Endpoint, rt.DB, rt.Backend, runID, servedparity.MustScenario(servedparity.ScenarioTestSetupEntitiesLifecycle))
}

func requireServedTestSetupPersistence(t *testing.T, db *sql.DB, backend, runID, entityID, bundleHash string) {
	t.Helper()
	var status, trigger, gotHash, source string
	var runQuery string
	var runArgs []any
	switch backend {
	case "postgres":
		runQuery = `SELECT status, trigger_event_type, COALESCE(bundle_hash, ''), bundle_source FROM runs WHERE run_id = $1::uuid`
		runArgs = []any{runID}
	case "sqlite":
		runQuery = `SELECT status, trigger_event_type, COALESCE(bundle_hash, ''), bundle_source FROM runs WHERE run_id = ?`
		runArgs = []any{runID}
	default:
		t.Fatalf("unknown test.setup_entities proof backend %q", backend)
	}
	if err := db.QueryRowContext(context.Background(), runQuery, runArgs...).Scan(&status, &trigger, &gotHash, &source); err != nil {
		t.Fatalf("%s load test.setup_entities run %s: %v", backend, runID, err)
	}
	wantSource := storerunlifecycle.BundleSourceEphemeral
	if backend == "postgres" {
		wantSource = storerunlifecycle.BundleSourcePersisted
	}
	if status != "running" || trigger != "widget.started" || gotHash != bundleHash || source != wantSource {
		t.Fatalf("%s setup run row = status:%q trigger:%q hash:%q source:%q", backend, status, trigger, gotHash, source)
	}

	var flow, typ, state string
	var score any
	var entityQuery string
	var entityArgs []any
	switch backend {
	case "postgres":
		entityQuery = `
			SELECT flow_instance, entity_type, current_state, fields->>'score'
			FROM entity_state
			WHERE run_id = $1::uuid AND entity_id = $2::uuid
		`
		entityArgs = []any{runID, entityID}
	case "sqlite":
		entityQuery = `
			SELECT flow_instance, entity_type, current_state, json_extract(fields, '$.score')
			FROM entity_state
			WHERE run_id = ? AND entity_id = ?
		`
		entityArgs = []any{runID, entityID}
	}
	if err := db.QueryRowContext(context.Background(), entityQuery, entityArgs...).Scan(&flow, &typ, &state, &score); err != nil {
		t.Fatalf("%s load setup entity %s: %v", backend, entityID, err)
	}
	if flow != "" || typ != "widget" || state != "waiting" || !jsonScalarInt(score, 5) {
		t.Fatalf("%s setup entity row = flow:%q type:%q state:%q score:%#v", backend, flow, typ, state, score)
	}

	var mutations int
	var mutationQuery string
	var mutationArgs []any
	switch backend {
	case "postgres":
		mutationQuery = `
			SELECT COUNT(*)
			FROM entity_mutations
			WHERE run_id = $1::uuid
			  AND entity_id = $2::uuid
			  AND writer_type = 'platform'
			  AND writer_id = 'test.setup_entities'
		`
		mutationArgs = []any{runID, entityID}
	case "sqlite":
		mutationQuery = `
			SELECT COUNT(*)
			FROM entity_mutations
			WHERE run_id = ?
			  AND entity_id = ?
			  AND writer_type = 'platform'
			  AND writer_id = 'test.setup_entities'
		`
		mutationArgs = []any{runID, entityID}
	}
	if err := db.QueryRowContext(context.Background(), mutationQuery, mutationArgs...).Scan(&mutations); err != nil {
		t.Fatalf("%s count setup entity mutations: %v", backend, err)
	}
	if mutations != 2 {
		t.Fatalf("%s setup mutation rows = %d, want 2", backend, mutations)
	}
}

func jsonScalarInt(value any, want int64) bool {
	switch v := value.(type) {
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		return err == nil && parsed == want
	case []byte:
		parsed, err := strconv.ParseInt(string(v), 10, 64)
		return err == nil && parsed == want
	case int:
		return int64(v) == want
	case int64:
		return v == want
	case float64:
		return int64(v) == want && v == float64(want)
	default:
		return false
	}
}

func seedServedDecisionCardFixture(t *testing.T, rt servedControlProofRuntime) servedDecisionCardFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Minute)
	runID, entityID, sourceEventID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	var cards decisioncard.Store
	var workflow *runtimepipeline.WorkflowInstanceStore
	var seedEvent func(context.Context, events.Event) error
	var insertNotice func(context.Context, runtimetools.MailboxItem) (string, error)
	switch rt.Backend {
	case "postgres":
		if _, err := rt.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, now); err != nil {
			t.Fatalf("seed postgres decision-card run: %v", err)
		}
		pg := &store.PostgresStore{DB: rt.DB}
		cards, workflow, insertNotice = pg, runtimepipeline.NewWorkflowInstanceStore(rt.DB), pg.InsertMailboxItem
		seedEvent = func(ctx context.Context, evt events.Event) error {
			return pg.RunEventMutation(ctx, func(mutation runtimebus.EventMutation) error {
				if err := mutation.AppendEvent(mutation.Context(), evt); err != nil {
					return err
				}
				if err := mutation.UpsertCommittedReplayScope(mutation.Context(), evt.ID(), runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
					return err
				}
				return mutation.UpsertPipelineReceipt(mutation.Context(), evt.ID(), "success", nil)
			})
		}
	case "sqlite":
		if _, err := rt.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES (?, 'running', ?)`, runID, now); err != nil {
			t.Fatalf("seed sqlite decision-card run: %v", err)
		}
		sqlite := &store.SQLiteRuntimeStore{SQLiteSchemaStore: &store.SQLiteSchemaStore{DB: rt.DB}}
		cards = sqlite
		workflow = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(rt.DB, sqlite)
		insertNotice = sqlite.InsertMailboxItem
		seedEvent = func(ctx context.Context, evt events.Event) error {
			return sqlite.RunEventMutation(ctx, func(mutation runtimebus.EventMutation) error {
				if err := mutation.AppendEvent(mutation.Context(), evt); err != nil {
					return err
				}
				if err := mutation.UpsertCommittedReplayScope(mutation.Context(), evt.ID(), runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
					return err
				}
				return mutation.UpsertPipelineReceipt(mutation.Context(), evt.ID(), "success", nil)
			})
		}
	default:
		t.Fatalf("unknown decision-card proof backend %q", rt.Backend)
	}
	evt := eventtest.RootIngress(sourceEventID, "review.requested", "", "", json.RawMessage(`{"review":true}`), 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "root"), now)
	if err := seedEvent(ctx, evt); err != nil {
		t.Fatalf("seed decision-card source event: %v", err)
	}
	noticeID, err := insertNotice(ctx, runtimetools.MailboxItem{
		EventID: sourceEventID, EntityID: entityID, FlowInstance: "root", FromAgent: "review-agent",
		Type: "notice", Priority: "normal", Status: "pending", Summary: "review queued", Context: []byte(`{"title":"review queued"}`),
	})
	if err != nil {
		t.Fatalf("seed mailbox notice: %v", err)
	}

	bundleHash := strings.TrimSpace(rt.BundleHash)
	if bundleHash == "" {
		bundleHash = "bundle-v1:sha256:" + strings.Repeat("a", 64)
	}
	routes, err := gateruntime.FreezeRoutes(map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", AdvancesTo: "done"},
		"reject":  {Verdict: "reject", AdvancesTo: "rework"},
	})
	if err != nil {
		t.Fatalf("FreezeRoutes: %v", err)
	}
	activation, err := gateruntime.New(runID, "root", entityID, "", "awaiting_review", "launch_review", bundleHash, routes, sourceEventID, now)
	if err != nil {
		t.Fatalf("new gate activation: %v", err)
	}
	carrier := runtimeengine.NewStateCarrier(map[string]any{"run_id": runID}, nil, nil)
	if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
		t.Fatalf("store gate activation: %v", err)
	}
	if err := workflow.Upsert(runtimecorrelation.WithRunID(ctx, runID), runtimepipeline.WorkflowInstance{
		InstanceID: entityID, StorageRef: entityID, WorkflowName: "root", WorkflowVersion: "1.0.0",
		CurrentState: "awaiting_review", EnteredStageAt: now, Metadata: carrier.PersistedMetadata(), StateBuckets: carrier.PersistedStateBuckets(),
	}); err != nil {
		t.Fatalf("seed gated workflow instance: %v", err)
	}
	snapshot, err := decisioncard.FreezeSnapshot(activation.DecisionID, "Launch review", map[string]any{"environment": "staging"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", AdvancesTo: "done", Input: map[string]runtimecontracts.WorkflowGateInputField{"score": {Type: "integer", Required: true}}},
		"reject":  {Verdict: "reject", AdvancesTo: "rework", Input: map[string]runtimecontracts.WorkflowGateInputField{"feedback": {Type: "text", Required: true}}},
	})
	if err != nil {
		t.Fatalf("freeze decision card snapshot: %v", err)
	}
	provenance, err := canonicaljson.FromGo(map[string]any{"source_event": sourceEventID})
	if err != nil {
		t.Fatalf("admit decision card provenance: %v", err)
	}
	anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
		FlowInstance: "root", EntityID: entityID, Stage: activation.Stage,
		StageActivationID: activation.ActivationID,
	})
	if err != nil {
		t.Fatalf("new decision card anchor: %v", err)
	}
	card, err := decisioncard.New(decisioncard.Card{
		CardID:        activation.CardID,
		RunID:         runID,
		ExecutionMode: executionmode.Live,
		Anchor:        anchor,
		Snapshot:      snapshot,
		BundleHash:    bundleHash, WorkflowVersion: "1.0.0",
		EffectiveCadence: decisioncard.Cadence{InputDraftTTL: "15m", ReminderInterval: "24h"},
		Provenance:       provenance, CreatedAt: now,
	})
	if err != nil {
		t.Fatalf("new decision card: %v", err)
	}
	if err := cards.CreateDecisionCard(runtimecorrelation.WithRunID(ctx, runID), card); err != nil {
		t.Fatalf("seed decision card: %v", err)
	}
	return servedDecisionCardFixture{RunID: runID, EntityID: entityID, CardID: card.CardID, ContentHash: card.CardContentHash, NoticeID: noticeID, Workflow: workflow}
}

func runServedDynamicAutoEmitBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	switch backend {
	case servedparity.BackendDefaultSQLite:
		runServedDynamicAutoEmitSQLiteProof(t)
	case servedparity.BackendExplicitPostgres:
		runServedDynamicAutoEmitPostgresProof(t)
	default:
		t.Fatalf("unknown served dynamic auto_emit backend %q", backend)
	}
}

func runServedCreateCarryProjectionBackendProof(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	switch backend {
	case servedparity.BackendDefaultSQLite:
		runServedCreateCarryProjectionSQLiteProof(t)
	case servedparity.BackendExplicitPostgres:
		runServedCreateCarryProjectionPostgresProof(t)
	default:
		t.Fatalf("unknown served create carry projection backend %q", backend)
	}
}

func runServedCreateCarryProjectionSQLiteProof(t *testing.T) {
	t.Helper()
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	contractsPath := canonicalrouting.CopyExample(t, canonicalrouting.TemplateCreateMintedKey)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	oldBuildStores := buildStoresForServe
	t.Cleanup(func() { buildStoresForServe = oldBuildStores })
	var servedDB *sql.DB
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		stores, err := oldBuildStores(ctx, selection, cfg)
		if err == nil {
			servedDB = stores.SQLDB
		}
		return stores, err
	}
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath:              writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})
	if servedDB == nil {
		t.Fatal("served sqlite SQLDB is required for create carry projection proof")
	}
	runServedCreateCarryProjectionProof(t, endpoint, servedDB, "sqlite", bundleHash)
}

func runServedCreateCarryProjectionPostgresProof(t *testing.T) {
	t.Helper()
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	contractsPath := canonicalrouting.CopyExample(t, canonicalrouting.TemplateCreateMintedKey)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		StoreMode:               "postgres",
		StoreModeSet:            true,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		Verbose:                 true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})
	runServedCreateCarryProjectionProof(t, endpoint, db, "postgres", bundleHash)
}

func runServedCreateCarryProjectionProof(t *testing.T, endpoint string, db *sql.DB, backend, bundleHash string) {
	t.Helper()
	root := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name":      "producer/validation.triggered",
		"bundle_hash":     bundleHash,
		"payload":         map[string]any{"candidate": "candidate-1"},
		"idempotency_key": "issue-2025-" + backend,
	})
	if !root.NewRunCreated || root.RunID == "" || root.EventID == "" {
		t.Fatalf("%s create carry root result = %#v, want new run", backend, root)
	}
	requestedEventID := waitServedEventPublishEventID(t, db, backend, root.RunID, "producer/validation.requested")

	requestedPayload := servedEventPayloadObject(t, db, backend, requestedEventID)
	if requestedPayload["candidate"] != "candidate-1" {
		t.Fatalf("%s requested payload = %#v, want authored candidate", backend, requestedPayload)
	}
	if _, exists := requestedPayload["validation_case_id"]; exists {
		t.Fatalf("%s persisted source payload mutated with receiver-owned validation_case_id: %#v", backend, requestedPayload)
	}
	projection, targetFlow, targetInstance := servedEventDeliveryProjection(t, db, backend, requestedEventID, "validator-node")
	validationCaseID := strings.TrimSpace(projection["validation_case_id"])
	if _, err := uuid.Parse(validationCaseID); err != nil {
		t.Fatalf("%s projected validation_case_id = %q, want UUID: %v", backend, validationCaseID, err)
	}
	if targetFlow != "validator" || targetInstance == "" {
		t.Fatalf("%s projected route target = %s/%s, want concrete validator instance", backend, targetFlow, targetInstance)
	}
	startedEventID := waitServedEventPublishEventID(t, db, backend, root.RunID, targetInstance+"/validation.started")
	startedPayload := servedEventPayloadObject(t, db, backend, startedEventID)
	if startedPayload["candidate"] != "candidate-1" || startedPayload["validation_case_id"] != validationCaseID {
		t.Fatalf("%s downstream payload = %#v, want explicit candidate and projected validation_case_id %s", backend, startedPayload, validationCaseID)
	}
	if current := servedEventPayloadObject(t, db, backend, requestedEventID); !reflect.DeepEqual(current, requestedPayload) {
		t.Fatalf("%s source payload changed after handler execution: before=%#v after=%#v", backend, requestedPayload, current)
	}

	requireServedParitySettlementPostconditions(t, endpoint, db, backend, root.RunID, servedparity.MustScenario(servedparity.ScenarioEventPublishDynamicAutoEmitLifecycle))
}

func servedEventPayloadObject(t *testing.T, db *sql.DB, backend, eventID string) map[string]any {
	t.Helper()
	query := "SELECT payload FROM events WHERE event_id = ?"
	args := []any{eventID}
	if backend == "postgres" {
		query = "SELECT payload::text FROM events WHERE event_id = $1::uuid"
	}
	var raw string
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&raw); err != nil {
		t.Fatalf("%s load event payload %s: %v", backend, eventID, err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("%s decode event payload %s: %v", backend, eventID, err)
	}
	return payload
}

func servedEventDeliveryProjection(t *testing.T, db *sql.DB, backend, eventID, subscriberID string) (map[string]string, string, string) {
	t.Helper()
	query := `SELECT delivery_payload_projection, delivery_target_route
		FROM event_deliveries WHERE event_id = ? AND subscriber_type = 'node' AND subscriber_id = ?`
	args := []any{eventID, subscriberID}
	if backend == "postgres" {
		query = `SELECT delivery_payload_projection::text, delivery_target_route::text
			FROM event_deliveries WHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2`
	}
	var raw, targetRaw string
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&raw, &targetRaw); err != nil {
		t.Fatalf("%s load delivery projection for %s/%s: %v", backend, eventID, subscriberID, err)
	}
	var projection struct {
		Fields map[string]string `json:"fields"`
	}
	if err := json.Unmarshal([]byte(raw), &projection); err != nil {
		t.Fatalf("%s decode delivery projection for %s/%s: %v", backend, eventID, subscriberID, err)
	}
	var target events.RouteIdentity
	if err := json.Unmarshal([]byte(targetRaw), &target); err != nil {
		t.Fatalf("%s decode delivery target for %s/%s: %v", backend, eventID, subscriberID, err)
	}
	target = target.Normalized()
	return projection.Fields, target.FlowID, target.FlowInstance
}

func runServedDynamicAutoEmitSQLiteProof(t *testing.T) {
	t.Helper()
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	contractsPath := writeServedDynamicAutoEmitFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	blocked := make(chan servedEventPublishPreHandlerProof, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	oldBuildStores := buildStoresForServe
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
	})
	var servedDB *sql.DB
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		stores, err := oldBuildStores(ctx, selection, cfg)
		if err == nil {
			servedDB = stores.SQLDB
		}
		return stores, err
	}
	var (
		hookMu sync.Mutex
		hook   runtimepipeline.WorkflowNodeHandlerStartHook
	)
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath:              writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
		ContractsPath:           contractsPath,
		PlatformSpecPath:        defaultPlatformSpecPath,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		NoRequireBundleMatch:    true,
		Verbose:                 true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		TestWorkflowNodeHandlerStartHook: func(ctx context.Context, nodeID string, evt events.Event) error {
			if !servedEventPublishMatchesNodeEvent(nodeID, evt, "portfolio-node", "opco.spinup_requested") {
				return nil
			}
			hookMu.Lock()
			if hook == nil {
				if servedDB == nil {
					hookMu.Unlock()
					return fmt.Errorf("served sqlite SQLDB is required for dynamic event.publish proof")
				}
				hook = servedEventPublishBlockingHandlerAuthorityHook(servedDB, "sqlite", "portfolio-node", "opco.spinup_requested", blocked, release)
			}
			h := hook
			hookMu.Unlock()
			return h(ctx, nodeID, evt)
		},
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	if servedDB == nil {
		t.Fatal("served sqlite SQLDB is required for dynamic event.publish proof")
	}
	runServedDynamicAutoEmitProof(t, endpoint, servedDB, "sqlite", bundleHash, blocked, release, &releaseOnce)
}

func runServedDynamicAutoEmitPostgresProof(t *testing.T) {
	t.Helper()
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	contractsPath := writeServedDynamicAutoEmitFixture(t)
	bundleHash := servedEventPublishFixtureBundleHash(t, contractsPath)
	blocked := make(chan servedEventPublishPreHandlerProof, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	endpoint, _ := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath:                       writeServeRuntimeTestConfig(t),
		ContractsPath:                    contractsPath,
		PlatformSpecPath:                 defaultPlatformSpecPath,
		StoreMode:                        "postgres",
		StoreModeSet:                     true,
		APIListenAddr:                    "127.0.0.1:0",
		MCPListenAddr:                    "127.0.0.1:0",
		SelfCheck:                        true,
		RequireBundleMatch:               false,
		Verbose:                          true,
		TestWorkflowNodeHandlerStartHook: servedEventPublishBlockingHandlerAuthorityHook(db, "postgres", "portfolio-node", "opco.spinup_requested", blocked, release),
		TestOutboxSweeperConfig:          servedEventPublishProofOutboxSweeperConfig(),
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	runServedDynamicAutoEmitProof(t, endpoint, db, "postgres", bundleHash, blocked, release, &releaseOnce)
}

func runServedDynamicAutoEmitProof(t *testing.T, endpoint string, db *sql.DB, backend, bundleHash string, blocked <-chan servedEventPublishPreHandlerProof, release chan struct{}, releaseOnce *sync.Once) {
	t.Helper()
	bootstrap := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name":      "opco.bootstrap_requested",
		"bundle_hash":     bundleHash,
		"payload":         map[string]any{"owner": "operator"},
		"idempotency_key": "issue-1384-" + backend + "-bootstrap",
	})
	if !bootstrap.NewRunCreated || bootstrap.EventID == "" || bootstrap.RunID == "" {
		t.Fatalf("bootstrap event.publish result = %#v, want new run and event id", bootstrap)
	}
	runID := bootstrap.RunID
	parentEntityID := requireServedEventPublishEntityState(t, db, backend, runID, "", "waiting")

	instanceID := "11111111-1111-4111-8111-111111111111"
	spinupEnvelope := requestServedJSONRPC(t, endpoint, "event.publish", map[string]any{
		"event_name":      "opco.spinup_requested",
		"run_id":          runID,
		"source_event_id": bootstrap.EventID,
		"payload": map[string]any{
			"instance_id": instanceID,
			"product_id":  "product-1",
		},
		"idempotency_key": "issue-1384-" + backend + "-spinup",
	})
	if spinupEnvelope.Error != nil {
		t.Fatalf("spinup event.publish error = %#v", spinupEnvelope.Error)
	}
	var spinup servedEventPublishRPCResult
	if err := json.Unmarshal(spinupEnvelope.Result, &spinup); err != nil {
		t.Fatalf("decode spinup event.publish result: %v\n%s", err, string(spinupEnvelope.Result))
	}
	if spinup.RunID != runID || spinup.SourceEventID != bootstrap.EventID || spinup.NewRunCreated || spinup.EventID == "" {
		t.Fatalf("spinup event.publish result = %#v, want existing run with source lineage", spinup)
	}
	requireServedEventPublishPreHandlerProof(t, db, backend, blocked, runID, spinup.EventID, "portfolio-node")
	assertServedEventPublishDeliveriesContainStatus(t, spinup.Deliveries, "node", "portfolio-node", "pending", "in_progress")
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, spinup.EventID, "node", "portfolio-node", "in_progress"); got != 1 {
		t.Fatalf("%s parent delivery in_progress count = %d, want 1\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, spinup.EventID, "node", "__runtime_replay_scope__"); got != 1 {
		t.Fatalf("%s parent replay-scope delivery count = %d, want 1\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, spinup.EventID, "", "workflow-runtime"); got != 0 {
		t.Fatalf("%s parent workflow-runtime delivery count = %d, want 0\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	requireServedReplayNoDeliveryHistoryNoMutation(t, endpoint, db, backend, spinup.EventID, "issue-1384-"+backend+"-replay-pending-parent")

	releaseOnce.Do(func() { close(release) })
	waitServedEventPublishDeliveryStatusCount(t, db, backend, spinup.EventID, "node", "portfolio-node", "delivered", 1)
	waitServedEventPublishReceiptOutcomeCount(t, db, backend, spinup.EventID, "node", "portfolio-node", "no_op", 1)
	requireServedEventReadback(t, endpoint, spinup.EventID, runID, parentEntityID, "opco.spinup_requested", "portfolio-node")
	requireServedTraceReadback(t, endpoint, runID, spinup.EventID, "opco.spinup_requested", "portfolio-node")

	autoEventName := "operating/" + instanceID + "/opco.product_initialization_requested"
	autoEventID := waitServedEventPublishEventID(t, db, backend, runID, autoEventName)
	autoEntityID := servedEventPublishEventEntityID(t, db, backend, autoEventID)
	assertServedDynamicAutoEmitPayloadProductOnly(t, db, backend, autoEventID)
	requireServedEventReadback(t, endpoint, autoEventID, runID, autoEntityID, autoEventName, "lifecycle-orchestrator")
	requireServedTraceReadback(t, endpoint, runID, autoEventID, autoEventName, "lifecycle-orchestrator")
	waitServedEventPublishReceiptOutcomeCount(t, db, backend, autoEventID, "node", "lifecycle-orchestrator", "no_op", 1)
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, autoEventID, "node", "__runtime_replay_scope__"); got != 1 {
		t.Fatalf("%s child replay-scope delivery count = %d, want 1\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, autoEventID, "", "workflow-runtime"); got != 0 {
		t.Fatalf("%s child workflow-runtime delivery count = %d, want 0\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	if got := servedEventPublishReceiptCountBySubscribers(t, db, backend, autoEventID, "workflow-runtime", "__runtime_replay_scope__"); got != 0 {
		t.Fatalf("%s child runtime/replay receipt count = %d, want 0\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}

	componentEventID := waitServedEventPublishEventID(t, db, backend, runID, "operating/component_scaffold.spawn_requested")
	assertServedDynamicAutoEmitPayloadProductOnly(t, db, backend, componentEventID)
	componentEntityID := servedEventPublishEventEntityID(t, db, backend, componentEventID)
	requireServedEventReadback(t, endpoint, componentEventID, runID, componentEntityID, "operating/component_scaffold.spawn_requested", "component-scaffold")
	requireServedTraceReadback(t, endpoint, runID, componentEventID, "operating/component_scaffold.spawn_requested", "component-scaffold")
	requireServedRunStatusWithDebug(t, endpoint, db, backend, runID, "completed")
	requireServedRunDiagnoseOperationalState(t, endpoint, runID, "completed")
	requireServedStatusCLIReadback(t, endpoint, runID, "  completed")
	requireServedReplayNoDeliveryHistoryNoMutation(t, endpoint, db, backend, autoEventID, "issue-1384-"+backend+"-replay-child-node-only")
	requireServedParitySettlementPostconditions(t, endpoint, db, backend, runID, servedparity.MustScenario(servedparity.ScenarioEventPublishDynamicAutoEmitLifecycle))
}

func requireServedParitySettlementPostconditions(t *testing.T, endpoint string, db *sql.DB, backend, runID string, scenario servedparity.Scenario) {
	t.Helper()
	requireServedParitySettlementPostconditionsWithDebug(t, endpoint, db, backend, runID, scenario, nil)
}

func requireServedParitySettlementPostconditionsWithDebug(t *testing.T, endpoint string, db *sql.DB, backend, runID string, scenario servedparity.Scenario, debug func() string) {
	t.Helper()
	var last cliapp.DiagnosticRunDiagnosisResult
	deadline := time.Now().Add(servedProofPollDeadline)
	for time.Now().Before(deadline) {
		var result cliapp.DiagnosticRunDiagnosisResult
		requireServedJSONRPCResult(t, endpoint, "run.diagnose", map[string]any{"run_id": runID}, &result)
		last = result
		if result.Run.RunID != runID || result.TestQuiescence == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		counts := servedparity.SettlementCounts{
			NonTerminalDeliveries: cliapp.IntPointerValue(result.TestQuiescence.ActiveDeliveries),
			PendingPipelineEvents: cliapp.IntPointerValue(result.TestQuiescence.UnsettledPipelineEvents),
			UnfiredDueTimers:      cliapp.IntPointerValue(result.TestQuiescence.DueTimers),
		}
		if len(servedparity.SettlementPostconditionFailures(scenario, counts)) == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	extra := ""
	if debug != nil {
		extra = "\n" + debug()
	} else {
		extra = "\n" + servedEventPublishDebugSummary(t, db, backend, runID)
	}
	t.Fatalf("served parity scenario %s settlement did not quiesce for run %s: ready=%v active_deliveries=%d unsettled_pipeline_events=%d due_timers=%d active_session_leases=%d%s",
		scenario.ID,
		runID,
		cliapp.BoolPointerValue(last.TestQuiescence.Ready),
		cliapp.IntPointerValue(last.TestQuiescence.ActiveDeliveries),
		cliapp.IntPointerValue(last.TestQuiescence.UnsettledPipelineEvents),
		cliapp.IntPointerValue(last.TestQuiescence.DueTimers),
		cliapp.IntPointerValue(last.TestQuiescence.ActiveSessionLeases),
		extra,
	)
}

func createServedControlWaitingRun(t *testing.T, rt servedControlProofRuntime, suffix string) (runID, eventID, entityID string) {
	t.Helper()
	result := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name":      "item.received",
		"bundle_hash":     rt.BundleHash,
		"payload":         map[string]any{"item_id": suffix},
		"idempotency_key": "issue-1864-" + rt.Backend + "-" + suffix,
	})
	if !result.NewRunCreated || result.RunID == "" || result.EventID == "" {
		t.Fatalf("%s control seed event.publish result = %#v, want new run", rt.Backend, result)
	}
	waitForServedEventPublishNodeDeliveryLifecycleForNode(t, rt.DB, rt.Backend, result.RunID, result.EventID, "item-handler", rt.Probe)
	entityID = requireServedEventPublishEntityState(t, rt.DB, rt.Backend, result.RunID, "", "waiting")
	requireServedRunStatus(t, rt.Endpoint, result.RunID, "running")
	return result.RunID, result.EventID, entityID
}

type servedReplayProofDelivery struct {
	DeliveryID       string `json:"delivery_id"`
	SubscriberID     string `json:"subscriber_id"`
	Status           string `json:"status"`
	SourceDeliveryID string `json:"source_delivery_id,omitempty"`
}

type servedEventReplayProofResult struct {
	EventID             string                      `json:"event_id"`
	ReplayEventID       string                      `json:"replay_event_id"`
	AuditEventID        string                      `json:"audit_event_id"`
	SubscribersReplayed []string                    `json:"subscribers_replayed"`
	OriginalDeliveries  []servedReplayProofDelivery `json:"original_deliveries"`
	NewDeliveries       []servedReplayProofDelivery `json:"new_deliveries"`
}

type servedAgentReplayProofResult struct {
	EventID          string                    `json:"event_id"`
	AgentID          string                    `json:"agent_id"`
	ReplayEventID    string                    `json:"replay_event_id"`
	AuditEventID     string                    `json:"audit_event_id"`
	OriginalDelivery servedReplayProofDelivery `json:"original_delivery"`
	NewDelivery      servedReplayProofDelivery `json:"new_delivery"`
}

type servedAgentReplayBacklogProofResult struct {
	OK            bool `json:"ok"`
	ReplayedCount int  `json:"replayed_count"`
}

type servedAgentRestartProofResult struct {
	OK bool `json:"ok"`
}

func runServedAgentRestartLifecycleProof(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	const agentID = "load-agent"
	beforeGeneration := servedAgentLifecycleGeneration(t, rt, agentID)
	key := "issue-1927-" + rt.Backend + "-restart-" + uuid.NewString()
	params := map[string]any{"agent_id": agentID, "idempotency_key": key}

	var first servedAgentRestartProofResult
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.restart", params, &first)
	if !first.OK {
		t.Fatalf("%s agent.restart result = %#v", rt.Backend, first)
	}
	afterGeneration := servedAgentLifecycleGeneration(t, rt, agentID)
	if afterGeneration != beforeGeneration+1 {
		t.Fatalf("%s restart generation = %d, want adjacent %d", rt.Backend, afterGeneration, beforeGeneration+1)
	}
	requireServedAgentRestartEvidence(t, rt, agentID, afterGeneration, 1)
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "agent.restart", key, 1)

	var replay servedAgentRestartProofResult
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.restart", params, &replay)
	if replay != first {
		t.Fatalf("%s restart replay = %#v, want %#v", rt.Backend, replay, first)
	}
	if got := servedAgentLifecycleGeneration(t, rt, agentID); got != afterGeneration {
		t.Fatalf("%s replay generation = %d, want unchanged %d", rt.Backend, got, afterGeneration)
	}
	requireServedAgentRestartEvidence(t, rt, agentID, afterGeneration, 1)
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "agent.restart", key, 1)

	runID, initialEventID, _ := createServedControlWaitingRun(t, rt, "issue-1927-restart-delivery-"+uuid.NewString())
	postRestart := publishServedLiveAgentHoldEvent(t, rt, runID, initialEventID, "agent-restart")
	waitServedEventPublishDeliveryStatusCountForRun(t, rt.DB, rt.Backend, runID, postRestart.EventID, "agent", agentID, "delivered", 1)
	requireServedParitySettlementPostconditions(t, rt.Endpoint, rt.DB, rt.Backend, runID, servedparity.MustScenario(servedparity.ScenarioAgentRestartLifecycle))
}

func servedAgentLifecycleGeneration(t *testing.T, rt servedControlProofRuntime, agentID string) uint64 {
	t.Helper()
	query := `SELECT lifecycle_generation FROM agents WHERE agent_id = ?`
	if rt.Backend == "postgres" {
		query = `SELECT lifecycle_generation FROM agents WHERE agent_id = $1`
	}
	var generation int64
	if err := rt.DB.QueryRow(query, agentID).Scan(&generation); err != nil {
		t.Fatalf("%s load lifecycle generation for %s: %v", rt.Backend, agentID, err)
	}
	return uint64(generation)
}

func requireServedAgentRestartEvidence(t *testing.T, rt servedControlProofRuntime, agentID string, generation uint64, want int) {
	t.Helper()
	placeholder := "?"
	factGenerationPlaceholder := "?"
	if rt.Backend == "postgres" {
		placeholder = "$1"
		factGenerationPlaceholder = "$2"
	}
	var operations, facts, outbox int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM agent_lifecycle_operations WHERE agent_id = `+placeholder+` AND operation_kind = 'restart'`, agentID).Scan(&operations); err != nil {
		t.Fatalf("%s count restart operations: %v", rt.Backend, err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM agent_lifecycle_transition_facts WHERE agent_id = `+placeholder+` AND trigger = 'restart' AND next_generation = `+factGenerationPlaceholder, agentID, generation).Scan(&facts); err != nil {
		t.Fatalf("%s count restart transition facts: %v", rt.Backend, err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE agent_id = `+placeholder+` AND operation_id IN (SELECT operation_id FROM agent_lifecycle_operations WHERE operation_kind = 'restart')`, agentID).Scan(&outbox); err != nil {
		t.Fatalf("%s count restart diagnostic outbox rows: %v", rt.Backend, err)
	}
	if operations != want || facts != want || outbox != want {
		t.Fatalf("%s restart evidence operations=%d facts=%d outbox=%d, want %d each", rt.Backend, operations, facts, outbox, want)
	}
}

func runServedLiveAgentEventReplayLifecycleProof(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	runID, initialEventID, _ := createServedControlWaitingRun(t, rt, "issue-1910-event-replay-"+uuid.NewString())

	eventReplayOriginal := publishServedLiveAgentHoldEvent(t, rt, runID, initialEventID, "event-replay")
	eventReplayKey := "issue-1910-" + rt.Backend + "-" + runID + "-event-replay"
	beforeHoldEvents := servedEventNameCount(t, rt.DB, rt.Backend, "item.processed")
	beforeAuditEvents := servedEventNameCount(t, rt.DB, rt.Backend, "event.replayed")
	var eventReplay servedEventReplayProofResult
	requireServedJSONRPCResult(t, rt.Endpoint, "event.replay", map[string]any{
		"event_id":        eventReplayOriginal.EventID,
		"idempotency_key": eventReplayKey,
	}, &eventReplay)
	requireServedLiveAgentEventReplayResult(t, rt, eventReplayOriginal.EventID, eventReplay)
	waitServedEventPublishDeliveryStatusCountForRun(t, rt.DB, rt.Backend, runID, eventReplay.ReplayEventID, "agent", "load-agent", "delivered", 1)
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "event.replay", eventReplayKey, 1)
	if got := servedEventNameCount(t, rt.DB, rt.Backend, "item.processed"); got != beforeHoldEvents+1 {
		t.Fatalf("%s item.processed events after event.replay = %d, want %d", rt.Backend, got, beforeHoldEvents+1)
	}
	if got := servedEventNameCount(t, rt.DB, rt.Backend, "event.replayed"); got != beforeAuditEvents+1 {
		t.Fatalf("%s event.replayed events after event.replay = %d, want %d", rt.Backend, got, beforeAuditEvents+1)
	}
	var eventReplayAgain servedEventReplayProofResult
	requireServedJSONRPCResult(t, rt.Endpoint, "event.replay", map[string]any{
		"event_id":        eventReplayOriginal.EventID,
		"idempotency_key": eventReplayKey,
	}, &eventReplayAgain)
	if eventReplayAgain.ReplayEventID != eventReplay.ReplayEventID || eventReplayAgain.AuditEventID != eventReplay.AuditEventID {
		t.Fatalf("%s event.replay idempotent result = %#v, want replay=%s audit=%s", rt.Backend, eventReplayAgain, eventReplay.ReplayEventID, eventReplay.AuditEventID)
	}
	if got := servedEventNameCount(t, rt.DB, rt.Backend, "item.processed"); got != beforeHoldEvents+1 {
		t.Fatalf("%s item.processed events after idempotent event.replay = %d, want %d", rt.Backend, got, beforeHoldEvents+1)
	}
	requireServedParitySettlementPostconditions(t, rt.Endpoint, rt.DB, rt.Backend, runID, servedparity.MustScenario(servedparity.ScenarioEventReplayLiveAgentLifecycle))

	agentReplayOriginal := publishServedLiveAgentHoldEvent(t, rt, runID, initialEventID, "agent-replay")
	agentReplayKey := "issue-1910-" + rt.Backend + "-" + runID + "-agent-replay"
	beforeHoldEvents = servedEventNameCount(t, rt.DB, rt.Backend, "item.processed")
	beforeAuditEvents = servedEventNameCount(t, rt.DB, rt.Backend, "event.replayed")
	var agentReplay servedAgentReplayProofResult
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.replay", map[string]any{
		"event_id":        agentReplayOriginal.EventID,
		"agent_id":        "load-agent",
		"idempotency_key": agentReplayKey,
	}, &agentReplay)
	requireServedLiveAgentAgentReplayResult(t, rt, agentReplayOriginal.EventID, agentReplay)
	waitServedEventPublishDeliveryStatusCountForRun(t, rt.DB, rt.Backend, runID, agentReplay.ReplayEventID, "agent", "load-agent", "delivered", 1)
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "agent.replay", agentReplayKey, 1)
	if got := servedEventNameCount(t, rt.DB, rt.Backend, "item.processed"); got != beforeHoldEvents+1 {
		t.Fatalf("%s item.processed events after agent.replay = %d, want %d", rt.Backend, got, beforeHoldEvents+1)
	}
	if got := servedEventNameCount(t, rt.DB, rt.Backend, "event.replayed"); got != beforeAuditEvents+1 {
		t.Fatalf("%s event.replayed events after agent.replay = %d, want %d", rt.Backend, got, beforeAuditEvents+1)
	}
	var agentReplayAgain servedAgentReplayProofResult
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.replay", map[string]any{
		"event_id":        agentReplayOriginal.EventID,
		"agent_id":        "load-agent",
		"idempotency_key": agentReplayKey,
	}, &agentReplayAgain)
	if agentReplayAgain.ReplayEventID != agentReplay.ReplayEventID || agentReplayAgain.AuditEventID != agentReplay.AuditEventID {
		t.Fatalf("%s agent.replay idempotent result = %#v, want replay=%s audit=%s", rt.Backend, agentReplayAgain, agentReplay.ReplayEventID, agentReplay.AuditEventID)
	}
	if got := servedEventNameCount(t, rt.DB, rt.Backend, "item.processed"); got != beforeHoldEvents+1 {
		t.Fatalf("%s item.processed events after idempotent agent.replay = %d, want %d", rt.Backend, got, beforeHoldEvents+1)
	}
	requireServedParitySettlementPostconditions(t, rt.Endpoint, rt.DB, rt.Backend, runID, servedparity.MustScenario(servedparity.ScenarioAgentReplayLiveAgentLifecycle))
}

func runServedLiveAgentReplayBacklogLifecycleProof(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	backlogRunID, backlogEventID := seedServedLiveAgentPendingBacklogDelivery(t, rt.DB, rt.Backend)
	backlogKey := "issue-1910-" + rt.Backend + "-" + backlogRunID + "-agent-replay-backlog"
	var backlog servedAgentReplayBacklogProofResult
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.replay_backlog", map[string]any{
		"agent_id":        "load-agent",
		"idempotency_key": backlogKey,
	}, &backlog)
	if !backlog.OK || backlog.ReplayedCount != 1 {
		t.Fatalf("%s agent.replay_backlog result = %#v, want one replayed event", rt.Backend, backlog)
	}
	waitServedEventPublishDeliveryStatusCountForRun(t, rt.DB, rt.Backend, backlogRunID, backlogEventID, "agent", "load-agent", "delivered", 1)
	waitServedEventPublishReceiptOutcomeCount(t, rt.DB, rt.Backend, backlogEventID, "agent", "load-agent", "success", 1)
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "agent.replay_backlog", backlogKey, 1)
	var backlogAgain servedAgentReplayBacklogProofResult
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.replay_backlog", map[string]any{
		"agent_id":        "load-agent",
		"idempotency_key": backlogKey,
	}, &backlogAgain)
	if backlogAgain.ReplayedCount != backlog.ReplayedCount {
		t.Fatalf("%s agent.replay_backlog idempotent result = %#v, want replayed_count=%d", rt.Backend, backlogAgain, backlog.ReplayedCount)
	}
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "agent.replay_backlog", backlogKey, 1)
	requireServedParitySettlementPostconditions(t, rt.Endpoint, rt.DB, rt.Backend, backlogRunID, servedparity.MustScenario(servedparity.ScenarioAgentReplayBacklogLiveAgentLifecycle))
}

type servedAgentDirectiveProofResult struct {
	OK                 bool   `json:"ok"`
	OperationID        string `json:"operation_id"`
	Response           string `json:"response"`
	RunID              string `json:"run_id"`
	RunIDResolution    string `json:"run_id_resolution"`
	DirectiveEventID   string `json:"directive_event_id"`
	DirectiveEventType string `json:"directive_event_type"`
}

func runServedAgentDirectiveOutcomeLifecycleProof(t *testing.T, rt servedControlProofRuntime, effects *atomic.Int32, faults *servedDirectivePersistenceFaults) {
	t.Helper()
	key := "issue-1932-" + rt.Backend + "-directive"
	params := map[string]any{
		"agent_id":        "load-agent",
		"directive":       "perform one durable directive step",
		"idempotency_key": key,
	}
	var first servedAgentDirectiveProofResult
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.send_directive", params, &first)
	requireServedAgentDirectiveResult(t, rt.Backend, first)
	requireServedAgentDirectivePersistence(t, rt, first, key, 1)
	requireServedAgentDirectiveEffectCount(t, rt.Backend, effects, 1)

	var replay servedAgentDirectiveProofResult
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.send_directive", params, &replay)
	if replay != first {
		t.Fatalf("%s directive replay = %#v, want %#v", rt.Backend, replay, first)
	}
	requireServedAgentDirectivePersistence(t, rt, first, key, 1)
	requireServedAgentDirectiveEffectCount(t, rt.Backend, effects, 1)

	conflict := requireServedJSONRPCError(t, rt.Endpoint, "agent.send_directive", map[string]any{
		"agent_id":        "load-agent",
		"directive":       "a conflicting directive body",
		"idempotency_key": key,
	})
	if conflict.Data["code"] != apiv1.IdempotencyConflictCode {
		t.Fatalf("%s directive conflict = %#v, want %s", rt.Backend, conflict.Data, apiv1.IdempotencyConflictCode)
	}
	requireServedAgentDirectivePersistence(t, rt, first, key, 1)
	requireServedAgentDirectiveEffectCount(t, rt.Backend, effects, 1)

	var keylessA, keylessB servedAgentDirectiveProofResult
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.send_directive", map[string]any{
		"agent_id":  "load-agent",
		"directive": "keyless operation A",
	}, &keylessA)
	requireServedJSONRPCResult(t, rt.Endpoint, "agent.send_directive", map[string]any{
		"agent_id":  "load-agent",
		"directive": "keyless operation B",
	}, &keylessB)
	requireServedAgentDirectiveResult(t, rt.Backend, keylessA)
	requireServedAgentDirectiveResult(t, rt.Backend, keylessB)
	if keylessA.OperationID == keylessB.OperationID || keylessA.DirectiveEventID == keylessB.DirectiveEventID {
		t.Fatalf("%s keyless directive identities were reused: %#v / %#v", rt.Backend, keylessA, keylessB)
	}
	requireServedAgentDirectiveOperationCount(t, rt.DB, rt.Backend, 3)
	requireServedAgentDirectiveEffectCount(t, rt.Backend, effects, 3)
	requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "agent.send_directive", key, 1)
	for _, runID := range []string{first.RunID, keylessA.RunID, keylessB.RunID} {
		requireServedParitySettlementPostconditions(t, rt.Endpoint, rt.DB, rt.Backend, runID, servedparity.MustScenario(servedparity.ScenarioAgentDirectiveOutcomeLifecycle))
	}

	for _, failureCase := range []struct {
		name      string
		directive string
		class     runtimefailures.Class
		detail    string
	}{
		{name: "untyped", directive: "return untyped directive failure", class: runtimefailures.ClassInternalFailure, detail: runtimeagentcontrol.DirectiveBoardStepFailedDetail},
		{name: "typed", directive: "return typed directive failure", class: runtimefailures.ClassAuthenticationNeeded, detail: "provider_unauthorized"},
	} {
		t.Run(failureCase.name+"_failure", func(t *testing.T) {
			failureKey := "issue-1869-" + rt.Backend + "-" + failureCase.name
			failureParams := map[string]any{
				"agent_id":        "load-agent",
				"directive":       failureCase.directive,
				"idempotency_key": failureKey,
			}
			before := effects.Load()
			firstFailure := requireServedJSONRPCError(t, rt.Endpoint, "agent.send_directive", failureParams)
			failure := requireServedDirectiveFailureEnvelope(t, rt, firstFailure, failureKey, failureCase.class, failureCase.detail)
			if got := effects.Load(); got != before+1 {
				t.Fatalf("%s %s failure effects = %d, want %d", rt.Backend, failureCase.name, got, before+1)
			}
			replayFailure := requireServedJSONRPCError(t, rt.Endpoint, "agent.send_directive", failureParams)
			replayed := requireServedDirectiveFailureEnvelope(t, rt, replayFailure, failureKey, failureCase.class, failureCase.detail)
			if got, want := mustFailureEnvelopeJSON(t, replayed), mustFailureEnvelopeJSON(t, failure); got != want {
				t.Fatalf("%s %s replay failure = %s, want %s", rt.Backend, failureCase.name, got, want)
			}
			if got := effects.Load(); got != before+1 {
				t.Fatalf("%s %s replay effects = %d, want %d", rt.Backend, failureCase.name, got, before+1)
			}
		})
	}
	runServedDirectiveResultPersistenceUncertaintyProof(t, rt, effects, faults)
	requireServedAgentDirectiveOperationCount(t, rt.DB, rt.Backend, 7)
}

func runServedDirectiveResultPersistenceUncertaintyProof(t *testing.T, rt servedControlProofRuntime, effects *atomic.Int32, faults *servedDirectivePersistenceFaults) {
	t.Helper()
	for _, test := range []struct {
		name        string
		afterCommit bool
	}{
		{name: "rollback"},
		{name: "acknowledgment_loss", afterCommit: true},
	} {
		t.Run("result_persistence_"+test.name, func(t *testing.T) {
			key := "issue-1869-" + rt.Backend + "-result-" + test.name
			params := map[string]any{
				"agent_id":        "load-agent",
				"directive":       "complete a directive with " + test.name,
				"idempotency_key": key,
			}
			before := effects.Load()
			faults.setRecordResultFault(test.afterCommit)
			immediate := requireServedJSONRPCError(t, rt.Endpoint, "agent.send_directive", params)
			failure := servedDirectiveErrorFailure(t, rt.Backend, immediate, apiv1.AgentDirectiveOutcomeIndeterminateCode)
			if failure.Class != runtimefailures.ClassOutcomeUncertain || failure.Detail.Code != runtimeagentcontrol.DirectiveResultPersistenceUnconfirmedDetail {
				t.Fatalf("%s immediate uncertainty = %#v", rt.Backend, failure)
			}
			if got := effects.Load(); got != before+1 {
				t.Fatalf("%s result persistence effects = %d, want %d", rt.Backend, got, before+1)
			}

			op := loadServedDirectiveOperationByKey(t, rt, key)
			if op.Failure != nil && op.Failure.Detail.Code == runtimeagentcontrol.DirectiveResultPersistenceUnconfirmedDetail {
				t.Fatalf("%s response-local uncertainty was persisted", rt.Backend)
			}
			if test.afterCommit {
				if op.State != runtimeagentcontrol.DirectiveOperationExecuted || len(op.Response) == 0 || op.Failure != nil {
					t.Fatalf("%s acknowledgment-loss durable operation = %#v", rt.Backend, op)
				}
				requireServedDirectivePipelineReceiptCount(t, rt, op.DirectiveEventID, 0)
				var result servedAgentDirectiveProofResult
				requireServedJSONRPCResult(t, rt.Endpoint, "agent.send_directive", params, &result)
				requireServedAgentDirectiveResult(t, rt.Backend, result)
				op = loadServedDirectiveOperationByKey(t, rt, key)
				if op.State != runtimeagentcontrol.DirectiveOperationSucceeded {
					t.Fatalf("%s acknowledgment-loss convergence state = %s", rt.Backend, op.State)
				}
				requireServedDirectivePipelineReceiptCount(t, rt, op.DirectiveEventID, 1)
			} else {
				if op.State != runtimeagentcontrol.DirectiveOperationExecuting || len(op.Response) != 0 || op.Failure != nil {
					t.Fatalf("%s rollback durable operation = %#v", rt.Backend, op)
				}
				requireServedDirectivePipelineReceiptCount(t, rt, op.DirectiveEventID, 0)
				expireServedDirectiveLease(t, rt, op.OperationID)
				replay := requireServedJSONRPCError(t, rt.Endpoint, "agent.send_directive", params)
				replayedFailure := servedDirectiveErrorFailure(t, rt.Backend, replay, apiv1.AgentDirectiveOutcomeIndeterminateCode)
				if replayedFailure.Detail.Code != runtimeagentcontrol.DirectiveExecutionLeaseExpiredDetail {
					t.Fatalf("%s rollback convergence failure = %#v", rt.Backend, replayedFailure)
				}
				op = loadServedDirectiveOperationByKey(t, rt, key)
				if op.State != runtimeagentcontrol.DirectiveOperationIndeterminate || op.Failure == nil || op.Failure.Detail.Code != runtimeagentcontrol.DirectiveExecutionLeaseExpiredDetail {
					t.Fatalf("%s rollback convergence operation = %#v", rt.Backend, op)
				}
				requireServedDirectivePipelineReceiptCount(t, rt, op.DirectiveEventID, 1)
			}
			if got := effects.Load(); got != before+1 {
				t.Fatalf("%s result persistence replay effects = %d, want %d", rt.Backend, got, before+1)
			}
		})
	}
}

func requireServedDirectiveFailureEnvelope(t *testing.T, rt servedControlProofRuntime, rpcErr *servedJSONRPCError, key string, class runtimefailures.Class, detail string) runtimefailures.Envelope {
	t.Helper()
	if rpcErr == nil || rpcErr.Data["code"] != apiv1.AgentDirectiveExecutionFailedCode {
		t.Fatalf("%s directive failure RPC = %#v", rt.Backend, rpcErr)
	}
	details, ok := rpcErr.Data["details"].(map[string]any)
	if !ok {
		t.Fatalf("%s directive failure details = %#v", rt.Backend, rpcErr.Data["details"])
	}
	for _, retired := range []string{"failure_code", "failure_message"} {
		if _, ok := details[retired]; ok {
			t.Fatalf("%s retired %s survived: %#v", rt.Backend, retired, details)
		}
	}
	apiFailure := decodeServedFailureEnvelope(t, details["failure"])
	if apiFailure.Class != class || apiFailure.Detail.Code != detail {
		t.Fatalf("%s API failure = %#v, want %s/%s", rt.Backend, apiFailure, class, detail)
	}

	var state string
	var operationFailure, receiptFailure []byte
	switch rt.Backend {
	case "postgres":
		if err := rt.DB.QueryRow(`SELECT state, failure FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = $1`, key).Scan(&state, &operationFailure); err != nil {
			t.Fatalf("load Postgres directive failure: %v", err)
		}
		if err := rt.DB.QueryRow(`SELECT er.failure FROM event_receipts er JOIN agent_directive_operations op ON op.directive_event_id = er.event_id WHERE op.method = 'agent.send_directive' AND op.idempotency_key = $1 AND er.subscriber_type = 'platform' AND er.subscriber_id = 'pipeline'`, key).Scan(&receiptFailure); err != nil {
			t.Fatalf("load Postgres directive receipt failure: %v", err)
		}
	case "sqlite":
		if err := rt.DB.QueryRow(`SELECT state, failure FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = ?`, key).Scan(&state, &operationFailure); err != nil {
			t.Fatalf("load SQLite directive failure: %v", err)
		}
		if err := rt.DB.QueryRow(`SELECT er.failure FROM event_receipts er JOIN agent_directive_operations op ON op.directive_event_id = er.event_id WHERE op.method = 'agent.send_directive' AND op.idempotency_key = ? AND er.subscriber_type = 'platform' AND er.subscriber_id = 'pipeline'`, key).Scan(&receiptFailure); err != nil {
			t.Fatalf("load SQLite directive receipt failure: %v", err)
		}
	default:
		t.Fatalf("unknown directive proof backend %q", rt.Backend)
	}
	if state != "failed" {
		t.Fatalf("%s directive failure state = %s, want failed", rt.Backend, state)
	}
	persisted, err := runtimefailures.UnmarshalEnvelope(operationFailure)
	if err != nil {
		t.Fatalf("decode operation failure: %v", err)
	}
	receipt, err := runtimefailures.UnmarshalEnvelope(receiptFailure)
	if err != nil {
		t.Fatalf("decode receipt failure: %v", err)
	}
	want := mustFailureEnvelopeJSON(t, apiFailure)
	for carrier, failure := range map[string]runtimefailures.Envelope{"operation": persisted, "receipt": receipt} {
		if got := mustFailureEnvelopeJSON(t, failure); got != want {
			t.Fatalf("%s %s failure = %s, want %s", rt.Backend, carrier, got, want)
		}
	}
	if strings.Contains(want, "raw provider failure must not survive") {
		t.Fatalf("%s raw BoardStep prose survived: %s", rt.Backend, want)
	}
	return apiFailure
}

func servedDirectiveErrorFailure(t *testing.T, backend string, rpcErr *servedJSONRPCError, code string) runtimefailures.Envelope {
	t.Helper()
	if rpcErr == nil || rpcErr.Data["code"] != code {
		t.Fatalf("%s directive error = %#v, want %s", backend, rpcErr, code)
	}
	details, ok := rpcErr.Data["details"].(map[string]any)
	if !ok {
		t.Fatalf("%s directive error details = %#v", backend, rpcErr.Data["details"])
	}
	for _, retired := range []string{"failure_code", "failure_message"} {
		if _, ok := details[retired]; ok {
			t.Fatalf("%s retired %s survived: %#v", backend, retired, details)
		}
	}
	return decodeServedFailureEnvelope(t, details["failure"])
}

func loadServedDirectiveOperationByKey(t *testing.T, rt servedControlProofRuntime, key string) runtimeagentcontrol.DirectiveOperation {
	t.Helper()
	var op runtimeagentcontrol.DirectiveOperation
	var state string
	var response, failure sql.NullString
	switch rt.Backend {
	case "postgres":
		if err := rt.DB.QueryRow(`SELECT operation_id::text, directive_event_id::text, state, response, failure FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = $1`, key).Scan(&op.OperationID, &op.DirectiveEventID, &state, &response, &failure); err != nil {
			t.Fatalf("load Postgres directive operation: %v", err)
		}
	case "sqlite":
		if err := rt.DB.QueryRow(`SELECT operation_id, directive_event_id, state, response, failure FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = ?`, key).Scan(&op.OperationID, &op.DirectiveEventID, &state, &response, &failure); err != nil {
			t.Fatalf("load SQLite directive operation: %v", err)
		}
	default:
		t.Fatalf("unknown directive proof backend %q", rt.Backend)
	}
	op.State = runtimeagentcontrol.DirectiveOperationState(state)
	if response.Valid {
		op.Response = json.RawMessage(response.String)
	}
	if failure.Valid {
		decoded, err := runtimefailures.UnmarshalEnvelope([]byte(failure.String))
		if err != nil {
			t.Fatalf("decode directive operation failure: %v", err)
		}
		op.Failure = &decoded
	}
	return op
}

func expireServedDirectiveLease(t *testing.T, rt servedControlProofRuntime, operationID string) {
	t.Helper()
	switch rt.Backend {
	case "postgres":
		if _, err := rt.DB.Exec(`UPDATE agent_directive_operations SET execution_lease_expires_at = $1 WHERE operation_id = $2::uuid`, time.Now().UTC().Add(-time.Minute), operationID); err != nil {
			t.Fatalf("expire Postgres directive lease: %v", err)
		}
	case "sqlite":
		if _, err := rt.DB.Exec(`UPDATE agent_directive_operations SET execution_lease_expires_at = ? WHERE operation_id = ?`, time.Now().UTC().Add(-time.Minute), operationID); err != nil {
			t.Fatalf("expire SQLite directive lease: %v", err)
		}
	default:
		t.Fatalf("unknown directive proof backend %q", rt.Backend)
	}
}

func requireServedDirectivePipelineReceiptCount(t *testing.T, rt servedControlProofRuntime, eventID string, want int) {
	t.Helper()
	var count int
	switch rt.Backend {
	case "postgres":
		if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, eventID).Scan(&count); err != nil {
			t.Fatalf("count Postgres directive receipts: %v", err)
		}
	case "sqlite":
		if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`, eventID).Scan(&count); err != nil {
			t.Fatalf("count SQLite directive receipts: %v", err)
		}
	default:
		t.Fatalf("unknown directive proof backend %q", rt.Backend)
	}
	if count != want {
		t.Fatalf("%s directive receipt count = %d, want %d", rt.Backend, count, want)
	}
}

func decodeServedFailureEnvelope(t *testing.T, value any) runtimefailures.Envelope {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	failure, err := runtimefailures.UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("decode served failure envelope: %v", err)
	}
	return failure
}

func mustFailureEnvelopeJSON(t *testing.T, failure runtimefailures.Envelope) string {
	t.Helper()
	raw, err := runtimefailures.MarshalEnvelope(failure)
	if err != nil {
		t.Fatalf("marshal failure envelope: %v", err)
	}
	return string(raw)
}

func requireServedAgentDirectiveEffectCount(t *testing.T, backend string, effects *atomic.Int32, want int32) {
	t.Helper()
	if got := effects.Load(); got != want {
		t.Fatalf("%s directive BoardStep effects = %d, want %d", backend, got, want)
	}
}

func requireServedAgentDirectiveResult(t *testing.T, backend string, result servedAgentDirectiveProofResult) {
	t.Helper()
	if !result.OK || result.OperationID == "" || result.RunID == "" || result.DirectiveEventID == "" || result.DirectiveEventType != "platform.agent_directive" || result.Response == "" {
		t.Fatalf("%s directive result = %#v", backend, result)
	}
}

func requireServedAgentDirectivePersistence(t *testing.T, rt servedControlProofRuntime, result servedAgentDirectiveProofResult, key string, wantOperations int) {
	t.Helper()
	ctx := context.Background()
	var operationID, eventID, runID, state, projectionResource, payloadOperationID string
	var operationCount, receiptCount int
	switch rt.Backend {
	case "postgres":
		if err := rt.DB.QueryRowContext(ctx, `SELECT operation_id::text, directive_event_id::text, resolved_run_id::text, state FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = $1`, key).Scan(&operationID, &eventID, &runID, &state); err != nil {
			t.Fatalf("postgres load directive operation: %v", err)
		}
		if err := rt.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = $1`, key).Scan(&operationCount); err != nil {
			t.Fatal(err)
		}
		if err := rt.DB.QueryRowContext(ctx, `SELECT payload->>'operation_id' FROM events WHERE event_id = $1::uuid`, result.DirectiveEventID).Scan(&payloadOperationID); err != nil {
			t.Fatal(err)
		}
		if err := rt.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline' AND outcome = 'success'`, result.DirectiveEventID).Scan(&receiptCount); err != nil {
			t.Fatal(err)
		}
		if err := rt.DB.QueryRowContext(ctx, `SELECT resource_id FROM api_idempotency WHERE method = 'agent.send_directive' AND idempotency_key = $1`, key).Scan(&projectionResource); err != nil {
			t.Fatal(err)
		}
	case "sqlite":
		if err := rt.DB.QueryRowContext(ctx, `SELECT operation_id, directive_event_id, resolved_run_id, state FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = ?`, key).Scan(&operationID, &eventID, &runID, &state); err != nil {
			t.Fatalf("sqlite load directive operation: %v", err)
		}
		if err := rt.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_directive_operations WHERE method = 'agent.send_directive' AND idempotency_key = ?`, key).Scan(&operationCount); err != nil {
			t.Fatal(err)
		}
		if err := rt.DB.QueryRowContext(ctx, `SELECT json_extract(payload, '$.operation_id') FROM events WHERE event_id = ?`, result.DirectiveEventID).Scan(&payloadOperationID); err != nil {
			t.Fatal(err)
		}
		if err := rt.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline' AND outcome = 'success'`, result.DirectiveEventID).Scan(&receiptCount); err != nil {
			t.Fatal(err)
		}
		if err := rt.DB.QueryRowContext(ctx, `SELECT resource_id FROM api_idempotency WHERE method = 'agent.send_directive' AND idempotency_key = ?`, key).Scan(&projectionResource); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown directive proof backend %q", rt.Backend)
	}
	if operationCount != wantOperations || operationID != result.OperationID || eventID != result.DirectiveEventID || runID != result.RunID || state != "succeeded" || projectionResource != result.OperationID || payloadOperationID != result.OperationID || receiptCount != 1 {
		t.Fatalf("%s directive persistence count=%d operation=%s event=%s run=%s state=%s projection=%s payload_operation=%s receipts=%d result=%#v", rt.Backend, operationCount, operationID, eventID, runID, state, projectionResource, payloadOperationID, receiptCount, result)
	}
}

func requireServedAgentDirectiveOperationCount(t *testing.T, db *sql.DB, backend string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM agent_directive_operations WHERE method = 'agent.send_directive'`).Scan(&count); err != nil {
		t.Fatalf("%s count directive operations: %v", backend, err)
	}
	if count != want {
		t.Fatalf("%s directive operation count = %d, want %d", backend, count, want)
	}
}

func publishServedLiveAgentHoldEvent(t *testing.T, rt servedControlProofRuntime, runID, sourceEventID, label string) servedEventPublishRPCResult {
	t.Helper()
	result := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
		"event_name":      "item.processed",
		"run_id":          runID,
		"source_event_id": sourceEventID,
		"payload":         map[string]any{"item_id": "hold"},
		"idempotency_key": "issue-1910-" + rt.Backend + "-" + runID + "-agent-hold-" + label,
	})
	if result.RunID != runID || result.SourceEventID != sourceEventID || result.NewRunCreated || result.EventID == "" {
		t.Fatalf("%s live-agent hold event.publish result = %#v, want existing run source=%s", rt.Backend, result, sourceEventID)
	}
	assertServedEventPublishDeliveriesContainStatus(t, result.Deliveries, "agent", "load-agent", "pending", "in_progress", "delivered")
	requireServedEventPublishCommittedReplayScope(t, rt.DB, rt.Backend, runID, result.EventID, "subscribed")
	waitServedEventPublishDeliveryStatusCountForRun(t, rt.DB, rt.Backend, runID, result.EventID, "agent", "load-agent", "delivered", 1)
	waitServedEventPublishReceiptOutcomeCount(t, rt.DB, rt.Backend, result.EventID, "agent", "load-agent", "success", 1)
	return result
}

func requireServedLiveAgentEventReplayResult(t *testing.T, rt servedControlProofRuntime, originalEventID string, result servedEventReplayProofResult) {
	t.Helper()
	if result.EventID != originalEventID || result.ReplayEventID == "" || result.AuditEventID == "" || result.ReplayEventID == originalEventID || result.AuditEventID == originalEventID || result.AuditEventID == result.ReplayEventID {
		t.Fatalf("%s event.replay result IDs = %#v, want distinct original/replay/audit IDs", rt.Backend, result)
	}
	if len(result.SubscribersReplayed) != 1 || result.SubscribersReplayed[0] != "load-agent" {
		t.Fatalf("%s event.replay subscribers = %#v, want [load-agent]", rt.Backend, result.SubscribersReplayed)
	}
	if len(result.OriginalDeliveries) != 1 || len(result.NewDeliveries) != 1 {
		t.Fatalf("%s event.replay deliveries = original %#v new %#v, want one original and one new", rt.Backend, result.OriginalDeliveries, result.NewDeliveries)
	}
	requireServedLiveAgentReplayDeliveryPair(t, rt.Backend, result.OriginalDeliveries[0], result.NewDeliveries[0])
}

func requireServedLiveAgentAgentReplayResult(t *testing.T, rt servedControlProofRuntime, originalEventID string, result servedAgentReplayProofResult) {
	t.Helper()
	if result.EventID != originalEventID || result.AgentID != "load-agent" || result.ReplayEventID == "" || result.AuditEventID == "" || result.ReplayEventID == originalEventID || result.AuditEventID == originalEventID || result.AuditEventID == result.ReplayEventID {
		t.Fatalf("%s agent.replay result IDs = %#v, want distinct original/replay/audit IDs for load-agent", rt.Backend, result)
	}
	requireServedLiveAgentReplayDeliveryPair(t, rt.Backend, result.OriginalDelivery, result.NewDelivery)
}

func requireServedLiveAgentReplayDeliveryPair(t *testing.T, backend string, original, replayed servedReplayProofDelivery) {
	t.Helper()
	if original.SubscriberID != "load-agent" || original.DeliveryID == "" {
		t.Fatalf("%s original replay delivery = %#v, want load-agent delivery", backend, original)
	}
	if replayed.SubscriberID != "load-agent" || replayed.DeliveryID == "" || replayed.SourceDeliveryID != original.DeliveryID {
		t.Fatalf("%s replay delivery = %#v, want source delivery %s", backend, replayed, original.DeliveryID)
	}
}

func seedServedLiveAgentPendingBacklogDelivery(t *testing.T, db *sql.DB, backend string) (string, string) {
	t.Helper()
	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	deliveryID := uuid.NewString()
	now := time.Now().UTC()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("seed %s live-agent backlog transaction: %v", backend, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	switch backend {
	case "postgres":
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, bundle_source, started_at)
			VALUES ($1::uuid, 'running', 'legacy', $2)
		`, runID, now); err != nil {
			t.Fatalf("seed postgres live-agent backlog run: %v", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO events (execution_mode, event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
			VALUES ('live', $1::uuid, $2::uuid, 'thing.agent_hold', 'global', '{"note":"backlog"}'::jsonb, 'test', 'agent', $3)
		`, eventID, runID, now); err != nil {
			t.Fatalf("seed postgres live-agent backlog event: %v", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO event_deliveries (delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, created_at)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'agent', 'load-agent', 'pending', $4)
		`, deliveryID, runID, eventID, now); err != nil {
			t.Fatalf("seed postgres live-agent backlog delivery: %v", err)
		}
		if err := (&store.PostgresStore{DB: db}).UpsertPipelineReceiptTx(ctx, tx, eventID, "processed", nil); err != nil {
			t.Fatalf("seed postgres live-agent backlog pipeline receipt: %v", err)
		}
	case "sqlite":
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, bundle_source, started_at)
			VALUES (?, 'running', 'legacy', ?)
		`, runID, now); err != nil {
			t.Fatalf("seed sqlite live-agent backlog run: %v", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO events (execution_mode, event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
			VALUES ('live', ?, ?, 'thing.agent_hold', 'global', '{"note":"backlog"}', 'test', 'agent', ?)
		`, eventID, runID, now); err != nil {
			t.Fatalf("seed sqlite live-agent backlog event: %v", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO event_deliveries (delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, created_at)
			VALUES (?, ?, ?, 'agent', 'load-agent', 'pending', ?)
		`, deliveryID, runID, eventID, now); err != nil {
			t.Fatalf("seed sqlite live-agent backlog delivery: %v", err)
		}
		sqliteStore := &store.SQLiteRuntimeStore{SQLiteSchemaStore: &store.SQLiteSchemaStore{DB: db}}
		if err := sqliteStore.UpsertPipelineReceiptTx(ctx, tx, eventID, "processed", nil); err != nil {
			t.Fatalf("seed sqlite live-agent backlog pipeline receipt: %v", err)
		}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit %s live-agent backlog seed: %v", backend, err)
	}
	committed = true
	if got := servedEventPublishReceiptOutcomeCount(t, db, backend, eventID, "platform", "pipeline", "success"); got != 1 {
		t.Fatalf("%s seeded live-agent backlog pipeline receipt count for event=%s = %d, want 1\n%s", backend, eventID, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	return runID, eventID
}

func requireServedOKJSONRPC(t *testing.T, endpoint, method string, params map[string]any) {
	t.Helper()
	var result map[string]any
	requireServedJSONRPCResult(t, endpoint, method, params, &result)
	if result["ok"] != true {
		t.Fatalf("%s result = %#v, want ok=true", method, result)
	}
}

func requireServedControlAPIIdempotencyRows(t *testing.T, db *sql.DB, backend, method, key string, want int) {
	t.Helper()
	if got := servedEventPublishAPIIdempotencyCount(t, db, backend, method, key); got != want {
		t.Fatalf("%s api_idempotency rows for %s/%s = %d, want %d", backend, method, key, got, want)
	}
}

func requireServedRunControlState(t *testing.T, db *sql.DB, backend, runID, wantControlStatus string, allowedRunStatuses ...string) {
	t.Helper()
	allowed := map[string]bool{}
	for _, status := range allowedRunStatuses {
		status = strings.TrimSpace(status)
		if status != "" {
			allowed[status] = true
		}
	}
	deadline := time.Now().Add(servedProofPollDeadline)
	var lastRunStatus, lastControlStatus, lastReason, lastControlledBy string
	for time.Now().Before(deadline) {
		runStatus, controlStatus, reason, controlledBy := servedRunControlState(t, db, backend, runID)
		lastRunStatus, lastControlStatus, lastReason, lastControlledBy = runStatus, controlStatus, reason, controlledBy
		if controlStatus == wantControlStatus && allowed[runStatus] {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s run_control_state for %s = run:%s control:%s reason:%s by:%s, want control=%s run in %v",
		backend, runID, lastRunStatus, lastControlStatus, lastReason, lastControlledBy, wantControlStatus, allowedRunStatuses)
}

func servedRunControlState(t *testing.T, db *sql.DB, backend, runID string) (runStatus, controlStatus, reason, controlledBy string) {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `
			SELECT r.status, COALESCE(rc.control_status, ''), COALESCE(rc.reason, ''), COALESCE(rc.controlled_by, '')
			FROM runs r
			LEFT JOIN run_control_state rc ON rc.run_id = r.run_id
			WHERE r.run_id = $1::uuid
		`
		args = []any{runID}
	case "sqlite":
		query = `
			SELECT r.status, COALESCE(rc.control_status, ''), COALESCE(rc.reason, ''), COALESCE(rc.controlled_by, '')
			FROM runs r
			LEFT JOIN run_control_state rc ON rc.run_id = r.run_id
			WHERE r.run_id = ?
		`
		args = []any{runID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&runStatus, &controlStatus, &reason, &controlledBy); err != nil {
		t.Fatalf("%s load run control state for %s: %v", backend, runID, err)
	}
	return strings.TrimSpace(runStatus), strings.TrimSpace(controlStatus), strings.TrimSpace(reason), strings.TrimSpace(controlledBy)
}

func seedServedRunControlPendingRunWithAgentDelivery(t *testing.T, rt servedControlProofRuntime) (string, string, string, string) {
	t.Helper()
	db := rt.DB
	backend := rt.Backend
	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	deliveryID := uuid.NewString()
	now := time.Now().UTC()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("seed %s run-control transaction: %v", backend, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	switch backend {
	case "postgres":
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
			VALUES ($1::uuid, 'running', $2, 'persisted', $3)
		`, runID, rt.BundleHash, now); err != nil {
			t.Fatalf("seed postgres run-control pending run: %v", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO events (execution_mode, event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
			VALUES ('live', $1::uuid, $2::uuid, 'control.stop.pending', 'global', '{}'::jsonb, 'test', 'agent', $3)
		`, eventID, runID, now); err != nil {
			t.Fatalf("seed postgres run-control pending event: %v", err)
		}
		if _, err := tx.ExecContext(ctx, `
				INSERT INTO event_deliveries (delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, created_at)
				VALUES ($1::uuid, $2::uuid, $3::uuid, 'agent', 'agent-pending', 'pending', $4)
			`, deliveryID, runID, eventID, now); err != nil {
			t.Fatalf("seed postgres run-control pending delivery: %v", err)
		}
		if err := (&store.PostgresStore{DB: db}).UpsertPipelineReceiptTx(ctx, tx, eventID, "processed", nil); err != nil {
			t.Fatalf("seed postgres run-control pipeline receipt: %v", err)
		}
	case "sqlite":
		if _, err := tx.ExecContext(ctx, `
				INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
				VALUES (?, 'running', ?, 'ephemeral', ?)
			`, runID, rt.BundleHash, now); err != nil {
			t.Fatalf("seed sqlite run-control pending run: %v", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO events (execution_mode, event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at)
			VALUES ('live', ?, ?, 'control.stop.pending', 'global', '{}', 'test', 'agent', ?)
		`, eventID, runID, now); err != nil {
			t.Fatalf("seed sqlite run-control pending event: %v", err)
		}
		if _, err := tx.ExecContext(ctx, `
				INSERT INTO event_deliveries (delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, created_at)
				VALUES (?, ?, ?, 'agent', 'agent-pending', 'pending', ?)
			`, deliveryID, runID, eventID, now); err != nil {
			t.Fatalf("seed sqlite run-control pending delivery: %v", err)
		}
		sqliteStore := &store.SQLiteRuntimeStore{SQLiteSchemaStore: &store.SQLiteSchemaStore{DB: db}}
		if err := sqliteStore.UpsertPipelineReceiptTx(ctx, tx, eventID, "processed", nil); err != nil {
			t.Fatalf("seed sqlite run-control pipeline receipt: %v", err)
		}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit %s run-control pending seed: %v", backend, err)
	}
	committed = true
	if got := servedEventPublishReceiptOutcomeCount(t, db, backend, eventID, "platform", "pipeline", "success"); got != 1 {
		t.Fatalf("%s seeded pipeline receipt count for event=%s = %d, want 1\n%s", backend, eventID, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	entityID, cardID := seedServedRunControlDecisionCard(t, rt, runID, now)
	return runID, eventID, entityID, cardID
}

func seedServedRunControlDecisionCard(t *testing.T, rt servedControlProofRuntime, runID string, now time.Time) (string, string) {
	t.Helper()
	ctx := runtimecorrelation.WithRunID(servedControlProofAuthorActivityContext(t, rt), runID)
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	bundleHash := strings.TrimSpace(rt.BundleHash)
	if bundleHash == "" {
		bundleHash = "bundle-v1:sha256:" + strings.Repeat("a", 64)
	}
	outcomes := map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"approve": {Verdict: "approve", AdvancesTo: "done"},
		"reject":  {Verdict: "reject", AdvancesTo: "rework"},
	}
	routes, err := gateruntime.FreezeRoutes(outcomes)
	if err != nil {
		t.Fatalf("freeze %s run.stop gate routes: %v", rt.Backend, err)
	}
	activation, err := gateruntime.New(runID, "root", entityID, "", "awaiting_review", "launch_review", bundleHash, routes, sourceEventID, now)
	if err != nil {
		t.Fatalf("new %s run.stop gate activation: %v", rt.Backend, err)
	}
	carrier := runtimeengine.NewStateCarrier(map[string]any{"run_id": runID}, nil, nil)
	if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
		t.Fatalf("store %s run.stop gate activation: %v", rt.Backend, err)
	}

	var cards decisioncard.Store
	var workflow *runtimepipeline.WorkflowInstanceStore
	switch rt.Backend {
	case "postgres":
		cards = &store.PostgresStore{DB: rt.DB}
		workflow = runtimepipeline.NewWorkflowInstanceStore(rt.DB)
	case "sqlite":
		sqlite := &store.SQLiteRuntimeStore{SQLiteSchemaStore: &store.SQLiteSchemaStore{DB: rt.DB}}
		cards = sqlite
		workflow = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(rt.DB, sqlite)
	default:
		t.Fatalf("unknown run.stop decision-card proof backend %q", rt.Backend)
	}
	if err := workflow.Upsert(ctx, runtimepipeline.WorkflowInstance{
		InstanceID: entityID, StorageRef: entityID, WorkflowName: "root", WorkflowVersion: "1.0.0",
		CurrentState: "awaiting_review", EnteredStageAt: now, Metadata: carrier.PersistedMetadata(), StateBuckets: carrier.PersistedStateBuckets(),
	}); err != nil {
		t.Fatalf("seed %s run.stop gated workflow instance: %v", rt.Backend, err)
	}
	snapshot, err := decisioncard.FreezeSnapshot(activation.DecisionID, "Run stop review", map[string]any{"operation": "run.stop"}, outcomes)
	if err != nil {
		t.Fatalf("freeze %s run.stop decision-card snapshot: %v", rt.Backend, err)
	}
	provenance, err := canonicaljson.FromGo(map[string]any{"source_event": sourceEventID})
	if err != nil {
		t.Fatalf("admit %s run.stop decision-card provenance: %v", rt.Backend, err)
	}
	anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
		FlowInstance: "root", EntityID: entityID, Stage: activation.Stage,
		StageActivationID: activation.ActivationID,
	})
	if err != nil {
		t.Fatalf("new %s run.stop decision-card anchor: %v", rt.Backend, err)
	}
	card, err := decisioncard.New(decisioncard.Card{
		CardID: activation.CardID, RunID: runID, ExecutionMode: executionmode.Live, Anchor: anchor,
		Snapshot: snapshot, BundleHash: bundleHash, WorkflowVersion: "1.0.0",
		EffectiveCadence: decisioncard.Cadence{InputDraftTTL: "15m", ReminderInterval: "24h"},
		Provenance:       provenance, CreatedAt: now,
	})
	if err != nil {
		t.Fatalf("new %s run.stop decision card: %v", rt.Backend, err)
	}
	if err := cards.CreateDecisionCard(ctx, card); err != nil {
		t.Fatalf("seed %s run.stop decision card: %v", rt.Backend, err)
	}
	return entityID, card.CardID
}

func requireServedTerminalDecisionCardStateChangeOnly(t *testing.T, rt servedControlProofRuntime, runID, entityID, cardID string) {
	t.Helper()
	var detail map[string]any
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.get", map[string]any{"mailbox_id": cardID}, &detail)
	card := servedAnyMap(t, detail["decision_card"])
	if detail["kind"] != decisioncard.KindDecisionCard || card["card_id"] != cardID || card["status"] != decisioncard.StatusSuperseded {
		t.Fatalf("%s terminal mailbox.get = %#v, want superseded decision card %s", rt.Backend, detail, cardID)
	}
	var listed map[string]any
	requireServedJSONRPCResult(t, rt.Endpoint, "mailbox.list", map[string]any{"run_id": runID, "status": decisioncard.StatusSuperseded}, &listed)
	found := false
	items, ok := listed["items"].([]any)
	if !ok {
		t.Fatalf("%s terminal mailbox.list items = %#v, want array", rt.Backend, listed["items"])
	}
	for _, raw := range items {
		entry := servedAnyMap(t, raw)
		if entry["kind"] != decisioncard.KindDecisionCard {
			continue
		}
		listedCard := servedAnyMap(t, entry["decision_card"])
		if listedCard["card_id"] == cardID && listedCard["status"] == decisioncard.StatusSuperseded {
			found = true
		}
	}
	if !found {
		t.Fatalf("%s terminal mailbox.list = %#v, want superseded card %s", rt.Backend, listed, cardID)
	}

	var cards decisioncard.Store
	var workflow *runtimepipeline.WorkflowInstanceStore
	switch rt.Backend {
	case "postgres":
		cards = &store.PostgresStore{DB: rt.DB}
		workflow = runtimepipeline.NewWorkflowInstanceStore(rt.DB)
	case "sqlite":
		sqlite := &store.SQLiteRuntimeStore{SQLiteSchemaStore: &store.SQLiteSchemaStore{DB: rt.DB}}
		cards = sqlite
		workflow = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(rt.DB, sqlite)
	default:
		t.Fatalf("unknown terminal decision-card proof backend %q", rt.Backend)
	}
	instance, ok, err := workflow.Load(runtimecorrelation.WithRunID(context.Background(), runID), entityID)
	if err != nil || !ok {
		t.Fatalf("load %s terminal gate instance: ok=%v err=%v", rt.Backend, ok, err)
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
	if err != nil {
		t.Fatalf("restore %s terminal gate carrier: %v", rt.Backend, err)
	}
	activation, ok, err := gateruntime.Load(carrier.StateBuckets, "", "launch_review")
	if err != nil || !ok || activation.Status != gateruntime.StatusSuperseded {
		t.Fatalf("%s terminal gate activation = %#v, found=%v err=%v", rt.Backend, activation, ok, err)
	}
	changes, err := cards.ListDecisionCardChanges(context.Background(), decisioncard.SubscriptionOptions{Limit: 50})
	if err != nil {
		t.Fatalf("list %s terminal decision-card changes: %v", rt.Backend, err)
	}
	changeCount := 0
	for _, change := range changes {
		if change.CardID == cardID && change.ChangeType == decisioncard.ChangeSuperseded {
			changeCount++
		}
	}
	if changeCount != 1 {
		t.Fatalf("%s terminal superseded changes for %s = %d, want 1", rt.Backend, cardID, changeCount)
	}
	if got := servedEventNameCountForRun(t, rt.DB, rt.Backend, runID, "mailbox.card_superseded"); got != 0 {
		t.Fatalf("%s terminal mailbox.card_superseded events for %s = %d, want 0", rt.Backend, runID, got)
	}
}

func servedEventNameCountForRun(t *testing.T, db *sql.DB, backend, runID, eventName string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name = ?`
	args := []any{runID, eventName}
	if backend == "postgres" {
		query = `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name = $2`
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s count run %s event name %s: %v", backend, runID, eventName, err)
	}
	return count
}

func requireServedStoppedPendingDelivery(t *testing.T, db *sql.DB, backend, eventID, subscriberID string) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	var lastStatus, lastReason string
	for time.Now().Before(deadline) {
		status, reason := servedDeliveryStatusReason(t, db, backend, eventID, "agent", subscriberID)
		lastStatus, lastReason = status, reason
		if status == "dead_letter" && reason == "run_stopped" {
			waitServedEventPublishReceiptOutcomeCount(t, db, backend, eventID, "agent", subscriberID, "dead_letter", 1)
			waitServedEventPublishReceiptOutcomeCount(t, db, backend, eventID, "platform", "pipeline", "success", 1)
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s stopped pending delivery %s/%s = %s/%s, want dead_letter/run_stopped\n%s", backend, eventID, subscriberID, lastStatus, lastReason, servedEventPublishDebugSummaryForEvent(t, db, backend, eventID))
}

func servedDeliveryStatusReason(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID string) (status, reason string) {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `
			SELECT status, COALESCE(reason_code, '')
			FROM event_deliveries
			WHERE event_id = $1::uuid AND subscriber_type = $2 AND subscriber_id = $3
		`
		args = []any{eventID, subscriberType, subscriberID}
	case "sqlite":
		query = `
			SELECT status, COALESCE(reason_code, '')
			FROM event_deliveries
			WHERE event_id = ? AND subscriber_type = ? AND subscriber_id = ?
		`
		args = []any{eventID, subscriberType, subscriberID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&status, &reason); err != nil {
		t.Fatalf("%s load delivery status/reason for %s: %v", backend, eventID, err)
	}
	return strings.TrimSpace(status), strings.TrimSpace(reason)
}

func requireNoServedDeliveryStatusDuring(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID, status string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if got := servedEventPublishDeliveryStatusCount(t, db, backend, eventID, subscriberType, subscriberID, status); got != 0 {
			t.Fatalf("%s delivery count for event=%s subscriber=%s/%s status=%q = %d during blocked interval, want 0",
				backend, eventID, subscriberType, subscriberID, status, got)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func requireNoServedReceiptOutcomeDuring(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID, outcome string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if got := servedEventPublishReceiptOutcomeCount(t, db, backend, eventID, subscriberType, subscriberID, outcome); got != 0 {
			t.Fatalf("%s receipt count for event=%s subscriber=%s/%s outcome=%q = %d during blocked interval, want 0",
				backend, eventID, subscriberType, subscriberID, outcome, got)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func requireServedRuntimeIngressState(t *testing.T, db *sql.DB, backend, wantStatus, wantTransitionEventName string) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	var lastStatus, lastEventID, lastEventName string
	for time.Now().Before(deadline) {
		status, eventID := servedRuntimeIngressState(t, db, backend)
		lastStatus, lastEventID = status, eventID
		if eventID != "" {
			lastEventName = servedEventNameByID(t, db, backend, eventID)
		}
		if status == wantStatus && (wantTransitionEventName == "" || lastEventName == wantTransitionEventName) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s runtime_ingress_state = status:%s transition_event:%s/%s, want %s/%s",
		backend, lastStatus, lastEventID, lastEventName, wantStatus, wantTransitionEventName)
}

func servedRuntimeIngressState(t *testing.T, db *sql.DB, backend string) (status, transitionEventID string) {
	t.Helper()
	var query string
	switch backend {
	case "postgres":
		query = `SELECT status, COALESCE(transition_event_id::text, '') FROM runtime_ingress_state WHERE id = 1`
	case "sqlite":
		query = `SELECT status, COALESCE(transition_event_id, '') FROM runtime_ingress_state WHERE id = 1`
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := db.QueryRowContext(context.Background(), query).Scan(&status, &transitionEventID); err != nil {
		t.Fatalf("%s load runtime ingress state: %v", backend, err)
	}
	return strings.TrimSpace(status), strings.TrimSpace(transitionEventID)
}

func servedEventNameByID(t *testing.T, db *sql.DB, backend, eventID string) string {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT event_name FROM events WHERE event_id = $1::uuid`
		args = []any{eventID}
	case "sqlite":
		query = `SELECT event_name FROM events WHERE event_id = ?`
		args = []any{eventID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var name string
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&name); err != nil {
		t.Fatalf("%s load event name for %s: %v", backend, eventID, err)
	}
	return strings.TrimSpace(name)
}

func requireServedEventNameCount(t *testing.T, db *sql.DB, backend, eventName string, want int) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	var got int
	for time.Now().Before(deadline) {
		got = servedEventNameCount(t, db, backend, eventName)
		if got == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s events named %s = %d, want %d", backend, eventName, got, want)
}

func servedEventNameCount(t *testing.T, db *sql.DB, backend, eventName string) int {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT COUNT(*) FROM events WHERE event_name = $1`
		args = []any{eventName}
	case "sqlite":
		query = `SELECT COUNT(*) FROM events WHERE event_name = ?`
		args = []any{eventName}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s count event name %s: %v", backend, eventName, err)
	}
	return count
}

func writeServedExternalEventFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyRootIngressServedExternalEvent(t)
}

func writeServedTestSetupFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyServedTestSetup(t)
}

func startServedJoinProofRuntime(t *testing.T) (string, *sql.DB, string, *runtimepkg.Runtime) {
	t.Helper()
	_, db, pg := installServeRuntimePostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	root := writeServedJoinProofFixture(t)
	bundleHash := seedServeRuntimeBundleCatalogRoot(t, context.Background(), pg, root)
	endpoint, rt := startServedEventPublishFollowUpRuntime(t, cliapp.ServeOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		BundleHash:              bundleHash,
		PlatformSpecPath:        defaultPlatformSpecPath,
		StoreMode:               "postgres",
		StoreModeSet:            true,
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      true,
		Verbose:                 true,
		TestOutboxSweeperConfig: servedJoinProofOutboxSweeperConfig(),
	})
	return endpoint, db, bundleHash, rt
}

func servedJoinProofOutboxSweeperConfig() runtimebus.OutboxSweeperConfig {
	cfg := runtimebus.DefaultOutboxSweeperConfig()

	cfg.Interval = time.Hour
	return cfg
}

func writeServedJoinProofFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyServedJoinProof(t)
}

func servedJoinEntityID(t *testing.T, db *sql.DB, runID string) string {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	for time.Now().Before(deadline) {
		var entityID string
		err := db.QueryRowContext(context.Background(), `
			SELECT entity_id::text
			FROM entity_state
			WHERE run_id = $1::uuid
			  AND entity_type = 'order'
			ORDER BY created_at, entity_id
			LIMIT 1
		`, runID).Scan(&entityID)
		if err == nil && strings.TrimSpace(entityID) != "" {
			return entityID
		}
		if err != nil && err != sql.ErrNoRows {
			t.Fatalf("load served join entity: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("served join entity was not created for run %s\n%s", runID, servedEventPublishDebugSummary(t, db, "postgres", runID))
	return ""
}

func seedServedJoinForkFrontier(t *testing.T, db *sql.DB, runID, entityID, sourceEventID string) string {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin served join fork frontier: %v", err)
	}
	defer tx.Rollback()
	var flowInstance string
	if err := tx.QueryRowContext(context.Background(), `
		SELECT COALESCE(flow_instance, '')
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
	`, runID, entityID).Scan(&flowInstance); err != nil {
		t.Fatalf("load served join flow instance: %v", err)
	}
	eventID := uuid.NewString()
	deliveryID := uuid.NewString()
	var createdAt time.Time
	if err := tx.QueryRowContext(context.Background(), `SELECT clock_timestamp()`).Scan(&createdAt); err != nil {
		t.Fatalf("load served join fork frontier timestamp: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO events (execution_mode,
			event_id, run_id, event_name, entity_id, flow_instance, scope, source_event_id,
			payload, produced_by, produced_by_type, created_at
		)
		VALUES ('live',
			$1::uuid, $2::uuid, 'fork.probe', $3::uuid, $4, 'entity', $5::uuid,
			'{"marker":"replayed"}'::jsonb, 'join-proof', 'platform', $6
		)
	`, eventID, runID, entityID, flowInstance, sourceEventID, createdAt); err != nil {
		t.Fatalf("seed served join fork event: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'agent', 'frontier-agent', 'pending', 'join_fork_replay_proof', $4)
	`, deliveryID, runID, eventID, createdAt); err != nil {
		t.Fatalf("seed served join fork delivery: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(), `
		INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects)
		VALUES ($1::uuid, 'platform', 'pipeline', 'success', 'pipeline_persisted', '{}'::jsonb)
	`, eventID); err != nil {
		t.Fatalf("seed served join fork pipeline receipt: %v", err)
	}
	if _, err := runforkrevision.Capture(
		context.Background(), tx, runID,
		runforkrevision.FamilyEvents,
		runforkrevision.FamilyEventDeliveries,
		runforkrevision.FamilyEventReceipts,
	); err != nil {
		t.Fatalf("capture served join fork frontier revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit served join fork frontier: %v", err)
	}
	return eventID
}

func writeServedEventPublishTargetRouteFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyRootIngressLegacyTemplateTargetRoute(t)
}

func writeServedEventPublishActiveLoadFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyRootIngressServedActiveLoad(t)
}

func writeServedSessionCleanupFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyRootIngressServedSessionCleanup(t)
}

func writeServedLiveAgentFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyRootIngressServedLiveAgent(t)
}

func writeServedDynamicAutoEmitFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyRootIngressLegacyTemplateAutoEmit(t)
}

func servedEventPublishProofOutboxSweeperConfig() runtimebus.OutboxSweeperConfig {
	cfg := runtimebus.DefaultOutboxSweeperConfig()
	cfg.Interval = 25 * time.Millisecond
	return cfg
}

func startServedEventPublishFollowUpRuntime(t *testing.T, opts cliapp.ServeOptions) (string, *runtimepkg.Runtime) {
	t.Helper()
	serveCtx, cancelServe := context.WithCancel(context.Background())
	var out lockedBuffer
	done := make(chan int, 1)
	runtimeReady := make(chan *runtimepkg.Runtime, 1)
	priorRuntimeReadyHook := opts.TestRuntimeReadyHook
	opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) {
		if priorRuntimeReadyHook != nil {
			priorRuntimeReadyHook(rt)
		}
		select {
		case runtimeReady <- rt:
		default:
		}
	}
	opts.Output = &out
	go func() {
		done <- Run(serveCtx, cliapp.RepoRoot(), opts)
	}()
	stopped := false
	waitForServeReadyLine(t, &out, done)
	var rt *runtimepkg.Runtime
	select {
	case rt = <-runtimeReady:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for serve runtime test hook\noutput:\n%s", out.String())
	}
	t.Cleanup(func() {
		if stopped {
			return
		}
		cancelServe()
		select {
		case code := <-done:
			if code != 0 {
				t.Errorf("Run exit code = %d\noutput:\n%s", code, out.String())
			}
		case <-time.After(servedProofPollDeadline):
			t.Errorf("timed out stopping Run\noutput:\n%s", out.String())
		}
		stopped = true
	})
	return "http://" + serveRuntimeAPIListenerFromOutput(t, out.String()) + "/v1/rpc", rt
}

type servedEventPublishPreHandlerProof struct {
	RunID   string
	EventID string
	NodeID  string
	Count   int
	Err     error
}

func servedEventPublishBlockingHandlerAuthorityHook(
	db *sql.DB,
	backend string,
	wantNodeID string,
	wantEventSuffix string,
	proofs chan<- servedEventPublishPreHandlerProof,
	release <-chan struct{},
) runtimepipeline.WorkflowNodeHandlerStartHook {
	var once sync.Once
	wantNodeID = strings.TrimSpace(wantNodeID)
	wantEventSuffix = strings.Trim(strings.TrimSpace(wantEventSuffix), "/")
	return func(ctx context.Context, nodeID string, evt events.Event) error {
		if !servedEventPublishMatchesNodeEvent(nodeID, evt, wantNodeID, wantEventSuffix) {
			return nil
		}
		once.Do(func() {
			runID := strings.TrimSpace(evt.RunID())
			if runID == "" {
				runID = runtimecorrelation.RunIDFromContext(ctx)
			}
			eventID := strings.TrimSpace(evt.ID())
			count, err := servedEventPublishNodeDeliveryCountValue(ctx, db, backend, runID, eventID, wantNodeID)
			proof := servedEventPublishPreHandlerProof{
				RunID:   runID,
				EventID: eventID,
				NodeID:  wantNodeID,
				Count:   count,
				Err:     err,
			}
			select {
			case proofs <- proof:
			default:
			}
			<-release
		})
		return nil
	}
}

func servedEventPublishMatchesNodeEvent(nodeID string, evt events.Event, wantNodeID, wantEventSuffix string) bool {
	wantNodeID = strings.TrimSpace(wantNodeID)
	wantEventSuffix = strings.Trim(strings.TrimSpace(wantEventSuffix), "/")
	if strings.TrimSpace(nodeID) != wantNodeID {
		return false
	}
	eventType := strings.Trim(strings.TrimSpace(string(evt.Type())), "/")
	return eventType == wantEventSuffix || strings.HasSuffix(eventType, "/"+wantEventSuffix)
}

func requireServedEventPublishPreHandlerProof(t *testing.T, db *sql.DB, backend string, proofs <-chan servedEventPublishPreHandlerProof, runID, eventID, nodeID string) {
	t.Helper()
	select {
	case proof := <-proofs:
		if proof.Err != nil {
			t.Fatalf("%s pre-handler node delivery authority query failed: %v\n%s", backend, proof.Err, servedEventPublishDebugSummary(t, db, backend, runID))
		}
		if proof.RunID != runID || proof.EventID != eventID || proof.NodeID != nodeID {
			t.Fatalf("%s pre-handler node delivery authority proof = %#v, want run=%s event=%s node=%s\n%s", backend, proof, runID, eventID, nodeID, servedEventPublishDebugSummary(t, db, backend, runID))
		}
		if proof.Count == 0 {
			t.Fatalf("%s pre-handler root-input node deliveries = %d, want committed node/%s authority before handler execution\n%s", backend, proof.Count, nodeID, servedEventPublishDebugSummary(t, db, backend, runID))
		}
	case <-time.After(45 * time.Second):
		t.Fatalf("%s pre-handler root-input node delivery authority hook did not run for run=%s event=%s node=%s\n%s", backend, runID, eventID, nodeID, servedEventPublishDebugSummary(t, db, backend, runID))
	}
}

type servedEventPublishRPCResult struct {
	EventID       string `json:"event_id"`
	RunID         string `json:"run_id"`
	SourceEventID string `json:"source_event_id,omitempty"`
	NewRunCreated bool   `json:"new_run_created"`
	Deliveries    []struct {
		SubscriberType string `json:"subscriber_type"`
		SubscriberID   string `json:"subscriber_id"`
		Status         string `json:"status"`
	} `json:"deliveries"`
}

func requireServedEventPublishRPCResult(t *testing.T, endpoint string, params map[string]any) servedEventPublishRPCResult {
	t.Helper()
	var result servedEventPublishRPCResult
	requireServedJSONRPCResult(t, endpoint, "event.publish", params, &result)
	return result
}

func assertServedEventPublishDeliveriesContainStatus(t *testing.T, deliveries []struct {
	SubscriberType string `json:"subscriber_type"`
	SubscriberID   string `json:"subscriber_id"`
	Status         string `json:"status"`
}, subscriberType, subscriberID string, statuses ...string) {
	t.Helper()
	allowed := map[string]bool{}
	for _, status := range statuses {
		allowed[status] = true
	}
	for _, delivery := range deliveries {
		if delivery.SubscriberType == subscriberType && delivery.SubscriberID == subscriberID && allowed[delivery.Status] {
			return
		}
	}
	t.Fatalf("event.publish deliveries = %#v, want %s/%s status in %v", deliveries, subscriberType, subscriberID, statuses)
}

func waitForServedEventPublishNodeDeliveryLifecycleForNode(t *testing.T, db *sql.DB, backend, runID, eventID, nodeID string, probe *lifecycletest.Probe) {
	t.Helper()
	if probe == nil {
		t.Fatalf("%s lifecycle probe is required for event %s", backend, eventID)
	}
	probe.RequireNodePending(eventID, nodeID)
	probe.Expect(eventID).
		PostCommitDispatchStarted().
		NodeInProgress(nodeID).
		HandlerStarted(nodeID).
		HandlerCompleted(nodeID).
		NodeDelivered(nodeID).
		PostCommitDispatchCompleted().
		Within(servedEventPublishLifecycleProbeWaitTimeout)
	if count := servedEventPublishNodeDeliveryCount(t, db, backend, runID, eventID, nodeID); count != 1 {
		t.Fatalf("%s node/%s delivery count for event %s = %d, want 1\n%s", backend, nodeID, eventID, count, servedEventPublishDebugSummary(t, db, backend, runID))
	}
}

func waitForServedEventPublishNodeDeliveryLifecycle(t *testing.T, db *sql.DB, backend, runID, eventID string, probe *lifecycletest.Probe) {
	t.Helper()
	waitForServedEventPublishNodeDeliveryLifecycleForNode(t, db, backend, runID, eventID, "item-handler", probe)
}

const (
	servedEventPublishLifecycleProbeWaitTimeout = 45 * time.Second
	// servedProofPollDeadline bounds poll-until-state helpers in served-path
	// proofs. Success exits early; the margin absorbs full-suite load where
	// Postgres-served runs are green in seconds isolated but can lag under
	// concurrent package load.
	servedProofPollDeadline = 60 * time.Second
)

func runServedEventPublishFollowUpProof(t *testing.T, endpoint string, db *sql.DB, backend, bundleHash string, probe *lifecycletest.Probe) {
	t.Helper()
	initialStdout, initialStderr, code := runServedCLICommand(t, endpoint, []string{
		"event", "publish", "item.received",
		"--bundle-hash", bundleHash,
		"--payload-json", `{"item_id":"item-1"}`,
		"--idempotency-key", "issue-1255-" + backend + "-initial",
	})
	if code != 0 {
		t.Fatalf("initial event publish code=%d stderr=%s stdout=%s", code, initialStderr, initialStdout)
	}
	initial := parseServedEventPublishOutput(t, initialStdout)
	runID := initial["run_id"]
	initialEventID := initial["event_id"]
	if initial["new_run_created"] != "true" || initial["deliveries"] == "0" || runID == "" || initialEventID == "" {
		t.Fatalf("initial event publish fields = %#v, want new run with delivery", initial)
	}
	if got := servedEventPublishRowCount(t, db, backend, "runs", runID, ""); got != 1 {
		t.Fatalf("%s runs for initial run = %d, want 1", backend, got)
	}
	if got := servedEventPublishNodeDeliveryCount(t, db, backend, runID, initialEventID, "item-handler"); got == 0 {
		t.Fatalf("%s initial root-input node deliveries = %d, want persisted node/item-handler authority", backend, got)
	}
	waitForServedEventPublishNodeDeliveryLifecycleForNode(t, db, backend, runID, initialEventID, "item-handler", probe)
	entityID := requireServedEventPublishEntityState(t, db, backend, runID, "", "waiting")
	requireServedEventReadback(t, endpoint, initialEventID, runID, entityID, "item.received", "item-handler")
	requireServedEntityReadback(t, endpoint, runID, entityID, "waiting")

	followUpStdout, followUpStderr, code := runServedCLICommand(t, endpoint, []string{
		"event", "publish", "item.processed",
		"--run-id", runID,
		"--payload-json", `{"item_id":"review"}`,
		"--idempotency-key", "issue-1255-" + backend + "-follow-up",
	})
	if code != 0 {
		t.Fatalf("follow-up event publish code=%d stderr=%s stdout=%s", code, followUpStderr, followUpStdout)
	}
	followUp := parseServedEventPublishOutput(t, followUpStdout)
	followUpEventID := followUp["event_id"]
	if followUp["run_id"] != runID || followUp["new_run_created"] != "false" || followUp["deliveries"] == "0" || followUpEventID == "" {
		t.Fatalf("follow-up event publish fields = %#v, want selected existing run with delivery", followUp)
	}
	if got := servedEventPublishRowCount(t, db, backend, "runs", runID, ""); got != 1 {
		t.Fatalf("%s runs for selected run after follow-up = %d, want 1", backend, got)
	}
	if got := servedEventPublishScalarCount(t, db, backend, "application_events_by_run", runID, ""); got != 2 {
		t.Fatalf("%s application events for selected run = %d, want 2\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	if got := servedEventPublishScalarCount(t, db, backend, "event_deliveries", runID, followUpEventID); got == 0 {
		t.Fatalf("%s follow-up deliveries = %d, want non-empty persisted evidence", backend, got)
	}
	waitForServedEventPublishNodeDeliveryLifecycleForNode(t, db, backend, runID, followUpEventID, "item-observer", probe)
	requireServedEventPublishEntityState(t, db, backend, runID, entityID, "done")
	requireServedEntityReadback(t, endpoint, runID, entityID, "done")
	requireServedRunStatus(t, endpoint, runID, "completed")
	requireServedEventReadback(t, endpoint, followUpEventID, runID, entityID, "item.processed", "item-observer")
	requireServedTraceReadback(t, endpoint, runID, followUpEventID, "item.processed", "item-observer")

	traceStdout, traceStderr, traceCode := runServedCLICommand(t, endpoint, []string{
		"run", "trace", runID,
		"--event-name", "item.processed",
		"--entity-id", entityID,
		"--limit", "10",
	})
	if traceCode != 0 {
		t.Fatalf("trace readback code=%d stderr=%s stdout=%s", traceCode, traceStderr, traceStdout)
	}
	for _, want := range []string{"item.processed", followUpEventID, "delivered", "node/item-observer"} {
		if !strings.Contains(traceStdout, want) {
			t.Fatalf("trace readback missing %q:\n%s", want, traceStdout)
		}
	}
	requireServedStatusCLIReadback(t, endpoint, runID, "  completed")
	entityListStdout, entityListStderr, entityListCode := runServedCLICommand(t, endpoint, []string{"entity", "list", "--run-id", runID, "--limit", "10"})
	if entityListCode != 0 {
		t.Fatalf("entities list readback code=%d stderr=%s stdout=%s", entityListCode, entityListStderr, entityListStdout)
	}
	for _, want := range []string{entityID, runID, "done"} {
		if !strings.Contains(entityListStdout, want) {
			t.Fatalf("entities list readback missing %q:\n%s", want, entityListStdout)
		}
	}
	entityViewStdout, entityViewStderr, entityViewCode := runServedCLICommand(t, endpoint, []string{"entity", "view", entityID, "--run-id", runID})
	if entityViewCode != 0 {
		t.Fatalf("entity view readback code=%d stderr=%s stdout=%s", entityViewCode, entityViewStderr, entityViewStdout)
	}
	for _, want := range []string{entityID, "state=done", "fields={}"} {
		if !strings.Contains(entityViewStdout, want) {
			t.Fatalf("entity view readback missing %q:\n%s", want, entityViewStdout)
		}
	}

	unhandledIdempotencyKey := "issue-1255-" + backend + "-unhandled"
	errResp := requireServedJSONRPCError(t, endpoint, "event.publish", map[string]any{
		"event_name":      "item.processed",
		"run_id":          runID,
		"payload":         map[string]any{"item_id": "review"},
		"idempotency_key": unhandledIdempotencyKey,
	})
	if errResp.Data["code"] != apiv1.RunAlreadyTerminalCode {
		t.Fatalf("unhandled follow-up error data = %#v, want %s", errResp.Data, apiv1.RunAlreadyTerminalCode)
	}
	details, ok := errResp.Data["details"].(map[string]any)
	if !ok || details["current_status"] != "completed" {
		t.Fatalf("unhandled follow-up details = %#v", errResp.Data["details"])
	}
	if got := servedEventPublishEventCountByIdempotencyKey(t, db, backend, unhandledIdempotencyKey); got != 0 {
		t.Fatalf("%s event rows for rejected follow-up idempotency key = %d, want 0", backend, got)
	}
	if got := servedEventPublishAPIIdempotencyCount(t, db, backend, "event.publish", unhandledIdempotencyKey); got != 0 {
		t.Fatalf("%s idempotency rows for rejected follow-up = %d, want 0", backend, got)
	}
}

func runServedEventPublishTargetRouteProof(t *testing.T, endpoint string, db *sql.DB, backend, bundleHash string, probe *lifecycletest.Probe) {
	t.Helper()
	bootstrapStdout, bootstrapStderr, code := runServedCLICommand(t, endpoint, []string{
		"event", "publish", "opco.bootstrap_requested",
		"--bundle-hash", bundleHash,
		"--payload-json", `{"owner":"operator"}`,
		"--idempotency-key", "issue-1438-" + backend + "-bootstrap",
	})
	if code != 0 {
		t.Fatalf("bootstrap target-route event publish code=%d stderr=%s stdout=%s", code, bootstrapStderr, bootstrapStdout)
	}
	bootstrap := parseServedEventPublishOutput(t, bootstrapStdout)
	runID := bootstrap["run_id"]
	bootstrapEventID := bootstrap["event_id"]
	if bootstrap["new_run_created"] != "true" || bootstrap["deliveries"] == "0" || runID == "" || bootstrapEventID == "" {
		t.Fatalf("bootstrap target-route event publish fields = %#v, want new run with delivery", bootstrap)
	}
	requireServedEventPublishEntityState(t, db, backend, runID, "", "waiting")

	instanceID := "11111111-1111-4111-8111-111111111111"
	spinupStdout, spinupStderr, code := runServedCLICommand(t, endpoint, []string{
		"event", "publish", "opco.spinup_requested",
		"--run-id", runID,
		"--source-event-id", bootstrapEventID,
		"--payload-json", fmt.Sprintf(`{"instance_id":%q,"product_id":"product-1"}`, instanceID),
		"--idempotency-key", "issue-1438-" + backend + "-spinup",
	})
	if code != 0 {
		t.Fatalf("spinup target-route event publish code=%d stderr=%s stdout=%s", code, spinupStderr, spinupStdout)
	}
	spinup := parseServedEventPublishOutput(t, spinupStdout)
	spinupEventID := spinup["event_id"]
	if spinup["run_id"] != runID || spinup["new_run_created"] != "false" || spinup["deliveries"] == "0" || spinupEventID == "" {
		t.Fatalf("spinup target-route event publish fields = %#v, want selected existing run with delivery", spinup)
	}
	waitServedEventPublishDeliveryStatusCount(t, db, backend, spinupEventID, "node", "portfolio-node", "delivered", 1)

	autoEventName := "operating/" + instanceID + "/opco.product_initialization_requested"
	autoEventID := waitServedEventPublishEventID(t, db, backend, runID, autoEventName)
	waitServedEventPublishDeliveryStatusCount(t, db, backend, autoEventID, "node", "lifecycle-orchestrator", "delivered", 1)
	entityID := servedEventPublishEventEntityID(t, db, backend, autoEventID)
	if entityID == "" {
		t.Fatalf("%s child auto event %s has no entity_id\n%s", backend, autoEventID, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	requireServedEventPublishEntityState(t, db, backend, runID, entityID, "waiting")
	targetFlowInstance := requireServedEventPublishEntityFlowInstance(t, db, backend, runID, entityID)

	targetStdout, targetStderr, code := runServedCLICommand(t, endpoint, []string{
		"event", "publish", "operating/opco.product_review_requested",
		"--run-id", runID,
		"--source-event-id", autoEventID,
		"--target-flow-instance", targetFlowInstance,
		"--target-entity-id", entityID,
		"--payload-json", `{"note":"approved-target"}`,
		"--idempotency-key", "issue-1438-" + backend + "-target",
	})
	if code != 0 {
		t.Fatalf("target-route event publish code=%d stderr=%s stdout=%s", code, targetStderr, targetStdout)
	}
	targeted := parseServedEventPublishOutput(t, targetStdout)
	targetEventID := targeted["event_id"]
	if targeted["run_id"] != runID || targeted["new_run_created"] != "false" || targeted["deliveries"] == "0" || targetEventID == "" {
		t.Fatalf("target-route event publish fields = %#v, want selected existing run with delivery", targeted)
	}
	requireServedEventPublishTargetRouteRow(t, db, backend, targetEventID, "operating/opco.product_review_requested", targetFlowInstance, entityID)
	requireServedEventPublishDeliveryTargetRoute(t, db, backend, targetEventID, "node", "lifecycle-orchestrator", targetFlowInstance, entityID)
	waitServedEventPublishDeliveryStatusCount(t, db, backend, targetEventID, "node", "lifecycle-orchestrator", "delivered", 1)
	requireServedEventPublishEntityState(t, db, backend, runID, entityID, "ready")
	requireServedEntityReadback(t, endpoint, runID, entityID, "ready")
	requireServedRunStatus(t, endpoint, runID, "completed")
	requireServedEventReadback(t, endpoint, targetEventID, runID, entityID, "operating/opco.product_review_requested", "lifecycle-orchestrator")
	requireServedTraceReadback(t, endpoint, runID, targetEventID, "operating/opco.product_review_requested", "lifecycle-orchestrator")

	if got := servedEventPublishAPIIdempotencyCount(t, db, backend, "event.publish", "issue-1438-"+backend+"-target"); got != 1 {
		t.Fatalf("%s target-route idempotency rows = %d, want 1", backend, got)
	}
}

func runServedEventPublishActiveLoadProof(
	t *testing.T,
	endpoint string,
	db *sql.DB,
	backend string,
	bundleHash string,
	probe *lifecycletest.Probe,
	agentStarted <-chan struct{},
	release chan struct{},
	releaseOnce *sync.Once,
) {
	t.Helper()
	initial := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
		"event_name":      "item.received",
		"bundle_hash":     bundleHash,
		"payload":         map[string]any{"item_id": "item-1"},
		"idempotency_key": "issue-1434-" + backend + "-initial",
	})
	runID := initial.RunID
	initialEventID := initial.EventID
	if !initial.NewRunCreated || runID == "" || initialEventID == "" {
		t.Fatalf("%s initial event.publish result = %#v, want new run", backend, initial)
	}
	waitForServedEventPublishNodeDeliveryLifecycleForNode(t, db, backend, runID, initialEventID, "item-handler", probe)
	entityID := requireServedEventPublishEntityState(t, db, backend, runID, "", "waiting")

	holdStart := time.Now()
	holdEnvelope := requestServedJSONRPC(t, endpoint, "event.publish", map[string]any{
		"event_name":      "item.processed",
		"run_id":          runID,
		"source_event_id": initialEventID,
		"payload":         map[string]any{"item_id": "hold"},
		"idempotency_key": "issue-1434-" + backend + "-agent-hold",
	})
	holdElapsed := time.Since(holdStart)
	if holdEnvelope.Error != nil {
		t.Fatalf("%s agent-hold event.publish error = %#v", backend, holdEnvelope.Error)
	}
	if holdElapsed > time.Second {
		t.Fatalf("%s agent-hold event.publish returned after %s, want durable ACK before held agent completes", backend, holdElapsed)
	}
	var hold servedEventPublishRPCResult
	if err := json.Unmarshal(holdEnvelope.Result, &hold); err != nil {
		t.Fatalf("%s decode agent-hold event.publish result: %v\n%s", backend, err, string(holdEnvelope.Result))
	}
	if hold.RunID != runID || hold.SourceEventID != initialEventID || hold.NewRunCreated || hold.EventID == "" {
		t.Fatalf("%s agent-hold event.publish result = %#v, want existing run with source lineage", backend, hold)
	}
	if got := servedEventPublishScalarCount(t, db, backend, "event_deliveries", runID, hold.EventID); got == 0 {
		t.Fatalf("%s agent-hold deliveries = %d, want persisted delivery authority\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	requireServedEventPublishCommittedReplayScope(t, db, backend, runID, hold.EventID, "subscribed")
	assertServedEventPublishDeliveriesContainStatus(t, hold.Deliveries, "agent", "load-agent", "pending", "in_progress")
	select {
	case <-agentStarted:
	case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
		t.Fatalf("%s timed out waiting for active-load agent to start\n%s", backend, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, hold.EventID, "agent", "load-agent", "in_progress"); got != 1 {
		t.Fatalf("%s agent-hold delivery in_progress count = %d, want active agent load before follow-up\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}

	unhandledKey := "issue-1434-" + backend + "-unhandled-active"
	unhandledStart := time.Now()
	unhandledEnvelope := requestServedJSONRPC(t, endpoint, "event.publish", map[string]any{
		"event_name":      "thing.unhandled",
		"run_id":          runID,
		"source_event_id": hold.EventID,
		"payload":         map[string]any{"note": "unhandled under active load"},
		"idempotency_key": unhandledKey,
	})
	unhandledElapsed := time.Since(unhandledStart)
	if unhandledEnvelope.Error == nil {
		t.Fatalf("%s unhandled event.publish error = nil, result=%s", backend, string(unhandledEnvelope.Result))
	}
	if unhandledElapsed > time.Second {
		t.Fatalf("%s unhandled event.publish returned after %s, want typed fail-closed error before client timeout", backend, unhandledElapsed)
	}
	if unhandledEnvelope.Error.Data["code"] != apiv1.EventNotDeclaredCode {
		t.Fatalf("%s unhandled event.publish error data = %#v, want %s", backend, unhandledEnvelope.Error.Data, apiv1.EventNotDeclaredCode)
	}
	if got := servedEventPublishEventCountByIdempotencyKey(t, db, backend, unhandledKey); got != 0 {
		t.Fatalf("%s event rows for unhandled active-load idempotency key = %d, want 0", backend, got)
	}
	if got := servedEventPublishAPIIdempotencyCount(t, db, backend, "event.publish", unhandledKey); got != 0 {
		t.Fatalf("%s idempotency rows for unhandled active-load publish = %d, want 0", backend, got)
	}
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, hold.EventID, "agent", "load-agent", "in_progress"); got != 1 {
		t.Fatalf("%s agent-hold delivery in_progress after fail-closed publish = %d, want still active\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}

	followStart := time.Now()
	followEnvelope := requestServedJSONRPC(t, endpoint, "event.publish", map[string]any{
		"event_name":      "item.processed",
		"run_id":          runID,
		"source_event_id": hold.EventID,
		"payload":         map[string]any{"item_id": "review"},
		"idempotency_key": "issue-1434-" + backend + "-follow-up",
	})
	followElapsed := time.Since(followStart)
	if followEnvelope.Error != nil {
		t.Fatalf("%s follow-up event.publish error = %#v", backend, followEnvelope.Error)
	}
	if followElapsed > time.Second {
		t.Fatalf("%s follow-up event.publish returned after %s, want durable ACK while unrelated delivery remains active", backend, followElapsed)
	}
	var followUp servedEventPublishRPCResult
	if err := json.Unmarshal(followEnvelope.Result, &followUp); err != nil {
		t.Fatalf("%s decode follow-up event.publish result: %v\n%s", backend, err, string(followEnvelope.Result))
	}
	if followUp.RunID != runID || followUp.SourceEventID != hold.EventID || followUp.NewRunCreated || followUp.EventID == "" {
		t.Fatalf("%s follow-up event.publish result = %#v, want existing run with hold event source lineage", backend, followUp)
	}
	if got := servedEventPublishScalarCount(t, db, backend, "event_deliveries", runID, followUp.EventID); got == 0 {
		t.Fatalf("%s follow-up deliveries = %d, want persisted delivery authority\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	requireServedEventPublishCommittedReplayScope(t, db, backend, runID, followUp.EventID, "subscribed")
	assertServedEventPublishDeliveriesContainStatus(t, followUp.Deliveries, "node", "item-observer", "pending", "in_progress", "delivered")
	if got := servedEventPublishDeliveryStatusCount(t, db, backend, hold.EventID, "agent", "load-agent", "in_progress"); got != 1 {
		t.Fatalf("%s agent-hold delivery in_progress after follow-up ACK = %d, want ACK before unrelated agent delivery release\n%s", backend, got, servedEventPublishDebugSummary(t, db, backend, runID))
	}
	requireServedEventReadback(t, endpoint, followUp.EventID, runID, entityID, "item.processed", "item-observer")

	releaseOnce.Do(func() { close(release) })
	waitServedEventPublishDeliveryStatusCount(t, db, backend, hold.EventID, "agent", "load-agent", "delivered", 1)
	waitForServedEventPublishNodeDeliveryLifecycleForNode(t, db, backend, runID, followUp.EventID, "item-observer", probe)
	requireServedEventPublishEntityState(t, db, backend, runID, entityID, "done")
	requireServedRunStatus(t, endpoint, runID, "completed")
	requireServedTraceReadback(t, endpoint, runID, followUp.EventID, "item.processed", "item-observer")
}

func runServedCLICommand(t *testing.T, endpoint string, args []string) (string, string, int) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	args = append(args, "--api-server", strings.TrimSuffix(endpoint, "/v1/rpc"))
	code := cliapp.Execute(context.Background(), t.TempDir(), args, &stdout, &stderr, nil)
	return stdout.String(), stderr.String(), code
}

func parseServedEventPublishOutput(t *testing.T, output string) map[string]string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, "event publish ok:") {
			continue
		}
		out := map[string]string{}
		for _, field := range strings.Fields(strings.TrimPrefix(line, "event publish ok:")) {
			key, value, ok := strings.Cut(field, "=")
			if ok {
				out[key] = value
			}
		}
		return out
	}
	t.Fatalf("event publish output missing success line:\n%s", output)
	return nil
}

type servedJSONRPCError struct {
	Code    int            `json:"code"`
	Message string         `json:"message"`
	Data    map[string]any `json:"data"`
}

type servedJSONRPCEnvelope struct {
	JSONRPC string              `json:"jsonrpc"`
	ID      string              `json:"id"`
	Result  json.RawMessage     `json:"result"`
	Error   *servedJSONRPCError `json:"error"`
}

func requireServedJSONRPCResult(t *testing.T, endpoint, method string, params map[string]any, out any) {
	t.Helper()
	resp := requestServedJSONRPC(t, endpoint, method, params)
	if resp.Error != nil {
		t.Fatalf("%s error = %#v", method, resp.Error)
	}
	if err := json.Unmarshal(resp.Result, out); err != nil {
		t.Fatalf("decode %s result: %v\n%s", method, err, string(resp.Result))
	}
}

func requireServedJSONRPCError(t *testing.T, endpoint, method string, params map[string]any) *servedJSONRPCError {
	t.Helper()
	resp := requestServedJSONRPC(t, endpoint, method, params)
	if resp.Error == nil {
		t.Fatalf("%s error = nil, result=%s", method, string(resp.Result))
	}
	return resp.Error
}

func requestServedJSONRPC(t *testing.T, endpoint, method string, params map[string]any) servedJSONRPCEnvelope {
	return requestServedJSONRPCWithTimeout(t, endpoint, method, params, 5*time.Second)
}

func requestServedJSONRPCWithTimeout(t *testing.T, endpoint, method string, params map[string]any, timeout time.Duration) servedJSONRPCEnvelope {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      method + "-proof",
		"method":  method,
		"params":  params,
	})
	if err != nil {
		t.Fatalf("marshal %s request: %v", method, err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build %s request: %v", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		t.Fatalf("post %s request: %v", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s HTTP status = %d, want 200", method, resp.StatusCode)
	}
	var envelope servedJSONRPCEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode %s envelope: %v", method, err)
	}
	return envelope
}

func requestServedRawJSONRPC(t *testing.T, endpoint, raw string) servedJSONRPCEnvelope {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, strings.NewReader(raw))
	if err != nil {
		t.Fatalf("build raw JSON-RPC request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("raw JSON-RPC request: %v", err)
	}
	defer resp.Body.Close()
	var envelope servedJSONRPCEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode raw JSON-RPC response: %v", err)
	}
	return envelope
}

func requireServedRunStatus(t *testing.T, endpoint, runID, want string) {
	t.Helper()
	var last string
	deadline := time.Now().Add(servedProofPollDeadline)
	for time.Now().Before(deadline) {
		var result struct {
			Run struct {
				RunID  string `json:"run_id"`
				Status string `json:"status"`
			} `json:"run"`
		}
		requireServedJSONRPCResult(t, endpoint, "run.get", map[string]any{"run_id": runID}, &result)
		last = result.Run.Status
		if result.Run.RunID == runID && result.Run.Status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run.get status for %s = %q, want %q", runID, last, want)
}

func requireServedRunStatusWithDebug(t *testing.T, endpoint string, db *sql.DB, backend, runID, want string) {
	t.Helper()
	var last string
	deadline := time.Now().Add(servedProofPollDeadline)
	for time.Now().Before(deadline) {
		var result struct {
			Run struct {
				RunID  string `json:"run_id"`
				Status string `json:"status"`
			} `json:"run"`
		}
		requireServedJSONRPCResult(t, endpoint, "run.get", map[string]any{"run_id": runID}, &result)
		last = result.Run.Status
		if result.Run.RunID == runID && result.Run.Status == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run.get status for %s = %q, want %q\n%s", runID, last, want, servedEventPublishDebugSummary(t, db, backend, runID))
}

func requireServedRunDiagnoseOperationalState(t *testing.T, endpoint, runID, want string) {
	t.Helper()
	type servedRunDiagnose struct {
		OperationalState string `json:"operational_state"`
		Run              struct {
			RunID  string `json:"run_id"`
			Status string `json:"status"`
		} `json:"run"`
	}
	var last servedRunDiagnose
	deadline := time.Now().Add(servedProofPollDeadline)
	for time.Now().Before(deadline) {
		var result servedRunDiagnose
		requireServedJSONRPCResult(t, endpoint, "run.diagnose", map[string]any{"run_id": runID}, &result)
		last = result
		if result.Run.RunID == runID && result.Run.Status == want && result.OperationalState == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run.diagnose for %s = %#v, want run/status and operational_state %q", runID, last, want)
}

func requireServedStatusCLIReadback(t *testing.T, endpoint, runID, want string) {
	t.Helper()
	var lastStdout, lastStderr string
	var lastCode int
	deadline := time.Now().Add(servedProofPollDeadline)
	for time.Now().Before(deadline) {
		lastStdout, lastStderr, lastCode = runServedCLICommand(t, endpoint, []string{"run", "status", runID, "--no-diagnose"})
		if lastCode == 0 && strings.Contains(lastStdout, want) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("status readback code=%d stderr=%s stdout=%s\nmissing %q", lastCode, lastStderr, lastStdout, want)
}

func requireServedEventReadback(t *testing.T, endpoint, eventID, runID, entityID, eventName, subscriberID string) {
	t.Helper()
	type servedEventReadback struct {
		EventID    string `json:"event_id"`
		EventName  string `json:"event_name"`
		EntityID   string `json:"entity_id"`
		RunID      string `json:"run_id"`
		Deliveries []struct {
			SubscriberType string `json:"subscriber_type"`
			SubscriberID   string `json:"subscriber_id"`
			Status         string `json:"status"`
		} `json:"deliveries"`
	}

	var last servedEventReadback
	deadline := time.Now().Add(servedProofPollDeadline)
	for time.Now().Before(deadline) {
		var event servedEventReadback
		requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": eventID}, &event)
		last = event
		if event.EventID != eventID || event.RunID != runID || event.EntityID != entityID || event.EventName != eventName {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		for _, delivery := range event.Deliveries {
			if delivery.SubscriberType == "node" && delivery.SubscriberID == subscriberID && delivery.Status == "delivered" {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("event.get result = %#v, want delivered node/%s for event %s on selected run", last, subscriberID, eventID)
}

func requireServedTraceReadback(t *testing.T, endpoint, runID, eventID, eventName, subscriberID string) {
	t.Helper()
	type servedTraceReadback struct {
		Trace []struct {
			EventID        string `json:"event_id"`
			EventName      string `json:"event_name"`
			DeliveryStatus string `json:"delivery_status"`
			SubscriberType string `json:"subscriber_type"`
			SubscriberID   string `json:"subscriber_id"`
		} `json:"trace"`
	}

	var last servedTraceReadback
	deadline := time.Now().Add(servedProofPollDeadline)
	for time.Now().Before(deadline) {
		var trace servedTraceReadback
		requireServedJSONRPCResult(t, endpoint, "run.trace", map[string]any{
			"run_id": runID,
			"filter": map[string]any{
				"event_name": []string{eventName},
			},
			"limit": 10,
		}, &trace)
		last = trace
		for _, row := range trace.Trace {
			if row.EventID == eventID && row.EventName == eventName && row.DeliveryStatus == "delivered" &&
				row.SubscriberType == "node" && row.SubscriberID == subscriberID {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run.trace rows = %#v, want delivered node/%s row for %s", last.Trace, subscriberID, eventID)
}

func requireServedEntityReadback(t *testing.T, endpoint, runID, entityID, wantState string) {
	t.Helper()
	var result struct {
		Entity struct {
			EntityID     string `json:"entity_id"`
			RunID        string `json:"run_id"`
			CurrentState string `json:"current_state"`
		} `json:"entity"`
	}
	requireServedJSONRPCResult(t, endpoint, "entity.get", map[string]any{"entity_id": entityID, "run_id": runID}, &result)
	if result.Entity.EntityID != entityID || result.Entity.RunID != runID || result.Entity.CurrentState != wantState {
		t.Fatalf("entity.get result = %#v, want %s/%s state %s", result.Entity, runID, entityID, wantState)
	}
}

func requireServedEventPublishEntityState(t *testing.T, db *sql.DB, backend, runID, entityID, wantState string) string {
	t.Helper()
	var lastState, lastEntityID string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		gotEntityID, state := servedEventPublishEntityState(t, db, backend, runID, entityID, wantState)
		if state == wantState && gotEntityID != "" {
			return gotEntityID
		}
		lastEntityID = gotEntityID
		lastState = state
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s entity_state for run %s entity %q = %q (entity %q), want %q\n%s", backend, runID, entityID, lastState, lastEntityID, wantState, servedEventPublishDebugSummary(t, db, backend, runID))
	return ""
}

func requireServedEventPublishEntityFlowInstance(t *testing.T, db *sql.DB, backend, runID, entityID string) string {
	t.Helper()
	var (
		query        string
		flowInstance string
		args         []any
	)
	switch backend {
	case "postgres":
		query = `SELECT COALESCE(flow_instance, '') FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
		args = []any{runID, entityID}
	case "sqlite":
		query = `SELECT COALESCE(flow_instance, '') FROM entity_state WHERE run_id = ? AND entity_id = ?`
		args = []any{runID, entityID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&flowInstance); err != nil {
		t.Fatalf("%s load entity flow_instance run=%s entity=%s: %v", backend, runID, entityID, err)
	}
	flowInstance = strings.Trim(strings.TrimSpace(flowInstance), "/")
	if flowInstance == "" {
		t.Fatalf("%s entity flow_instance for run=%s entity=%s is empty", backend, runID, entityID)
	}
	return flowInstance
}

func requireServedEventPublishTargetRouteRow(t *testing.T, db *sql.DB, backend, eventID, eventName, flowInstance, entityID string) {
	t.Helper()
	var (
		query           string
		gotEventName    string
		gotEntityID     string
		gotFlowInstance string
		targetRoute     string
		args            []any
	)
	switch backend {
	case "postgres":
		query = `
			SELECT event_name, COALESCE(entity_id::text, ''), COALESCE(flow_instance, ''), COALESCE(target_route::text, '{}')
			FROM events
			WHERE event_id = $1::uuid
		`
		args = []any{eventID}
	case "sqlite":
		query = `
			SELECT event_name, COALESCE(entity_id, ''), COALESCE(flow_instance, ''), COALESCE(target_route, '{}')
			FROM events
			WHERE event_id = ?
		`
		args = []any{eventID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&gotEventName, &gotEntityID, &gotFlowInstance, &targetRoute); err != nil {
		t.Fatalf("%s load target-route event row: %v", backend, err)
	}
	if gotEventName != eventName || gotEntityID != entityID || gotFlowInstance != flowInstance {
		t.Fatalf("%s target event row = event:%q entity:%q flow:%q, want %q/%q/%q", backend, gotEventName, gotEntityID, gotFlowInstance, eventName, entityID, flowInstance)
	}
	requireServedEventPublishRouteJSON(t, backend, "event.target_route", targetRoute, flowInstance, entityID)
}

func requireServedEventPublishDeliveryTargetRoute(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID, flowInstance, entityID string) {
	t.Helper()
	var (
		query       string
		targetRoute string
		args        []any
	)
	switch backend {
	case "postgres":
		query = `
			SELECT COALESCE(delivery_target_route::text, '{}')
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = $2
			  AND subscriber_id = $3
			LIMIT 1
		`
		args = []any{eventID, subscriberType, subscriberID}
	case "sqlite":
		query = `
			SELECT COALESCE(delivery_target_route, '{}')
			FROM event_deliveries
			WHERE event_id = ?
			  AND subscriber_type = ?
			  AND subscriber_id = ?
			LIMIT 1
		`
		args = []any{eventID, subscriberType, subscriberID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&targetRoute); err != nil {
		t.Fatalf("%s load delivery target route: %v", backend, err)
	}
	requireServedEventPublishRouteJSON(t, backend, "event_deliveries.delivery_target_route", targetRoute, flowInstance, entityID)
}

func requireServedEventPublishRouteJSON(t *testing.T, backend, field, raw, flowInstance, entityID string) {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("%s decode %s route JSON %q: %v", backend, field, raw, err)
	}
	if decoded["flow_instance"] != flowInstance || decoded["entity_id"] != entityID {
		t.Fatalf("%s %s route = %#v, want flow/entity %s/%s", backend, field, decoded, flowInstance, entityID)
	}
}

func servedEventPublishDebugSummary(t *testing.T, db *sql.DB, backend, runID string) string {
	t.Helper()
	sections := []string{
		servedEventPublishDebugQuery(t, db, backend, "entity_state", runID),
		servedEventPublishDebugQuery(t, db, backend, "events", runID),
		servedEventPublishDebugQuery(t, db, backend, "event_deliveries", runID),
		servedEventPublishDebugQuery(t, db, backend, "event_receipts", runID),
		servedEventPublishDebugQuery(t, db, backend, "dead_letters", runID),
	}
	return strings.Join(sections, "\n")
}

func servedEventPublishDebugQuery(t *testing.T, db *sql.DB, backend, scope, runID string) string {
	t.Helper()
	sqlText := ""
	args := []any{runID}
	switch backend {
	case "postgres":
		switch scope {
		case "entity_state":
			sqlText = `SELECT entity_id::text, COALESCE(flow_instance, ''), COALESCE(current_state, '') FROM entity_state WHERE run_id = $1::uuid ORDER BY created_at, entity_id LIMIT 5`
		case "events":
			sqlText = `SELECT event_id::text, event_name, COALESCE(entity_id::text, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = $1::uuid ORDER BY created_at, event_id LIMIT 5`
		case "event_deliveries":
			sqlText = `SELECT event_id::text, subscriber_type, subscriber_id, status, COALESCE(reason_code, '') FROM event_deliveries WHERE run_id = $1::uuid ORDER BY created_at, event_id LIMIT 8`
		case "event_receipts":
			sqlText = `SELECT r.event_id::text, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, ''), COALESCE(r.side_effects::text, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = $1::uuid ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "dead_letters":
			sqlText = `SELECT d.original_event, COALESCE(d.entity_id::text, ''), COALESCE(d.failure->>'class', ''), COALESCE(d.failure->'detail'->>'code', '') FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = $1::uuid ORDER BY d.created_at LIMIT 5`
		}
	case "sqlite":
		switch scope {
		case "entity_state":
			sqlText = `SELECT entity_id, COALESCE(flow_instance, ''), COALESCE(current_state, '') FROM entity_state WHERE run_id = ? ORDER BY created_at, entity_id LIMIT 5`
		case "events":
			sqlText = `SELECT event_id, event_name, COALESCE(entity_id, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = ? ORDER BY created_at, event_id LIMIT 5`
		case "event_deliveries":
			sqlText = `SELECT event_id, subscriber_type, subscriber_id, status, COALESCE(reason_code, '') FROM event_deliveries WHERE run_id = ? ORDER BY created_at, event_id LIMIT 8`
		case "event_receipts":
			sqlText = `SELECT r.event_id, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, ''), COALESCE(r.side_effects, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = ? ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "dead_letters":
			sqlText = `SELECT d.original_event, COALESCE(d.entity_id, ''), COALESCE(json_extract(d.failure, '$.class'), ''), COALESCE(json_extract(d.failure, '$.detail.code'), '') FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = ? ORDER BY d.created_at LIMIT 5`
		}
	}
	if sqlText == "" {
		return scope + ": unsupported debug query"
	}
	rows, err := db.QueryContext(context.Background(), sqlText, args...)
	if err != nil {
		return fmt.Sprintf("%s: %v", scope, err)
	}
	defer rows.Close()
	out := []string{}
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Sprintf("%s columns: %v", scope, err)
	}
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		scan := make([]any, len(values))
		for i := range values {
			scan[i] = &values[i]
		}
		if err := rows.Scan(scan...); err != nil {
			return fmt.Sprintf("%s scan: %v", scope, err)
		}
		cols := make([]string, len(values))
		for i, value := range values {
			if value.Valid {
				cols[i] = value.String
			}
		}
		out = append(out, fmt.Sprintf("%v", cols))
	}
	if err := rows.Err(); err != nil {
		return fmt.Sprintf("%s rows: %v", scope, err)
	}
	if len(out) == 0 {
		return scope + ": []"
	}
	return scope + ": " + strings.Join(out, "; ")
}

func servedEventPublishRowCount(t *testing.T, db *sql.DB, backend, scope, runID, eventID string) int {
	t.Helper()
	return servedEventPublishScalarCount(t, db, backend, scope, runID, eventID)
}

func servedEventPublishScalarCount(t *testing.T, db *sql.DB, backend, scope, runID, eventID string) int {
	t.Helper()
	var sqlText string
	var args []any
	switch backend {
	case "postgres":
		switch scope {
		case "runs":
			sqlText = `SELECT COUNT(*) FROM runs WHERE run_id = $1::uuid`
			args = []any{runID}
		case "events_by_run":
			sqlText = `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`
			args = []any{runID}
		case "application_events_by_run":
			sqlText = `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name NOT LIKE 'platform.%'`
			args = []any{runID}
		case "event_deliveries":
			sqlText = `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid AND event_id = $2::uuid`
			args = []any{runID, eventID}
		default:
			t.Fatalf("unknown postgres proof count scope %q", scope)
		}
	case "sqlite":
		switch scope {
		case "runs":
			sqlText = `SELECT COUNT(*) FROM runs WHERE run_id = ?`
			args = []any{runID}
		case "events_by_run":
			sqlText = `SELECT COUNT(*) FROM events WHERE run_id = ?`
			args = []any{runID}
		case "application_events_by_run":
			sqlText = `SELECT COUNT(*) FROM events WHERE run_id = ? AND event_name NOT LIKE 'platform.%'`
			args = []any{runID}
		case "event_deliveries":
			sqlText = `SELECT COUNT(*) FROM event_deliveries WHERE run_id = ? AND event_id = ?`
			args = []any{runID, eventID}
		default:
			t.Fatalf("unknown sqlite proof count scope %q", scope)
		}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), sqlText, args...).Scan(&count); err != nil {
		t.Fatalf("%s count %s: %v", backend, scope, err)
	}
	return count
}

func servedEventPublishEventCountByIdempotencyKey(t *testing.T, db *sql.DB, backend, idempotencyKey string) int {
	t.Helper()
	var (
		query string
		args  []any
	)
	switch backend {
	case "postgres":
		query = `SELECT COUNT(*) FROM events WHERE idempotency_key = $1`
		args = []any{idempotencyKey}
	case "sqlite":
		query = `SELECT COUNT(*) FROM events WHERE idempotency_key = ?`
		args = []any{idempotencyKey}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s count events by idempotency key %q: %v", backend, idempotencyKey, err)
	}
	return count
}

func servedEventPublishAPIIdempotencyCount(t *testing.T, db *sql.DB, backend, method, idempotencyKey string) int {
	t.Helper()
	var (
		query string
		args  []any
	)
	switch backend {
	case "postgres":
		query = `SELECT COUNT(*) FROM api_idempotency WHERE method = $1 AND idempotency_key = $2`
		args = []any{method, idempotencyKey}
	case "sqlite":
		query = `SELECT COUNT(*) FROM api_idempotency WHERE method = ? AND idempotency_key = ?`
		args = []any{method, idempotencyKey}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s count api idempotency method=%q key=%q: %v", backend, method, idempotencyKey, err)
	}
	return count
}

func waitServedEventPublishDeliveryStatusCount(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID, status string, want int) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	var got int
	for time.Now().Before(deadline) {
		got = servedEventPublishDeliveryStatusCount(t, db, backend, eventID, subscriberType, subscriberID, status)
		if got == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s delivery count for event=%s subscriber=%s/%s status=%q = %d, want %d", backend, eventID, subscriberType, subscriberID, status, got, want)
}

func waitServedEventPublishDeliveryStatusCountForRun(t *testing.T, db *sql.DB, backend, runID, eventID, subscriberType, subscriberID, status string, want int) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	var got int
	for time.Now().Before(deadline) {
		got = servedEventPublishDeliveryStatusCount(t, db, backend, eventID, subscriberType, subscriberID, status)
		if got == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s delivery count for event=%s subscriber=%s/%s status=%q = %d, want %d\n%s",
		backend, eventID, subscriberType, subscriberID, status, got, want, servedEventPublishDebugSummary(t, db, backend, runID))
}

func servedEventPublishDeliveryStatusCount(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID string, statuses ...string) int {
	t.Helper()
	where := []string{}
	args := []any{}
	switch backend {
	case "postgres":
		where = append(where, fmt.Sprintf("event_id = $%d::uuid", len(args)+1))
		args = append(args, eventID)
		if strings.TrimSpace(subscriberType) != "" {
			where = append(where, fmt.Sprintf("subscriber_type = $%d", len(args)+1))
			args = append(args, subscriberType)
		}
		where = append(where, fmt.Sprintf("subscriber_id = $%d", len(args)+1))
		args = append(args, subscriberID)
		for _, status := range statuses {
			if strings.TrimSpace(status) == "" {
				continue
			}
			where = append(where, fmt.Sprintf("status = $%d", len(args)+1))
			args = append(args, status)
		}
	case "sqlite":
		where = append(where, "event_id = ?")
		args = append(args, eventID)
		if strings.TrimSpace(subscriberType) != "" {
			where = append(where, "subscriber_type = ?")
			args = append(args, subscriberType)
		}
		where = append(where, "subscriber_id = ?")
		args = append(args, subscriberID)
		for _, status := range statuses {
			if strings.TrimSpace(status) == "" {
				continue
			}
			where = append(where, "status = ?")
			args = append(args, status)
		}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	query := "SELECT COUNT(*) FROM event_deliveries WHERE " + strings.Join(where, " AND ")
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s delivery count query failed: %v\n%s", backend, err, query)
	}
	return count
}

func requireServedEventPublishCommittedReplayScope(t *testing.T, db *sql.DB, backend, runID, eventID, wantScope string) {
	t.Helper()
	wantReason := "replay_scope_" + strings.TrimSpace(wantScope)
	var (
		query string
		args  []any
	)
	switch backend {
	case "postgres":
		query = `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE run_id = $1::uuid
			  AND event_id = $2::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = '__runtime_replay_scope__'
			  AND status = 'delivered'
			  AND reason_code = $3
			  AND delivered_at IS NOT NULL
		`
		args = []any{runID, eventID, wantReason}
	case "sqlite":
		query = `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE run_id = ?
			  AND event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = '__runtime_replay_scope__'
			  AND status = 'delivered'
			  AND reason_code = ?
			  AND delivered_at IS NOT NULL
		`
		args = []any{runID, eventID, wantReason}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s committed replay scope query failed: %v\n%s", backend, err, query)
	}
	if count != 1 {
		t.Fatalf("%s committed replay scope rows for run=%s event=%s reason=%q = %d, want 1\n%s", backend, runID, eventID, wantReason, count, servedEventPublishDebugSummary(t, db, backend, runID))
	}
}

func waitServedEventPublishReceiptOutcomeCount(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID, outcome string, want int) {
	t.Helper()
	deadline := time.Now().Add(servedProofPollDeadline)
	var got int
	for time.Now().Before(deadline) {
		got = servedEventPublishReceiptOutcomeCount(t, db, backend, eventID, subscriberType, subscriberID, outcome)
		if got == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s receipt count for event=%s subscriber=%s/%s outcome=%q = %d, want %d\n%s", backend, eventID, subscriberType, subscriberID, outcome, got, want, servedEventPublishDebugSummaryForEvent(t, db, backend, eventID))
}

func servedEventPublishDebugSummaryForEvent(t *testing.T, db *sql.DB, backend, eventID string) string {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT run_id::text FROM events WHERE event_id = $1::uuid`
		args = []any{eventID}
	case "sqlite":
		query = `SELECT run_id FROM events WHERE event_id = ?`
		args = []any{eventID}
	default:
		return fmt.Sprintf("unknown proof backend %q", backend)
	}
	var runID string
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&runID); err != nil {
		return fmt.Sprintf("%s event %s run lookup failed: %v", backend, eventID, err)
	}
	return servedEventPublishDebugSummary(t, db, backend, runID)
}

func servedEventPublishReceiptOutcomeCount(t *testing.T, db *sql.DB, backend, eventID, subscriberType, subscriberID, outcome string) int {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = $2 AND subscriber_id = $3 AND outcome = $4`
		args = []any{eventID, subscriberType, subscriberID, outcome}
	case "sqlite":
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = ? AND subscriber_id = ? AND outcome = ?`
		args = []any{eventID, subscriberType, subscriberID, outcome}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s receipt count query failed: %v\n%s", backend, err, query)
	}
	return count
}

func servedEventPublishReceiptCountBySubscribers(t *testing.T, db *sql.DB, backend, eventID string, subscriberIDs ...string) int {
	t.Helper()
	if len(subscriberIDs) == 0 {
		t.Fatal("subscriberIDs is required")
	}
	var query string
	args := []any{eventID}
	switch backend {
	case "postgres":
		placeholders := make([]string, 0, len(subscriberIDs))
		for _, subscriberID := range subscriberIDs {
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)+1))
			args = append(args, subscriberID)
		}
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_id IN (` + strings.Join(placeholders, ", ") + `)`
	case "sqlite":
		placeholders := make([]string, 0, len(subscriberIDs))
		for _, subscriberID := range subscriberIDs {
			placeholders = append(placeholders, "?")
			args = append(args, subscriberID)
		}
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_id IN (` + strings.Join(placeholders, ", ") + `)`
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s receipt subscriber count query failed: %v\n%s", backend, err, query)
	}
	return count
}

func waitServedEventPublishEventID(t *testing.T, db *sql.DB, backend, runID, eventName string) string {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT event_id::text FROM events WHERE run_id = $1::uuid AND event_name = $2`
		args = []any{runID, eventName}
	case "sqlite":
		query = `SELECT event_id FROM events WHERE run_id = ? AND event_name = ?`
		args = []any{runID, eventName}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	deadline := time.Now().Add(servedProofPollDeadline)
	for {
		var eventID string
		err := db.QueryRowContext(context.Background(), query, args...).Scan(&eventID)
		if err == nil && strings.TrimSpace(eventID) != "" {
			return strings.TrimSpace(eventID)
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("%s event id query failed: %v\n%s", backend, err, query)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s timed out waiting for event %q in run %s\n%s", backend, eventName, runID, servedEventPublishDebugSummary(t, db, backend, runID))
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func servedEventPublishEventEntityID(t *testing.T, db *sql.DB, backend, eventID string) string {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT COALESCE(entity_id::text, '') FROM events WHERE event_id = $1::uuid`
		args = []any{eventID}
	case "sqlite":
		query = `SELECT COALESCE(entity_id, '') FROM events WHERE event_id = ?`
		args = []any{eventID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var entityID string
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&entityID); err != nil {
		t.Fatalf("%s load event entity_id %s: %v", backend, eventID, err)
	}
	return strings.TrimSpace(entityID)
}

func assertServedDynamicAutoEmitPayloadProductOnly(t *testing.T, db *sql.DB, backend, eventID string) {
	t.Helper()
	var raw string
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT payload::text FROM events WHERE event_id = $1::uuid`
		args = []any{eventID}
	case "sqlite":
		query = `SELECT payload FROM events WHERE event_id = ?`
		args = []any{eventID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&raw); err != nil {
		t.Fatalf("%s load event payload %s: %v", backend, eventID, err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode event payload %s: %v\n%s", eventID, err, raw)
	}
	if got := payload["product_id"]; got != "product-1" {
		t.Fatalf("payload product_id = %#v, want product-1: %#v", got, payload)
	}
	for _, key := range []string{"instance_id", "template_id", "flow_path", "parent_entity_id"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("payload includes hidden activation context %q: %#v", key, payload)
		}
	}
}

func requireServedReplayNoDeliveryHistoryNoMutation(t *testing.T, endpoint string, db *sql.DB, backend, eventID, idempotencyKey string) {
	t.Helper()
	beforeSourcedEvents := servedEventPublishSourcedEventCount(t, db, backend, eventID)
	errResp := requireServedJSONRPCError(t, endpoint, "event.replay", map[string]any{
		"event_id":        eventID,
		"idempotency_key": idempotencyKey,
	})
	if errResp.Data["code"] != apiv1.EventReplayNoDeliveryHistoryCode {
		t.Fatalf("event.replay data = %#v, want %s", errResp.Data, apiv1.EventReplayNoDeliveryHistoryCode)
	}
	if got := servedEventPublishSourcedEventCount(t, db, backend, eventID); got != beforeSourcedEvents {
		t.Fatalf("%s events sourced from rejected node-only replay target = %d, want %d", backend, got, beforeSourcedEvents)
	}
	if got := servedEventPublishAPIIdempotencyCount(t, db, backend, "event.replay", idempotencyKey); got != 0 {
		t.Fatalf("%s api_idempotency rows for rejected node-only replay = %d, want 0", backend, got)
	}
}

func servedEventPublishSourcedEventCount(t *testing.T, db *sql.DB, backend, sourceEventID string) int {
	t.Helper()
	var query string
	var args []any
	switch backend {
	case "postgres":
		query = `SELECT COUNT(*) FROM events WHERE source_event_id = $1::uuid`
		args = []any{sourceEventID}
	case "sqlite":
		query = `SELECT COUNT(*) FROM events WHERE source_event_id = ?`
		args = []any{sourceEventID}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("%s sourced event count query failed: %v\n%s", backend, err, query)
	}
	return count
}

func servedEventPublishNodeDeliveryCount(t *testing.T, db *sql.DB, backend, runID, eventID, subscriberID string) int {
	t.Helper()
	count, err := servedEventPublishNodeDeliveryCountValue(context.Background(), db, backend, runID, eventID, subscriberID)
	if err != nil {
		t.Fatalf("%s count node delivery %s/%s: %v", backend, eventID, subscriberID, err)
	}
	return count
}

func servedEventPublishNodeDeliveryCountValue(ctx context.Context, db *sql.DB, backend, runID, eventID, subscriberID string) (int, error) {
	var sqlText string
	var args []any
	switch backend {
	case "postgres":
		sqlText = `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE run_id = $1::uuid
			  AND event_id = $2::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = $3
		`
		args = []any{runID, eventID, subscriberID}
	case "sqlite":
		sqlText = `
			SELECT COUNT(*)
			FROM event_deliveries
			WHERE run_id = ?
			  AND event_id = ?
			  AND subscriber_type = 'node'
			  AND subscriber_id = ?
		`
		args = []any{runID, eventID, subscriberID}
	default:
		return 0, fmt.Errorf("unknown proof backend %q", backend)
	}
	var count int
	if err := db.QueryRowContext(ctx, sqlText, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func servedEventPublishEntityState(t *testing.T, db *sql.DB, backend, runID, entityID, wantState string) (string, string) {
	t.Helper()
	var sqlText string
	var args []any
	switch backend {
	case "postgres":
		if strings.TrimSpace(entityID) != "" {
			sqlText = `SELECT COALESCE(current_state, '') FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`
			args = []any{runID, entityID}
		} else {
			sqlText = `SELECT entity_id::text, COALESCE(current_state, '') FROM entity_state WHERE run_id = $1::uuid AND current_state = $2 ORDER BY created_at, entity_id LIMIT 1`
			args = []any{runID, wantState}
		}
	case "sqlite":
		if strings.TrimSpace(entityID) != "" {
			sqlText = `SELECT COALESCE(current_state, '') FROM entity_state WHERE run_id = ? AND entity_id = ?`
			args = []any{runID, entityID}
		} else {
			sqlText = `SELECT entity_id, COALESCE(current_state, '') FROM entity_state WHERE run_id = ? AND current_state = ? ORDER BY created_at, entity_id LIMIT 1`
			args = []any{runID, wantState}
		}
	default:
		t.Fatalf("unknown proof backend %q", backend)
	}
	var gotEntityID, state string
	if strings.TrimSpace(entityID) != "" {
		gotEntityID = strings.TrimSpace(entityID)
		err := db.QueryRowContext(context.Background(), sqlText, args...).Scan(&state)
		if err == sql.ErrNoRows {
			return gotEntityID, ""
		}
		if err != nil {
			t.Fatalf("%s entity_state query: %v", backend, err)
		}
		return gotEntityID, state
	}
	err := db.QueryRowContext(context.Background(), sqlText, args...).Scan(&gotEntityID, &state)
	if err == sql.ErrNoRows {
		return "", ""
	}
	if err != nil {
		t.Fatalf("%s entity_state query: %v", backend, err)
	}
	return gotEntityID, state
}

func TestRunServeRuntimePassesDataFlagToWorkspaceLifecycle(t *testing.T) {
	dataDir := t.TempDir()
	var capturedMountSources cliapp.WorkspaceMountSources
	_, _, _ = installServeRuntimePostgresTestStoresWithWorkspaceFactory(t, func(mountSources cliapp.WorkspaceMountSources) cliapp.ServeWorkspaceLifecycle {
		capturedMountSources = mountSources
		return serveRuntimeWorkspaceStub{}
	})

	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		DataSource:         dataDir,
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
		Verbose:            true,
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
	if capturedMountSources.DataSource != dataDir || capturedMountSources.DataSourceSource != "--data" {
		t.Fatalf("workspace mount sources = %#v, want %q from --data", capturedMountSources, dataDir)
	}
}

func TestRunServeRuntimeHostWorkspaceBackendBootsWithoutDockerForSystemOnlyFlow(t *testing.T) {
	missingDocker := filepath.Join(t.TempDir(), "missing-docker")
	hostRoot := filepath.Join(t.TempDir(), "host-workspaces")
	dataDir := t.TempDir()
	configPath := writeStoreBackendRuntimeConfigWithWorkspaceFields(t, storebackend.BackendSQLite.String(), filepath.Join(t.TempDir(), "runtime.db"), []string{
		fmt.Sprintf("  docker_bin: %q", missingDocker),
		fmt.Sprintf("  host_root: %q", hostRoot),
	})

	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:           configPath,
		ContractsPath:        filepath.Join("examples", "routing", "root-ingress"),
		DataSource:           dataDir,
		WorkspaceBackend:     workspace.BackendHost,
		WorkspaceBackendSet:  true,
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            storebackend.ActiveDefaultBackend().String(),
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		RequireBundleMatch:   false,
		ShutdownGrace:        runtimepkg.DefaultShutdownGrace,
		Verbose:              true,
		NoRequireBundleMatch: true,
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
	if strings.Contains(serve.outputString(), "workspace image") || strings.Contains(serve.outputString(), "Docker is not reachable") {
		t.Fatalf("host workspace serve output shows Docker dependency despite host backend:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeNoAgentDefaultBootsWithoutDocker(t *testing.T) {
	missingDocker := filepath.Join(t.TempDir(), "missing-docker")

	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath: writeStoreBackendRuntimeConfigWithWorkspaceFields(t, storebackend.BackendSQLite.String(), filepath.Join(t.TempDir(), "runtime.db"), []string{
			fmt.Sprintf("  docker_bin: %q", missingDocker),
		}),
		ContractsPath:        filepath.Join("examples", "routing", "root-ingress"),
		DataSource:           t.TempDir(),
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            storebackend.ActiveDefaultBackend().String(),
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		RequireBundleMatch:   false,
		ShutdownGrace:        runtimepkg.DefaultShutdownGrace,
		Verbose:              true,
		NoRequireBundleMatch: true,
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
	if !strings.Contains(serve.outputString(), "workspace                  not required") {
		t.Fatalf("serve output missing no-workspace decision:\n%s", serve.outputString())
	}
	if strings.Contains(serve.outputString(), "workspace image") || strings.Contains(serve.outputString(), "Docker is not reachable") {
		t.Fatalf("no-agent serve output shows Docker dependency despite no-workspace decision:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeAPIAgentDefaultHostBootsWithoutDocker(t *testing.T) {
	missingDocker := filepath.Join(t.TempDir(), "missing-docker")
	hostRoot := filepath.Join(t.TempDir(), "host-workspaces")

	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath: writeStoreBackendRuntimeConfigWithWorkspaceFields(t, storebackend.BackendSQLite.String(), filepath.Join(t.TempDir(), "runtime.db"), []string{
			fmt.Sprintf("  docker_bin: %q", missingDocker),
			fmt.Sprintf("  host_root: %q", hostRoot),
		}),
		ContractsPath:        writeServeRuntimeAgentSlugFixture(t, "api-agent-host-default", "api-worker"),
		DataSource:           t.TempDir(),
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            storebackend.ActiveDefaultBackend().String(),
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		RequireBundleMatch:   false,
		ShutdownGrace:        runtimepkg.DefaultShutdownGrace,
		Verbose:              true,
		NoRequireBundleMatch: true,
		TestLLMRuntime:       runtimellm.NoopRuntime{},
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
	output := serve.outputString()
	if !strings.Contains(output, "workspace                  host · agent work runs on this machine") {
		t.Fatalf("serve output missing host workspace decision for API-backed agent:\n%s", output)
	}
	if strings.Contains(strings.ToLower(output), "docker is not reachable") {
		t.Fatalf("API-backed agent serve output shows Docker dependency despite host decision:\n%s", output)
	}
}

func TestRunServeRuntimeNativeBashDefaultDockerFailsWithoutDocker(t *testing.T) {
	missingDocker := filepath.Join(t.TempDir(), "missing-docker")

	var out lockedBuffer
	code := Run(context.Background(), cliapp.RepoRoot(), cliapp.ServeOptions{
		ConfigPath: writeStoreBackendRuntimeConfigWithWorkspaceFields(t, storebackend.BackendSQLite.String(), filepath.Join(t.TempDir(), "runtime.db"), []string{
			fmt.Sprintf("  docker_bin: %q", missingDocker),
		}),
		ContractsPath:        writeServeRuntimeNativeBashFixture(t),
		DataSource:           t.TempDir(),
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            storebackend.ActiveDefaultBackend().String(),
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		RequireBundleMatch:   false,
		ShutdownGrace:        runtimepkg.DefaultShutdownGrace,
		Verbose:              true,
		NoRequireBundleMatch: true,
		TestLLMRuntime:       runtimellm.NoopRuntime{},
		Output:               &out,
	})
	if code == 0 {
		t.Fatalf("Run code = 0, want Docker prerequisite failure\noutput:\n%s", out.String())
	}
	output := out.String()
	for _, want := range []string{"[5/22] startup_ownership_lease", "workspace                  docker · agent \"native-bash-worker\" runs in a container", "Docker is not reachable", missingDocker + " info"} {
		if !strings.Contains(output, want) {
			t.Fatalf("serve output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "[22/22] ready") {
		t.Fatalf("native bash serve reached readiness despite missing Docker:\n%s", output)
	}
}

func TestRunServeRuntimeFreshEmptyPostgresBootstrapsSchemaBeforeDiskContractsServe(t *testing.T) {
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
	for _, table := range []string{"bundles", "runs", "events", "event_deliveries"} {
		assertPostgresTableExists(t, db, table)
	}
	if !strings.Contains(serve.outputString(), "state_stores=verified") {
		t.Fatalf("serve output missing state store proof:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeFreshEmptyPostgresBootstrapsSchemaBeforeDevAbandon(t *testing.T) {
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:           writeServeRuntimeTestConfig(t),
		ContractsPath:        filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            "postgres",
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		Dev:                  true,
		RequireBundleMatch:   false,
		NoRequireBundleMatch: true,
		AbandonActiveRuns:    true,
		Verbose:              true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
	for _, table := range []string{"bundles", "runs", "run_control_state", "event_receipts"} {
		assertPostgresTableExists(t, db, table)
	}
	if strings.Contains(serve.outputString(), "relation") && strings.Contains(serve.outputString(), "does not exist") {
		t.Fatalf("serve output contains missing-table failure:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeFreshEmptySQLiteBootsWithDevAbandon(t *testing.T) {
	runServeRuntimeFreshEmptySQLiteBootsWithAbandon(t, true)
}

func TestRunServeRuntimeFreshEmptySQLiteBootsWithDirectAbandon(t *testing.T) {
	runServeRuntimeFreshEmptySQLiteBootsWithAbandon(t, false)
}

func runServeRuntimeFreshEmptySQLiteBootsWithAbandon(t *testing.T, dev bool) {
	t.Helper()
	stubServeRuntimeWorkspaceLifecycle(t)
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	requireBundleMatch := !dev
	noRequireBundleMatch := dev
	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:           writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
		ContractsPath:        filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:     defaultPlatformSpecPath,
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		Dev:                  dev,
		RequireBundleMatch:   requireBundleMatch,
		NoRequireBundleMatch: noRequireBundleMatch,
		AbandonActiveRuns:    true,
		Verbose:              true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
	if strings.Contains(serve.outputString(), "postgres store is required") {
		t.Fatalf("serve output contains stale postgres-only abandon failure:\n%s", serve.outputString())
	}
	if _, err := os.Stat(sqlitePath); err != nil {
		t.Fatalf("sqlite dev db not created at %s: %v", sqlitePath, err)
	}
}

func TestRunServeRuntimeArtifactRepoCommitFailsBeforeReadinessForUnusableArtifactRoot(t *testing.T) {
	stubServeRuntimeWorkspaceLifecycle(t)
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	rootFile := filepath.Join(t.TempDir(), "artifact-root")
	if err := os.WriteFile(rootFile, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write unusable artifact root: %v", err)
	}
	t.Setenv("SWARM_ARTIFACT_ROOT", rootFile)

	var out lockedBuffer
	code := Run(context.Background(), cliapp.RepoRoot(), cliapp.ServeOptions{
		ConfigPath:           writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
		ContractsPath:        writeArtifactRepoCommitServeFixture(t),
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            "sqlite",
		StoreModeSet:         true,
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		Dev:                  true,
		RequireBundleMatch:   false,
		NoRequireBundleMatch: true,
		Verbose:              true,
		Output:               &out,
	})
	if code == 0 {
		t.Fatalf("Run code = 0, want startup failure\noutput:\n%s", out.String())
	}
	for _, want := range []string{
		"[5/22] startup_ownership_lease",
		"artifact repo root startup validation failed",
		rootFile,
		"SWARM_ARTIFACT_ROOT=<writable runtime-private absolute path>",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "[22/22]") {
		t.Fatalf("serve reached readiness despite unusable artifact root:\n%s", out.String())
	}
}

func TestRunServeRuntimeArtifactRepoCommitFailsBeforeReadinessForBlockedRepoStorageBase(t *testing.T) {
	stubServeRuntimeWorkspaceLifecycle(t)
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	artifactRoot := t.TempDir()
	reposFile := filepath.Join(artifactRoot, "repos")
	if err := os.WriteFile(reposFile, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write unusable repos base: %v", err)
	}
	t.Setenv("SWARM_ARTIFACT_ROOT", artifactRoot)

	var out lockedBuffer
	code := Run(context.Background(), cliapp.RepoRoot(), cliapp.ServeOptions{
		ConfigPath:           writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
		ContractsPath:        writeArtifactRepoCommitServeFixture(t),
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            "sqlite",
		StoreModeSet:         true,
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		Dev:                  true,
		RequireBundleMatch:   false,
		NoRequireBundleMatch: true,
		Verbose:              true,
		Output:               &out,
	})
	if code == 0 {
		t.Fatalf("Run code = 0, want startup failure\noutput:\n%s", out.String())
	}
	for _, want := range []string{
		"[5/22] startup_ownership_lease",
		"artifact repo root startup validation failed",
		artifactRoot,
		reposFile,
		"SWARM_ARTIFACT_ROOT=<writable runtime-private absolute path>",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "[22/22]") {
		t.Fatalf("serve reached readiness despite blocked artifact repo storage base:\n%s", out.String())
	}
}

func TestRunServeRuntimeNonArtifactBundleDoesNotExerciseUnusableArtifactRoot(t *testing.T) {
	stubServeRuntimeWorkspaceLifecycle(t)
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	rootFile := filepath.Join(t.TempDir(), "artifact-root")
	if err := os.WriteFile(rootFile, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write unusable artifact root: %v", err)
	}
	t.Setenv("SWARM_ARTIFACT_ROOT", rootFile)

	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:           writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
		ContractsPath:        filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:     defaultPlatformSpecPath,
		StoreMode:            "sqlite",
		StoreModeSet:         true,
		APIListenAddr:        "127.0.0.1:0",
		MCPListenAddr:        "127.0.0.1:0",
		SelfCheck:            true,
		Dev:                  true,
		RequireBundleMatch:   false,
		NoRequireBundleMatch: true,
		Verbose:              true,
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
	if strings.Contains(serve.outputString(), "artifact repo root startup validation failed") {
		t.Fatalf("non-artifact bundle hit artifact root admission:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeSQLiteAbandonActiveRunsQuiescesBeforeReadiness(t *testing.T) {
	stubServeRuntimeWorkspaceLifecycle(t)
	unsetStoreSelectorEnv(t)
	sqlitePath := filepath.Join(t.TempDir(), ".swarm", "dev.db")
	runID, eventID := seedServeRuntimeSQLiteAbandonWork(t, sqlitePath)
	ctx := context.Background()
	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:         writeStoreBackendRuntimeConfig(t, storebackend.BackendSQLite.String(), sqlitePath),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		AbandonActiveRuns:  true,
		Verbose:            false,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
	if !strings.Contains(serve.outputString(), "active work cleared for a clean start") {
		t.Fatalf("concise abandon output omitted author outcome:\n%s", serve.outputString())
	}
	for _, forbidden := range []string{"deliveries", "sessions", "timers", "containers", "pipeline receipts"} {
		if strings.Contains(serve.outputString(), forbidden) {
			t.Fatalf("concise abandon output exposed bookkeeping %q:\n%s", forbidden, serve.outputString())
		}
	}
	sqliteStore, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	}()
	ctx = context.Background()
	var runStatus, controlStatus, reason, controlledBy string
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT r.status, rc.control_status, COALESCE(rc.reason, ''), COALESCE(rc.controlled_by, '')
		FROM runs r
		JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = ?
	`, runID).Scan(&runStatus, &controlStatus, &reason, &controlledBy); err != nil {
		t.Fatalf("load sqlite serve-abandoned run/control: %v", err)
	}
	if runStatus != "cancelled" || controlStatus != "stopped" || reason != runtimerunquiescence.ServeAbandonReasonCode || controlledBy != runtimerunquiescence.ServeAbandonControlledBy {
		t.Fatalf("sqlite serve run/control = %s/%s/%s/%s, want cancelled/stopped/%s/%s", runStatus, controlStatus, reason, controlledBy, runtimerunquiescence.ServeAbandonReasonCode, runtimerunquiescence.ServeAbandonControlledBy)
	}
	var deliveryStatus, deliveryReason string
	var activeSession sql.NullString
	if err := sqliteStore.DB.QueryRowContext(ctx, `
		SELECT status, COALESCE(reason_code, ''), active_session_id
		FROM event_deliveries
		WHERE event_id = ?
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'agent-a'
	`, eventID).Scan(&deliveryStatus, &deliveryReason, &activeSession); err != nil {
		t.Fatalf("load sqlite serve-abandoned delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || deliveryReason != runtimerunquiescence.ServeAbandonReasonCode || activeSession.Valid {
		t.Fatalf("sqlite serve delivery = %s/%s active=%v, want dead_letter/%s inactive", deliveryStatus, deliveryReason, activeSession.Valid, runtimerunquiescence.ServeAbandonReasonCode)
	}
	for _, subscriberID := range []string{"agent-a", "pipeline"} {
		var outcome, receiptReason string
		if err := sqliteStore.DB.QueryRowContext(ctx, `
			SELECT outcome, COALESCE(reason_code, '')
			FROM event_receipts
			WHERE event_id = ?
			  AND subscriber_id = ?
		`, eventID, subscriberID).Scan(&outcome, &receiptReason); err != nil {
			t.Fatalf("load sqlite serve receipt %s: %v", subscriberID, err)
		}
		if outcome != "dead_letter" || receiptReason != runtimerunquiescence.ServeAbandonReasonCode {
			t.Fatalf("sqlite serve receipt %s = %s/%s, want dead_letter/%s", subscriberID, outcome, receiptReason, runtimerunquiescence.ServeAbandonReasonCode)
		}
	}
}

func TestRunServeRuntimeBundleHashMissingFailsBeforeReadiness(t *testing.T) {
	_, _, _ = installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	missingHash := "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"
	var out lockedBuffer
	code := Run(context.Background(), cliapp.RepoRoot(), cliapp.ServeOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		BundleHash:         missingHash,
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
		Output:             &out,
	})
	if code == 0 {
		t.Fatalf("Run code = 0, want startup failure\noutput:\n%s", out.String())
	}
	for _, want := range []string{
		"bundle_hash=" + missingHash,
		"BUNDLE_UNAVAILABLE",
		"bundle_hash " + missingHash + " is not present in bundles",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "ready                      ok") || strings.Contains(out.String(), "[22/22]") {
		t.Fatalf("serve reached readiness after missing DB-loaded bundle:\n%s", out.String())
	}
	if strings.Contains(out.String(), "bundle catalog read surface requires bundles columns") || strings.Contains(out.String(), "relation \"bundles\" does not exist") {
		t.Fatalf("serve reported schema/bootstrap failure instead of typed bundle unavailability:\n%s", out.String())
	}
}

func TestValidateServeMultiContextToolGatewayAdmission(t *testing.T) {
	claudeCfg := &config.Config{}
	claudeCfg.LLM.Backend = "claude_cli"
	anthropicCfg := &config.Config{}
	anthropicCfg.LLM.Backend = "anthropic"
	twoContexts := []serveRuntimeBundle{{}, {}}

	tests := []struct {
		name        string
		cfg         *config.Config
		loaded      []serveRuntimeBundle
		wantErr     string
		wantDetails bool
	}{
		{
			name:   "single claude context allowed",
			cfg:    claudeCfg,
			loaded: []serveRuntimeBundle{{}},
		},
		{
			name:   "multi context non claude backend allowed",
			cfg:    anthropicCfg,
			loaded: twoContexts,
		},
		{
			name:        "multi context claude backend fails closed",
			cfg:         claudeCfg,
			loaded:      twoContexts,
			wantErr:     "multi-context swarm serve --bundle-hash with llm.backend=claude_cli is not supported in this configuration",
			wantDetails: true,
		},
		{
			name:    "multi context needs config",
			cfg:     nil,
			loaded:  twoContexts,
			wantErr: "runtime config is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateServeMultiContextToolGatewayAdmission(tt.cfg, tt.loaded)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateServeMultiContextToolGatewayAdmission: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateServeMultiContextToolGatewayAdmission err = %v, want %q", err, tt.wantErr)
			}
			if tt.wantDetails {
				for _, want := range []string{"ToolGatewayBinding", "MCP /mcp and /tools routes", "forkchat sandbox runtime", "single-context"} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("admission error missing %q:\n%s", want, err.Error())
					}
				}
			}
		})
	}
}

func TestRunServeRuntimeDuplicateAgentSlugFailsBeforeReadiness(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	_, _, pg := installServeRuntimePostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	firstRoot := writeServeRuntimeAgentSlugFixtureWithKey(t, "duplicate-agent-slug-a", "alpha", "shared-worker")
	secondRoot := writeServeRuntimeAgentSlugFixtureWithKey(t, "duplicate-agent-slug-b", "beta", "shared-worker")
	firstHash := seedServeRuntimeBundleCatalogRoot(t, ctx, pg, firstRoot)
	secondHash := seedServeRuntimeBundleCatalogRoot(t, ctx, pg, secondRoot)
	if firstHash == secondHash {
		t.Fatalf("test fixtures produced duplicate bundle hash %s", firstHash)
	}

	var out lockedBuffer
	code := Run(ctx, cliapp.RepoRoot(), cliapp.ServeOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		BundleHash:         firstHash,
		BundleHashes:       []string{secondHash},
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
		Verbose:            true,
		Output:             &out,
		TestLLMRuntime:     runtimellm.NoopRuntime{},
	})
	if code == 0 {
		t.Fatalf("Run code = 0, want startup failure\noutput:\n%s", out.String())
	}
	for _, want := range []string{
		"[5/22] startup_ownership_lease",
		`duplicate runtime context agent_id "shared-worker"`,
		firstHash,
		secondHash,
		"bundle_source=persisted",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, out.String())
		}
	}
	for _, notWant := range []string{"[22/22]", "ready                      ok", "manager_event_loop_start", "platform_boot_event_published"} {
		if strings.Contains(out.String(), notWant) {
			t.Fatalf("serve reached %q after duplicate agent slug admission failure:\n%s", notWant, out.String())
		}
	}
}

func TestRunServeRuntimeDistinctAgentSlugsBootPinnedContextsReachReadiness(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	_, _, pg := installServeRuntimePostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	firstRoot := writeServeRuntimeAgentSlugFixture(t, "distinct-agent-slug-a", "alpha-worker")
	secondRoot := writeServeRuntimeAgentSlugFixture(t, "distinct-agent-slug-b", "beta-worker")
	firstHash := seedServeRuntimeBundleCatalogRoot(t, ctx, pg, firstRoot)
	secondHash := seedServeRuntimeBundleCatalogRoot(t, ctx, pg, secondRoot)
	if firstHash == secondHash {
		t.Fatalf("test fixtures produced duplicate bundle hash %s", firstHash)
	}

	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:              writeServeRuntimeTestConfig(t),
		BundleHash:              firstHash,
		BundleHashes:            []string{secondHash},
		PlatformSpecPath:        defaultPlatformSpecPath,
		StoreMode:               "postgres",
		APIListenAddr:           "127.0.0.1:0",
		MCPListenAddr:           "127.0.0.1:0",
		SelfCheck:               true,
		RequireBundleMatch:      false,
		Verbose:                 false,
		TestLLMRuntime:          runtimellm.NoopRuntime{},
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	})
	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
	for _, want := range []string{"swarm serve · 2 persisted bundles", "2 bundles", "ready in "} {
		if !strings.Contains(serve.outputString(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, serve.outputString())
		}
	}
	for _, forbidden := range []string{firstHash, secondHash, "bundle-v1:sha256:", "sha256:", "fingerprint"} {
		if strings.Contains(serve.outputString(), forbidden) {
			t.Fatalf("concise multi-context output exposed identity %q:\n%s", forbidden, serve.outputString())
		}
	}
}

func TestRunServeRuntimeMultiContextClaudeCLIFailsClosedBeforePrimaryGatewayOrForkchat(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	var workspaceConfigured atomic.Bool
	_, _, pg := installServeRuntimePostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		workspaceConfigured.Store(true)
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	firstHash := seedServeRuntimeBundleCatalog(t, ctx, pg, filepath.Join("tests", "tier8-boot-verification", "test-boot-success"))
	secondHash := seedServeRuntimeBundleCatalog(t, ctx, pg, filepath.Join("examples", "routing", "root-ingress"))
	if firstHash == secondHash {
		t.Fatalf("test fixtures produced duplicate bundle hash %s", firstHash)
	}

	var out lockedBuffer
	code := Run(ctx, cliapp.RepoRoot(), cliapp.ServeOptions{
		ConfigPath:         writeDoctorClaudeConfig(t, ""),
		Backend:            "claude_cli",
		BundleHash:         firstHash,
		BundleHashes:       []string{secondHash},
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
		Verbose:            true,
		Output:             &out,
	})
	if code != 3 {
		t.Fatalf("Run code = %d, want 3\noutput:\n%s", code, out.String())
	}
	for _, want := range []string{
		"[4/22] bundle_load",
		"[5/22] startup_ownership_lease",
		"multi-context swarm serve --bundle-hash",
		"llm.backend=claude_cli",
		"ToolGatewayBinding",
		"MCP /mcp and /tools routes",
		"forkchat sandbox runtime",
		"single-context",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, out.String())
		}
	}
	for _, notWant := range []string{"http_listener_bind", "[20/22]", "[22/22]", "ready                      ok", "init forkchat sandbox runtime"} {
		if strings.Contains(out.String(), notWant) {
			t.Fatalf("serve reached %q after multi-context claude_cli admission failure:\n%s", notWant, out.String())
		}
	}
	if workspaceConfigured.Load() {
		t.Fatalf("workspace lifecycle was configured despite fail-closed admission:\n%s", out.String())
	}
}

func TestRunServeRuntimeUnavailableBundleStartupRecoveryFailsPersistedMissingBeforeCleanup(t *testing.T) {
	_, db, _ := installServeRuntimePostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{}
	})
	ctx := context.Background()
	persistedMissingRunID := uuid.NewString()
	legacyRunID := uuid.NewString()
	missingHash := "bundle-v1:sha256:2222222222222222222222222222222222222222222222222222222222222222"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES
			($1::uuid, 'running', $2, 'persisted', now()),
			($3::uuid, 'running', NULL, 'legacy', now())
	`, persistedMissingRunID, missingHash, legacyRunID); err != nil {
		t.Fatalf("seed mixed active runs: %v", err)
	}

	var out lockedBuffer
	code := Run(context.Background(), cliapp.RepoRoot(), cliapp.ServeOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		StoreModeSet:       true,
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
		Verbose:            true,
		Output:             &out,
	})
	if code != serveExitDataIntegrity {
		t.Fatalf("Run code = %d, want %d\noutput:\n%s", code, serveExitDataIntegrity, out.String())
	}
	assertServeRuntimeRunStillActive(t, ctx, &store.PostgresStore{DB: db}, persistedMissingRunID)
	assertServeRuntimeRunStillActive(t, ctx, &store.PostgresStore{DB: db}, legacyRunID)
	if strings.Contains(out.String(), "ready") {
		t.Fatalf("serve reached readiness despite persisted-missing startup recovery failure:\n%s", out.String())
	}
}

func TestRunServeRuntimeUnavailableBundleStartupRecoveryOrphansExpectedUnavailableRuns(t *testing.T) {
	stoppedContainers := []string{}
	_, db, _ := installServeRuntimePostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle {
		return serveRuntimeWorkspaceStub{
			managedContainers: []runtimedestructivereset.ContainerRef{
				{Name: "swarm-legacy-agent", RunID: serveRuntimeLegacyRunIDForTest, Kind: "agent"},
				{Name: "swarm-unrelated-agent", RunID: uuid.NewString(), Kind: "agent"},
			},
			stoppedContainers: &stoppedContainers,
		}
	})
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, flow_instance, role, model, memory_enabled, memory_source, runtime_descriptor)
		VALUES ('agent-a', 'startup-recovery', 'operator', 'default', TRUE, 'authored', '{"type":"default","execution_mode":"live"}'::jsonb)
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	contractsRoot, err := cliapp.NormalizeContractsRoot(cliapp.ResolvePath(cliapp.RepoRoot(), filepath.Join("tests", "tier8-boot-verification", "test-boot-success")))
	if err != nil {
		t.Fatalf("contracts root: %v", err)
	}
	_, bundle, err := cliapp.NewSwarmWorkflowModule(cliapp.RepoRoot(), contractsRoot, cliapp.ResolvePath(cliapp.RepoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("load test workflow bundle: %v", err)
	}
	bootIdentity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatalf("boot bundle identity: %v", err)
	}
	persistedRunID := uuid.NewString()
	persistedHash := "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: serve-test', '{}'::jsonb)
	`, persistedHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_hash, bundle_source, started_at)
		VALUES ($1::uuid, 'running', $2, 'persisted', now())
	`, persistedRunID, persistedHash); err != nil {
		t.Fatalf("seed persisted-present run: %v", err)
	}

	orphanTargets := []struct {
		runID       string
		source      string
		cause       string
		fingerprint string
	}{
		{runID: uuid.NewString(), source: storerunlifecycle.BundleSourceEphemeral, cause: preservationcleanup.BundleEphemeralOrphanedReason},
		{runID: uuid.NewString(), source: storerunlifecycle.BundleSourceDeleted, cause: preservationcleanup.BundleDeletedOrphanedReason},
		{runID: serveRuntimeLegacyRunIDForTest, source: storerunlifecycle.BundleSourceLegacy, cause: preservationcleanup.BundleLegacyOrphanedReason, fingerprint: bootIdentity.Fingerprint},
	}
	for _, target := range orphanTargets {
		seedServeRuntimeUnavailableBundleRunState(t, ctx, db, target.runID, target.source, target.fingerprint)
	}

	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		StoreModeSet:       true,
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            false,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}
	if !strings.Contains(serve.outputString(), "unfinished work could not be resumed and was closed") {
		t.Fatalf("concise recovery output omitted author outcome:\n%s", serve.outputString())
	}
	for _, forbidden := range []string{"deliveries", "sessions", "timers", "containers", "pipeline receipts"} {
		if strings.Contains(serve.outputString(), forbidden) {
			t.Fatalf("concise recovery output exposed bookkeeping %q:\n%s", forbidden, serve.outputString())
		}
	}
	assertServeRuntimeRunStillActive(t, context.Background(), &store.PostgresStore{DB: db}, persistedRunID)
	for _, target := range orphanTargets {
		assertServeRuntimeUnavailableBundleRunOrphaned(t, context.Background(), &store.PostgresStore{DB: db}, target.runID, target.cause)
	}
	if len(stoppedContainers) != 1 || stoppedContainers[0] != "swarm-legacy-agent" {
		t.Fatalf("stopped containers = %#v, want only legacy run container", stoppedContainers)
	}
}

const serveRuntimeLegacyRunIDForTest = "11111111-2222-3333-4444-555555555555"

func installServeRuntimePostgresTestStores(t *testing.T, workspaceFactory func() cliapp.ServeWorkspaceLifecycle) (string, *sql.DB, *store.PostgresStore) {
	t.Helper()
	return installServeRuntimePostgresTestStoresWithWorkspaceFactory(t, func(cliapp.WorkspaceMountSources) cliapp.ServeWorkspaceLifecycle {
		return workspaceFactory()
	})
}

func installServeRuntimeEmptyPostgresTestStores(t *testing.T, workspaceFactory func() cliapp.ServeWorkspaceLifecycle) (string, *sql.DB, *store.PostgresStore) {
	t.Helper()
	return installServeRuntimePostgresTestStoresForDatabase(t, func(cliapp.WorkspaceMountSources) cliapp.ServeWorkspaceLifecycle {
		return workspaceFactory()
	}, false)
}

func seedServeRuntimeSQLiteAbandonWork(t *testing.T, sqlitePath string) (string, string) {
	t.Helper()
	spec, err := loadServePlatformSpecDocument(filepath.Join(cliapp.RepoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	plans, err := store.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatalf("GeneratePlatformTableDDLs: %v", err)
	}
	sqliteStore, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("NewSQLiteRuntimeStore: %v", err)
	}
	bootstrapSQLiteSchemaForTest(t, context.Background(), sqliteStore, plans)
	ctx := context.Background()
	now := time.Date(2026, 5, 18, 4, 30, 0, 0, time.UTC)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	activeSessionID := uuid.NewString()
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_fingerprint, started_at)
		VALUES (?, 'running', ?, ?)
	`, runID, "sha256:2222222222222222222222222222222222222222222222222222222222222222", now.Add(-time.Hour)); err != nil {
		_ = sqliteStore.Close()
		t.Fatalf("seed sqlite active run: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES ('live',
			?, ?, 'serve.abandon.test', 'global', '{}', 'test', 'agent', ?
		)
	`, eventID, runID, now); err != nil {
		_ = sqliteStore.Close()
		t.Fatalf("seed sqlite active delivery event: %v", err)
	}
	if _, err := sqliteStore.DB.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, reason_code, created_at
		) VALUES (
			?, ?, ?, 'agent', 'agent-a', 'in_progress', ?, 'agent_processing', ?
		)
	`, uuid.NewString(), runID, eventID, activeSessionID, now); err != nil {
		_ = sqliteStore.Close()
		t.Fatalf("seed sqlite active delivery: %v", err)
	}
	if err := sqliteStore.Close(); err != nil {
		t.Fatalf("close seeded sqlite store: %v", err)
	}
	return runID, eventID
}

func stubServeRuntimeWorkspaceLifecycle(t *testing.T) {
	t.Helper()
	oldWorkspaceLifecycle := cliapp.ConfiguredWorkspaceLifecycleForServe
	cliapp.ConfiguredWorkspaceLifecycleForServe = func(*sql.DB, *config.Config, string, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
		return serveRuntimeWorkspaceStub{}, nil
	}
	t.Cleanup(func() {
		cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspaceLifecycle
	})
}

func installServeRuntimePostgresTestStoresWithWorkspaceFactory(t *testing.T, workspaceFactory func(cliapp.WorkspaceMountSources) cliapp.ServeWorkspaceLifecycle) (string, *sql.DB, *store.PostgresStore) {
	t.Helper()
	return installServeRuntimePostgresTestStoresForDatabase(t, workspaceFactory, true)
}

func installServeRuntimePostgresTestStoresForDatabase(t *testing.T, workspaceFactory func(cliapp.WorkspaceMountSources) cliapp.ServeWorkspaceLifecycle, useTemplate bool) (string, *sql.DB, *store.PostgresStore) {
	t.Helper()
	oldBuildStores := buildStoresForServe
	oldWorkspaceLifecycle := cliapp.ConfiguredWorkspaceLifecycleForServe
	var dsn string
	var db *sql.DB
	var cleanup func()
	if useTemplate {
		dsn, db, cleanup = testutil.StartPostgres(t)
	} else {
		dsn, db, cleanup = testutil.StartEmptyPostgres(t)
	}
	t.Cleanup(cleanup)
	runtimePG, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = runtimePG.DB.Close() })
	buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		if _, err := runtimePG.BindSchemaCapabilities(ctx); err != nil {
			return storeBundle{}, err
		}
		return selectedPostgresStoreBundle(runtimePG, cfg), nil
	}
	cliapp.ConfiguredWorkspaceLifecycleForServe = func(_ *sql.DB, _ *config.Config, _ string, _ semanticview.Source, mountSources cliapp.WorkspaceMountSources, _ cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
		return workspaceFactory(mountSources), nil
	}
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
		cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspaceLifecycle
	})
	return dsn, db, runtimePG
}

func assertPostgresTableExists(t *testing.T, db *sql.DB, tableName string) {
	t.Helper()
	var found sql.NullString
	if err := db.QueryRowContext(context.Background(), `SELECT to_regclass($1)::text`, "public."+tableName).Scan(&found); err != nil {
		t.Fatalf("check table %s exists: %v", tableName, err)
	}
	if !found.Valid || strings.TrimSpace(found.String) == "" {
		t.Fatalf("table %s was not bootstrapped", tableName)
	}
}

func seedServeRuntimeUnavailableBundleRunState(t *testing.T, ctx context.Context, db *sql.DB, runID, source, fingerprint string) {
	t.Helper()
	sessionID := uuid.NewString()
	timerID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_source, bundle_fingerprint, started_at)
		VALUES ($1::uuid, 'running', $2, NULLIF($3, ''), now())
	`, runID, source, fingerprint); err != nil {
		t.Fatalf("seed unavailable bundle run %s: %v", source, err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES ('live',
			$1::uuid, $2::uuid, $3, 'global', '{}'::jsonb, 'test', 'agent', now()
		)
	`, eventID, runID, "startup."+source+".event"); err != nil {
		t.Fatalf("seed event %s: %v", source, err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, reason_code, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-a', 'in_progress', $3::uuid, 'agent_processing', now()
		)
	`, runID, eventID, sessionID); err != nil {
		t.Fatalf("seed delivery %s: %v", source, err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, status)
		VALUES ($1::uuid, $2::uuid, 'agent-a', 'startup-recovery', TRUE, 'authored', 'active')
	`, sessionID, runID); err != nil {
		t.Fatalf("seed session %s: %v", source, err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO timers (timer_id, timer_name, run_id, fire_event, fire_at, status)
		VALUES ($1::uuid, $2, $3::uuid, 'timer.fired', now() + interval '1 hour', 'active')
	`, timerID, "timer-"+source, runID); err != nil {
		t.Fatalf("seed timer %s: %v", source, err)
	}
}

func assertServeRuntimeRunStillActive(t *testing.T, ctx context.Context, pg *store.PostgresStore, runID string) {
	t.Helper()
	var status string
	if err := pg.DB.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, runID).Scan(&status); err != nil {
		t.Fatalf("load run %s: %v", runID, err)
	}
	if status != "running" {
		t.Fatalf("run %s status = %s, want running", runID, status)
	}
	var controlRows int
	if err := pg.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_control_state WHERE run_id = $1::uuid`, runID).Scan(&controlRows); err != nil {
		t.Fatalf("count control rows %s: %v", runID, err)
	}
	if controlRows != 0 {
		t.Fatalf("run %s control rows = %d, want none", runID, controlRows)
	}
}

func assertServeRuntimeUnavailableBundleRunOrphaned(t *testing.T, ctx context.Context, pg *store.PostgresStore, runID, reason string) {
	t.Helper()
	var runStatus, controlStatus, controlReason string
	var failure []byte
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT r.status, r.failure, rc.control_status, COALESCE(rc.reason, '')
		FROM runs r
		JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = $1::uuid
	`, runID).Scan(&runStatus, &failure, &controlStatus, &controlReason); err != nil {
		t.Fatalf("load orphaned run %s: %v", runID, err)
	}
	if runStatus != "cancelled" || len(failure) != 0 || controlStatus != "stopped" || controlReason != reason {
		t.Fatalf("orphaned run %s = %s/failure:%s/%s/%s, want cancelled/no-failure/stopped/%s", runID, runStatus, failure, controlStatus, controlReason, reason)
	}
	var deadLetters int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND status = 'dead_letter'
		  AND reason_code = $2
		  AND active_session_id IS NULL
	`, runID, reason).Scan(&deadLetters); err != nil {
		t.Fatalf("count orphaned deliveries %s: %v", runID, err)
	}
	if deadLetters != 1 {
		t.Fatalf("orphaned run %s dead-letter deliveries = %d, want 1", runID, deadLetters)
	}
	var receipts int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts er
		JOIN events e ON e.event_id = er.event_id
		WHERE e.run_id = $1::uuid
		  AND er.outcome = 'dead_letter'
		  AND er.reason_code = $2
		  AND er.subscriber_id IN ('agent-a', 'pipeline')
	`, runID, reason).Scan(&receipts); err != nil {
		t.Fatalf("count orphaned receipts %s: %v", runID, err)
	}
	if receipts != 2 {
		t.Fatalf("orphaned run %s receipts = %d, want agent and pipeline", runID, receipts)
	}
	var sessions int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_sessions
		WHERE run_id = $1::uuid
		  AND status = 'terminated'
		  AND termination_reason = 'orphaned'
		  AND termination_detail = $2
	`, runID, reason).Scan(&sessions); err != nil {
		t.Fatalf("count orphaned sessions %s: %v", runID, err)
	}
	if sessions != 1 {
		t.Fatalf("orphaned run %s sessions = %d, want 1", runID, sessions)
	}
	var timers int
	if err := pg.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM timers
		WHERE run_id = $1::uuid
		  AND status = 'cancelled'
	`, runID).Scan(&timers); err != nil {
		t.Fatalf("count orphaned timers %s: %v", runID, err)
	}
	if timers != 1 {
		t.Fatalf("orphaned run %s timers = %d, want 1", runID, timers)
	}
}

func TestPrepareServeBundleSourcePersistsCatalogForContractsServe(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier12-runtime-tools/test-flow-data-access")
	identity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatalf("BootBundleIdentity: %v", err)
	}

	fact, err := prepareServeBundleSource(ctx, selectedPostgresStoreBundle(pg, &config.Config{}), bundle, identity.Fingerprint, false)
	if err != nil {
		t.Fatalf("prepareServeBundleSource: %v", err)
	}
	if fact.BundleSource != storerunlifecycle.BundleSourcePersisted || fact.BundleHash == "" || fact.BundleFingerprint != identity.Fingerprint {
		t.Fatalf("source fact = %#v", fact)
	}
	detail, err := pg.LoadBundleCatalog(ctx, fact.BundleHash)
	if err != nil {
		t.Fatalf("LoadBundleCatalog(%s): %v", fact.BundleHash, err)
	}
	if detail.BundleHash != fact.BundleHash || detail.AgentCount == 0 || detail.ContentYAML == "" {
		t.Fatalf("bundle catalog detail = %#v", detail)
	}
}

func TestPrepareServeBundleSourceDevStampsEphemeralWithoutCatalogRow(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	bundle := loadWorkflowValidationFixtureBundle(t, "tests/tier12-runtime-tools/test-flow-data-access")
	identity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatalf("BootBundleIdentity: %v", err)
	}

	fact, err := prepareServeBundleSource(ctx, selectedPostgresStoreBundle(pg, &config.Config{}), bundle, identity.Fingerprint, true)
	if err != nil {
		t.Fatalf("prepareServeBundleSource(dev): %v", err)
	}
	if fact.BundleSource != storerunlifecycle.BundleSourceEphemeral || fact.BundleHash == "" || fact.BundleFingerprint != identity.Fingerprint {
		t.Fatalf("source fact = %#v", fact)
	}
	if _, err := pg.LoadBundleCatalog(ctx, fact.BundleHash); err != store.ErrBundleNotFound {
		t.Fatalf("LoadBundleCatalog(dev hash) error = %v, want ErrBundleNotFound", err)
	}
}

func TestPrepareServeBundleSourceSQLiteStampsEphemeralWithoutPostgresCatalog(t *testing.T) {
	ctx := context.Background()
	bundle := loadWorkflowValidationFixtureBundle(t, "examples/routing/root-ingress")
	identity, err := runtimecontracts.BootBundleIdentity(bundle)
	if err != nil {
		t.Fatalf("BootBundleIdentity: %v", err)
	}

	fact, err := prepareServeBundleSource(ctx, storeBundle{}, bundle, identity.Fingerprint, false)
	if err != nil {
		t.Fatalf("prepareServeBundleSource(sqlite local): %v", err)
	}
	if fact.BundleSource != storerunlifecycle.BundleSourceEphemeral || fact.BundleHash == "" || fact.BundleFingerprint != identity.Fingerprint {
		t.Fatalf("source fact = %#v", fact)
	}
}

func TestServeListenerServersPartitionAPIAndMCPRoutes(t *testing.T) {
	var ready atomic.Bool
	var apiHit atomic.Bool
	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiHit.Store(true)
		if r.URL.Path != "/v1/rpc" && r.URL.Path != "/v1/ws" {
			t.Errorf("api path = %q, want /v1/rpc or /v1/ws", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"test","result":{"ok":true}}`))
	})
	var inboundHit atomic.Bool
	inboundHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inboundHit.Store(true)
		if r.URL.Path != "/webhooks/customer-a/github" {
			t.Errorf("inbound path = %q, want /webhooks/customer-a/github", r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted"}`))
	})
	toolGateway := runtimemcp.NewGateway(nil, "", runtimemcp.GatewayHooks{})
	apiHandlerMux := newAPIServer(&ready, apiHandler, inboundHandler).Handler
	mcpHandlerMux := newMCPServer(toolGateway).Handler

	rec := httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "ok" {
		t.Fatalf("/healthz status/body = %d/%q, want 200 ok", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before ready status = %d, want 503", rec.Code)
	}
	ready.Store(true)
	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "ready" {
		t.Fatalf("/readyz ready status/body = %d/%q, want 200 ready", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"test","method":"rpc.unsubscribe","params":{"subscription_id":"sub"}}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("/v1/rpc status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if !apiHit.Load() {
		t.Fatal("/v1/rpc did not route to v1 API handler")
	}

	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/ws", strings.NewReader(`{"jsonrpc":"2.0","id":"test","method":"rpc.unsubscribe","params":{"subscription_id":"sub"}}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("/v1/ws status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(`{"zen":"ok"}`)))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("/webhooks/ status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !inboundHit.Load() {
		t.Fatal("/webhooks/ did not route to inbound handler")
	}

	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/mcp", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("api listener /mcp status = %d, want 404", rec.Code)
	}
	rec = httptest.NewRecorder()
	apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/tools/query_entities", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("api listener /tools/ status = %d, want 404", rec.Code)
	}

	rec = httptest.NewRecorder()
	mcpHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/mcp", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "ok" {
		t.Fatalf("/mcp status/body = %d/%q, want 200 ok", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	mcpHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/tools/query_entities", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("/tools/ route status = %d, want mounted gateway 405", rec.Code)
	}

	for _, path := range []string{"/healthz", "/readyz", "/v1/rpc", "/v1/ws", "/webhooks/customer-a/github"} {
		rec = httptest.NewRecorder()
		mcpHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("mcp listener %s status = %d, want 404", path, rec.Code)
		}
	}

	for _, path := range []string{"/api", "/api/healthz", "/api/rpc", "/rpc", "/api/ws", "/ws"} {
		rec = httptest.NewRecorder()
		apiHandlerMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404", path, rec.Code)
		}
	}
}

func seedRunForkSelectedExecutionSourceEvent(t *testing.T, db *sql.DB, runID, entityID, eventID, eventName, subscriberID, currentState, entityName, writerID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ('live', $1::uuid, $2::uuid, $3, $4::uuid, 'flow-a/1', 'entity', $5::jsonb, 'test', 'platform', $6)
	`, runID, eventID, eventName, entityID, fmt.Sprintf(`{"entity_id":%q}`, entityID), at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', $3, 'pending', 'source_pending_node_delivery', $4)
	`, runID, eventID, subscriberID, at); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, to_jsonb($5::text), $3::uuid, 'platform', $6, 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, to_jsonb($7::text), $3::uuid, 'platform', $6, 'seed', $4)
	`, runID, entityID, eventID, at, currentState, writerID, entityName); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', $4,
			$5, '{}'::jsonb, jsonb_build_object('name', $4::text), '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, runID, entityID, at, entityName, currentState); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
	captureRunForkCLIRevision(t, db, runID, runforkrevision.AllFamilies()...)
}

func captureRunForkCLIRevision(t *testing.T, db *sql.DB, runID string, families ...runforkrevision.Family) int64 {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin run-fork revision fixture: %v", err)
	}
	revision, err := runforkrevision.Capture(context.Background(), tx, runID, families...)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("capture run-fork revision fixture: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit run-fork revision fixture: %v", err)
	}
	return revision
}

type verifyAccumulatorSafetyCommandFixtureOptions struct {
	eventSource string
}

func seedServeRuntimeBundleCatalog(t *testing.T, ctx context.Context, pg *store.PostgresStore, relativeRoot string) string {
	t.Helper()
	return seedServeRuntimeBundleCatalogFromBundle(t, ctx, pg, relativeRoot, loadWorkflowValidationFixtureBundle(t, relativeRoot))
}

func seedServeRuntimeBundleCatalogRoot(t *testing.T, ctx context.Context, pg *store.PostgresStore, root string) string {
	t.Helper()
	return seedServeRuntimeBundleCatalogFromBundle(t, ctx, pg, root, loadWorkflowValidationBundleAt(t, root))
}

func seedServeRuntimeBundleCatalogFromBundle(t *testing.T, ctx context.Context, pg *store.PostgresStore, label string, bundle *runtimecontracts.WorkflowContractBundle) string {
	t.Helper()
	if pg == nil {
		t.Fatal("postgres store is required")
	}
	if _, err := pg.BindSchemaCapabilities(ctx); err != nil {
		t.Fatalf("BindSchemaCapabilities: %v", err)
	}
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection(%s): %v", label, err)
	}
	if _, err := pg.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		ParsedJSON:  projection.ParsedJSON,
		DataBlob:    projection.DataBlob,
		Metadata:    projection.Metadata,
	}); err != nil {
		t.Fatalf("UpsertBundleCatalog(%s): %v", label, err)
	}
	return projection.BundleHash
}

func writeArtifactRepoCommitServeFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyArtifactRepoCommitAdmission(t)
}

func TestServeBootRegistryDetail_UsesRuntimeToolInventoryCount(t *testing.T) {
	source := semanticview.Wrap(testWorkflowValidationBundle())
	wantTools := len(runtimetools.RuntimeAvailableToolNamesForSource(source))
	if wantTools == 0 {
		t.Fatal("runtime tool inventory unexpectedly empty")
	}

	out := serveBootBundleLoadDetail("sha256:test", source)
	if !strings.Contains(out, "sha256:test") {
		t.Fatalf("boot progress detail missing fingerprint:\n%s", out)
	}
	if !strings.Contains(out, fmt.Sprintf("tools=%d", wantTools)) {
		t.Fatalf("log output missing runtime tool count %d:\n%s", wantTools, out)
	}
	if strings.Contains(out, "tools=0") {
		t.Fatalf("log output still reports zero tools:\n%s", out)
	}
}

func TestInitializeServeSchemaStateStoresNeverExposeTableInventory(t *testing.T) {
	ctx := context.Background()
	bundle := loadStoreBackendSelectionWorkflowBundle(t)
	stores := storeBundle{SchemaBootstrapper: recordingSchemaBootstrapper{}}

	defaultSummary, err := initializeStateStores(ctx, stores, bundle)
	if err != nil {
		t.Fatalf("initializeStateStores: %v", err)
	}
	if strings.Contains(defaultSummary, "(") {
		t.Fatalf("loaded-bundle state store summary leaked table detail:\n%s", defaultSummary)
	}

	loadedDefaultSummaries, err := initializeLoadedServeRuntimeStateStores(ctx, stores, []serveRuntimeBundle{{bundle: bundle}, {bundle: bundle}})
	if err != nil {
		t.Fatalf("initializeLoadedServeRuntimeStateStores: %v", err)
	}
	for _, summary := range loadedDefaultSummaries {
		if strings.Contains(summary, "(") {
			t.Fatalf("loaded runtime state store summary leaked table detail:\n%v", loadedDefaultSummaries)
		}
	}

	defaultPlatformSummary, err := initializeServePlatformStateStores(ctx, stores, filepath.Join(cliapp.RepoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("initializeServePlatformStateStores: %v", err)
	}
	if strings.Contains(defaultPlatformSummary, "(") {
		t.Fatalf("pre-catalog platform state store summary leaked table detail:\n%s", defaultPlatformSummary)
	}
}

func TestInitializeStateStoresDoesNotPlanGeneratedEntityTables(t *testing.T) {
	ctx := context.Background()
	bundle := workflowBundleWithGeneratedEntitySchemaForStateStoreTest(t)
	recorder := &capturingSchemaBootstrapper{}

	summary, err := initializeStateStores(ctx, storeBundle{SchemaBootstrapper: recorder}, bundle)
	if err != nil {
		t.Fatalf("initializeStateStores: %v", err)
	}
	if !schemaPlanContainsTable(recorder.plans, "entity_state") {
		t.Fatal("normal boot schema plan missing canonical entity_state table")
	}
	if schemaPlanContainsTable(recorder.plans, "products") {
		t.Fatalf("normal boot schema plan included generated typed entity table products: %s", summary)
	}
}

func TestInitializeStateStoresSQLiteDoesNotCreateGeneratedEntityTables(t *testing.T) {
	ctx := context.Background()
	bundle := workflowBundleWithGeneratedEntitySchemaForStateStoreTest(t)
	sqliteStore, err := store.NewSQLiteRuntimeStore(filepath.Join(t.TempDir(), "dev.db"))
	if err != nil {
		t.Fatalf("NewSQLiteRuntimeStore: %v", err)
	}
	t.Cleanup(func() {
		if err := sqliteStore.Close(); err != nil {
			t.Fatalf("close sqlite store: %v", err)
		}
	})

	if _, err := initializeStateStores(ctx, storeBundle{SchemaBootstrapper: sqliteStore}, bundle); err != nil {
		t.Fatalf("initializeStateStores(sqlite): %v", err)
	}
	if !sqliteMainTestTableExists(t, sqliteStore.DB, "entity_state") {
		t.Fatal("sqlite normal boot missing canonical entity_state table")
	}
	if sqliteMainTestTableExists(t, sqliteStore.DB, "products") {
		t.Fatal("sqlite normal boot created misleading generated typed entity table products")
	}
}

func TestInitializeStateStoresPostgresDoesNotCreateGeneratedEntityTables(t *testing.T) {
	ctx := context.Background()
	bundle := workflowBundleWithGeneratedEntitySchemaForStateStoreTest(t)
	dsn, db, cleanup := testutil.StartEmptyPostgres(t)
	t.Cleanup(cleanup)
	pg, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() {
		if err := pg.DB.Close(); err != nil {
			t.Fatalf("close postgres store: %v", err)
		}
	})

	if _, err := initializeStateStores(ctx, selectedPostgresStoreBundle(pg, &config.Config{}), bundle); err != nil {
		t.Fatalf("initializeStateStores(postgres): %v", err)
	}
	if !postgresMainTestTableExists(t, db, "entity_state") {
		t.Fatal("postgres normal boot missing canonical entity_state table")
	}
	if postgresMainTestTableExists(t, db, "products") {
		t.Fatal("postgres normal boot created misleading generated typed entity table products")
	}
}

func TestServeRuntimeStateStoreSummaryDedupesSchemaSummaries(t *testing.T) {
	got := serveRuntimeStateStoreSummary([]serveRuntimeBundleContext{
		{stateStoreSummary: "verified 2 generated tables"},
		{stateStoreSummary: "verified 2 generated tables"},
		{stateStoreSummary: " "},
	})

	if strings.Count(got, "verified 2 generated tables") != 1 {
		t.Fatalf("summary = %q, want one concise summary after de-dupe", got)
	}
}

type recordingSchemaBootstrapper struct{}

type capturingSchemaBootstrapper struct {
	plans []store.SchemaTableDDL
}

func workflowBundleWithGeneratedEntitySchemaForStateStoreTest(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	spec, err := loadServePlatformSpecDocument(filepath.Join(cliapp.RepoRoot(), defaultPlatformSpecPath))
	if err != nil {
		t.Fatalf("load platform spec: %v", err)
	}
	return &runtimecontracts.WorkflowContractBundle{
		Platform: spec,
		Semantics: runtimecontracts.WorkflowSemanticView{
			EntitySchema: runtimecontracts.EntitySchema{
				Groups: []runtimecontracts.EntitySchemaGroup{{
					Name: "products",
					Fields: []runtimecontracts.EntitySchemaField{
						{Name: "slug", Type: "text", Indexed: true},
						{Name: "score", Type: "numeric(12,2)", Nullable: true},
					},
				}},
			},
		},
	}
}

func schemaPlanContainsTable(plans []store.SchemaTableDDL, tableName string) bool {
	for _, plan := range plans {
		if strings.EqualFold(strings.TrimSpace(plan.TableName), tableName) {
			return true
		}
	}
	return false
}

func sqliteMainTestTableExists(t *testing.T, db *sql.DB, tableName string) bool {
	t.Helper()
	var name string
	err := db.QueryRowContext(context.Background(), `
		SELECT name
		FROM sqlite_master
		WHERE type='table' AND name=?
	`, tableName).Scan(&name)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		t.Fatalf("check sqlite table %s: %v", tableName, err)
	}
	return strings.TrimSpace(name) != ""
}

func postgresMainTestTableExists(t *testing.T, db *sql.DB, tableName string) bool {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(context.Background(), `SELECT to_regclass($1)::text IS NOT NULL`, "public."+tableName).Scan(&exists); err != nil {
		t.Fatalf("check postgres table %s: %v", tableName, err)
	}
	return exists
}

func TestWaitForServeHealthEndpointsProvesOnlyPreCommitLiveness(t *testing.T) {
	var readyCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
		case "/readyz":
			readyCalls.Add(1)
			http.Error(w, "not committed", http.StatusServiceUnavailable)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	if err := waitForServeHealthEndpoints(context.Background(), server.Listener.Addr()); err != nil {
		t.Fatalf("waitForServeHealthEndpoints: %v", err)
	}
	if readyCalls.Load() != 0 {
		t.Fatalf("pre-commit liveness probe called /readyz %d times", readyCalls.Load())
	}
}

func TestRunServeRuntimeVerboseEmitsPlatformSpecBootSequence(t *testing.T) {
	steps := loadServeBootProgressSequenceFromSpec(t)
	if got, want := len(steps), runtimepkg.BootProgressTotalSteps; got != want {
		t.Fatalf("serve boot progress spec step count = %d, want %d", got, want)
	}

	sqlitePath := filepath.Join(t.TempDir(), "verbose-sequence.sqlite")
	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:         writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, nil),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "sqlite",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		Verbose:            true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}

	rows := parseServeBootProgressRows(t, serve.outputString())
	if got, want := len(rows), len(steps); got != want {
		t.Fatalf("serve boot progress rows = %d, want %d\noutput:\n%s", got, want, serve.outputString())
	}
	for i, want := range steps {
		got := rows[i]
		if got.Step != want.Step || got.Total != runtimepkg.BootProgressTotalSteps || got.Name != want.Name {
			t.Fatalf("row %d = step=%d total=%d name=%q, want step=%d total=%d name=%q\noutput:\n%s", i, got.Step, got.Total, got.Name, want.Step, runtimepkg.BootProgressTotalSteps, want.Name, serve.outputString())
		}
	}
	if strings.Contains(serve.outputString(), "health_endpoints_respond       ok  (/healthz /readyz /v1/rpc /v1/ws)") {
		t.Fatalf("serve verbose output still claims unproven v1 endpoint response:\n%s", serve.outputString())
	}
	for _, want := range []string{"http_listener_bind", "api_listener=", "api_routes=" + serveAPIRoutes, "mcp_listener=", "mcp_routes=" + serveMCPRoutes, "health_endpoints_respond", serveReadinessRoutes} {
		if !strings.Contains(serve.outputString(), want) {
			t.Fatalf("serve verbose output missing %q:\n%s", want, serve.outputString())
		}
	}
	if strings.Contains(serve.outputString(), "health=127.") {
		t.Fatalf("serve verbose output still labels the unified listener as health-only:\n%s", serve.outputString())
	}
}

func TestRunServeRuntimeListenerBindFailuresExitBeforeReadiness(t *testing.T) {
	for _, tt := range []struct {
		name      string
		occupyAPI bool
	}{
		{name: "api listener", occupyAPI: true},
		{name: "mcp listener", occupyAPI: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
			t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")

			occupied, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("occupy listener: %v", err)
			}
			defer occupied.Close()
			apiAddr := "127.0.0.1:0"
			mcpAddr := "127.0.0.1:0"
			if tt.occupyAPI {
				apiAddr = occupied.Addr().String()
			} else {
				mcpAddr = occupied.Addr().String()
			}
			verbose := !tt.occupyAPI

			var out lockedBuffer
			code := Run(context.Background(), cliapp.RepoRoot(), cliapp.ServeOptions{
				ConfigPath:         writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", filepath.Join(t.TempDir(), "listener-bind.sqlite"), nil),
				ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
				PlatformSpecPath:   defaultPlatformSpecPath,
				StoreMode:          "sqlite",
				APIListenAddr:      apiAddr,
				MCPListenAddr:      mcpAddr,
				SelfCheck:          true,
				RequireBundleMatch: true,
				Verbose:            verbose,
				Output:             &out,
			})
			if code != 3 {
				t.Fatalf("Run code = %d, want 3\noutput:\n%s", code, out.String())
			}
			if verbose {
				if !strings.Contains(out.String(), "http_listener_bind") || !strings.Contains(out.String(), "FAILED") {
					t.Fatalf("verbose serve output missing bind failure proof:\n%s", out.String())
				}
			} else if !strings.Contains(out.String(), "serve failed · http listener bind") {
				t.Fatalf("concise serve output missing bind failure proof:\n%s", out.String())
			}
			if strings.Contains(out.String(), "ready                      ok") || strings.Contains(out.String(), "\n  ready in ") {
				t.Fatalf("serve reported readiness after listener bind failure:\n%s", out.String())
			}
		})
	}
}

func TestCreateServeToolGatewayBindingAlignsToMCPListenerWithoutMutatingURLEnv(t *testing.T) {
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mcp: %v", err)
	}
	defer listener.Close()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener addr: %v", err)
	}

	binding, err := createServeToolGatewayBinding(listener.Addr())
	if err != nil {
		t.Fatalf("create gateway binding: %v", err)
	}
	if got, want := binding.HostEndpoint, "http://127.0.0.1:"+port; got != want {
		t.Fatalf("HostEndpoint = %q, want %q", got, want)
	}
	if got, want := binding.WorkspaceEndpoint, "http://host.docker.internal:"+port; got != want {
		t.Fatalf("WorkspaceEndpoint = %q, want %q", got, want)
	}
	if strings.TrimSpace(binding.Token) == "" {
		t.Fatal("binding token was not generated")
	}
	if got, want := len(binding.Token), base64.RawURLEncoding.EncodedLen(toolgateway.AuthTokenBytes); got != want {
		t.Fatalf("binding token length = %d, want %d", got, want)
	}
	if got := os.Getenv("SWARM_TOOL_GATEWAY_URL"); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_URL = %q, want empty", got)
	}
	if got := os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL"); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_CONTAINER_URL = %q, want empty", got)
	}
	if got := os.Getenv("SWARM_TOOL_GATEWAY_TOKEN"); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_TOKEN = %q, want empty", got)
	}
}

func TestCreateServeToolGatewayBindingRejectsRetiredGatewayTokenEnv(t *testing.T) {
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "operator-token")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mcp: %v", err)
	}
	defer listener.Close()

	_, err = createServeToolGatewayBinding(listener.Addr())
	if err == nil || !strings.Contains(err.Error(), "SWARM_TOOL_GATEWAY_TOKEN is retired") || !strings.Contains(err.Error(), "ToolGatewayBinding") {
		t.Fatalf("create gateway binding error = %v, want retired token env rejection", err)
	}
}

func TestRunServeRuntimeDevClaudeCLIStaleGatewayEnvUsesTypedBinding(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	stubServeWorkspaceLifecycleForTest(t)
	mcpPort := freeDoctorTCPPort(t)
	mcpAddr := "127.0.0.1:" + mcpPort
	stalePort := staleGatewayTestPort(mcpPort)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:"+stalePort)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:"+stalePort)
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	bindingCh := make(chan toolgateway.Binding, 1)
	opts := cliapp.ServeOptions{
		ConfigPath:         writeDoctorClaudeConfig(t, ""),
		Backend:            "claude_cli",
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		DataSource:         t.TempDir(),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "sqlite",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      mcpAddr,
		SelfCheck:          true,
		RequireBundleMatch: false,
		Verbose:            true,
		Dev:                true,
		TestRuntimeReadyHook: func(rt *runtimepkg.Runtime) {
			bindingCh <- rt.Options.ToolGatewayBinding
		},
	}
	assertServePreflightStaleGatewayWarning(t, opts, "serve_dev")

	process := startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	binding := receiveToolGatewayBinding(t, bindingCh, process.outputString())
	assertToolGatewayBindingUsesMCPPort(t, binding, mcpPort, stalePort)
	if code := process.stop(); code != 0 {
		t.Fatalf("serve exited with code %d, want 0\noutput:\n%s", code, process.outputString())
	}
}

func TestStartLocalRunServeClaudeCLIStaleGatewayEnvUsesTypedBinding(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	stubServeWorkspaceLifecycleForTest(t)
	apiPortText := freeDoctorTCPPort(t)
	mcpPort := freeDoctorTCPPort(t)
	mcpAddr := "127.0.0.1:" + mcpPort
	stalePort := staleGatewayTestPort(mcpPort)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	bindingCh := make(chan toolgateway.Binding, 1)
	serveStarted := make(chan cliapp.ServeOptions, 1)
	runServe := func(ctx context.Context, repo string, serveOpts cliapp.ServeOptions) int {
		t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:"+stalePort)
		t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:"+stalePort)
		if !serveOpts.LocalRun {
			t.Errorf("local CLI run produced LocalRun = false, want shared run_local preflight consumer")
		}
		if serveOpts.APIListenAddr != "127.0.0.1:"+apiPortText {
			t.Errorf("local CLI run APIListenAddr = %q, want 127.0.0.1:%s", serveOpts.APIListenAddr, apiPortText)
		}
		serveOpts.MCPListenAddr = mcpAddr
		serveOpts.Verbose = true
		serveOpts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) {
			bindingCh <- rt.Options.ToolGatewayBinding
		}
		assertServePreflightStaleGatewayWarning(t, serveOpts, "run_local")
		serveStarted <- serveOpts
		return Run(ctx, repo, serveOpts)
	}
	payloadPath := filepath.Join(t.TempDir(), "payload.json")
	if err := os.WriteFile(payloadPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	configPath := writeDoctorClaudeConfig(t, "")
	dataSource := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	var stdout, stderr bytes.Buffer
	go func() {
		done <- cliapp.Execute(ctx, cliapp.RepoRoot(), []string{
			"run", "start",
			"--event", "task.requested",
			"--payload", payloadPath,
			"--config", configPath,
			"--backend", "claude_cli",
			"--contracts", filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
			"--data", dataSource,
			"--platform-spec", defaultPlatformSpecPath,
			"--api-port", apiPortText,
		}, &stdout, &stderr, runServe)
	}()
	select {
	case <-serveStarted:
	case <-time.After(serveRuntimeReadyTimeout):
		t.Fatalf("local CLI run did not invoke the serve owner\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	}
	binding := receiveToolGatewayBinding(t, bindingCh, "")
	assertToolGatewayBindingUsesMCPPort(t, binding, mcpPort, stalePort)
	cancel()
	select {
	case <-done:
	case <-time.After(serveRuntimeReadyTimeout):
		t.Fatal("local CLI run did not stop after cancellation")
	}
}

func TestStartLocalRunServeLateReadinessGateFailureDoesNotCommit(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	apiPortText := freeDoctorTCPPort(t)
	mcpListenAddr := "127.0.0.1:" + freeDoctorTCPPort(t)
	var readyStatus atomic.Int32
	runServe := func(ctx context.Context, repo string, serveOpts cliapp.ServeOptions) int {
		serveOpts.MCPListenAddr = mcpListenAddr
		serveOpts.TestBeforeReadinessCommit = func() error {
			response, probeErr := http.Get("http://127.0.0.1:" + apiPortText + "/readyz")
			if probeErr != nil {
				return fmt.Errorf("probe pre-commit readiness: %w", probeErr)
			}
			readyStatus.Store(int32(response.StatusCode))
			_ = response.Body.Close()
			return errors.New("late readiness proof failed")
		}
		return Run(ctx, repo, serveOpts)
	}

	payloadPath := filepath.Join(t.TempDir(), "payload.json")
	if err := os.WriteFile(payloadPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	var stdout, stderr bytes.Buffer
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"run", "start",
		"--event", "task.requested",
		"--payload", payloadPath,
		"--config", writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", filepath.Join(t.TempDir(), "late-gate.sqlite"), nil),
		"--contracts", filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		"--data", t.TempDir(),
		"--platform-spec", defaultPlatformSpecPath,
		"--api-port", apiPortText,
	}, &stdout, &stderr, runServe)
	if code == 0 || !strings.Contains(stderr.String(), "exited before readiness") {
		t.Fatalf("local CLI run exit=%d stderr=%q, want pre-readiness exit", code, stderr.String())
	}
	if readyStatus.Load() != http.StatusServiceUnavailable {
		t.Fatalf("/readyz during final pre-commit gate = %d, want %d", readyStatus.Load(), http.StatusServiceUnavailable)
	}
	text := stderr.String()
	if !strings.Contains(text, "serve failed · ready · late readiness proof failed") {
		t.Fatalf("local run did not replay the truthful late-gate failure:\n%s", text)
	}
	if strings.Contains(text, "ready in ") || strings.Contains(text, "shutdown · complete") {
		t.Fatalf("local run late-gate failure exposed readiness:\n%s", text)
	}
}

func TestCreateServeToolGatewayBindingIgnoresStaleLocalURLEnv(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mcp: %v", err)
	}
	defer listener.Close()
	_, mcpPort, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener addr: %v", err)
	}
	oldUnifiedPort := "8081"
	if mcpPort == oldUnifiedPort {
		oldUnifiedPort = "8080"
	}
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:"+oldUnifiedPort)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:"+oldUnifiedPort)

	binding, err := createServeToolGatewayBinding(listener.Addr())
	if err != nil {
		t.Fatalf("create gateway binding: %v", err)
	}
	assertToolGatewayBindingUsesMCPPort(t, binding, mcpPort, oldUnifiedPort)
}

func TestRunServeRuntimeNonDevClaudeCLIRetiredGatewayURLEnvFailsClosed(t *testing.T) {
	for _, tt := range []struct {
		name  string
		env   string
		value string
	}{
		{name: "host-url", env: "SWARM_TOOL_GATEWAY_URL", value: "http://127.0.0.1:" + freeDoctorTCPPort(t)},
		{name: "container-url", env: "SWARM_TOOL_GATEWAY_CONTAINER_URL", value: "http://host.docker.internal:" + freeDoctorTCPPort(t)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
			t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
			t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
			t.Setenv(tt.env, tt.value)

			assertRunServeRuntimeRetiredGatewayURLAdmissionFailure(t, tt.env, cliapp.ServeOptions{
				ConfigPath:         writeDoctorClaudeConfig(t, ""),
				Backend:            "claude_cli",
				ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
				DataSource:         t.TempDir(),
				PlatformSpecPath:   defaultPlatformSpecPath,
				StoreMode:          "not-a-store",
				APIListenAddr:      "127.0.0.1:0",
				MCPListenAddr:      "127.0.0.1:0",
				SelfCheck:          true,
				RequireBundleMatch: false,
			})
		})
	}
}

func TestRunServeRuntimeBundleHashRetiredGatewayURLEnvFailsBeforeStartupSideEffects(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "http://127.0.0.1:"+freeDoctorTCPPort(t))
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	assertRunServeRuntimeRetiredGatewayURLAdmissionFailure(t, "SWARM_TOOL_GATEWAY_URL", cliapp.ServeOptions{
		ConfigPath:         writeDoctorClaudeConfig(t, ""),
		Backend:            "claude_cli",
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		DataSource:         t.TempDir(),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "not-a-store",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
		BundleHash:         "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
}

func TestRunServeRuntimeNonClaudeRetiredGatewayURLEnvFailsBeforeStartupSideEffects(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "http://host.docker.internal:"+freeDoctorTCPPort(t))
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	assertRunServeRuntimeRetiredGatewayURLAdmissionFailure(t, "SWARM_TOOL_GATEWAY_CONTAINER_URL", cliapp.ServeOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		Backend:            "anthropic",
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		DataSource:         t.TempDir(),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "not-a-store",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: false,
	})
}

func assertRunServeRuntimeRetiredGatewayURLAdmissionFailure(t *testing.T, envName string, opts cliapp.ServeOptions) {
	t.Helper()
	var out lockedBuffer
	opts.Verbose = true
	opts.Output = &out
	code := Run(context.Background(), cliapp.RepoRoot(), opts)
	if code != cliapp.CLIExitRuntime {
		t.Fatalf("Run code = %d, want %d\noutput:\n%s", code, cliapp.CLIExitRuntime, out.String())
	}
	for _, want := range []string{
		"config_load",
		"serve admission",
		envName,
		"retired",
		"unset " + envName,
		"ToolGatewayBinding",
		"non-dev serve rejects retired gateway URL env",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("serve output missing %q:\n%s", want, out.String())
		}
	}
	for _, notWant := range []string{"local_preflight", "not-a-store", "db_connection", "bundle_load", "http_listener_bind", "ready"} {
		if strings.Contains(out.String(), notWant) {
			t.Fatalf("serve reached %q after retired gateway URL env failure:\n%s", notWant, out.String())
		}
	}
}

func assertServePreflightStaleGatewayWarning(t *testing.T, opts cliapp.ServeOptions, wantMode string) {
	t.Helper()
	cfgResult, err := cliapp.LoadRuntimeConfigWithOptions(cliapp.RuntimeConfigLoadOptions{
		RepoRoot:        cliapp.RepoRoot(),
		ExplicitPath:    opts.ConfigPath,
		BackendOverride: opts.Backend,
	})
	if err != nil {
		t.Fatalf("load config for preflight proof: %v", err)
	}
	resolvedPaths, err := cliapp.ResolveCLIContractPlatformSpecPaths(cliapp.RepoRoot(), cliapp.CLIContractPlatformSpecPathOptions{
		ContractsPath:    opts.ContractsPath,
		PlatformSpecPath: opts.PlatformSpecPath,
	})
	if err != nil {
		t.Fatalf("resolve preflight paths: %v", err)
	}
	workspaceBackend, err := cliapp.ResolveWorkspaceBackend(opts.WorkspaceBackend, opts.WorkspaceBackendSet, cfgResult.Config)
	if err != nil {
		t.Fatalf("resolve workspace backend for preflight proof: %v", err)
	}
	providerPacks, err := cliapp.LoadConfiguredProviderTriggerPacks(cliapp.RepoRoot(), cfgResult)
	if err != nil {
		t.Fatalf("load provider packs for preflight proof: %v", err)
	}
	report := cliapp.RunServeLocalClaudeCLIPreflight(context.Background(), cliapp.RepoRoot(), opts, cfgResult.Config, resolvedPaths, workspaceBackend, cliapp.WorkspaceMountSources{DataSource: t.TempDir(), DataSourceSource: "test"}, providerPacks.Loaded, providerPacks.Catalog)
	if report.Mode != wantMode {
		t.Fatalf("preflight mode = %q, want %q", report.Mode, wantMode)
	}
	for _, code := range []string{"swarm_tool_gateway_url_retired", "swarm_tool_gateway_container_url_retired"} {
		if !localPreflightReportHasFinding(report, code, cliapp.LocalPreflightSeverityWarning, cliapp.LocalPreflightStatusFailed) {
			t.Fatalf("preflight report missing warning %q:\n%#v", code, report)
		}
	}
	if report.HasBlockers() {
		t.Fatalf("stale local gateway URL env produced blockers, want warnings only:\n%#v", report)
	}
	if len(report.CapabilitySubjects) != 15 {
		t.Fatalf("%s capability subjects = %#v, want eight triggers plus seven connector actions", wantMode, report.CapabilitySubjects)
	}
}

func localPreflightReportHasFinding(report cliapp.LocalPreflightReport, code string, severity cliapp.LocalPreflightSeverity, status cliapp.LocalPreflightFindingStatus) bool {
	for _, finding := range report.Findings {
		if finding.Code == code && finding.Severity == severity && finding.Status == status {
			return true
		}
	}
	return false
}

func stubServeWorkspaceLifecycleForTest(t *testing.T) {
	t.Helper()
	oldWorkspaceLifecycle := cliapp.ConfiguredWorkspaceLifecycleForServe
	cliapp.ConfiguredWorkspaceLifecycleForServe = func(*sql.DB, *config.Config, string, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
		return serveRuntimeWorkspaceStub{}, nil
	}
	t.Cleanup(func() {
		cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspaceLifecycle
	})
}

func receiveToolGatewayBinding(t *testing.T, ch <-chan toolgateway.Binding, output string) toolgateway.Binding {
	t.Helper()
	select {
	case binding := <-ch:
		return binding
	case <-time.After(serveRuntimeReadyTimeout):
		t.Fatalf("timed out waiting for runtime tool gateway binding\noutput:\n%s", output)
		return toolgateway.Binding{}
	}
}

func assertToolGatewayBindingUsesMCPPort(t *testing.T, binding toolgateway.Binding, mcpPort, stalePort string) {
	t.Helper()
	if mcpPort == stalePort {
		t.Fatalf("invalid gateway binding test setup: listener and stale ports are both %q", mcpPort)
	}
	if err := binding.Validate(); err != nil {
		t.Fatalf("runtime tool gateway binding is invalid: %v\nbinding=%#v", err, binding)
	}
	if got, want := binding.HostEndpoint, "http://127.0.0.1:"+mcpPort; got != want {
		t.Fatalf("binding HostEndpoint = %q, want %q", got, want)
	}
	if got, want := binding.WorkspaceEndpoint, "http://host.docker.internal:"+mcpPort; got != want {
		t.Fatalf("binding WorkspaceEndpoint = %q, want %q", got, want)
	}
	if strings.TrimSpace(binding.Token) == "" {
		t.Fatalf("runtime binding token is empty: %#v", binding)
	}
}

func staleGatewayTestPort(mcpPort string) string {
	if strings.TrimSpace(mcpPort) == "8081" {
		return "8080"
	}
	return "8081"
}

func TestValidateServeGatewayURLEnvForNonDevRejectsAnyRetiredURLEnv(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value string
	}{
		{name: "SWARM_TOOL_GATEWAY_URL", value: "http://127.0.0.1:8082"},
		{name: "SWARM_TOOL_GATEWAY_CONTAINER_URL", value: "http://host.docker.internal:8082"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
			t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
			t.Setenv(tt.name, tt.value)
			err := validateServeGatewayURLEnvForNonDev()
			if err == nil {
				t.Fatal("non-dev gateway env validation unexpectedly accepted retired URL env")
			}
			for _, want := range []string{tt.name, "retired", "unset " + tt.name, "ToolGatewayBinding"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %v, want %q", err, want)
				}
			}
			if strings.Contains(err.Error(), "MCP listener port") {
				t.Fatalf("error still suggests port-matching URL env is valid: %v", err)
			}
		})
	}
}

func TestRunServeRuntimeAbandonActiveRunsQuiescesBeforeBundleMatchAdmission(t *testing.T) {
	oldBuildStores := buildStoresForServe
	oldWorkspaceLifecycle := cliapp.ConfiguredWorkspaceLifecycleForServe
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	runtimePG, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
		if _, err := runtimePG.BindSchemaCapabilities(ctx); err != nil {
			return storeBundle{}, err
		}
		return selectedPostgresStoreBundle(runtimePG, cfg), nil
	}
	cliapp.ConfiguredWorkspaceLifecycleForServe = func(*sql.DB, *config.Config, string, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
		return serveRuntimeWorkspaceStub{}, nil
	}
	t.Cleanup(func() {
		buildStoresForServe = oldBuildStores
		cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspaceLifecycle
	})

	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	activeSessionID := uuid.NewString()
	mismatchFingerprint := "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, bundle_fingerprint, started_at)
		VALUES ($1::uuid, 'running', $2, now())
	`, runID, mismatchFingerprint); err != nil {
		t.Fatalf("seed active mismatched run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES ('live',
			$1::uuid, $2::uuid, 'serve.abandon.test', 'global', '{}'::jsonb, 'test', 'agent', now()
		)
	`, eventID, runID); err != nil {
		t.Fatalf("seed active delivery event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, active_session_id, reason_code, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-a', 'in_progress', $3::uuid, 'agent_processing', now()
		)
	`, runID, eventID, activeSessionID); err != nil {
		t.Fatalf("seed active delivery: %v", err)
	}

	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath:         writeServeRuntimeTestConfig(t),
		ContractsPath:      filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath:   defaultPlatformSpecPath,
		StoreMode:          "postgres",
		APIListenAddr:      "127.0.0.1:0",
		MCPListenAddr:      "127.0.0.1:0",
		SelfCheck:          true,
		RequireBundleMatch: true,
		AbandonActiveRuns:  true,
		Verbose:            true,
	})

	serve.waitForReadyLine()
	if code := serve.stop(); code != 0 {
		t.Fatalf("Run code = %d\noutput:\n%s", code, serve.outputString())
	}

	var runStatus, controlStatus, reason, controlledBy string
	if err := db.QueryRowContext(context.Background(), `
		SELECT r.status, rc.control_status, COALESCE(rc.reason, ''), COALESCE(rc.controlled_by, '')
		FROM runs r
		JOIN run_control_state rc ON rc.run_id = r.run_id
		WHERE r.run_id = $1::uuid
	`, runID).Scan(&runStatus, &controlStatus, &reason, &controlledBy); err != nil {
		t.Fatalf("load run/control state: %v", err)
	}
	if runStatus != "cancelled" || controlStatus != "stopped" || reason != runtimerunquiescence.ServeAbandonReasonCode || controlledBy != runtimerunquiescence.ServeAbandonControlledBy {
		t.Fatalf("run/control = %s/%s/%s/%s, want cancelled/stopped/%s/%s", runStatus, controlStatus, reason, controlledBy, runtimerunquiescence.ServeAbandonReasonCode, runtimerunquiescence.ServeAbandonControlledBy)
	}

	var deliveryStatus, deliveryReason string
	var deliveryActiveSession sql.NullString
	if err := db.QueryRowContext(context.Background(), `
		SELECT status, COALESCE(reason_code, ''), active_session_id::text
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'agent-a'
	`, eventID).Scan(&deliveryStatus, &deliveryReason, &deliveryActiveSession); err != nil {
		t.Fatalf("load delivery: %v", err)
	}
	if deliveryStatus != "dead_letter" || deliveryReason != runtimerunquiescence.ServeAbandonReasonCode || deliveryActiveSession.Valid {
		t.Fatalf("delivery = %s/%s active=%v, want serve abandon dead_letter", deliveryStatus, deliveryReason, deliveryActiveSession.Valid)
	}
	for _, subscriberID := range []string{"agent-a", "pipeline"} {
		var outcome, receiptReason string
		if err := db.QueryRowContext(context.Background(), `
			SELECT outcome, COALESCE(reason_code, '')
			FROM event_receipts
			WHERE event_id = $1::uuid
			  AND subscriber_id = $2
		`, eventID, subscriberID).Scan(&outcome, &receiptReason); err != nil {
			t.Fatalf("load receipt %s: %v", subscriberID, err)
		}
		if outcome != "dead_letter" || receiptReason != runtimerunquiescence.ServeAbandonReasonCode {
			t.Fatalf("receipt %s = %s/%s, want serve abandon dead_letter", subscriberID, outcome, receiptReason)
		}
	}
}

type serveRuntimeWorkspaceStub struct {
	stubWorkspaceLifecycle
	cleanup           func(context.Context) (runtimedestructivereset.ContainerResetResult, error)
	managedContainers []runtimedestructivereset.ContainerRef
	stoppedContainers *[]string
}

func (s serveRuntimeWorkspaceStub) CleanupDevEntityContainers(ctx context.Context) (runtimedestructivereset.ContainerResetResult, error) {
	if s.cleanup != nil {
		return s.cleanup(ctx)
	}
	return runtimedestructivereset.ContainerResetResult{}, nil
}

func (s serveRuntimeWorkspaceStub) ManagedResetContainerInventory(context.Context) ([]runtimedestructivereset.ContainerRef, error) {
	return append([]runtimedestructivereset.ContainerRef(nil), s.managedContainers...), nil
}

func (serveRuntimeWorkspaceStub) InspectManagedContainer(context.Context, string) (runtimedestructivereset.ManagedContainerInspection, error) {
	return runtimedestructivereset.ManagedContainerInspection{}, nil
}

func (s serveRuntimeWorkspaceStub) StopManagedContainer(_ context.Context, name string) error {
	if s.stoppedContainers != nil {
		*s.stoppedContainers = append(*s.stoppedContainers, name)
	}
	return nil
}

type serveBootProgressSpecStep struct {
	Step  int    `yaml:"step"`
	Name  string `yaml:"name"`
	Owner string `yaml:"owner"`
}

func loadServeBootProgressSequenceFromSpec(t *testing.T) []serveBootProgressSpecStep {
	t.Helper()
	var spec struct {
		CLISpecification struct {
			CommandCatalog struct {
				Serve struct {
					BootObservability struct {
						BootProgressSequence struct {
							TotalSteps int                         `yaml:"total_steps"`
							Steps      []serveBootProgressSpecStep `yaml:"steps"`
						} `yaml:"boot_progress_sequence"`
					} `yaml:"boot_observability"`
				} `yaml:"serve"`
			} `yaml:"command_catalog"`
		} `yaml:"cli_specification"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(cliapp.RepoRoot(), defaultPlatformSpecPath), &spec)
	sequence := spec.CLISpecification.CommandCatalog.Serve.BootObservability.BootProgressSequence
	if sequence.TotalSteps != runtimepkg.BootProgressTotalSteps {
		t.Fatalf("platform spec total_steps = %d, want %d", sequence.TotalSteps, runtimepkg.BootProgressTotalSteps)
	}
	for i, step := range sequence.Steps {
		if step.Step != i+1 {
			t.Fatalf("platform spec step index %d has step %d", i, step.Step)
		}
		if strings.TrimSpace(step.Name) == "" {
			t.Fatalf("platform spec step %d missing name", step.Step)
		}
		if strings.TrimSpace(step.Owner) == "" {
			t.Fatalf("platform spec step %d missing owner", step.Step)
		}
	}
	return sequence.Steps
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func waitForServeReadyLine(t *testing.T, out *lockedBuffer, done <-chan int) {
	t.Helper()
	deadline := time.After(serveRuntimeReadyTimeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case code := <-done:
			t.Fatalf("Run exited before ready line with code %d\noutput:\n%s", code, out.String())
		case <-deadline:
			t.Fatalf("timed out waiting for serve ready line\noutput:\n%s", out.String())
		case <-ticker.C:
			if serveOutputIsReady(out.String()) {
				return
			}
		}
	}
}

const (
	serveRuntimeReadyTimeout = 30 * time.Second
	serveRuntimeStopTimeout  = runtimepkg.DefaultShutdownGrace + 15*time.Second
)

type serveRuntimeTestProcess struct {
	t      *testing.T
	cancel context.CancelFunc
	done   <-chan int
	out    *lockedBuffer

	mu      sync.Mutex
	stopped bool
	code    int
}

func startServeRuntimeTestProcess(t *testing.T, opts cliapp.ServeOptions) *serveRuntimeTestProcess {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := &lockedBuffer{}
	opts.Output = out
	done := make(chan int, 1)
	process := &serveRuntimeTestProcess{
		t:      t,
		cancel: cancel,
		done:   done,
		out:    out,
	}
	t.Cleanup(process.cleanup)
	go func() {
		done <- Run(ctx, cliapp.RepoRoot(), opts)
	}()
	return process
}

func (p *serveRuntimeTestProcess) waitForReadyLine() {
	p.t.Helper()
	deadline := time.After(serveRuntimeReadyTimeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case code := <-p.done:
			p.recordStopped(code)
			p.t.Fatalf("Run exited before ready line with code %d\noutput:\n%s", code, p.outputString())
		case <-deadline:
			p.cancel()
			if code, ok := p.waitForExit(serveRuntimeStopTimeout); ok {
				p.t.Fatalf("timed out waiting for serve ready line; Run stopped after cancellation with code %d\noutput:\n%s", code, p.outputString())
			}
			p.t.Fatalf("timed out waiting for serve ready line and stopping Run\noutput:\n%s", p.outputString())
		case <-ticker.C:
			if serveOutputIsReady(p.outputString()) {
				return
			}
		}
	}
}

func (p *serveRuntimeTestProcess) cleanup() {
	p.t.Helper()
	p.mu.Lock()
	stopped := p.stopped
	p.mu.Unlock()
	if stopped {
		return
	}
	p.cancel()
	if _, ok := p.waitForExit(serveRuntimeStopTimeout); !ok {
		p.t.Errorf("timed out stopping Run during cleanup\noutput:\n%s", p.outputString())
	}
}

func serveRuntimeAPIListenerFromOutput(t *testing.T, output string) string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		for i, field := range fields {
			field = strings.Trim(field, "(),")
			if addr, ok := strings.CutPrefix(field, "api_listener="); ok && strings.TrimSpace(addr) != "" {
				return addr
			}
			if strings.TrimSpace(line) != "" && fields[0] == "listeners" && field == "api" && i+1 < len(fields) {
				return strings.Trim(fields[i+1], "(),")
			}
		}
	}
	t.Fatalf("serve output missing api_listener:\n%s", output)
	return ""
}

func serveOutputIsReady(output string) bool {
	return strings.Contains(output, "[22/22]") || strings.Contains(output, "\n  ready in ")
}

type serveBootProgressRow struct {
	Step  int
	Total int
	Name  string
}

func parseServeBootProgressRows(t *testing.T, output string) []serveBootProgressRow {
	t.Helper()
	rows := []serveBootProgressRow{}
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "[") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			t.Fatalf("malformed serve boot progress line: %q", line)
		}
		parts := strings.Split(strings.Trim(fields[0], "[]"), "/")
		if len(parts) != 2 {
			t.Fatalf("malformed serve boot progress marker %q in line %q", fields[0], line)
		}
		step, err := strconv.Atoi(parts[0])
		if err != nil {
			t.Fatalf("parse step from %q: %v", fields[0], err)
		}
		total, err := strconv.Atoi(parts[1])
		if err != nil {
			t.Fatalf("parse total from %q: %v", fields[0], err)
		}
		rows = append(rows, serveBootProgressRow{Step: step, Total: total, Name: fields[1]})
	}
	return rows
}

func writeServeRuntimeTestConfig(t *testing.T) string {
	t.Helper()
	return writeServeRuntimeTestConfigWithWorkspaceFields(t, nil)
}

func writeServeRuntimeTestConfigWithWorkspaceFields(t *testing.T, workspaceFields []string) string {
	t.Helper()
	configText := strings.Join([]string{
		"runtime:",
		"  recovery_on_startup: false",
		"workspace:",
		"  data_source: " + t.TempDir(),
	}, "\n") + "\n"
	if len(workspaceFields) > 0 {
		configText += strings.Join(workspaceFields, "\n") + "\n"
	}
	configText += strings.Join([]string{
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	}, "\n") + "\n"
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	configText = withTestProviderTriggerPlatformInventory(t, configText)
	if err := os.WriteFile(path, []byte(configText), 0o644); err != nil {
		t.Fatalf("write serve runtime config: %v", err)
	}
	return path
}

func (a delayedRunStatusAgent) ID() string { return a.id }

func (delayedRunStatusAgent) Type() string { return "test" }

func (a delayedRunStatusAgent) Subscriptions() []events.EventType {
	return append([]events.EventType(nil), a.subscriptions...)
}

func (a delayedRunStatusAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	select {
	case a.started <- struct{}{}:
	default:
	}
	select {
	case <-a.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return []events.Event{eventtest.RootIngress(uuid.NewString(), events.EventType("scan.completed"), a.id, "", []byte(`{}`), 0, evt.RunID(), "", events.EnvelopeForEntityID(events.EventEnvelope{}, evt.EntityID()), time.Now().UTC())}, nil
}

func writeServedEventPublishFollowUpFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopyRootIngressServedFollowUp(t)
}

func servedEventPublishFixtureBundleHash(t *testing.T, contractsPath string) string {
	t.Helper()
	bundle := loadWorkflowValidationBundleAt(t, contractsPath)
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("BundleHash(%s): %v", contractsPath, err)
	}
	return bundleHash
}

func testWorkflowValidationBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	return bundle
}

func loadWorkflowValidationFixtureBundle(t *testing.T, relativeRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	RepoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(RepoRoot)
	fixtureRoot := filepath.Join(RepoRoot, relativeRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(RepoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixtureRoot, err)
	}
	return bundle
}

func loadWorkflowValidationBundleAt(t *testing.T, fixtureRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	RepoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(RepoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(RepoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixtureRoot, err)
	}
	return bundle
}

func writeWorkflowValidationFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func writeServeRuntimeAgentSlugFixture(t *testing.T, workflowName, agentID string) string {
	t.Helper()
	return writeServeRuntimeAgentSlugFixtureWithKey(t, workflowName, agentID, agentID)
}

func writeServeRuntimeAgentSlugFixtureWithKey(t *testing.T, workflowName, agentKey, agentID string) string {
	t.Helper()
	return canonicalrouting.CopyAgentSlugAdmission(t, workflowName, agentKey, agentID)
}

func writeServeRuntimeNativeBashFixture(t *testing.T) string {
	t.Helper()
	const agentID = "native-bash-worker"
	root := writeServeRuntimeAgentSlugFixture(t, "native-bash-docker-required", agentID)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), fmt.Sprintf(`
%s:
  id: %s
  role: %s
  prompt_ref: %s
  model: regular
  native_tools:
    bash: true
  subscriptions: [agent.requested]
`, agentID, agentID, agentID, agentID))
	return root
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}

func joinedContains(values []string, want string) bool {
	return strings.Contains(strings.Join(values, "\n"), want)
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (p *serveRuntimeTestProcess) outputString() string {
	return p.out.String()
}

func (p *serveRuntimeTestProcess) stop() int {
	p.t.Helper()
	p.cancel()
	code, ok := p.waitForExit(serveRuntimeStopTimeout)
	if !ok {
		p.t.Fatalf("timed out stopping Run\noutput:\n%s", p.outputString())
	}
	return code
}

func (p *serveRuntimeTestProcess) waitForExit(timeout time.Duration) (int, bool) {
	p.t.Helper()
	p.mu.Lock()
	if p.stopped {
		code := p.code
		p.mu.Unlock()
		return code, true
	}
	p.mu.Unlock()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case code := <-p.done:
		p.recordStopped(code)
		return code, true
	case <-timer.C:
		return 0, false
	}
}

func (p *serveRuntimeTestProcess) recordStopped(code int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	p.code = code
}

func writeStoreBackendRuntimeConfigWithWorkspaceFields(t *testing.T, backend string, sqlitePath string, workspaceFields []string) string {
	t.Helper()
	lines := []string{
		"runtime:",
		"  recovery_on_startup: false",
		"workspace:",
		"  data_source: " + t.TempDir(),
	}
	lines = append(lines, workspaceFields...)
	if strings.TrimSpace(backend) != "" || strings.TrimSpace(sqlitePath) != "" {
		lines = append(lines,
			"store:",
			"  backend: "+backend,
			"  sqlite:",
			"    path: "+sqlitePath,
		)
	}
	lines = append(lines,
		"llm:",
		"  backend: anthropic",
		"  session:",
		"    lock_ttl: 10s",
		"    rotate_after_turns: 40",
		"    rotate_on_parse_failures: 3",
	)
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	configText := withTestProviderTriggerPlatformInventory(t, strings.Join(lines, "\n")+"\n")
	if err := os.WriteFile(path, []byte(configText), 0o644); err != nil {
		t.Fatalf("write runtime config: %v", err)
	}
	return path
}
