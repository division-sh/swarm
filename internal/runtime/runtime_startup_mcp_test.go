package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	actors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func startupMCPRequestFixture(t *testing.T, rt *Runtime, mutate func(*effects.Authority)) (*http.Request, *mcp.TurnContextRegistry) {
	t.Helper()
	grant, err := rt.startupGrant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	identity := agentidentitytest.RootDeclared(t, "startup-agent", "startup-mcp-test")
	plan, err := identity.Plan()
	if err != nil {
		t.Fatal(err)
	}
	probeID := uuid.NewString()
	authority := effects.Authority{
		Kind: effects.AuthorityStartupProbe, ID: probeID, ExecutionMode: effects.ExecutionModeLive,
		ExecutionOwner: grant.ProcessOwnerID, LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: grant.RuntimeGeneration,
		StartupProbe: effects.StartupProbeAuthority{
			ProbeID: probeID, StartupAuthorityID: grant.GrantID, StartupStateVersion: grant.StateVersion,
			ActorID: plan.AgentID(), ExecutionKind: string(managedcapabilities.ExecutionNormalAgent), ExecutionAuthorityID: grant.GrantID,
		},
	}
	if mutate != nil {
		mutate(&authority)
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorPlan: plan, RuntimeMode: "startup_probe", Provider: "claude_cli", Transport: "cli", ProviderContract: "claude-cli-test",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityStartupProbe, ID: probeID,
			ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: authority.StartupProbe.StartupAuthorityID,
			StartupOwnerID: authority.ExecutionOwner, StartupGeneration: authority.FenceGeneration,
		},
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := actors.WithActor(context.Background(), actors.AgentConfig{ID: plan.AgentID(), FlowPath: plan.FlowInstance()})
	ctx = effects.WithAuthority(ctx, authority)
	registry := mcp.NewTurnContextRegistry(actors.ActorFromContext)
	token := registry.RegisterTurnContextWithCapabilitySurface(ctx, time.Minute, surface)
	if token == "" {
		t.Fatal("startup context was not registered")
	}
	rt.ToolGateway = mcp.NewGateway(nil, "auth", mcp.GatewayHooks{ResolveTurnContext: registry.ResolveTurnContext})
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`))
	req.Header.Set("Authorization", "Bearer auth")
	req.Header.Set("X-SWARM-Context-Token", token)
	return req, registry
}

func TestStartupMCPAdmissionRequiresCurrentPreparedGrant(t *testing.T) {
	for _, mode := range []string{"current", "foreign_grant", "old_version", "foreign_owner", "foreign_generation", "foreign_actor", "foreign_execution", "wrong_kind", "expired_authority", "missing_auth", "unknown_token", "unregistered", "expired", "settled", "admitted", "retired", "fenced", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			rt := newStartupReadinessTestRuntime(t)
			req, registry := startupMCPRequestFixture(t, rt, func(a *effects.Authority) {
				switch mode {
				case "foreign_grant":
					a.StartupProbe.StartupAuthorityID = uuid.NewString()
					a.StartupProbe.ExecutionAuthorityID = a.StartupProbe.StartupAuthorityID
				case "old_version":
					a.StartupProbe.StartupStateVersion++
				case "foreign_owner":
					a.ExecutionOwner = "different-process"
				case "foreign_generation":
					a.FenceGeneration++
				case "wrong_kind":
					a.Kind = effects.AuthorityNormalAgent
				case "foreign_actor":
					a.StartupProbe.ActorID = "different-actor"
				case "foreign_execution":
					a.StartupProbe.ExecutionAuthorityID = uuid.NewString()
				case "expired_authority":
					a.LeaseExpiresAt = time.Now().Add(-time.Minute)
				}
			})
			switch mode {
			case "missing_auth":
				req.Header.Del("Authorization")
			case "unknown_token":
				req.Header.Set("X-SWARM-Context-Token", "not-registered")
			case "unregistered":
				registry.UnregisterTurnContext(mcp.ContextTokenFromRequest(req))
			case "expired":
				registry.PruneTurnContextsBefore(time.Now().Add(2 * time.Minute))
			case "settled", "admitted":
				if _, err := rt.startupGrant.MarkProbesSettled(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
				if mode == "admitted" {
					if _, err := rt.startupGrant.AdmitExecution(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
			case "retired":
				rt.workOccurrence.Retire()
			case "fenced":
				if err := rt.workOccurrence.Fence(); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				ctx, cancel := context.WithCancel(req.Context())
				cancel()
				req = req.WithContext(ctx)
			}
			handler, lease, err := rt.AcquireStartupMCPRequest(req)
			if lease != nil {
				defer lease.Done()
			}
			if mode != "current" {
				if handler != nil || lease != nil {
					t.Fatal("non-current probe escaped the execution fence")
				}
				return
			}
			if err != nil || handler == nil || lease == nil {
				t.Fatalf("current prepared grant probe rejected: %v", err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req.WithContext(lease.Context()))
			if response.Code != http.StatusOK {
				t.Fatalf("startup initialize failed: %d %s", response.Code, response.Body.String())
			}
			grant, err := rt.startupGrant.Evidence()
			if err != nil || grant.State != startupownership.GrantPrepared {
				t.Fatalf("MCP preflight changed execution admission: %+v %v", grant, err)
			}
		})
	}
}

func TestStartupMCPRequestJoinsBeforeGrantRetirement(t *testing.T) {
	rt := newStartupReadinessTestRuntime(t)
	grant := rt.startupGrant
	req, _ := startupMCPRequestFixture(t, rt, nil)
	_, lease, err := rt.AcquireStartupMCPRequest(req)
	if err != nil || lease == nil {
		t.Fatalf("admit probe: %v", err)
	}
	defer lease.Done()
	finished := make(chan error, 1)
	go func() { finished <- rt.ShutdownWithOptions(ShutdownOptions{Grace: 5 * time.Second}) }()
	select {
	case <-lease.Context().Done():
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not fence admitted probe")
	}
	select {
	case <-grant.Done():
		t.Fatal("grant retired before the admitted MCP operation settled")
	case err := <-finished:
		t.Fatalf("shutdown returned before probe settlement: %v", err)
	default:
	}
	if err := lease.Done(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not join settled MCP request")
	}
	select {
	case <-grant.Done():
	default:
		t.Fatal("joined runtime did not retire grant")
	}
}
