package effects

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/google/uuid"
)

func preparedProbeEvidenceForTest(t *testing.T) (Authority, managedcapabilities.Surface) {
	t.Helper()
	name, err := agentidentity.DeclaredName("worker", "target")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agentidentity.NewPlan(name, agentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := plan.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	preparation := managedcapabilities.PreparedSelectedForkProbeAuthority{
		ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "process", ProcessBootID: uuid.NewString(),
		BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), SourceFingerprint: strings.Repeat("b", 64),
		AdmittedPlanFingerprint: strings.Repeat("c", 64), ConfigurationFingerprint: strings.Repeat("d", 64),
		CatalogFingerprint: strings.Repeat("e", 64), ActorPlanFingerprint: fingerprint,
	}
	probeID, preparationID := uuid.NewString(), uuid.NewString()
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorPlan: plan, RuntimeMode: "startup_probe", Provider: "claude_cli", Transport: "cli", ProviderContract: "prepared-probe-test",
		Authority: managedcapabilities.Authority{Kind: managedcapabilities.AuthorityStartupProbe, ID: probeID,
			ExecutionKind: managedcapabilities.ExecutionSelectedForkPreparation, ExecutionAuthorityID: preparationID, Preparation: &preparation},
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return Authority{Kind: AuthorityStartupProbe, ID: probeID, ExecutionOwner: preparation.ProcessOwnerID,
		LeaseExpiresAt: time.Now().UTC().Add(time.Minute), FenceGeneration: 1, ExecutionMode: ExecutionModeLive,
		StartupProbe: StartupProbeAuthority{ProbeID: probeID, ActorID: plan.AgentID(),
			ExecutionKind: string(managedcapabilities.ExecutionSelectedForkPreparation), ExecutionAuthorityID: preparationID, Preparation: &preparation}}, surface
}

func TestPreparedProbeRejectsCrossedAuthorityAndBusinessEffects(t *testing.T) {
	authority, surface := preparedProbeEvidenceForTest(t)
	store := &effectStoreProbe{}
	controller := NewController(store).WithExecutionPosture(executionposture.Live)
	ctx := WithAuthority(context.Background(), authority)
	request := AuthorizeRequest{OperationID: uuid.NewString(), Adapter: "claude_cli_startup_probe", RequestFingerprint: "probe", CapabilitySurface: &surface}
	if _, err := controller.Authorize(ctx, request); err != nil {
		t.Fatal(err)
	}
	if len(store.authorizations) != 1 {
		t.Fatal("probe did not reach effect persistence")
	}
	for _, adapter := range []string{"anthropic_api", "authored_http_tool", "native_read_file", "native_bash", "native_write_file", "mcp_tools_call_http"} {
		t.Run(adapter, func(t *testing.T) {
			request := request
			request.Adapter = adapter
			if _, err := controller.Authorize(ctx, request); err == nil {
				t.Fatal("prepared probe authorized business work")
			}
		})
	}
	for _, test := range []struct {
		name   string
		mutate func(*Authority)
	}{
		{"process", func(a *Authority) { a.StartupProbe.Preparation.ProcessAuthorityID = uuid.NewString() }},
		{"boot", func(a *Authority) { a.StartupProbe.Preparation.ProcessBootID = uuid.NewString() }},
		{"catalog", func(a *Authority) { a.StartupProbe.Preparation.CatalogFingerprint = strings.Repeat("f", 64) }},
		{"actor", func(a *Authority) { a.StartupProbe.ActorID = "sibling" }},
		{"preparation", func(a *Authority) { a.StartupProbe.ExecutionAuthorityID = uuid.NewString() }},
		{"grant", func(a *Authority) { a.StartupProbe.StartupAuthorityID = uuid.NewString() }},
		{"grant_version", func(a *Authority) { a.StartupProbe.StartupStateVersion = 1 }},
		{"execution_kind", func(a *Authority) {
			a.StartupProbe.ExecutionKind = string(managedcapabilities.ExecutionSelectedContractFork)
		}},
		{"missing_preparation", func(a *Authority) { a.StartupProbe.Preparation = nil }},
		{"selected_execution_payload", func(a *Authority) { a.SelectedFork.ExecutionID = uuid.NewString() }},
		{"fork_chat_payload", func(a *Authority) { a.ForkChat.ForkID = uuid.NewString() }},
		{"registration_payload", func(a *Authority) { a.ServeRegistration.IntentID = uuid.NewString() }},
		{"confirmation_payload", func(a *Authority) { a.ChannelConfirmation.EffectOperationID = uuid.NewString() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := authority.clonePreparation()
			test.mutate(&invalid)
			if _, err := controller.Authorize(WithAuthority(context.Background(), invalid), request); err == nil {
				t.Fatal("crossed preparation reached persistence")
			}
		})
	}
	for _, field := range []string{"origin_payload", "run_lineage"} {
		t.Run(field, func(t *testing.T) {
			invalid := request
			if field == "origin_payload" {
				invalid.Origin.Directive.OperationID = uuid.NewString()
			} else {
				invalid.Lineage = map[string]string{"run_id": uuid.NewString()}
			}
			if _, err := controller.Authorize(ctx, invalid); err == nil {
				t.Fatal("prepared probe accepted execution payload")
			}
		})
	}
	if len(store.authorizations) != 1 {
		t.Fatalf("invalid authority reached store %d times", len(store.authorizations))
	}
	copy, _ := AuthorityFromContext(ctx)
	copy.StartupProbe.Preparation.CatalogFingerprint = strings.Repeat("0", 64)
	unchanged, _ := AuthorityFromContext(ctx)
	if unchanged.StartupProbe.Preparation.CatalogFingerprint != surface.Authority.Preparation.CatalogFingerprint {
		t.Fatal("context evidence was mutable")
	}
}
