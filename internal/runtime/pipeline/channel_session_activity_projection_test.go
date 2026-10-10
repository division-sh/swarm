package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channelactivation"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
	"github.com/google/uuid"
)

func nativeActivityDeclarationFixture(t *testing.T, toolID string, tool contracts.ToolSchemaEntry) (semanticview.Source, contracts.ActivitySite) {
	t.Helper()
	node, err := identity.AdmitExecutableNodeDeclaration(".", "reply")
	if err != nil {
		t.Fatal(err)
	}
	handler := contracts.SystemNodeEventHandler{Activity: contracts.ActivitySpec{ID: "reply", Tool: toolID}}
	source := semanticviewtest.WrapRootAgents(&contracts.WorkflowContractBundle{
		Nodes: map[string]contracts.SystemNodeContract{"reply": {EventHandlers: map[string]contracts.SystemNodeEventHandler{"message": handler}}},
		Tools: map[string]contracts.ToolSchemaEntry{toolID: tool},
		RootSchema: &contracts.FlowSchemaDocument{Name: "native-reply", Ingress: &contracts.ProjectFlowIngress{Alias: "whatsapp",
			Providers: []contracts.ProjectFlowIngressProvider{{Provider: "whatsapp", Admission: contracts.ProjectFlowIngressAdmission{Kind: "pack", Pack: &contracts.ProjectFlowIngressAdmissionPack{ID: "provider.whatsapp.input"}}}}}},
	})
	return source, contracts.ActivitySitesForNode(node, map[string]contracts.SystemNodeEventHandler{"message": handler})[0]
}

func TestSessionActivityLaunchUsesAuthoredCustomerNotMailboxDestination(t *testing.T) {
	activation, tool, intent, _, _ := nativeActivityHandoffFixture(t)
	toolID, declaration, err := activation.Plan.ConnectorDeclaration("deliver")
	if err != nil {
		t.Fatal(err)
	}
	source, site := nativeActivityDeclarationFixture(t, toolID, declaration)
	publication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{activation})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := channelonboarding.CompileSessionActivityTarget(source, activation.Coordinate.BundleHash, site, publication)
	if err != nil {
		t.Fatal(err)
	}
	intent.Tool = toolID
	intent.NativeSessionTarget = projection.PrivateTarget().ToolID()
	intent.ChannelActivationGeneration = publication.Generation()
	intent.Owner = activityidentity.MustNodeOwner(site.Node)
	intent.HandlerEventKey = site.HandlerEventKey
	intent.ActivityID = contracts.ActivityResultEventsForSite(site).ActivityID
	intent.WorkflowVersion = "fixture-version"
	intent.RoutingSource = eventtest.RootRoutingSource(intent.EntityID.String())
	input := map[string]any{"destination": "15551234571@s.whatsapp.net", "text": "authored customer reply"}
	intent.Input, err = canonicaljson.FromGo(input)
	if err != nil {
		t.Fatal(err)
	}
	started := activityAttemptStartRecord(intent, activityInputHash(intent.Input))
	started.StartedAt = time.Now().UTC()
	if _, _, err := withNativeChannelActivityLaunch(context.Background(), started, intent, activation, "deliver", tool, func(context.Context) error { return nil }); err == nil {
		t.Fatal("serialized selection minted a native business launch without the compiler-owned projection")
	}
	target := ChannelActivityTarget{value: &channelActivityTargetValue{authored: projection}}
	ctx := withActivityChannelTarget(context.Background(), target, true)
	launch, release, err := withNativeChannelActivityLaunch(ctx, started, intent, activation, "deliver", tool, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	permit, err := ConsumeNativeChannelActivity(launch, activation.OnboardingOperationID, activation.SessionAccount, tool, input)
	if err != nil {
		t.Fatal(err)
	}
	fields, _ := permit.Input().ObjectMap()
	if fields["destination"].Equal(activation.Plan.Destination()) || !permit.Input().Equal(intent.Input) {
		t.Fatal("business activity adopted the operator mailbox audience")
	}
	copy := intent
	copy.NativeSessionTarget = "platform.channel_activity.foreign.deliver"
	if _, _, err := withNativeChannelActivityLaunch(ctx, started, copy, activation, "deliver", tool, func(context.Context) error { return nil }); err == nil {
		t.Fatal("business activity substituted the frozen private target")
	}
	copy = intent
	empty, err := channelonboarding.NewChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	copy.ChannelActivationGeneration = empty.Generation()
	if _, _, err := withNativeChannelActivityLaunch(ctx, started, copy, activation, "deliver", tool, func(context.Context) error { return nil }); err == nil {
		t.Fatal("business activity substituted the frozen publication")
	}
	request, err := activityRequestEmitIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	readback, err := activityIntentFromRequestEvent(request.Event)
	if err != nil || readback.Tool != intent.Tool || readback.NativeSessionTarget != intent.NativeSessionTarget ||
		!readback.PlanGeneration.Equal(intent.PlanGeneration) || !readback.ChannelActivationGeneration.Equal(intent.ChannelActivationGeneration) || !readback.Input.Equal(intent.Input) {
		t.Fatal("durable request lost the immutable authored selection", err)
	}
}

type sessionActivityProjectionSource struct {
	semanticview.Source
	schema contracts.FlowSchemaDocument
}

func (s sessionActivityProjectionSource) WorkflowVersion() string { return "fixture-version" }

func TestSessionActivityPreparationFreezesAndRefusesReplacement(t *testing.T) {
	activation, _, intent, _, _ := nativeActivityHandoffFixture(t)
	toolID, declaration, err := activation.Plan.ConnectorDeclaration("deliver")
	if err != nil {
		t.Fatal(err)
	}
	base, site := nativeActivityDeclarationFixture(t, toolID, declaration)
	schema, _ := base.FlowSchemaByID(".")
	source := sessionActivityProjectionSource{Source: base, schema: schema}
	publication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{activation})
	if err != nil {
		t.Fatal(err)
	}
	empty, err := channelonboarding.NewChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := channelactivation.NewOwner(empty)
	if err != nil {
		t.Fatal(err)
	}
	// Projection-only admission: this owner has no SDK issuer or launch facade.
	admit := func(ctx context.Context, _ channelonboarding.ChannelActivationPublication) error { return ctx.Err() }
	if err := owner.ReplaceAdmittedContext(context.Background(), publication, admit); err != nil {
		t.Fatal(err)
	}
	pc := &PipelineCoordinator{module: staticSemanticWorkflowModule{source: source}, channelActivations: owner}
	intent.Tool, intent.ActivityID = toolID, contracts.ActivityResultEventsForSite(site).ActivityID
	intent.Owner, intent.HandlerEventKey = activityidentity.MustNodeOwner(site.Node), site.HandlerEventKey
	intent.PlanGeneration, intent.ChannelActivationGeneration = plangeneration.Generation{}, channelonboarding.ChannelActivationGeneration{}
	fact, err := correlation.NewSourceArtifactFact(activation.Coordinate.BundleHash)
	if err != nil {
		t.Fatal(err)
	}
	ctx := correlation.WithSourceArtifactFact(context.Background(), fact)
	prepared, release, err := pc.prepareSessionActivityIntents(ctx, []engine.ActivityIntent{intent})
	if err != nil {
		t.Fatal(err)
	}
	release()
	selected := prepared[0]
	if selected.Tool != toolID || selected.NativeSessionTarget == "" || !selected.PlanGeneration.Valid() || !selected.ChannelActivationGeneration.Equal(publication.Generation()) ||
		selected.BundleHash != fact.BundleHash() || selected.WorkflowVersion != source.WorkflowVersion() || !selected.Input.Equal(intent.Input) {
		t.Fatal("preparation did not freeze the authored source/target/input", selected)
	}
	lease, found := owner.AcquireActivityOperation(selected.NativeSessionTarget, selected.ChannelActivationGeneration)
	if !found {
		t.Fatal("frozen private target was not installed")
	}
	if _, err := pc.validateAuthoredSessionActivity(ctx, selected, lease); err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if _, _, err := pc.prepareSessionActivityIntents(context.Background(), []engine.ActivityIntent{intent}); err == nil {
		t.Fatal("missing source authority selected a session")
	}
	activation.SessionAccount.AdmissionID = uuid.NewString()
	replacement, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{activation})
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.ReplaceAdmittedContext(ctx, replacement, admit); err != nil {
		t.Fatal(err)
	}
	if _, _, err := pc.prepareSessionActivityIntents(ctx, []engine.ActivityIntent{selected}); err == nil {
		t.Fatal("frozen activity adopted a successor publication")
	}
}

func (s sessionActivityProjectionSource) FlowSchemaByID(flowID string) (contracts.FlowSchemaDocument, bool) {
	return s.schema, flowID == "."
}

func TestSessionActivityProjectionRejectsMissingAndAmbiguousDeclarations(t *testing.T) {
	activation, _, _, _, _ := nativeActivityHandoffFixture(t)
	toolID, declaration, err := activation.Plan.ConnectorDeclaration("deliver")
	if err != nil {
		t.Fatal(err)
	}
	base, site := nativeActivityDeclarationFixture(t, toolID, declaration)
	publication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{activation})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"missing", "foreign_provider", "duplicate", "raw", "unknown_kind", "foreign_pack", "empty_pack"} {
		t.Run(name, func(t *testing.T) {
			schema, _ := base.FlowSchemaByID(".")
			provider := schema.Ingress.Providers[0]
			providers := []contracts.ProjectFlowIngressProvider{provider}
			switch name {
			case "missing":
				providers = nil
			case "foreign_provider":
				providers[0].Provider = "telegram"
			case "duplicate":
				providers = append(providers, provider)
			case "raw":
				providers[0].Admission.Kind = "raw"
			case "unknown_kind":
				providers[0].Admission.Kind = "authored"
			case "foreign_pack":
				providers[0].Admission.Pack = &contracts.ProjectFlowIngressAdmissionPack{ID: "foreign"}
			case "empty_pack":
				providers[0].Admission.Pack = &contracts.ProjectFlowIngressAdmissionPack{}
			}
			schema.Ingress = &contracts.ProjectFlowIngress{Alias: "whatsapp", Providers: providers}
			source := sessionActivityProjectionSource{Source: base, schema: schema}
			if _, err := channelonboarding.CompileSessionActivityTarget(source, activation.Coordinate.BundleHash, site, publication); err == nil {
				t.Fatal("contradictory explicit ingress selected a native session")
			}
		})
	}
	schema, _ := base.FlowSchemaByID(".")
	schema.Ingress = &contracts.ProjectFlowIngress{Alias: "whatsapp", Providers: []contracts.ProjectFlowIngressProvider{{Provider: "whatsapp"}}}
	if _, err := channelonboarding.CompileSessionActivityTarget(sessionActivityProjectionSource{Source: base, schema: schema}, activation.Coordinate.BundleHash, site, publication); err != nil {
		t.Fatal("existing default pack admission was replaced with new authoring syntax", err)
	}
}

func TestSessionActivityProjectionKeepsSameProviderSiblingsDistinct(t *testing.T) {
	activation, _, _, _, _ := nativeActivityHandoffFixture(t)
	toolID, declaration, err := activation.Plan.ConnectorDeclaration("deliver")
	if err != nil {
		t.Fatal(err)
	}
	source, site := nativeActivityDeclarationFixture(t, toolID, declaration)
	sibling := activation
	sibling.OnboardingOperationID = uuid.NewString()
	sibling.SessionAccount.ConnectionID = uuid.NewString()
	sibling.SessionAccount.AccountRef = "15551234569@s.whatsapp.net"
	sibling.SessionAccount.AdmissionID = uuid.NewString()
	sibling.Plan, err = packs.NewOutboundBindingPlanWithRegistration("sibling", nativeActivityStructuralPlan(t), "15551234570@s.whatsapp.net", nil, nil, "ingress:other:whatsapp")
	if err != nil {
		t.Fatal(err)
	}
	for _, activations := range [][]channelonboarding.CompiledActivation{{activation, sibling}, {sibling, activation}, {sibling}} {
		publication, err := channelonboarding.NewChannelActivationPublication(activations)
		if err != nil {
			t.Fatal(err)
		}
		projection, err := channelonboarding.CompileSessionActivityTarget(source, activation.Coordinate.BundleHash, site, publication)
		if len(activations) == 1 {
			if err == nil {
				t.Fatal("same-provider sibling became a fallback target")
			}
			continue
		}
		if err != nil || projection.Activation().SessionAccount != activation.SessionAccount || projection.Activation().OnboardingOperationID != activation.OnboardingOperationID {
			t.Fatal("provider order replaced the explicit original target", err)
		}
	}
	sibling.Plan, err = packs.NewOutboundBindingPlanWithRegistration("duplicate", nativeActivityStructuralPlan(t), "15551234570@s.whatsapp.net", nil, nil, "ingress:.:whatsapp")
	if err != nil {
		t.Fatal(err)
	}
	publication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{activation, sibling})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := channelonboarding.CompileSessionActivityTarget(source, activation.Coordinate.BundleHash, site, publication); err == nil {
		t.Fatal("ambiguous original responsibilities used first-match authority")
	}
}

func TestSessionActivityProjectionJoinsExactDeclarationAndIngress(t *testing.T) {
	activation, _, _, _, _ := nativeActivityHandoffFixture(t)
	toolID, declaration, err := activation.Plan.ConnectorDeclaration("deliver")
	if err != nil {
		t.Fatal(err)
	}
	source, site := nativeActivityDeclarationFixture(t, toolID, declaration)
	publication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{activation})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := channelonboarding.CompileSessionActivityTarget(source, activation.Coordinate.BundleHash, site, publication)
	if err != nil {
		t.Fatal(err)
	}
	selected := projection.Activation()
	if selected.OnboardingOperationID != activation.OnboardingOperationID || selected.SessionAccount != activation.SessionAccount ||
		selected.Coordinate != activation.Coordinate || projection.Operation() != "deliver" || projection.AuthoredToolID() != site.Spec.Tool ||
		!projection.PublicationGeneration().Equal(publication.Generation()) {
		t.Fatal("compiler lost the exact source/connector/ingress responsibility", projection)
	}
	private, err := activation.Plan.RuntimeActivityTarget("deliver")
	if err != nil || projection.PrivateTarget().ToolID() != private.ToolID() || !projection.PrivateTarget().Generation().Equal(private.Generation()) {
		t.Fatal("compiler did not retain the exact private target", err)
	}
	if _, err := channelonboarding.CompileSessionActivityTarget(source, "bundle-v2:sha256:"+strings.Repeat("b", 64), site, publication); err == nil {
		t.Fatal("foreign source selected a session")
	}
	foreignSite := site
	foreignSite.Spec.Tool = "invented.send_text"
	if _, err := channelonboarding.CompileSessionActivityTarget(source, activation.Coordinate.BundleHash, foreignSite, publication); err == nil {
		t.Fatal("invented declaration selected a session")
	}
	foreignSite = site
	foreignSite.HandlerEventKey = "invented"
	if _, err := channelonboarding.CompileSessionActivityTarget(source, activation.Coordinate.BundleHash, foreignSite, publication); err == nil {
		t.Fatal("invented activity site selected a session")
	}
	empty, err := channelonboarding.NewChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := channelonboarding.CompileSessionActivityTarget(source, activation.Coordinate.BundleHash, site, empty); err == nil {
		t.Fatal("missing activation selected an ambient session")
	}
}

func TestSessionActivityProjectionKeepsDeclarationAndChannelResultMappingDistinct(t *testing.T) {
	activation, compiled, _, _, _ := nativeActivityHandoffFixture(t)
	id, declaration, err := activation.Plan.ConnectorDeclaration("deliver")
	if err != nil {
		t.Fatal(err)
	}
	if _, mapped := declaration.CompiledResultExecution(); mapped {
		t.Fatal("connector declaration acquired channel result semantics")
	}
	if _, mapped := compiled.CompiledResultExecution(); !mapped {
		t.Fatal("compiled channel operation lost its result mapping")
	}
	publication, err := channelonboarding.NewChannelActivationPublication([]channelonboarding.CompiledActivation{activation})
	if err != nil {
		t.Fatal(err)
	}
	source, site := nativeActivityDeclarationFixture(t, id, declaration)
	if _, err := channelonboarding.CompileSessionActivityTarget(source, activation.Coordinate.BundleHash, site, publication); err != nil {
		t.Fatal("exact imported connector was rejected after legitimate channel compilation", err)
	}
	alteredOutput, err := declaration.WithSchemas(declaration.InputSchema(), contracts.MustToolInputSchema(contracts.ToolSchemaObject))
	if err != nil {
		t.Fatal(err)
	}
	for _, altered := range []contracts.ToolSchemaEntry{compiled, alteredOutput} {
		source, site := nativeActivityDeclarationFixture(t, id, altered)
		if _, err := channelonboarding.CompileSessionActivityTarget(source, activation.Coordinate.BundleHash, site, publication); err == nil {
			t.Fatal("altered connector declaration acquired the original session")
		}
	}
}
