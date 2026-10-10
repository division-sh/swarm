package serveapp

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestServeSessionRuntimeSelectionRejectsUndeclaredCandidate(t *testing.T) {
	// The loaded source has no session ingress. A valid runtime coordinate alone
	// must not install a caller-supplied channel/trigger at that source.
	bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repoRootForTest(), canonicalrouting.ExampleRoot(t, canonicalrouting.RootIngress), runtimePlatformSpecPath(t), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	fact := sourceartifactfixture.Fact()
	owner := newSupervisorTestRuntimeOccurrence(t, fact.BundleHash())
	bus, err := runtimebus.NewEphemeralEventBusWithOptions(&processIngressEventStore{}, runtimebus.EventBusOptions{
		WorkOwner: owner, ContractBundle: source, SourceArtifactFact: fact, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := runtimepkg.NewRuntimeContextManager(nil, completeServeTestPackContext(t, runtimepkg.BundleContext{
		Source: source, SourceArtifactFact: fact, WorkOwner: owner,
		BundleIdentity: contracts.BundleIdentity{WorkflowName: "root", WorkflowVersion: "1", BundleHash: fact.BundleHash()},
		Runtime:        &runtimepkg.Runtime{Bus: bus, ExecutionPosture: executionposture.Live, Options: runtimepkg.RuntimeOptions{RuntimeInstanceID: owner.Identity().RuntimeInstanceID}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := manager.QuiesceAllRuntimeContexts(context.Background()); err != nil {
			t.Error(err)
		}
	})
	loaded := manager.LoadedContexts()[0]
	catalog := sessionDiscoveryCatalog(t)
	plan := packfixture.WhatsAppSessionChannel(t, runtimePlatformSpecPath(t), catalog)
	trigger, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: "reception", Provider: "whatsapp"})
	if err != nil {
		t.Fatal(err)
	}
	candidates, err := serveChannelCandidatesForPlan(loaded, "undeclared-session", []runtimepkg.StandingTargetDeclaration{{
		FlowPath: ".", Alias: "reception", Ingress: []runtimepkg.StandingIngressBinding{{Provider: "whatsapp", AdmissionPlan: trigger}},
	}}, plan)
	if err != nil || len(candidates) != 1 {
		t.Fatal(candidates, err)
	}
	owned, incoming, release, err := serveSessionBootstrapRuntimeSelector(manager)(context.Background(), candidates[0])
	if release != nil {
		if err := release(); err != nil {
			t.Fatal(err)
		}
	}
	if err == nil || owned != nil || incoming != nil {
		t.Fatal("runtime coordinate alone admitted an undeclared session channel")
	}
	if !errors.Is(err, channelonboarding.ErrRevisionConflict) {
		t.Fatal("selection failed outside the declaration boundary", err)
	}
	caller, cancel := context.WithCancel(context.Background())
	cancel()
	if owned, incoming, release, err := serveSessionBootstrapRuntimeSelector(manager)(caller, candidates[0]); !errors.Is(err, context.Canceled) || owned != nil || incoming != nil || release != nil {
		t.Fatal("canceled caller acquired session ownership", err)
	}
	if owned, incoming, release, err := serveSessionBootstrapRuntimeSelector(manager)(nil, candidates[0]); !errors.Is(err, channelonboarding.ErrInvalidRequest) || owned != nil || incoming != nil || release != nil {
		t.Fatal("nil caller acquired session ownership", err)
	}
	join, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := owner.WaitForQuiescence(join); err != nil {
		t.Fatal("failed selection retained finite runtime work", err)
	}
}

func runtimePlatformSpecPath(t testing.TB) string {
	t.Helper()
	return filepath.Join(repoRootForTest(), defaultPlatformSpecPath)
}

func TestServeSessionIncomingSelectionConsumesExactCompiledOwners(t *testing.T) {
	catalog := sessionDiscoveryCatalog(t)
	plan := packfixture.WhatsAppSessionChannel(t, runtimePlatformSpecPath(t), catalog)
	var declarations []runtimepkg.StandingTargetDeclaration
	for _, flow := range []string{".", "support"} {
		alias := "reception"
		if flow != "." {
			alias = "support"
		}
		trigger, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: alias, Provider: "whatsapp"})
		if err != nil {
			t.Fatal(err)
		}
		declarations = append(declarations, runtimepkg.StandingTargetDeclaration{FlowPath: flow, Alias: alias,
			Ingress: []runtimepkg.StandingIngressBinding{{Provider: "whatsapp", AdmissionPlan: trigger}}})
	}
	definition := runtimepkg.BundleContext{SourceArtifactFact: sourceartifactfixture.Fact(), RuntimeInstanceID: uuid.NewString(),
		PublicationGeneration: 1, PackInventoryDigest: "sha256:incoming-selection", ChannelPlans: []packs.SatisfactionPlan{plan},
		BundleIdentity: contracts.BundleIdentity{WorkflowName: "incoming", WorkflowVersion: "1", BundleHash: sourceartifactfixture.BundleHash}}
	// This is a compiled-selection component proof. The manager/source test above
	// separately proves that an undeclared plan cannot reach this projection.
	definition.Runtime = &runtimepkg.Runtime{Bus: &runtimebus.EventBus{}, ExecutionPosture: executionposture.Live}
	candidates, err := serveChannelContextCandidates(definition, declarations)
	if err != nil || len(candidates) != 2 {
		t.Fatal(candidates, err)
	}
	declared, err := channelonboarding.NewCandidateCatalog(candidates)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range candidates {
		if _, found := declared.FindExactDeclaration(candidate.Provider, candidate.Interface, candidate.Coordinate, candidate.Target.Selector); !found {
			t.Fatal("canonical declaration lookup demanded executable authority")
		}
		if _, found := declared.FindExact(candidate.Provider, candidate.Interface, candidate.Coordinate, candidate.Target.Selector); found {
			t.Fatal("declaration entered the executable lookup")
		}
	}
	for _, reverse := range []bool{false, true} {
		ordered := append([]runtimepkg.StandingTargetDeclaration(nil), declarations...)
		if reverse {
			ordered[0], ordered[1] = ordered[1], ordered[0]
		}
		for _, candidate := range candidates {
			incoming, err := serveSessionIncomingSelection(definition, ordered, candidate)
			if err != nil || incoming == nil || incoming.Bus != definition.Runtime.Bus || incoming.Posture != executionposture.Live ||
				incoming.Alias != candidate.Target.Alias || incoming.Trigger.Provider() != "whatsapp" ||
				incoming.Trigger.Transport() != packs.ChannelTransportSession || !incoming.Trigger.Generation().Equal(candidate.Target.AdmissionGeneration) ||
				candidate.Target.Generation != 0 || candidate.Target.PublicationSequence != 0 {
				t.Fatal("selection dropped scope or manufactured executable readiness", incoming, err)
			}
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*channelonboarding.Candidate)
	}{
		{"alias", func(c *channelonboarding.Candidate) { c.Target.Alias = "foreign" }},
		{"service", func(c *channelonboarding.Candidate) { c.Target.ServiceID = "foreign" }},
		{"flow", func(c *channelonboarding.Candidate) { c.Target.FlowPath = "support" }},
		{"selector", func(c *channelonboarding.Candidate) { c.Target.Selector = "ingress:support:whatsapp" }},
		{"provider", func(c *channelonboarding.Candidate) { c.Provider = "discord" }},
		{"target generation", func(c *channelonboarding.Candidate) { c.Target.Generation = 1; c.Target.PublicationSequence = 1 }},
		{"signing", func(c *channelonboarding.Candidate) { c.Target.SigningCredentialKey = "secret" }},
		{"trigger generation", func(c *channelonboarding.Candidate) { c.Target.AdmissionGeneration = triggergeneration.Generation{} }},
		{"source", func(c *channelonboarding.Candidate) { c.Coordinate.BundleHash = serveRuntimeTestBundleHash }},
		{"runtime", func(c *channelonboarding.Candidate) { c.Coordinate.RuntimeInstanceID = uuid.NewString() }},
		{"publication", func(c *channelonboarding.Candidate) { c.Coordinate.ContextPublicationGeneration++ }},
		{"inventory", func(c *channelonboarding.Candidate) { c.Coordinate.PackInventoryGeneration = "foreign" }},
		{"ceremony", func(c *channelonboarding.Candidate) { c.Ceremony = "foreign" }},
		{"confirmation", func(c *channelonboarding.Candidate) { c.ConfirmationOperation = "foreign" }},
		{"health", func(c *channelonboarding.Candidate) { c.ConnectionHealth = "foreign" }},
		{"provider role", func(c *channelonboarding.Candidate) { c.ProviderCredentialRole = "foreign" }},
		{"signing role", func(c *channelonboarding.Candidate) { c.SigningCredentialRole = "foreign" }},
		{"missing plan", func(c *channelonboarding.Candidate) { c.Plan = packs.SatisfactionPlan{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := candidates[0]
			tc.mutate(&candidate)
			if incoming, err := serveSessionIncomingSelection(definition, declarations, candidate); err == nil || incoming != nil {
				t.Fatal("contradictory candidate supplied incoming owners", incoming, err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func(*runtimepkg.BundleContext)
	}{
		{"no runtime", func(c *runtimepkg.BundleContext) { c.Runtime = nil }},
		{"no bus", func(c *runtimepkg.BundleContext) {
			c.Runtime = &runtimepkg.Runtime{ExecutionPosture: executionposture.Live}
		}},
		{"no posture", func(c *runtimepkg.BundleContext) { c.Runtime = &runtimepkg.Runtime{Bus: definition.Runtime.Bus} }},
		{"no channel", func(c *runtimepkg.BundleContext) { c.ChannelPlans = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broken := definition
			tc.mutate(&broken)
			if incoming, err := serveSessionIncomingSelection(broken, declarations, candidates[0]); err == nil || incoming != nil {
				t.Fatal("incomplete runtime supplied incoming owners", incoming, err)
			}
		})
	}
	for _, declarations := range [][]runtimepkg.StandingTargetDeclaration{nil, {declarations[0], declarations[0]}} {
		if incoming, err := serveSessionIncomingSelection(definition, declarations, candidates[0]); err == nil || incoming != nil {
			t.Fatal("missing/duplicate declaration supplied incoming owners", incoming, err)
		}
	}
}
