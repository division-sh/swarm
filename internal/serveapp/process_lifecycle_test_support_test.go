package serveapp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedataaccess "github.com/division-sh/swarm/internal/runtime/dataaccess"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func runtimeDepsForServeTest(t testing.TB, stores *selectedStoreOwner, cfg *config.Config, options runtimepkg.RuntimeOptions) runtimepkg.RuntimeDeps {
	t.Helper()
	if options.WorkflowModule != nil {
		if bundle, ok := semanticview.Bundle(options.WorkflowModule.SemanticSource()); ok && bundle != nil && bundle.PackInventory != nil && bundle.PackAdmission == nil {
			projection, err := packadmission.Admit(bundle.PackInventory, bundle.Platform)
			if err != nil {
				t.Fatalf("admit serve test pack projection: %v", err)
			}
			bundle.PackAdmission = projection
		}
	}
	if !options.ExecutionPosture.Valid() {
		options.ExecutionPosture = executionposture.Live
	}
	if options.ProviderCredentials == nil {
		options.ProviderCredentials = processIngressCredentialStore{}
	}
	deps := stores.RuntimeDeps()
	deps.Config = cfg
	deps.Options = options
	return deps
}

func testPlatformPackBaseGenerations(t *testing.T) *packartifact.PlatformPackBaseGenerationOwner {
	t.Helper()
	owner, err := packartifact.NewPlatformPackBaseGenerationOwner(packfixture.EmbeddedBase(t))
	if err != nil {
		t.Fatal(err)
	}
	return owner
}

func runtimeContextTestHash(fill string) string {
	return "bundle-v2:sha256:" + strings.Repeat(fill, 64)
}

func newSupervisorTestRuntimeOccurrence(t *testing.T, bundleHash string) *worklifetime.RuntimeOccurrence {
	t.Helper()
	process := worklifetime.NewProcess()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "11111111-1111-4111-8111-111111111111",
		BundleHash:        bundleHash,
	})
	if err != nil {
		t.Fatalf("create process test runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(ctx); err != nil {
			t.Errorf("retire process test runtime occurrence: %v", err)
		}
		process.Retire()
		if _, err := process.Join(ctx); err != nil {
			t.Errorf("join process test owner: %v", err)
		}
	})
	return owner
}

func newSupervisorTestProcessOwner(t *testing.T) *worklifetime.Process {
	t.Helper()
	owner := worklifetime.NewProcess()
	t.Cleanup(func() {
		owner.Retire()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.Join(ctx); err != nil {
			t.Errorf("join process test owner: %v", err)
		}
	})
	return owner
}

type stubWorkflowModule struct{ source semanticview.Source }

func (m stubWorkflowModule) SemanticSource() semanticview.Source         { return m.source }
func (stubWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode { return nil }
func (stubWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry  { return nil }

type stubWorkspaceLifecycle struct {
	validateErr error
	prereqErr   error
	systemErr   error
}

func (s stubWorkspaceLifecycle) ResolveWorkspace(context.Context, runtimeactors.AgentConfig) (*workspace.Target, error) {
	return nil, nil
}
func (s stubWorkspaceLifecycle) ResolveWorkspaceForCapabilityAdmission(context.Context, runtimeactors.AgentConfig) (*workspace.Target, error) {
	return nil, nil
}
func (s stubWorkspaceLifecycle) ValidateSource(context.Context, semanticview.Source) error {
	return s.validateErr
}
func (s stubWorkspaceLifecycle) EnsurePrereqs(context.Context) error { return s.prereqErr }
func (s stubWorkspaceLifecycle) EnsureSystemWorkspaces(context.Context) error {
	return s.systemErr
}
func (stubWorkspaceLifecycle) BindSourceProjection(*sourceartifact.RuntimeProjection) error {
	return nil
}
func (stubWorkspaceLifecycle) ReleaseSourceProjection(context.Context) error        { return nil }
func (stubWorkspaceLifecycle) SetDataProjectionProvider(runtimedataaccess.Provider) {}

type processIngressCredentialStore map[string]string

func (s processIngressCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	value, ok := s[key]
	return value, ok, nil
}
func (processIngressCredentialStore) Set(context.Context, string, string) error { return nil }
func (processIngressCredentialStore) List(context.Context) ([]string, error)    { return nil, nil }
func (processIngressCredentialStore) Delete(context.Context, string) error      { return nil }
func (s processIngressCredentialStore) Snapshot(ctx context.Context, key string) (runtimecredentials.AtomicSnapshot, error) {
	value, present, err := s.Get(ctx, key)
	return runtimecredentials.NewAtomicSnapshot(runtimecredentials.Metadata{Key: key, Present: present}, value), err
}

type processIngressProofStore struct {
	recorded  bool
	store     runtimebus.EventStore
	lastError error
}

func (s *processIngressProofStore) CommitInboundPublication(ctx context.Context, command runtimeinbound.CommitCommand) (runtimeinbound.CommitResult, error) {
	if err := command.Validate(); err != nil {
		s.lastError = err
		return runtimeinbound.CommitResult{}, err
	}
	s.recorded = true
	owner, ok := s.store.(runtimebus.CommitPublicationOwner)
	if !ok {
		return runtimeinbound.CommitResult{}, errors.New("process ingress store does not implement closed publication commit")
	}
	committed := make([]runtimebus.CommittedPublication, len(command.Publications))
	children := make([]runtimeinbound.EventRecord, len(command.Finalization.Events))
	for i, finalized := range command.Finalization.Events {
		var err error
		committed[i], err = owner.CommitPublication(ctx, command.Publications[i])
		if err != nil {
			s.lastError = err
			return runtimeinbound.CommitResult{}, err
		}
		eventFingerprint, err := runtimeinbound.EventIntegrityFingerprint(finalized.Event, finalized.Kind, finalized.Authorization)
		if err != nil {
			return runtimeinbound.CommitResult{}, err
		}
		children[i] = runtimeinbound.EventRecord{
			Ordinal: finalized.Ordinal, EventID: finalized.Event.ID(), EventName: string(finalized.Event.Type()),
			Kind: finalized.Kind, Authorization: finalized.Authorization, EventIntegrityFingerprint: eventFingerprint,
			RecipientManifestFingerprint: strings.Repeat("0", 64), RecipientCount: len(command.Publications[i].Commit.DeliveryRoutes),
			Event: finalized.Event,
		}
	}
	return runtimeinbound.CommitResult{
		Acknowledged: true,
		Record:       runtimeinbound.Record{Request: command.Request, State: "committed", OutputCount: len(children), Events: children, Created: true},
		Publications: committed,
	}, nil
}

func (*processIngressProofStore) LoadInboundPublicationByIdentity(context.Context, runtimeinbound.Identity) (runtimeinbound.Record, bool, error) {
	return runtimeinbound.Record{}, false, nil
}

func (*processIngressProofStore) ValidateInboundPublicationIntegrity(context.Context) error {
	return nil
}

type processIngressEventStore struct {
	events      []events.Event
	observation runtimepipeline.FlowInstanceObservation
}

// These transport fixtures admit one exact root; they do not simulate runtime
// attachment or substitute for the native two-store lifecycle proofs.
func newProcessIngressEventStore(t testing.TB, source semanticview.Source, runID string) *processIngressEventStore {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("transport construction requires its admitted source")
	}
	fact := mustServeTestEphemeralSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	flowID := semanticview.RootExecutionFlowID(source)
	identity, err := runtimeflowidentity.StandingForGeneration(source, flowID, runID)
	if err != nil {
		t.Fatal(err)
	}
	request, err := runtimepipeline.NewExactFlowInstanceLookup(source, fact, runtimeflowidentity.RunScopedFlowInstance{RunID: runID, Route: identity.Route()})
	if err != nil {
		t.Fatal(err)
	}
	schema, found := source.FlowSchemaByID(flowID)
	if !found || !schema.Instance.Empty() {
		t.Fatal("transport construction requires its declared keyless root")
	}
	topology, found := semanticview.WorkflowStageTopology(source, flowID)
	if !found {
		t.Fatal("transport construction requires its compiled stage catalog")
	}
	initial, err := topology.InitialStoredStage()
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1700000000, 0).UTC()
	header := runtimepipeline.WorkflowInstance{
		WorkflowName: flowID, WorkflowVersion: source.WorkflowVersion(), Mode: schema.EffectiveMode(), Status: "active",
		InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityID: identity.EntityID,
		CurrentState: initial.ID(), StageDefined: !initial.IsStatelessPosture(), Revision: 1, CreatedAt: at, UpdatedAt: at,
	}
	if entity, declared := entityruntime.ResolveForFlow(source, flowID); declared {
		header.EntityType = entity.EntityType
	}
	origin, err := runlifecycle.StandingGenerationRunOrigin(runtimeflowidentity.StandingServiceID(flowID), 1)
	if err != nil {
		t.Fatal(err)
	}
	run := runlifecycle.Snapshot{RunID: runID, State: runlifecycle.StateRunning, Origin: origin, BundleHash: fact.BundleHash(), StartedAt: at}
	receipt := runtimepipeline.FlowConstructionPublicationEvidence{Identity: identity}
	readiness := runtimepipeline.DynamicFlowRuntimeReadiness{
		Plan:            runtimepipeline.DynamicFlowRuntimeReadinessPlan{Identity: identity, RunID: runID, BundleHash: fact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live},
		OwningRunSource: fact, RunStatus: "running", InstanceStatus: "active",
	}
	observed, err := runtimepipeline.AdmitNativeFlowInstanceObservation(request, header, run, 1, receipt, readiness)
	if err != nil {
		t.Fatal(err)
	}
	return &processIngressEventStore{observation: observed}
}

func (s *processIngressEventStore) LookupFlowInstance(ctx context.Context, request runtimepipeline.FlowInstanceLookupRequest) (runtimepipeline.FlowInstanceObservation, bool, error) {
	if err := ctx.Err(); err != nil {
		return runtimepipeline.FlowInstanceObservation{}, false, err
	}
	identity := s.observation.Identity()
	if !s.observation.Valid() || s.observation.Owner().RunID != request.RunID() || identity.TemplateID != request.FlowID() {
		return runtimepipeline.FlowInstanceObservation{}, false, nil
	}
	if err := s.observation.ValidateSelection(request); err != nil {
		return runtimepipeline.FlowInstanceObservation{}, false, err
	}
	return s.observation, true, nil
}

func (s *processIngressEventStore) ListFlowInstances(ctx context.Context, scope runtimepipeline.FlowInstanceLookupScope) ([]runtimepipeline.FlowInstanceObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	observed := s.observation
	if !observed.Valid() || observed.Owner().RunID != scope.RunID() {
		return nil, nil
	}
	if !slices.Contains(scope.FlowIDs(), observed.Identity().TemplateID) && !slices.Contains(scope.Coordinates(), observed.Owner()) {
		return nil, nil
	}
	request, err := runtimepipeline.NewExactFlowInstanceLookup(scope.Source(), scope.SourceFact(), observed.Owner())
	if err != nil {
		return nil, err
	}
	if err := observed.ValidateSelection(request); err != nil {
		return nil, err
	}
	return []runtimepipeline.FlowInstanceObservation{observed}, nil
}

func (s *processIngressEventStore) LoadFlowConstructionPublication(ctx context.Context, owner runtimeflowidentity.RunScopedFlowInstance, entityID string) (runtimepipeline.FlowConstructionPublicationEvidence, error) {
	if err := ctx.Err(); err != nil {
		return runtimepipeline.FlowConstructionPublicationEvidence{}, err
	}
	identity := s.observation.Identity()
	if !s.observation.Valid() || s.observation.Owner() != owner || identity.EntityID != entityID {
		return runtimepipeline.FlowConstructionPublicationEvidence{}, errors.New("transport fixture has no construction observation for that owner")
	}
	receipt, native, err := s.observation.NativeConstruction()
	if err != nil || !native {
		return runtimepipeline.FlowConstructionPublicationEvidence{}, errors.New("transport fixture has no native construction")
	}
	return receipt, nil
}

func (s *processIngressEventStore) CommitPublication(_ context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	if err := command.Validate(); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	event := command.Commit.Event.Event()
	for _, existing := range s.events {
		if existing.ID() != event.ID() {
			continue
		}
		if !reflect.DeepEqual(existing, event) {
			return runtimebus.CommittedPublication{}, fmt.Errorf("event %s conflicts with its committed fixture", event.ID())
		}
		return runtimebus.CommittedPublication{Acknowledged: true, AppendOutcome: runtimebus.EventAppendExactDuplicate}, nil
	}
	s.events = append(s.events, event)
	return runtimebus.CommittedPublication{Acknowledged: true, AppendOutcome: runtimebus.EventAppendInserted}, nil
}

func (s *processIngressEventStore) LoadPreparedPublishEvent(context.Context, string) (events.AdmittedEvent, bool, error) {
	return events.AdmittedEvent{}, false, nil
}

func (*processIngressEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, nil
}

func completeServeTestPackContext(t testing.TB, contextDef runtimepkg.BundleContext) runtimepkg.BundleContext {
	t.Helper()
	bundle, ok := semanticview.Bundle(contextDef.Source)
	if !ok || bundle == nil || bundle.PackInventory == nil {
		return contextDef
	}
	catalog, _, err := providertriggers.NewCatalogSnapshotFromInventory(
		bundle.PackInventory,
		strings.TrimSpace(bundle.Platform.Platform.Version),
	)
	if err != nil {
		t.Fatalf("derive test runtime-context provider-trigger catalog: %v", err)
	}
	subjects, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatalf("derive test runtime-context installed provider-trigger subjects: %v", err)
	}
	contextDef.PackInventoryDigest = bundle.PackInventory.Digest()
	contextDef.ProviderTriggerGeneration = catalog.Generation()
	contextDef.InstalledTriggerSubjects = subjects
	return contextDef
}
