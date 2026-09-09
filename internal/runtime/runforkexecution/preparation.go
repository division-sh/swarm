package runforkexecution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/durabledata"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/google/uuid"
)

// PreparedSelectedFork has no executable method, actor, grant or occurrence.
// Only the selected orchestration owner can consume it with real materialization.
type PreparedSelectedFork struct {
	owner                 SelectedContractExecutionOwner
	bindMu                sync.Mutex
	closed                bool
	closeErr              error
	loadedSource          LoadedSelectedContractSource
	plan                  runfork.RunForkPlan
	frontier              runfork.RunForkContractFrontierAdmission
	routeAdmission        runfork.RunForkSelectedContractRouteAdmission
	routeTopology         runfork.RunForkSelectedContractRouteTopology
	model                 runfork.RunForkSelectedContractExecution
	agentRuntime          selectedContractAgentRuntimePlan
	readiness             runforkreadiness.Admission
	deferredWorkAdmission selectedContractDeferredWorkAdmission
	dataPinOverrides      []durabledata.ExplicitPin
	descriptorLease       *runtimeauthoractivity.EventCatalogLease
	operation             *selectedContractOperation
	coordinates           managedcapabilities.SelectedForkPreparationCoordinates
	preparationID         string
	processGeneration     uint64
	sourceRunID           string
	forkEventID           string
	declarationRevision   string
	actors                []runfork.SelectedForkPreparedActor
	catalog               *runtimepkg.PreparedSelectedForkProviderCatalog
	bound                 bool
	bindingFingerprint    string
}

// Prepare admits process-owned, non-executable selected work. The caller must
// Close it on every path; only the execution owner can bind it to concrete work.
func (o SelectedContractExecutionOwner) Prepare(ctx context.Context, req SelectedContractExecutionRequest) (_ *PreparedSelectedFork, finalErr error) {
	ports, err := o.require()
	if err != nil {
		return nil, err
	}
	if req.SourceLoader == nil {
		return nil, fmt.Errorf("selected-contract execution requires selected source loader")
	}
	selection, err := normalizeSelectedContractExecutionSelection(req.ContractSelection)
	if err != nil {
		return nil, err
	}
	operation, err := o.beginPreparation(ctx)
	if err != nil {
		return nil, err
	}
	owned := &PreparedSelectedFork{owner: o, operation: operation}
	transferred := false
	defer func() {
		if !transferred {
			finalErr = errors.Join(finalErr, o.disposePreparation(owned))
		}
	}()
	ctx = operation.PreparationContext()
	expectedBundleHash := strings.TrimSpace(req.ExpectedBundleHash)
	if req.SourceArtifactFact.BundleHash() != "" {
		if err := req.SourceArtifactFact.Validate(); err != nil {
			return nil, fmt.Errorf("selected-contract execution expected bundle source fact is invalid: %w", err)
		}
		if expectedBundleHash != "" && expectedBundleHash != req.SourceArtifactFact.BundleHash() {
			return nil, fmt.Errorf(
				"selected-contract execution expected bundle_hash %s does not match source fact %s",
				expectedBundleHash,
				req.SourceArtifactFact.BundleHash(),
			)
		}
		expectedBundleHash = req.SourceArtifactFact.BundleHash()
	}
	loadedSource, err := loadRunForkSelectedContractSource(ctx, req.SourceLoader, SelectedContractSourceLoadRequest{
		SourceRunID:        req.SourceRunID,
		BundleHash:         expectedBundleHash,
		SourceArtifactFact: req.SourceArtifactFact,
		Selection:          selection,
	})
	if err != nil {
		return nil, fmt.Errorf("load selected semantic source for execution: %w", err)
	}
	owned.loadedSource = loadedSource
	selection = loadedSource.Selection
	if loadedSource.Module == nil {
		return nil, fmt.Errorf("selected-contract execution requires executable selected workflow module")
	}
	if err := loadedSource.SourceArtifactFact.Validate(); err != nil {
		return nil, fmt.Errorf("selected-contract source loader returned incomplete bundle identity: %w", err)
	}
	if err := loadedSource.EffectiveSourceIdentity.Validate(); err != nil {
		return nil, fmt.Errorf("selected-contract source loader returned incomplete effective source identity: %w", err)
	}
	if err := req.EffectiveSourceIdentity.Validate(); err == nil && !req.EffectiveSourceIdentity.Equal(loadedSource.EffectiveSourceIdentity) {
		return nil, fmt.Errorf("selected-contract effective source identity does not match loaded effective source")
	}
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, loadedSource.SourceArtifactFact)
	materializationBundleHash := loadedSource.SourceArtifactFact.BundleHash()
	selectedScope, err := runtimeauthoractivity.BundleScopeForTarget(ctx, materializationBundleHash)
	if err != nil {
		return nil, fmt.Errorf("resolve selected-contract author activity scope: %w", err)
	}
	ctx = runtimeauthoractivity.WithScope(ctx, selectedScope)
	operation.preparing = ctx
	operation.owned = runtimeauthoractivity.WithScope(runtimecorrelation.WithSourceArtifactFact(operation.owned, loadedSource.SourceArtifactFact), selectedScope)
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(loadedSource.Source)
	if err != nil {
		return nil, fmt.Errorf("project selected-contract author activity descriptors: %w", err)
	}
	descriptorLease, err := ports.fork.RegisterAuthorActivityEventCatalog(selectedScope, descriptors)
	if err != nil {
		return nil, fmt.Errorf("register selected-contract author activity descriptors: %w", err)
	}
	owned.descriptorLease = descriptorLease
	plan, err := ports.fork.PlanRunFork(ctx, runfork.RunForkPlanRequest{
		SourceRunID: strings.TrimSpace(req.SourceRunID),
		At:          strings.TrimSpace(req.At),
	})
	if err != nil {
		return nil, fmt.Errorf("plan selected-contract execution: %w", err)
	}
	deferredWorkAdmission, err := admitSelectedContractDeferredWork(plan, loadedSource.Source)
	if err != nil {
		return nil, err
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan:              plan,
		Source:            loadedSource.Source,
		ContractSelection: selection,
	})
	if err != nil {
		return nil, err
	}
	if frontier.FrontierEventCount == 0 {
		return nil, fmt.Errorf("selected-contract execution requires selected frontier events")
	}
	routeAdmission, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            loadedSource.Source,
		ContractSelection: selection,
		FrontierAdmission: frontier,
	})
	if err != nil {
		return nil, err
	}
	if err := validateSelectedContractExecutionFrontierForMutation(frontier); err != nil {
		return nil, err
	}
	routeTopology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
	})
	if err != nil {
		return nil, err
	}
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		return nil, err
	}
	sourceEventIDs := selectedContractExecutionFrontierEventIDs(frontier.FrontierEvents)
	agentRuntime, readiness, err := prepareSelectedContractWorkflowReadiness(
		ctx, ports.replay, loadedSource, *model.RecipientPlanning, plan, frontier, sourceEventIDs, req.AgentRuntime,
	)
	if err != nil {
		return nil, err
	}
	owned.agentRuntime = agentRuntime
	if req.AgentRuntime.ProcessCapability == nil {
		return nil, errors.New("selected-contract execution requires process capability before materialization")
	}
	if err := req.AgentRuntime.ProcessCapability.ProveCurrent(ctx); err != nil {
		return nil, fmt.Errorf("prove selected-contract process before materialization: %w", err)
	}
	prepared, err := prepareSelectedFork(ctx, operation, ports, loadedSource, plan, frontier, *model.RecipientPlanning, agentRuntime)
	if err != nil {
		return nil, err
	}

	prepared.loadedSource, prepared.descriptorLease = loadedSource, descriptorLease
	prepared.plan, prepared.frontier, prepared.routeAdmission = plan, frontier, routeAdmission
	prepared.routeTopology, prepared.model = routeTopology, model
	prepared.owner = o
	prepared.agentRuntime, prepared.readiness = agentRuntime, readiness
	prepared.deferredWorkAdmission = deferredWorkAdmission
	prepared.dataPinOverrides = append([]durabledata.ExplicitPin(nil), req.DataPinOverrides...)
	transferred = true
	return prepared, nil
}

// MaterializationRequest projects detached evidence for the named store mutation.
// It grants no execution permission; that still requires the owner's exact bind.
func (p *PreparedSelectedFork) MaterializationRequest() (runforkreadiness.MaterializeRequest, error) {
	if p == nil {
		return runforkreadiness.MaterializeRequest{}, errors.New("selected preparation is missing")
	}
	p.bindMu.Lock()
	defer p.bindMu.Unlock()
	if p.closed || p.bound || p.operation == nil {
		return runforkreadiness.MaterializeRequest{}, errors.New("selected preparation is closed or already bound")
	}
	if err := p.operation.PreparationContext().Err(); err != nil {
		return runforkreadiness.MaterializeRequest{}, err
	}
	if p.model.RecipientPlanning == nil {
		return runforkreadiness.MaterializeRequest{}, errors.New("selected preparation has no admitted recipient plan")
	}
	type materializationCoordinates struct {
		Frontier   runfork.RunForkContractFrontierAdmission
		Routes     runfork.RunForkSelectedContractRouteTopology
		Recipients runfork.RunForkSelectedContractRecipientPlanning
	}
	raw, err := json.Marshal(materializationCoordinates{p.frontier, p.routeTopology, *p.model.RecipientPlanning})
	if err != nil {
		return runforkreadiness.MaterializeRequest{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var detached materializationCoordinates
	if err := decoder.Decode(&detached); err != nil {
		return runforkreadiness.MaterializeRequest{}, err
	}
	return runforkreadiness.MaterializeRequest{
		Preparation: p.evidence(), SourceRunID: p.plan.SourceRunID, At: p.plan.ForkPoint.EventID,
		ContractSelection: p.loadedSource.Selection, SourceArtifactFact: p.loadedSource.SourceArtifactFact,
		EffectiveSourceIdentity: p.loadedSource.EffectiveSourceIdentity, FrontierAdmission: detached.Frontier,
		RouteTopology: detached.Routes, RecipientPlanning: detached.Recipients, Readiness: p.readiness,
		DataPinOverrides: append([]durabledata.ExplicitPin(nil), p.dataPinOverrides...),
		FanOutPlanRefs:   append([]runtimecontracts.FanOutPlanRef(nil), p.deferredWorkAdmission.fanOutPlanRefs...),
	}, nil
}

// Close releases only resources owned by this preparation and its selected
// occurrence. It cannot retire a loaded normal runtime or a successor.
func (p *PreparedSelectedFork) Close() (finalErr error) {
	if p == nil {
		return nil
	}
	p.bindMu.Lock()
	defer p.bindMu.Unlock()
	if p.closed {
		return p.closeErr
	}
	p.closed = true
	defer func() {
		panicked := recover()
		if panicked != nil {
			p.closeErr = errors.Join(p.closeErr, fmt.Errorf("selected preparation cleanup panicked: %v", panicked))
		}
		if p.owner.ports != nil {
			p.owner.recordPreparationClosed(p.operation, p.closeErr)
		}
		finalErr = p.closeErr
		if panicked != nil {
			panic(panicked)
		}
	}()
	if err := p.operation.retireSelected(); err != nil {
		p.closeErr = err
		return err
	}
	// Release possession last, including when a resource cleanup panics. Every
	// resource is settled before the owner records completion and wakes joiners.
	defer func() { p.closeErr = errors.Join(p.closeErr, p.operation.Finish()) }()
	defer func() {
		if p.loadedSource.Cleanup != nil {
			p.closeErr = errors.Join(p.closeErr, p.loadedSource.Cleanup())
		}
	}()
	defer p.descriptorLease.Release()
	p.closeErr = p.agentRuntime.releaseWorkspaceProjection()
	return p.closeErr
}

func prepareSelectedFork(ctx context.Context, operation *selectedContractOperation, ports *selectedContractExecutionPorts, loaded LoadedSelectedContractSource, plan runfork.RunForkPlan, frontier runfork.RunForkContractFrontierAdmission, planning runfork.RunForkSelectedContractRecipientPlanning, agentPlan selectedContractAgentRuntimePlan) (*PreparedSelectedFork, error) {
	if operation == nil || operation.preparation == nil || operation.selected != nil {
		return nil, errors.New("selected preparation requires unbound process-owned work")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options := agentPlan.Options
	if options.ProcessCapability == nil {
		return nil, errors.New("selected preparation requires process capability")
	}
	if err := options.ProcessCapability.ProveCurrent(ctx); err != nil {
		return nil, err
	}
	process, err := options.ProcessCapability.Evidence()
	if err != nil {
		return nil, err
	}
	sourceFingerprint, err := runfork.SelectedPreparationSourceFingerprint(loaded.EffectiveSourceIdentity)
	if err != nil {
		return nil, err
	}
	planFingerprint, err := runfork.SelectedPreparationPlanFingerprint(plan, frontier, planning, agentPlan.Declarations.Revision)
	if err != nil {
		return nil, err
	}
	configurationFingerprint, err := selectedPreparationFingerprint(options.Config)
	if err != nil {
		return nil, err
	}
	p := &PreparedSelectedFork{
		declarationRevision: agentPlan.Declarations.Revision,
		operation:           operation, preparationID: uuid.NewString(), processGeneration: process.AuthorityGeneration, sourceRunID: plan.SourceRunID, forkEventID: plan.ForkPoint.EventID,
		coordinates: managedcapabilities.SelectedForkPreparationCoordinates{
			ProcessAuthorityID: process.AuthorityID, ProcessOwnerID: process.OwnerID, ProcessBootID: process.BootID,
			BundleHash: loaded.SourceArtifactFact.BundleHash(), SourceFingerprint: sourceFingerprint,
			AdmittedPlanFingerprint: planFingerprint, ConfigurationFingerprint: configurationFingerprint,
		},
	}
	if len(agentPlan.Blueprints) == 0 {
		p.catalog, err = runtimepkg.PrepareSelectedForkProviderCatalog(ctx, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		p.coordinates.CatalogFingerprint = p.catalog.Fingerprint()
		p.actors = p.catalog.Actors()
		return p, p.coordinates.Validate()
	}
	if options.Config == nil {
		return nil, errors.New("selected preparation requires explicit provider configuration for its actor census")
	}
	_, profile, err := selectedContractAgentModelOptions(options)
	if err != nil {
		return nil, err
	}
	turns := mcp.NewTurnContextRegistry(actors.ActorFromContext)
	executor := newSelectedContractToolExecutor(options, loaded.Source, nil, nil, nil)
	work, err := operation.process.Begin(ctx)
	if err != nil {
		return nil, err
	}
	gateway, cleanup, err := startSelectedContractAgentRuntimeGateway(executor, turns, work, nil)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	runtimes, err := llm.NewPreparedAgentRuntimeSet(profile, llm.RuntimeFactory{
		Cfg: options.Config, Workspaces: options.Workspace,
		MCPTurns: turns, ToolGateway: gateway, Credentials: options.ProviderCredentials,
	})
	if err != nil {
		return nil, err
	}
	executor.SetModelRuntimes(runtimes)
	p.catalog, err = runtimepkg.PrepareSelectedForkProviderCatalog(ctx, runtimes, executor, agentPlan.Blueprints)
	if err != nil {
		return nil, err
	}
	p.coordinates.CatalogFingerprint = p.catalog.Fingerprint()
	probes := make([]runtimepkg.PreparedSelectedForkProviderProbe, 0, len(agentPlan.Blueprints))
	for _, blueprint := range agentPlan.Blueprints {
		fingerprint, err := blueprint.Identity.Fingerprint()
		if err != nil {
			return nil, err
		}
		probes = append(probes, runtimepkg.PreparedSelectedForkProviderProbe{Agent: blueprint,
			Authority: managedcapabilities.PreparedSelectedForkProbeAuthority{SelectedForkPreparationCoordinates: p.coordinates, ActorPlanFingerprint: fingerprint}})
	}
	receipts := &selectedPreparationSurfaceCapture{store: ports.managedCapabilities, surfaces: make(map[string]managedcapabilities.Surface)}
	ids, err := runtimepkg.ValidatePreparedSelectedForkProviderPreflight(ctx, options.Config, gateway, p.catalog, turns,
		p.preparationID, options.ProcessCapability, probes, effects.NewController(ports.effects).WithExecutionPosture(options.ExecutionPosture), receipts)
	if err != nil {
		return nil, err
	}
	p.actors = p.catalog.Actors()
	remaining := make(map[string]managedcapabilities.Surface, len(ids))
	for _, id := range ids {
		surface, ok := receipts.surface(id)
		if !ok {
			return nil, errors.New("selected preparation returned an unpersisted receipt")
		}
		remaining[id] = surface
	}
	for i := range p.actors {
		actor := &p.actors[i]
		if !actor.RequiresProbe() {
			continue
		}
		for id, surface := range remaining {
			if surface.ActorPlan == actor.Plan {
				actor.SurfaceID, actor.SurfaceIntegrity = id, surface.IntegrityHash
				delete(remaining, id)
				break
			}
		}
		if actor.SurfaceID == "" {
			return nil, fmt.Errorf("selected preparation omitted receipt for %s", actor.Plan.Description())
		}
	}
	if len(remaining) != 0 {
		return nil, errors.New("selected preparation returned receipts outside its actor census")
	}
	return p, nil
}

func (p *PreparedSelectedFork) bind(ctx context.Context, forkRunID string, loaded LoadedSelectedContractSource, agents selectedContractAgentRuntimePlan) (runfork.SelectedForkPreparationBinding, error) {
	if p == nil {
		return runfork.SelectedForkPreparationBinding{}, errors.New("selected preparation is missing")
	}
	p.bindMu.Lock()
	defer p.bindMu.Unlock()
	if p.operation == nil || p.closed || p.bound || p.operation.selected != nil {
		return runfork.SelectedForkPreparationBinding{}, errors.New("selected preparation is missing or already consumed")
	}
	if err := ctx.Err(); err != nil {
		return runfork.SelectedForkPreparationBinding{}, err
	}
	if err := p.operation.Context().Err(); err != nil {
		return runfork.SelectedForkPreparationBinding{}, err
	}
	if agents.Options.ProcessCapability == nil {
		return runfork.SelectedForkPreparationBinding{}, errors.New("selected binding requires process capability")
	}
	if err := agents.Options.ProcessCapability.ProveCurrent(ctx); err != nil {
		return runfork.SelectedForkPreparationBinding{}, err
	}
	evidence, err := agents.Options.ProcessCapability.Evidence()
	if err != nil {
		return runfork.SelectedForkPreparationBinding{}, err
	}
	if evidence.AuthorityID != p.coordinates.ProcessAuthorityID || evidence.OwnerID != p.coordinates.ProcessOwnerID || evidence.BootID != p.coordinates.ProcessBootID || evidence.AuthorityGeneration != p.processGeneration {
		return runfork.SelectedForkPreparationBinding{}, errors.New("selected preparation process changed")
	}
	fingerprint, err := runfork.SelectedPreparationSourceFingerprint(loaded.EffectiveSourceIdentity)
	if err != nil || fingerprint != p.coordinates.SourceFingerprint {
		return runfork.SelectedForkPreparationBinding{}, errors.New("selected preparation source changed")
	}
	fingerprint, err = selectedPreparationFingerprint(agents.Options.Config)
	if err != nil || fingerprint != p.coordinates.ConfigurationFingerprint {
		return runfork.SelectedForkPreparationBinding{}, errors.New("selected preparation configuration changed")
	}
	if len(agents.Records) != len(p.actors) || len(agents.Blueprints) != len(p.actors) {
		return runfork.SelectedForkPreparationBinding{}, errors.New("selected preparation materialized actor census changed")
	}
	if p.declarationRevision != agents.Declarations.Revision {
		return runfork.SelectedForkPreparationBinding{}, errors.New("selected preparation declaration plan changed")
	}
	if err := p.catalog.ValidateActors(agents.Blueprints); err != nil {
		return runfork.SelectedForkPreparationBinding{}, err
	}
	byPlan := make(map[agentidentity.Plan]manager.AgentMaterializationBlueprint, len(agents.Blueprints))
	for _, blueprint := range agents.Blueprints {
		byPlan[blueprint.Identity] = blueprint
	}
	concrete := make(map[agentidentity.Identity]manager.PersistedAgent, len(agents.Records))
	materialized := make([]manager.AgentMaterializationBlueprint, 0, len(agents.Records))
	for _, record := range agents.Records {
		identity, err := record.Config.ConcreteIdentity()
		_, duplicate := concrete[identity]
		if err != nil || duplicate {
			return runfork.SelectedForkPreparationBinding{}, errors.New("selected materialization contains invalid or duplicate actor")
		}
		plan, err := identity.Plan()
		if err != nil {
			return runfork.SelectedForkPreparationBinding{}, err
		}
		concrete[identity] = record
		materialized = append(materialized, manager.AgentMaterializationBlueprint{Identity: plan, Config: record.Config})
	}
	if err := p.catalog.ValidateActors(materialized); err != nil {
		return runfork.SelectedForkPreparationBinding{}, fmt.Errorf("selected materialized configuration: %w", err)
	}
	for _, actor := range p.actors {
		blueprint, ok := byPlan[actor.Plan]
		if !ok {
			return runfork.SelectedForkPreparationBinding{}, errors.New("selected materialization changed declaration owner")
		}
		revision, err := manager.AgentConfigPlanRevision(blueprint.Config, actor.Plan)
		if err != nil || revision != actor.ConfigurationRevision {
			return runfork.SelectedForkPreparationBinding{}, errors.New("selected materialization changed actor configuration")
		}
		identity, err := actor.Plan.Live(forkRunID)
		_, exists := concrete[identity]
		if err != nil || !exists {
			return runfork.SelectedForkPreparationBinding{}, errors.New("selected materialization differs from admitted actor correspondence")
		}
	}
	binding := runfork.SelectedForkPreparationBinding{SelectedForkPreparation: p.evidence(), ForkRunID: forkRunID}
	if err := binding.Validate(); err != nil {
		return runfork.SelectedForkPreparationBinding{}, err
	}
	p.bindingFingerprint, err = binding.Fingerprint()
	if err != nil {
		return runfork.SelectedForkPreparationBinding{}, err
	}
	p.bound = true
	return binding, nil
}

func (p *PreparedSelectedFork) evidence() runfork.SelectedForkPreparation {
	return runfork.SelectedForkPreparation{PreparationID: p.preparationID, ProcessGeneration: p.processGeneration, Coordinates: p.coordinates,
		DeclarationPlanFingerprint: p.declarationRevision,
		SourceRunID:                p.sourceRunID, ForkEventID: p.forkEventID,
		Actors: append(make([]runfork.SelectedForkPreparedActor, 0, len(p.actors)), p.actors...)}
}

func selectedPreparationFingerprint(value any) (string, error) {
	raw, err := canonicaljson.Bytes(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// Capture only successfully persisted evidence. This is not a second reader or
// authority: the execution transaction must re-read each original receipt.
type selectedPreparationSurfaceCapture struct {
	store    managedcapabilities.Persistence
	mu       sync.Mutex
	surfaces map[string]managedcapabilities.Surface
}

func (p *selectedPreparationSurfaceCapture) SaveManagedCapabilitySurface(ctx context.Context, surface managedcapabilities.Surface) error {
	if err := p.store.SaveManagedCapabilitySurface(ctx, surface); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.surfaces[surface.ID] = surface.Clone()
	return nil
}

func (p *selectedPreparationSurfaceCapture) surface(id string) (managedcapabilities.Surface, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.surfaces[id]
	return s.Clone(), ok
}
