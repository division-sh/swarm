package sessionprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	inbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/sessionprovider/input"
)

type sessionBusinessStore interface {
	inbound.Runner
	pipeline.StandingServicePersistence
}

type preparedSessionBusiness struct {
	ctx     context.Context
	bus     *bus.EventBus
	store   sessionBusinessStore
	plan    bus.InboundDeliveryPlan
	command inbound.CommitCommand
	history *inbound.Record
}

func prepareSessionBusinessPublication(ctx context.Context, input input.Admission, trigger providertriggers.InboundAdmissionPlan, alias string, eventBus *bus.EventBus, store sessionBusinessStore, posture executionposture.Posture) (preparedSessionBusiness, error) {
	var absent preparedSessionBusiness
	if ctx == nil || ctx.Err() != nil || eventBus == nil || store == nil || !input.LifetimeCurrent(ctx) {
		return absent, fmt.Errorf("native publication requires its current input and runtime owners")
	}
	ownedContext, err := eventBus.AdmitSourceArtifactFact(bus.WithCurrentRuntimeEpoch(input.Context()))
	if err != nil {
		return absent, err
	}
	var event capturedEvent
	if err := json.Unmarshal(input.OriginalCapture(), &event); err != nil {
		return absent, err
	}
	identity, err := event.PublicationIdentity()
	if err != nil {
		return absent, err
	}
	record, found, err := store.LoadInboundPublicationByIdentity(ownedContext, identity)
	if err != nil {
		return absent, err
	}
	if found {
		if _, err := verifyHistoricalCapture(event, record); err != nil {
			return absent, err
		}
		return preparedSessionBusiness{ctx: ownedContext, bus: eventBus, store: store, history: &record}, nil
	}
	admitted, err := trigger.AdmitSessionInput(ownedContext, input)
	if err != nil {
		return absent, err
	}
	request, err := sessionBusinessRequest(ownedContext, event, alias, store)
	if err != nil {
		return absent, err
	}
	delivery, admission, err := trigger.ProjectPublication(admitted, input.Coordinate().BundleHash, request.FlowPath)
	if err != nil {
		return absent, err
	}
	batch := bus.InboundDeliveryBatch{Provider: request.Provider, Admission: admission}
	projection := authoractivity.InboundProjection{}
	for ordinal, output := range delivery.Events {
		item, err := inbound.ProjectOutputEvent(request, ordinal, output, posture)
		if err != nil {
			return absent, err
		}
		batch.Events = append(batch.Events, item)
		if output.Kind == providertriggers.OutputKindNormalized {
			projection = authoractivity.InboundProjection{SubjectType: output.AuthorSubjectType, SubjectID: output.AuthorSubjectID}
		}
	}
	batch.AuthorSubjectType, batch.AuthorSubjectID = projection.SubjectType, projection.SubjectID
	plan, err := eventBus.PrepareInboundDeliveryBatch(ownedContext, batch)
	if err != nil {
		return absent, err
	}
	command, err := sessionBusinessCommand(ownedContext, eventBus, plan, request, projection, posture)
	if err != nil {
		return absent, errors.Join(err, eventBus.AbandonInboundDeliveryPlan(context.WithoutCancel(ownedContext), plan))
	}
	return preparedSessionBusiness{ctx: ownedContext, bus: eventBus, store: store, plan: plan, command: command}, nil
}

func sessionBusinessRequest(ctx context.Context, event capturedEvent, alias string, store sessionBusinessStore) (inbound.Request, error) {
	var request inbound.Request
	target, err := packs.ParseChannelRegistrationTarget(event.Scope.TargetSelector)
	source, found := correlation.SourceArtifactFactFromContext(ctx)
	if err != nil || !found || source.BundleHash() != event.Source.Coordinate.BundleHash {
		return request, errCaptureScopeChanged
	}
	standing, found, err := store.LoadReconciledStandingService(ctx, pipeline.StandingServiceCandidate{
		ServiceID: event.Scope.PublicationBinding.ServiceID, FlowPath: target.FlowPath, BindingEnabled: true, Source: source})
	if err != nil {
		return request, err
	}
	if !found || standing.RunID != event.Scope.PublicationBinding.RunID || standing.Generation != event.Scope.PublicationBinding.Generation {
		return request, errCaptureScopeChanged
	}
	identity, err := event.PublicationIdentity()
	if err != nil {
		return request, err
	}
	publication, marker, err := inbound.DeterministicIDs(identity)
	if err != nil {
		return request, err
	}
	fingerprint, err := event.PublicationFingerprint()
	if err != nil {
		return request, err
	}
	request = inbound.Request{PublicationID: publication, MarkerEventID: marker, Provider: target.Provider, ProviderEventID: identity.ProviderEventID,
		RequestFingerprint: fingerprint, RequestProjectionVersion: inbound.RequestSemanticProjectionVersion,
		StableServiceID: identity.ServiceID, ResolvedRunID: identity.RunID, ExpectedGeneration: identity.Generation,
		ExpectedPublicationSequence: standing.PublicationSequence, FlowPath: target.FlowPath, TargetAlias: alias,
		OriginalReceivedAt: event.ReceivedAt, AcknowledgementMode: inbound.AcknowledgementDurableBeforeDispatch}
	request, err = withCaptureProvenance(event, request)
	if err != nil {
		return inbound.Request{}, err
	}
	return request, request.Validate()
}

func sessionBusinessCommand(ctx context.Context, eventBus *bus.EventBus, plan bus.InboundDeliveryPlan, request inbound.Request, projection authoractivity.InboundProjection, posture executionposture.Posture) (inbound.CommitCommand, error) {
	command := inbound.CommitCommand{Admission: plan.Admission(), Request: request, Publications: plan.CommitCommands(), AuthorProjection: projection}
	var ids, names []string
	items := plan.Events()
	for ordinal, prepared := range plan.PreparedPublications() {
		manifest, _, _, err := inbound.CanonicalRecipientManifest(prepared.DeliveryRoutes())
		if err != nil {
			return command, err
		}
		command.Finalization.Events = append(command.Finalization.Events, inbound.EventFinalization{
			Ordinal: ordinal, Event: prepared.Event, Kind: items[ordinal].Kind, Authorization: items[ordinal].Authorization, RecipientManifest: manifest})
		ids, names = append(ids, prepared.Event.ID()), append(names, string(prepared.Event.Type()))
	}
	evidence, err := inbound.ProjectEvidence(request, ids, names, posture)
	if err != nil {
		return command, err
	}
	command.Finalization.EvidenceEvent, err = eventBus.PrepareInboundEvidence(ctx, evidence)
	if err != nil {
		return command, err
	}
	return command, command.Validate()
}

// Only acknowledged new commits dispatch. Historical retries reconcile the
// original receipt and never turn into another business execution.
func (p preparedSessionBusiness) commitAndDispatch() (inbound.CommitResult, error) {
	if p.history != nil {
		return inbound.CommitResult{Record: *p.history, Acknowledged: true}, nil
	}
	result, commitErr := p.store.CommitInboundPublication(p.ctx, p.command)
	cleanupContext := context.WithoutCancel(p.ctx)
	if !result.Acknowledged || !result.Record.Created {
		return result, errors.Join(commitErr, p.bus.AbandonInboundDeliveryPlan(cleanupContext, p.plan))
	}
	prepared, err := p.bus.ApplyInboundDeliveryCommit(cleanupContext, p.plan, result.Publications)
	if err != nil {
		return result, errors.Join(commitErr, err)
	}
	for index, publication := range prepared {
		if err := p.bus.DispatchPreparedPublish(cleanupContext, publication); err != nil {
			for _, remaining := range prepared[index+1:] {
				err = errors.Join(err, p.bus.AbandonPreparedPublish(cleanupContext, remaining))
			}
			return result, errors.Join(commitErr, err)
		}
	}
	return result, commitErr
}
