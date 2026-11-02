package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	"github.com/google/uuid"
)

// PublishPrivateTestProviderOutput is an isolated host issuer, not a public
// publication endpoint. The current catalog seals the original scenario input
// before the ordinary verified inbound preparation and native commit.
func (g *InboundGateway) PublishPrivateTestProviderOutput(ctx context.Context, catalog *providertriggers.CatalogSnapshot, bundleHash, flowPath, runID, eventName, operatorAccount string, payload map[string]any, key string, now time.Time) (string, bool, error) {
	if g == nil || g.bus == nil || g.store == nil || g.executionPosture != executionposture.MockOnly || !scenarioexecution.HasPrivateHostScope(ctx) {
		return "", false, fmt.Errorf("provider double requires its isolated test host and inbound owners")
	}
	if strings.TrimSpace(runID) == "" {
		return "", false, fmt.Errorf("private provider output requires its scenario run")
	}
	if g.beginAdmission != nil {
		admittedCtx, release, admitted := g.beginAdmission(ctx)
		if !admitted {
			return "", true, fmt.Errorf("private provider output admission is closed")
		}
		defer release()
		ctx = admittedCtx
	} else if g.shutdownAdmissionClosed != nil && g.shutdownAdmissionClosed() {
		return "", true, fmt.Errorf("private provider output admission is closed")
	}
	providerEventID := uuid.NewString()
	if key != "" {
		providerEventID = uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm-private-provider-output\x00"+runID+"\x00"+eventName+"\x00"+key)).String()
	}
	delivery, admission, handled, err := catalog.IssuePrivateTestPublication(ctx, bundleHash, flowPath, providerEventID, eventName, operatorAccount, payload, now)
	if err != nil || !handled {
		return "", handled, err
	}
	identity := runtimeinbound.Identity{RunID: runID, Provider: admission.Provider(), ProviderEventID: providerEventID}
	publicationID, markerEventID, err := runtimeinbound.DeterministicIDs(identity)
	if err != nil {
		return "", true, err
	}
	fingerprintBytes, err := canonicaljson.Bytes(payload)
	if err != nil {
		return "", true, err
	}
	request := runtimeinbound.Request{
		PublicationID: publicationID, MarkerEventID: markerEventID,
		Provider: admission.Provider(), ProviderEventID: providerEventID,
		RequestFingerprint: strings.TrimPrefix(canonicaljson.HashBytes(fingerprintBytes), "sha256:"), RequestProjectionVersion: runtimeinbound.RequestSemanticProjectionVersion,
		FlowPath: flowPath, TargetAlias: "private-test", ResolvedRunID: runID,
		AcknowledgementMode: runtimeinbound.AcknowledgementAfterPublish, OriginalReceivedAt: now.UTC(),
		OriginalUserAgent: "swarm-test", OriginalTransportMetadata: []byte(`{"test_host":true}`),
	}
	if err := request.Validate(); err != nil {
		return "", true, err
	}
	pubCtx, err := g.bus.AdmitSourceArtifactFact(runtimebus.WithCurrentRuntimeEpoch(ctx))
	if err != nil {
		return "", true, err
	}
	pubCtx, err = scenarioexecution.WithPrivateRunScope(pubCtx, runID, flowPath)
	if err != nil {
		return "", true, err
	}
	items := make([]runtimebus.InboundDeliveryEvent, 0, len(delivery.Events))
	ids, names := make([]string, 0, len(delivery.Events)), make([]string, 0, len(delivery.Events))
	for index, output := range delivery.Events {
		item, err := runtimeinbound.ProjectOutputEvent(request, index, output, g.executionPosture)
		if err != nil {
			return "", true, err
		}
		items = append(items, item)
		ids, names = append(ids, item.Event.ID()), append(names, string(output.Name))
	}
	evidence, err := runtimeinbound.ProjectEvidence(request, ids, names, g.executionPosture)
	if err != nil {
		return "", true, err
	}
	evidence, err = g.bus.PrepareInboundEvidence(pubCtx, evidence)
	if err != nil {
		return "", true, err
	}
	plan, err := g.bus.PrepareInboundDeliveryBatch(pubCtx, runtimebus.InboundDeliveryBatch{Provider: admission.Provider(), Admission: admission, Events: items})
	if err != nil {
		return "", true, err
	}
	abandon := func(cause error) (string, bool, error) {
		return "", true, errors.Join(cause, g.bus.AbandonInboundDeliveryPlan(context.WithoutCancel(pubCtx), plan))
	}
	prepared := plan.PreparedPublications()
	finalization := runtimeinbound.Finalization{EvidenceEvent: evidence, Events: make([]runtimeinbound.EventFinalization, len(prepared))}
	for index := range prepared {
		manifest, _, _, err := runtimeinbound.CanonicalRecipientManifest(prepared[index].DeliveryRoutes())
		if err != nil {
			return abandon(err)
		}
		finalization.Events[index] = runtimeinbound.EventFinalization{
			Ordinal: index, Event: prepared[index].Event, Kind: items[index].Kind,
			Authorization: items[index].Authorization, RecipientManifest: manifest,
		}
	}
	committed, commitErr := g.store.CommitInboundPublication(pubCtx, runtimeinbound.CommitCommand{
		Admission: admission, Request: request, Finalization: finalization, Publications: plan.CommitCommands(),
	})
	if !committed.Acknowledged {
		return abandon(errors.Join(commitErr, fmt.Errorf("private provider output lacks native acknowledgement")))
	}
	if err := validateInboundCommittedResultRecord(request, committed.Record); err != nil {
		return abandon(errors.Join(commitErr, err))
	}
	if !committed.Record.Created {
		if err := g.bus.AbandonInboundDeliveryPlan(context.WithoutCancel(pubCtx), plan); err != nil {
			return "", true, errors.Join(commitErr, err)
		}
		return ids[1], true, commitErr
	}
	prepared, err = g.bus.ApplyInboundDeliveryCommit(pubCtx, plan, committed.Publications)
	if err != nil {
		return abandon(errors.Join(commitErr, err))
	}
	for _, item := range prepared {
		if err := g.bus.DispatchPreparedPublish(pubCtx, item); err != nil {
			return "", true, errors.Join(commitErr, err)
		}
	}
	return ids[1], true, commitErr
}
