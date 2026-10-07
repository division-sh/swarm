package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const inboundWebhookMaxBodyBytes = 1 << 20

type InboundPersistence = runtimeinbound.Runner

type InboundTarget struct {
	BundleHash          string
	ServiceID           string
	FlowPath            string
	RunID               string
	Generation          int64
	PublicationSequence int64
	Alias               string
	Provider            string
	SigningSecret       string
	AdmissionPlan       providertriggers.InboundAdmissionPlan
}

type InboundGateway struct {
	bus                     *runtimebus.EventBus
	store                   InboundPersistence
	logger                  *RuntimeLogger
	shutdownAdmissionClosed func() bool
	beginAdmission          func(context.Context) (context.Context, func(), bool)
	runtimeIngress          *runtimeingress.Controller
	admitCredentials        func(context.Context, InboundTarget) (runtimecredentials.SecretBinding, func(context.Context) error, error)
	standingAdmissionMu     sync.Mutex
	standingAdmissions      map[string]*shutdownAdmission
	publicationMu           sync.Mutex
	publicationFlights      map[string]chan struct{}
	executionPosture        executionposture.Posture
	channelPlans            []packs.SatisfactionPlan
}

func (g *InboundGateway) claimPublicationFlight(key string) (<-chan struct{}, bool, func()) {
	g.publicationMu.Lock()
	defer g.publicationMu.Unlock()
	if g.publicationFlights == nil {
		g.publicationFlights = make(map[string]chan struct{})
	}
	if done := g.publicationFlights[key]; done != nil {
		return done, false, func() {}
	}
	done := make(chan struct{})
	g.publicationFlights[key] = done
	return done, true, func() {
		g.publicationMu.Lock()
		if g.publicationFlights[key] == done {
			delete(g.publicationFlights, key)
			close(done)
		}
		g.publicationMu.Unlock()
	}
}

func (g *InboundGateway) SetAdmissionGuard(begin func(context.Context) (context.Context, func(), bool)) {
	if g != nil {
		g.beginAdmission = begin
	}
}

func NewInboundGateway(bus *runtimebus.EventBus, logger *RuntimeLogger, shutdownAdmissionClosed func() bool, posture executionposture.Posture, stores ...InboundPersistence) *InboundGateway {
	var store InboundPersistence
	if len(stores) > 0 {
		store = stores[0]
	}
	g := &InboundGateway{
		bus:                     bus,
		store:                   store,
		logger:                  logger,
		shutdownAdmissionClosed: shutdownAdmissionClosed,
		executionPosture:        posture,
	}
	return g
}

func (g *InboundGateway) SetCredentialAdmission(admit func(context.Context, InboundTarget) (runtimecredentials.SecretBinding, func(context.Context) error, error)) {
	if g != nil {
		g.admitCredentials = admit
	}
}

func (g *InboundGateway) SetRuntimeIngress(controller *runtimeingress.Controller) {
	if g == nil {
		return
	}
	g.runtimeIngress = controller
}

func (g *InboundGateway) SetChannelPlans(plans []packs.SatisfactionPlan) {
	if g == nil {
		return
	}
	g.channelPlans = append([]packs.SatisfactionPlan(nil), plans...)
}

func (g *InboundGateway) CloseStandingServiceAdmission(serviceID string) error {
	_, err := g.FenceStandingServiceAdmission(serviceID)
	return err
}

// FenceStandingServiceAdmission returns whether this operation withdrew an
// open gate, so rollback cannot reopen a previously unavailable service.
func (g *InboundGateway) FenceStandingServiceAdmission(serviceID string) (bool, error) {
	if g == nil {
		return false, nil
	}
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return false, fmt.Errorf("standing service_id is required")
	}
	g.standingAdmissionMu.Lock()
	defer g.standingAdmissionMu.Unlock()
	gate := g.standingAdmissionLocked(serviceID)
	wasOpen := !gate.Closed()
	gate.Close()
	return wasOpen, nil
}

func (g *InboundGateway) ReopenStandingServiceAdmission(serviceID string) error {
	if g == nil {
		return nil
	}
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return fmt.Errorf("standing service_id is required")
	}
	g.standingAdmissionMu.Lock()
	defer g.standingAdmissionMu.Unlock()
	gate := g.standingAdmissionLocked(serviceID)
	if !gate.ReopenIfDrained() {
		return fmt.Errorf("standing service %s admission cannot reopen before admitted requests drain", serviceID)
	}
	return nil
}

func (g *InboundGateway) WaitForStandingServiceAdmission(ctx context.Context, serviceID string) error {
	if g == nil {
		return nil
	}
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return fmt.Errorf("standing service_id is required")
	}
	g.standingAdmissionMu.Lock()
	gate := g.standingAdmissionLocked(serviceID)
	g.standingAdmissionMu.Unlock()
	return gate.Wait(ctx)
}

func (g *InboundGateway) standingAdmissionLocked(serviceID string) *shutdownAdmission {
	if g.standingAdmissions == nil {
		g.standingAdmissions = map[string]*shutdownAdmission{}
	}
	gate := g.standingAdmissions[serviceID]
	if gate == nil {
		gate = &shutdownAdmission{}
		g.standingAdmissions[serviceID] = gate
	}
	return gate
}

func (g *InboundGateway) beginStandingServiceAdmission(parent context.Context, serviceID string) (context.Context, func(), bool) {
	serviceID = strings.TrimSpace(serviceID)
	if g == nil || serviceID == "" {
		return parent, func() {}, true
	}
	g.standingAdmissionMu.Lock()
	defer g.standingAdmissionMu.Unlock()
	return g.standingAdmissionLocked(serviceID).BeginContext(parent)
}

func (g *InboundGateway) HandleResolvedWebhook(w http.ResponseWriter, r *http.Request, target InboundTarget, source semanticview.Source) {
	if g == nil {
		http.Error(w, "runtime ingress unavailable", http.StatusServiceUnavailable)
		return
	}
	g.handleResolvedWebhook(w, r, target, source)
}

func (g *InboundGateway) handleResolvedWebhook(w http.ResponseWriter, r *http.Request, target InboundTarget, source semanticview.Source) {
	if g.beginAdmission != nil {
		admissionCtx, release, admitted := g.beginAdmission(r.Context())
		if !admitted {
			http.Error(w, "runtime shutting down", http.StatusServiceUnavailable)
			return
		}
		defer release()
		r = r.WithContext(admissionCtx)
	} else if g.shutdownAdmissionClosed != nil && g.shutdownAdmissionClosed() {
		http.Error(w, "runtime shutting down", http.StatusServiceUnavailable)
		return
	}
	standingCtx, releaseStanding, admittedStanding := g.beginStandingServiceAdmission(r.Context(), target.ServiceID)
	if !admittedStanding {
		http.Error(w, "standing service admission unavailable", http.StatusServiceUnavailable)
		return
	}
	defer releaseStanding()
	r = r.WithContext(standingCtx)
	if g.runtimeIngress != nil {
		if err := g.runtimeIngress.AdmitQueueableIngress(r.Context(), "inbound.webhook"); err != nil {
			http.Error(w, "runtime ingress unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	provider := providertriggers.NormalizeProviderName(target.Provider)
	if provider == "" {
		_, provider, _ = parseWebhookPath(r.URL.Path)
	}
	if provider == "" {
		http.Error(w, "provider is required", http.StatusBadRequest)
		return
	}
	if !target.AdmissionPlan.Valid() {
		http.Error(w, fmt.Sprintf("ingress target %q provider %q has no compiled admission plan; request rejected before provider admission", target.Alias, provider), http.StatusServiceUnavailable)
		return
	}
	if g.admitCredentials == nil {
		http.Error(w, "standing ingress credential admission unavailable", http.StatusServiceUnavailable)
		return
	}
	binding, validateCredentials, err := g.admitCredentials(r.Context(), target)
	if err != nil || validateCredentials == nil {
		http.Error(w, "standing ingress credential admission unavailable; restart or explicitly admit fresh credentials", http.StatusServiceUnavailable)
		return
	}
	validate := func() bool {
		if err := validateCredentials(r.Context()); err != nil {
			http.Error(w, "standing ingress credential admission became stale; restart or explicitly admit fresh credentials", http.StatusServiceUnavailable)
			return false
		}
		return true
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, inboundWebhookMaxBodyBytes+1))
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	if len(body) > inboundWebhookMaxBodyBytes {
		http.Error(w, "webhook body too large", http.StatusRequestEntityTooLarge)
		return
	}
	requestURL := inboundRequestURL(r)
	queryValues, queryParseError := inboundQueryValues(r)
	formValues, formParsed, formParseError := inboundFormValues(r.Header.Get("Content-Type"), body)

	signingValue := ""
	if target.AdmissionPlan.RequiresSecret() {
		if target.SigningSecret == "" {
			http.Error(w, fmt.Sprintf("ingress alias %q provider %q requires a signing secret for %s request authentication", target.Alias, provider, target.AdmissionPlan.RequestAuthentication()), http.StatusServiceUnavailable)
			return
		}
		if !binding.Bound() {
			http.Error(w, fmt.Sprintf("signing secret %s is UNBOUND; run `swarm secrets set %s`", target.SigningSecret, target.SigningSecret), http.StatusServiceUnavailable)
			return
		}
		signingValue = binding.CredentialValue()
	}
	if !validate() {
		return
	}
	var payload any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		payload = string(body)
	} else if err := decoder.Decode(&struct{}{}); err != io.EOF {
		payload = string(body)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	admitted, err := target.AdmissionPlan.AdmitRequest(providertriggers.Request{
		Provider: provider,
		Target: providertriggers.Target{
			WebhookSecret: signingValue,
		},
		Method:          r.Method,
		URL:             requestURL,
		Body:            body,
		Headers:         r.Header,
		Payload:         payload,
		ContentType:     r.Header.Get("Content-Type"),
		Query:           queryValues,
		QueryParseError: queryParseError,
		Form:            formValues,
		FormParsed:      formParsed,
		FormParseError:  formParseError,
		Received:        now,
		UserAgent:       r.UserAgent(),
	})
	if err != nil {
		status := http.StatusBadRequest
		if providerErr, ok := err.(providertriggers.Error); ok {
			status = providerErr.Status
		}
		http.Error(w, err.Error(), status)
		return
	}
	if admitted.Response != nil {
		if !validate() {
			return
		}
		status := admitted.Response.Status
		if status == 0 {
			status = http.StatusOK
		}
		contentType := strings.TrimSpace(admitted.Response.ContentType)
		if contentType == "" {
			contentType = "text/plain; charset=utf-8"
		}
		w.Header().Set("content-type", contentType)
		w.WriteHeader(status)
		_, _ = w.Write(admitted.Response.Body)
		return
	}
	providerEventID := admitted.ProviderEventID
	requestCtx := r.Context()
	if strings.TrimSpace(target.RunID) != "" {
		requestCtx = runtimecorrelation.WithRunID(requestCtx, target.RunID)
	}

	if g.store == nil || g.bus == nil {
		http.Error(w, "inbound publication owner unavailable", http.StatusServiceUnavailable)
		return
	}
	fingerprint, err := runtimeinbound.SemanticFingerprint(struct {
		ProjectionVersion string `json:"projection_version"`
		Provider          string `json:"provider"`
		ProviderEventID   string `json:"provider_event_id"`
		ProviderEventType string `json:"provider_event_type"`
		SemanticDigest    string `json:"semantic_content_digest"`
		StableServiceID   string `json:"stable_service_id"`
		FlowPath          string `json:"flow_path"`
		Generation        int64  `json:"generation"`
	}{
		ProjectionVersion: runtimeinbound.RequestSemanticProjectionVersion,
		Provider:          provider, ProviderEventID: providerEventID,
		ProviderEventType: admitted.ProviderEventType, SemanticDigest: admitted.SemanticContentDigest,
		StableServiceID: target.ServiceID, FlowPath: target.FlowPath,
		Generation: target.Generation,
	})
	if err != nil {
		http.Error(w, "derive inbound request fingerprint failed", http.StatusInternalServerError)
		return
	}
	identity := runtimeinbound.Identity{ServiceID: target.ServiceID, RunID: target.RunID,
		Generation: target.Generation, Provider: provider, ProviderEventID: providerEventID}
	publicationID, markerEventID, err := runtimeinbound.DeterministicIDs(identity)
	if err != nil {
		http.Error(w, "inbound binding generation is not admitted", http.StatusServiceUnavailable)
		return
	}
	ackMode := runtimeinbound.AcknowledgementAfterPublish
	if admitted.AcknowledgeBeforeDispatch {
		ackMode = runtimeinbound.AcknowledgementDurableBeforeDispatch
	}
	publicationRequest := runtimeinbound.Request{
		PublicationID: publicationID, Provider: provider, ProviderEventID: providerEventID,
		RequestFingerprint: fingerprint, RequestProjectionVersion: runtimeinbound.RequestSemanticProjectionVersion,
		StableServiceID: target.ServiceID, FlowPath: target.FlowPath,
		TargetAlias:                 target.Alias,
		ExpectedPublicationSequence: target.PublicationSequence, ExpectedGeneration: target.Generation,
		ResolvedRunID: target.RunID,
		MarkerEventID: markerEventID, AcknowledgementMode: ackMode,
		OriginalReceivedAt: now, OriginalUserAgent: r.UserAgent(),
		OriginalTransportMetadata: mustJSON(map[string]any{"method": r.Method, "content_type": r.Header.Get("Content-Type")}),
	}
	publicationFlightKey := publicationID
	for {
		done, owner, releaseFlight := g.claimPublicationFlight(publicationFlightKey)
		if owner {
			defer releaseFlight()
			break
		}
		select {
		case <-r.Context().Done():
			http.Error(w, "publish inbound canceled", http.StatusServiceUnavailable)
			return
		case <-done:
		}
		if !validate() {
			return
		}
		if existing, found, loadErr := g.store.LoadInboundPublicationByIdentity(requestCtx, identity); loadErr != nil {
			http.Error(w, "read inbound publication failed", http.StatusServiceUnavailable)
			return
		} else if found {
			if !validate() {
				return
			}
			if existing.RequestProjectionVersion != publicationRequest.RequestProjectionVersion || existing.RequestFingerprint != publicationRequest.RequestFingerprint {
				http.Error(w, "inbound provider identity conflicts with the committed semantic request", http.StatusConflict)
				return
			}
			writeJSON(w, http.StatusOK, inboundPublicationResponse("duplicate", existing, admitted.ProviderEventType))
			return
		}
	}
	pubCtx := runtimebus.WithCurrentRuntimeEpoch(requestCtx)
	pubCtx, err = g.bus.AdmitSourceArtifactFact(pubCtx)
	if err != nil {
		http.Error(w, "inbound bundle source admission failed", http.StatusConflict)
		return
	}
	if !validate() {
		return
	}
	if existing, found, loadErr := g.store.LoadInboundPublicationByIdentity(pubCtx, identity); loadErr != nil {
		http.Error(w, "read inbound publication failed", http.StatusServiceUnavailable)
		return
	} else if found {
		if !validate() {
			return
		}
		if existing.RequestProjectionVersion != publicationRequest.RequestProjectionVersion || existing.RequestFingerprint != publicationRequest.RequestFingerprint {
			http.Error(w, "inbound provider identity conflicts with the committed semantic request", http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusOK, inboundPublicationResponse("duplicate", existing, admitted.ProviderEventType))
		return
	}

	bareSelector := func(text operatorchannel.InboundText) (bool, error) {
		owner, ok := g.store.(interface {
			HasCurrentChannelInputDraft(context.Context, operatorchannel.InboundText, time.Time) (bool, error)
		})
		if !ok {
			return false, fmt.Errorf("selected store lacks channel input draft classification")
		}
		return owner.HasCurrentChannelInputDraft(pubCtx, text, now)
	}
	delivery, publicationAdmission, projectionErr := target.AdmissionPlan.ProjectPublication(admitted, target.BundleHash, target.FlowPath)
	if projectionErr != nil {
		writeInboundPublicationError(w, projectionErr)
		return
	}
	published, evidence, authorProjection, operatorEvent, projectionErr := projectInboundPublication(target, delivery, admitted, publicationRequest, now, g.executionPosture, g.channelPlans, bareSelector)
	if projectionErr != nil {
		writeInboundPublicationError(w, projectionErr)
		return
	}
	if operatorEvent != nil && operatorEvent.BareCandidate == nil {
		if !validate() {
			return
		}
		commitResult, err := g.store.CommitInboundPublication(pubCtx, runtimeinbound.CommitCommand{
			Request: publicationRequest, Finalization: runtimeinbound.Finalization{EvidenceEvent: evidence},
			OperatorChannelClaim: operatorEvent.Claim, OperatorChannelAction: operatorEvent.Action,
			OperatorChannelText: operatorEvent.Text,
		})
		if !commitResult.Acknowledged {
			if err == nil {
				err = errors.New("inbound operator claim commit acknowledgement missing")
			}
			writeInboundCommitError(w, g.logger, requestCtx, provider, target.ServiceID, providerEventID, err)
			return
		}
		if recordErr := validateInboundCommittedResultRecord(publicationRequest, commitResult.Record); recordErr != nil {
			writeInboundCommitError(w, g.logger, requestCtx, provider, target.ServiceID, providerEventID, errors.Join(err, recordErr))
			return
		}
		status := "accepted"
		if !commitResult.Record.Created {
			status = "duplicate"
		}
		response := inboundPublicationResponse(status, commitResult.Record, admitted.ProviderEventType)
		if commitResult.OperatorChannelClaim != nil {
			response["operator_channel_claim_disposition"] = commitResult.OperatorChannelClaim.Disposition
			response["operator_channel_operation_id"] = commitResult.OperatorChannelClaim.Operation.OperationID
		}
		if operatorEvent.Action != nil {
			response["operator_channel_action_disposition"] = "pending"
		}
		if operatorEvent.Text != nil {
			response["operator_channel_text_disposition"] = "pending"
		}
		if err != nil {
			reportInboundCommittedCleanup(g.logger, requestCtx, provider, target.ServiceID, providerEventID, err)
		}
		writeJSON(w, http.StatusAccepted, response)
		return
	}
	batchPlan, err := g.bus.PrepareInboundDeliveryBatch(pubCtx, runtimebus.InboundDeliveryBatch{
		Provider:          provider,
		Admission:         publicationAdmission,
		AuthorSubjectType: authorProjection.SubjectType,
		AuthorSubjectID:   authorProjection.SubjectID,
		Events:            published,
	})
	if err != nil {
		http.Error(w, "publish inbound failed: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	prepared := batchPlan.PreparedPublications()
	plannedEvents := batchPlan.Events()
	finalization := runtimeinbound.Finalization{EvidenceEvent: evidence, Events: make([]runtimeinbound.EventFinalization, len(prepared))}
	for index := range prepared {
		manifest, _, _, manifestErr := runtimeinbound.CanonicalRecipientManifest(prepared[index].DeliveryRoutes())
		if manifestErr != nil {
			_ = g.bus.AbandonInboundDeliveryPlan(context.WithoutCancel(pubCtx), batchPlan)
			http.Error(w, "publish inbound failed", http.StatusServiceUnavailable)
			return
		}
		finalization.Events[index] = runtimeinbound.EventFinalization{
			Ordinal: index, Event: prepared[index].Event, Kind: plannedEvents[index].Kind,
			Authorization: plannedEvents[index].Authorization, RecipientManifest: manifest,
		}
	}
	var bareCandidate *operatorchannel.InboundText
	if operatorEvent != nil {
		bareCandidate = operatorEvent.BareCandidate
	}
	if !validate() {
		_ = g.bus.AbandonInboundDeliveryPlan(context.WithoutCancel(pubCtx), batchPlan)
		return
	}
	commitResult, err := g.store.CommitInboundPublication(pubCtx, runtimeinbound.CommitCommand{
		Request: publicationRequest, Finalization: finalization,
		Publications: batchPlan.CommitCommands(), AuthorProjection: authorProjection,
		PotentialBareText: bareCandidate,
	})
	commitErr := err
	record := commitResult.Record
	if !commitResult.Acknowledged {
		if err == nil {
			err = errors.New("inbound publication commit acknowledgement missing")
		}
		err = errors.Join(err, g.bus.AbandonInboundDeliveryPlan(context.WithoutCancel(pubCtx), batchPlan))
		writeInboundCommitError(w, g.logger, requestCtx, provider, target.ServiceID, providerEventID, err)
		return
	}
	if recordErr := validateInboundCommittedResultRecord(publicationRequest, record); recordErr != nil {
		err = errors.Join(commitErr, recordErr, g.bus.AbandonInboundDeliveryPlan(context.WithoutCancel(pubCtx), batchPlan))
		writeInboundCommitError(w, g.logger, requestCtx, provider, target.ServiceID, providerEventID, err)
		return
	}
	if !record.Created {
		commitErr = errors.Join(commitErr, g.bus.AbandonInboundDeliveryPlan(context.WithoutCancel(pubCtx), batchPlan))
		if commitErr != nil {
			reportInboundCommittedCleanup(g.logger, requestCtx, provider, target.ServiceID, providerEventID, commitErr)
		}
		writeJSON(w, http.StatusOK, inboundPublicationResponse("duplicate", record, admitted.ProviderEventType))
		return
	}
	handoffCtx := pubCtx
	if commitErr != nil {
		handoffCtx = context.WithoutCancel(pubCtx)
	}
	prepared, err = g.bus.ApplyInboundDeliveryCommit(handoffCtx, batchPlan, commitResult.Publications)
	if err != nil {
		_ = g.bus.AbandonInboundDeliveryPlan(context.WithoutCancel(pubCtx), batchPlan)
		err = errors.Join(commitErr, err)
		if g.logger != nil {
			handleRuntimeLogPersistenceError("inbound-gateway", "invalid_commit_evidence", g.logger.Error(requestCtx, "inbound-gateway", "invalid_commit_evidence", map[string]any{
				"provider": provider, "service_id": target.ServiceID, "provider_event_id": providerEventID,
			}, err))
		}
		http.Error(w, "committed inbound publication evidence is invalid", http.StatusServiceUnavailable)
		return
	}
	if len(prepared) != len(record.Events) {
		http.Error(w, "committed inbound publication does not match prepared batch", http.StatusServiceUnavailable)
		return
	}
	for index := range prepared {
		if prepared[index].Event.ID() != record.Events[index].EventID || string(prepared[index].Event.Type()) != record.Events[index].EventName {
			http.Error(w, "committed inbound publication does not match prepared batch", http.StatusServiceUnavailable)
			return
		}
	}
	if record.AcknowledgementMode == runtimeinbound.AcknowledgementDurableBeforeDispatch {
		for index, item := range prepared {
			if err := g.bus.DispatchPreparedPublishAsync(handoffCtx, item); err != nil {
				err = errors.Join(commitErr, err)
				for _, pending := range prepared[index+1:] {
					_ = g.bus.AbandonPreparedPublish(context.WithoutCancel(pubCtx), pending)
				}
				if g.logger != nil {
					handleRuntimeLogPersistenceError("inbound-gateway", "dispatch_failed", g.logger.Error(requestCtx, "inbound-gateway", "dispatch_failed", map[string]any{
						"provider": provider, "service_id": target.ServiceID, "provider_event_id": providerEventID,
					}, err))
				}
				http.Error(w, "publish inbound failed", http.StatusServiceUnavailable)
				return
			}
		}
	} else {
		for index, item := range prepared {
			if err := g.bus.DispatchPreparedPublish(handoffCtx, item); err != nil {
				err = errors.Join(commitErr, err)
				for _, pending := range prepared[index+1:] {
					_ = g.bus.AbandonPreparedPublish(context.WithoutCancel(pubCtx), pending)
				}
				if g.logger != nil {
					handleRuntimeLogPersistenceError("inbound-gateway", "dispatch_failed", g.logger.Error(requestCtx, "inbound-gateway", "dispatch_failed", map[string]any{
						"provider": provider, "service_id": target.ServiceID, "provider_event_id": providerEventID,
					}, err))
				}
				http.Error(w, "publish inbound failed", http.StatusServiceUnavailable)
				return
			}
		}
	}
	if commitErr != nil {
		reportInboundCommittedCleanup(g.logger, requestCtx, provider, target.ServiceID, providerEventID, commitErr)
	}
	writeJSON(w, http.StatusAccepted, inboundPublicationResponse("accepted", record, admitted.ProviderEventType))
}

func writeInboundPublicationError(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	message := "publish inbound failed"
	var providerErr providertriggers.Error
	if errors.As(err, &providerErr) {
		status = providerErr.Status
		message = providerErr.Error()
	}
	http.Error(w, message, status)
}

type operatorInboundProjection struct {
	Claim         *operatorchannel.InboundClaim
	Action        *operatorchannel.InboundAction
	Text          *operatorchannel.InboundText
	BareCandidate *operatorchannel.InboundText
}

func projectOperatorInboundOutput(output providertriggers.DeliveryEvent, request runtimeinbound.Request, channelPlans []packs.SatisfactionPlan, selectBare func(operatorchannel.InboundText) (bool, error), operatorEvent *operatorInboundProjection) (*operatorInboundProjection, error) {
	if output.Kind == providertriggers.OutputKindNormalized {
		for _, plan := range channelPlans {
			actionFact, actionMatched, err := plan.ProjectActionFact(string(output.Name), output.Authorization, output.Payload)
			if err != nil {
				return nil, err
			}
			if actionMatched {
				if operatorEvent != nil {
					return nil, fmt.Errorf("normalized provider output ambiguously satisfies multiple operator channel interfaces")
				}
				operatorEvent = &operatorInboundProjection{Action: &operatorchannel.InboundAction{
					ActionFact: actionFact, Provider: request.Provider, ProviderEventID: request.ProviderEventID,
					PublicationID:         request.PublicationID,
					ProviderAuthorization: operatorchannel.Hash(output.Authorization.Provider(), output.Authorization.Event(), output.Authorization.PackID(), output.Authorization.PackVersion(), output.Authorization.ManifestHash(), output.Authorization.Generation().Diagnostic()),
				}}
			}
			fact, matched, err := plan.ProjectTextFact(string(output.Name), output.Authorization, output.Payload)
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}
			if operatorEvent != nil {
				return nil, fmt.Errorf("normalized provider output ambiguously satisfies multiple operator channel text interfaces")
			}
			operatorEvent, err = projectOperatorTextOutput(fact, output, request, selectBare)
			if err != nil {
				return nil, err
			}
		}
	}
	return operatorEvent, nil
}

func projectOperatorTextOutput(fact operatorchannel.TextFact, output providertriggers.DeliveryEvent, request runtimeinbound.Request, selectBare func(operatorchannel.InboundText) (bool, error)) (*operatorInboundProjection, error) {
	var operatorEvent *operatorInboundProjection
	var err error
	challenge, challengeShaped := operatorchannel.ChallengeFromText(fact.Text)
	if !challengeShaped && fact.EntryReference == "" && fact.ReplyToReference == "" {
		candidate := operatorchannel.InboundText{
			TextFact: fact, Provider: request.Provider, ProviderEventID: request.ProviderEventID,
			PublicationID:         request.PublicationID,
			ProviderAuthorization: operatorchannel.Hash(output.Authorization.Provider(), output.Authorization.Event(), output.Authorization.PackID(), output.Authorization.PackVersion(), output.Authorization.ManifestHash(), output.Authorization.Generation().Diagnostic()),
		}
		selected := false
		if selectBare != nil {
			selected, err = selectBare(candidate)
			if err != nil {
				return nil, err
			}
		}
		if selected {
			operatorEvent = &operatorInboundProjection{Text: &candidate}
		} else {
			operatorEvent = &operatorInboundProjection{BareCandidate: &candidate}
		}
		return operatorEvent, nil
	}
	if challengeShaped {
		claim := operatorchannel.InboundClaim{
			TextFact: fact, Provider: request.Provider, ProviderEventID: request.ProviderEventID,
			PublicationID: request.PublicationID, Challenge: challenge,
			ProviderAuthorization: operatorchannel.Hash(output.Authorization.Provider(), output.Authorization.Event(), output.Authorization.PackID(), output.Authorization.PackVersion(), output.Authorization.ManifestHash(), output.Authorization.Generation().Diagnostic()),
		}
		operatorEvent = &operatorInboundProjection{Claim: &claim}
	} else {
		operatorEvent = &operatorInboundProjection{Text: &operatorchannel.InboundText{
			TextFact: fact, Provider: request.Provider, ProviderEventID: request.ProviderEventID,
			PublicationID:         request.PublicationID,
			ProviderAuthorization: operatorchannel.Hash(output.Authorization.Provider(), output.Authorization.Event(), output.Authorization.PackID(), output.Authorization.PackVersion(), output.Authorization.ManifestHash(), output.Authorization.Generation().Diagnostic()),
		}}
	}
	return operatorEvent, nil
}

func projectInboundPublication(target InboundTarget, delivery providertriggers.Delivery, admitted providertriggers.AdmittedRequest, request runtimeinbound.Request, now time.Time, posture executionposture.Posture, channelPlans []packs.SatisfactionPlan, selectBare func(operatorchannel.InboundText) (bool, error)) ([]runtimebus.InboundDeliveryEvent, events.Event, runtimeauthoractivity.InboundProjection, *operatorInboundProjection, error) {
	var noEvidence events.Event
	if delivery.ProviderEventID != admitted.ProviderEventID || delivery.ProviderEventType != admitted.ProviderEventType {
		return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, nil, fmt.Errorf("compiled provider projection changed admitted request identity")
	}
	routingSource, err := events.NewExternalIngressRoutingSource(target.FlowPath, events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, nil, err
	}
	published := make([]runtimebus.InboundDeliveryEvent, 0, len(delivery.Events))
	eventIDs := make([]string, 0, len(delivery.Events))
	eventNames := make([]string, 0, len(delivery.Events))
	authorProjection := runtimeauthoractivity.InboundProjection{}
	var operatorEvent *operatorInboundProjection
	for ordinal, output := range delivery.Events {
		operatorEvent, err = projectOperatorInboundOutput(output, request, channelPlans, selectBare, operatorEvent)
		if err != nil {
			return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, nil, err
		}
		eventID, err := runtimeinbound.DeterministicEventID(request.PublicationID, ordinal)
		if err != nil {
			return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, nil, err
		}
		event, err := events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{Facts: events.EventFacts{
			ID: eventID, Type: output.Name, Producer: events.ProducerClaim{Type: events.EventProducerExternal, ID: "inbound-gateway"},
			Payload: mustJSON(output.Payload), RoutingSource: routingSource,
			CreatedAt: now, ExecutionMode: posture.RootMode(),
		}, RunID: request.ResolvedRunID})
		if err != nil {
			return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, nil, err
		}
		published = append(published, runtimebus.InboundDeliveryEvent{
			Event: event, Kind: runtimeprovideroutput.Kind(output.Kind), Authorization: output.Authorization,
		})
		eventIDs = append(eventIDs, eventID)
		eventNames = append(eventNames, string(output.Name))
		if output.Kind == providertriggers.OutputKindNormalized {
			authorProjection = runtimeauthoractivity.InboundProjection{
				SubjectType: output.AuthorSubjectType,
				SubjectID:   output.AuthorSubjectID,
			}
		}
	}
	if operatorEvent != nil && operatorEvent.BareCandidate == nil {
		published = nil
		eventIDs = nil
		eventNames = nil
		authorProjection = runtimeauthoractivity.InboundProjection{}
	}
	evidencePayload, err := runtimeinbound.BuildEvidencePayload(request, eventIDs, eventNames)
	if err != nil {
		return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, nil, err
	}
	evidence, err := events.NewRunScopedDiagnosticDirectEvent(events.RunScopedRuntimeEventInput{Facts: events.EventFacts{
		ID: request.MarkerEventID, Type: events.EventTypePlatformInboundRecord,
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"}, Payload: evidencePayload,
		CreatedAt: now, ExecutionMode: posture.RootMode(),
	}, RunID: request.ResolvedRunID})
	if err != nil {
		return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, nil, err
	}
	return published, evidence, authorProjection, operatorEvent, nil
}

func writeInboundCommitError(w http.ResponseWriter, logger *RuntimeLogger, requestCtx context.Context, provider, serviceID, providerEventID string, err error) {
	if logger != nil {
		handleRuntimeLogPersistenceError("inbound-gateway", "publish_failed", logger.Error(requestCtx, "inbound-gateway", "publish_failed", map[string]any{
			"provider": provider, "service_id": serviceID, "provider_event_id": providerEventID,
		}, err))
	}
	status := http.StatusServiceUnavailable
	message := "publish inbound failed"
	if errors.Is(err, runtimeinbound.ErrRequestIdentityConflict) || errors.Is(err, operatorchannel.ErrConflict) || errors.Is(err, operatorchannel.ErrRevisionConflict) {
		status = http.StatusConflict
	} else {
		var providerErr providertriggers.Error
		if errors.As(err, &providerErr) {
			status = providerErr.Status
			message = providerErr.Error()
		}
	}
	http.Error(w, message, status)
}

func validateInboundCommittedResultRecord(request runtimeinbound.Request, record runtimeinbound.Record) error {
	if err := record.Request.Validate(); err != nil {
		return fmt.Errorf("acknowledged inbound receipt is invalid: %w", err)
	}
	want, got := request.Normalized(), record.Request.Normalized()
	if record.State != "committed" || got.PublicationID != want.PublicationID || got.MarkerEventID != want.MarkerEventID ||
		got.Identity() != want.Identity() ||
		got.FlowPath != want.FlowPath ||
		got.RequestFingerprint != want.RequestFingerprint || got.RequestProjectionVersion != want.RequestProjectionVersion {
		return errors.New("acknowledged inbound publication does not match the committed request")
	}
	if !record.Created {
		return nil // The store's canonical retry rule admits the original committed record without redispatch.
	}
	if got.ResolvedRunID != want.ResolvedRunID || got.ExpectedPublicationSequence != want.ExpectedPublicationSequence ||
		got.AcknowledgementMode != want.AcknowledgementMode {
		return errors.New("acknowledged inbound publication changed committed execution authority")
	}
	return nil
}

func reportInboundCommittedCleanup(logger *RuntimeLogger, requestCtx context.Context, provider, serviceID, providerEventID string, err error) {
	diaglog.ProcessLog(diaglog.LevelWarn, "inbound-gateway", "committed inbound publication cleanup failed",
		"provider", provider, "service_id", serviceID, "provider_event_id", providerEventID, "error", err.Error())
	if logger != nil {
		handleRuntimeLogPersistenceError("inbound-gateway", "publish_post_commit_cleanup_failed", logger.Error(requestCtx, "inbound-gateway", "publish_post_commit_cleanup_failed", map[string]any{
			"provider": provider, "service_id": serviceID, "provider_event_id": providerEventID,
		}, err))
	}
}

func inboundPublicationResponse(status string, record runtimeinbound.Record, providerEventType string) map[string]any {
	return map[string]any{
		"status": status, "service_id": record.StableServiceID, "run_id": record.ResolvedRunID,
		"generation": record.ExpectedGeneration, "flow_path": record.FlowPath,
		"provider": record.Provider, "provider_event_id": record.ProviderEventID,
		"provider_event_type": providerEventType, "publication_id": record.PublicationID,
		"event_ids": record.EventIDs(), "event_names": record.EventNames(),
	}
}

func parseWebhookPath(path string) (entityID, provider string, ok bool) {
	p := strings.Trim(path, "/")
	parts := strings.Split(p, "/")
	if len(parts) != 3 || parts[0] != "webhooks" {
		return "", "", false
	}
	entityID = parts[1]
	provider = strings.TrimSpace(parts[2])
	if entityID == "" || provider == "" {
		return "", "", false
	}
	return entityID, provider, true
}

func inboundRequestURL(r *http.Request) string {
	if r == nil || r.URL == nil || strings.TrimSpace(r.URL.Fragment) != "" {
		return ""
	}
	scheme := "http"
	host := strings.TrimSpace(r.Host)
	if r.URL.IsAbs() {
		if strings.TrimSpace(r.URL.Scheme) == "" {
			return ""
		}
		scheme = strings.TrimSpace(r.URL.Scheme)
		if host == "" {
			host = strings.TrimSpace(r.URL.Host)
		}
	} else if r.TLS != nil {
		scheme = "https"
	}
	if host == "" {
		return ""
	}
	uri := r.URL.RequestURI()
	if strings.TrimSpace(uri) == "" {
		uri = "/"
	}
	return scheme + "://" + host + uri
}

func inboundQueryValues(r *http.Request) (url.Values, string) {
	if r == nil || r.URL == nil || strings.TrimSpace(r.URL.RawQuery) == "" {
		return nil, ""
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, err.Error()
	}
	return values, ""
}

func inboundFormValues(contentType string, body []byte) (url.Values, bool, string) {
	contentType = strings.TrimSpace(contentType)
	if contentType == "" {
		return nil, false, ""
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, false, err.Error()
	}
	if !strings.EqualFold(mediaType, "application/x-www-form-urlencoded") {
		return nil, false, ""
	}
	values, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, true, err.Error()
	}
	return values, true, ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
