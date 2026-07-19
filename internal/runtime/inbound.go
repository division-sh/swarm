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
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const inboundWebhookMaxBodyBytes = 1 << 20

type InboundPersistence = runtimeinbound.Runner

type InboundTarget struct {
	BundleHash          string
	ServiceID           string
	PackageKey          string
	FlowID              string
	RunID               string
	Generation          int64
	PublicationSequence int64
	InstanceID          string
	FlowInstance        string
	EntityID            string
	EntitySlug          string
	Alias               string
	Provider            string
	SigningSecret       string
	AdmissionPlan       providertriggers.InboundAdmissionPlan
}

func (t InboundTarget) EffectiveEntityID() string {
	return firstNonEmpty(t.EntityID)
}

func (t InboundTarget) EffectiveEntitySlug() string {
	return firstNonEmpty(t.EntitySlug)
}

func (t *InboundTarget) NormalizeEntity() {
	if t == nil {
		return
	}
	entityID := t.EffectiveEntityID()
	entitySlug := t.EffectiveEntitySlug()
	t.EntityID = entityID
	t.EntitySlug = entitySlug
}

type InboundGateway struct {
	bus                     *runtimebus.EventBus
	store                   InboundPersistence
	logger                  *RuntimeLogger
	shutdownAdmissionClosed func() bool
	beginAdmission          func(context.Context) (context.Context, func(), bool)
	runtimeIngress          *runtimeingress.Controller
	credentials             runtimecredentials.Store
	standingAdmissionMu     sync.Mutex
	standingAdmissions      map[string]*shutdownAdmission
}

func (g *InboundGateway) SetAdmissionGuard(begin func(context.Context) (context.Context, func(), bool)) {
	if g != nil {
		g.beginAdmission = begin
	}
}

func NewInboundGateway(bus *runtimebus.EventBus, logger *RuntimeLogger, shutdownAdmissionClosed func() bool, stores ...InboundPersistence) *InboundGateway {
	var store InboundPersistence
	if len(stores) > 0 {
		store = stores[0]
	}
	g := &InboundGateway{
		bus:                     bus,
		store:                   store,
		logger:                  logger,
		shutdownAdmissionClosed: shutdownAdmissionClosed,
	}
	return g
}

func (g *InboundGateway) SetCredentialStore(store runtimecredentials.Store) {
	if g != nil {
		g.credentials = store
	}
}

func (g *InboundGateway) SetRuntimeIngress(controller *runtimeingress.Controller) {
	if g == nil {
		return
	}
	g.runtimeIngress = controller
}

func (g *InboundGateway) CloseStandingServiceAdmission(serviceID string) error {
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
	gate.Close()
	return nil
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
		if g.credentials == nil {
			http.Error(w, fmt.Sprintf("signing secret %s is UNBOUND; run `swarm secrets set %s`", target.SigningSecret, target.SigningSecret), http.StatusServiceUnavailable)
			return
		}
		resolved, ok, err := g.credentials.Get(r.Context(), target.SigningSecret)
		if err != nil {
			http.Error(w, "read signing secret failed", http.StatusServiceUnavailable)
			return
		}
		if !ok || strings.TrimSpace(resolved) == "" {
			http.Error(w, fmt.Sprintf("signing secret %s is UNBOUND; run `swarm secrets set %s`", target.SigningSecret, target.SigningSecret), http.StatusServiceUnavailable)
			return
		}
		signingValue = resolved
	}
	target.NormalizeEntity()
	var payload any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		payload = string(body)
	} else if err := decoder.Decode(&struct{}{}); err != io.EOF {
		payload = string(body)
	}
	entityID := target.EffectiveEntityID()
	entitySlug := target.EffectiveEntitySlug()
	now := time.Now().UTC().Truncate(time.Microsecond)
	admitted, err := target.AdmissionPlan.AdmitRequest(providertriggers.Request{
		Provider: provider,
		Target: providertriggers.Target{
			EntityID:      target.EntityID,
			EntitySlug:    target.EntitySlug,
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
		EntityID          string `json:"entity_id"`
		ProviderEventID   string `json:"provider_event_id"`
		ProviderEventType string `json:"provider_event_type"`
		SemanticDigest    string `json:"semantic_content_digest"`
		StableServiceID   string `json:"stable_service_id"`
		PackageKey        string `json:"package_key"`
		FlowID            string `json:"flow_id"`
		InstanceID        string `json:"instance_id"`
		TargetAlias       string `json:"target_alias"`
		TargetFlow        string `json:"target_flow_instance"`
	}{
		ProjectionVersion: runtimeinbound.RequestSemanticProjectionVersion,
		Provider:          provider, EntityID: entityID, ProviderEventID: providerEventID,
		ProviderEventType: admitted.ProviderEventType, SemanticDigest: admitted.SemanticContentDigest,
		StableServiceID: target.ServiceID, PackageKey: target.PackageKey, FlowID: target.FlowID,
		InstanceID: target.InstanceID, TargetAlias: target.Alias, TargetFlow: target.FlowInstance,
	})
	if err != nil {
		http.Error(w, "derive inbound request fingerprint failed", http.StatusInternalServerError)
		return
	}
	publicationID, markerEventID := runtimeinbound.DeterministicIDs(provider, entityID, providerEventID)
	ackMode := runtimeinbound.AcknowledgementAfterPublish
	if admitted.AcknowledgeBeforeDispatch {
		ackMode = runtimeinbound.AcknowledgementDurableBeforeDispatch
	}
	publicationRequest := runtimeinbound.Request{
		PublicationID: publicationID, Provider: provider, EntityID: entityID, ProviderEventID: providerEventID,
		RequestFingerprint: fingerprint, RequestProjectionVersion: runtimeinbound.RequestSemanticProjectionVersion,
		StableServiceID: target.ServiceID, PackageKey: target.PackageKey, FlowID: target.FlowID,
		InstanceID: target.InstanceID, TargetAlias: target.Alias, TargetFlowInstance: target.FlowInstance,
		ExpectedPublicationSequence: target.PublicationSequence, ResolvedRunID: target.RunID,
		MarkerEventID: markerEventID, AcknowledgementMode: ackMode,
		OriginalReceivedAt: now, OriginalUserAgent: r.UserAgent(),
		OriginalTransportMetadata: mustJSON(map[string]any{"method": r.Method, "content_type": r.Header.Get("Content-Type")}),
	}
	pubCtx := runtimebus.WithCurrentRuntimeEpoch(requestCtx)
	pubCtx = g.bus.WithBundleFingerprint(pubCtx)
	if existing, found, loadErr := g.store.LoadInboundPublicationByIdentity(pubCtx, provider, entityID, providerEventID); loadErr != nil {
		http.Error(w, "read inbound publication failed", http.StatusServiceUnavailable)
		return
	} else if found {
		if existing.RequestProjectionVersion != publicationRequest.RequestProjectionVersion || existing.RequestFingerprint != publicationRequest.RequestFingerprint {
			http.Error(w, "inbound provider identity conflicts with the committed semantic request", http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusOK, inboundPublicationResponse("duplicate", existing, admitted.ProviderEventType, entityID, entitySlug))
		return
	}

	published, evidence, authorProjection, projectionErr := projectInboundPublication(target, admitted, publicationRequest, now)
	var prepared []runtimebus.PreparedPublish
	record, err := g.store.RunInboundPublicationMutation(pubCtx, publicationRequest, func(mutation runtimeinbound.Mutation) error {
		if projectionErr != nil {
			return projectionErr
		}
		mutationCtx := runtimeauthoractivity.WithInboundProjection(mutation.Context(), authorProjection)
		var prepareErr error
		prepared, prepareErr = g.bus.PrepareInboundDeliveryBatchInMutation(mutationCtx, runtimebus.InboundDeliveryBatch{
			Provider:          provider,
			AuthorSubjectType: authorProjection.SubjectType,
			AuthorSubjectID:   authorProjection.SubjectID,
			AuthorSummary:     authorProjection.Summary,
			Events:            published,
		})
		if prepareErr != nil {
			return prepareErr
		}
		finalization := runtimeinbound.Finalization{EvidenceEvent: evidence, Events: make([]runtimeinbound.EventFinalization, len(prepared))}
		for index := range prepared {
			manifest, _, _, manifestErr := runtimeinbound.CanonicalRecipientManifest(prepared[index].DeliveryRoutes())
			if manifestErr != nil {
				return manifestErr
			}
			finalization.Events[index] = runtimeinbound.EventFinalization{
				Ordinal: index, Event: prepared[index].Event, Kind: published[index].Kind,
				Authorization: published[index].Authorization, RecipientManifest: manifest,
			}
		}
		return mutation.FinalizeInboundPublication(mutationCtx, finalization)
	})
	if err != nil {
		if g.logger != nil {
			handleRuntimeLogPersistenceError("inbound-gateway", "publish_failed", g.logger.Error(requestCtx, "inbound-gateway", "publish_failed", map[string]any{
				"provider": provider, "entity_id": entityID, "provider_event_id": providerEventID,
			}, err))
		}
		status := http.StatusServiceUnavailable
		message := "publish inbound failed"
		if errors.Is(err, runtimeinbound.ErrRequestIdentityConflict) {
			status = http.StatusConflict
		} else {
			var providerErr providertriggers.Error
			if errors.As(err, &providerErr) {
				status = providerErr.Status
				message = providerErr.Error()
			}
		}
		http.Error(w, message, status)
		return
	}
	if !record.Created {
		writeJSON(w, http.StatusOK, inboundPublicationResponse("duplicate", record, admitted.ProviderEventType, entityID, entitySlug))
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
		for _, item := range prepared {
			g.bus.DispatchPreparedPublishAsync(pubCtx, item)
		}
	} else {
		for _, item := range prepared {
			if err := g.bus.DispatchPreparedPublish(pubCtx, item); err != nil {
				http.Error(w, "publish inbound failed", http.StatusServiceUnavailable)
				return
			}
		}
	}
	writeJSON(w, http.StatusAccepted, inboundPublicationResponse("accepted", record, admitted.ProviderEventType, entityID, entitySlug))
}

func projectInboundPublication(target InboundTarget, admitted providertriggers.AdmittedRequest, request runtimeinbound.Request, now time.Time) ([]runtimebus.InboundDeliveryEvent, events.Event, runtimeauthoractivity.InboundProjection, error) {
	var noEvidence events.Event
	delivery, err := target.AdmissionPlan.ProjectDelivery(admitted)
	if err != nil {
		return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, err
	}
	if delivery.ProviderEventID != admitted.ProviderEventID || delivery.ProviderEventType != admitted.ProviderEventType {
		return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, fmt.Errorf("compiled provider projection changed admitted request identity")
	}
	routingSource, err := events.NewDeclaredIngressRoutingSource(target.FlowID, "", request.EntityID, "provider_admission_plan")
	if err != nil {
		return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, err
	}
	published := make([]runtimebus.InboundDeliveryEvent, 0, len(delivery.Events))
	eventIDs := make([]string, 0, len(delivery.Events))
	eventNames := make([]string, 0, len(delivery.Events))
	authorProjection := runtimeauthoractivity.InboundProjection{}
	for ordinal, output := range delivery.Events {
		eventID, err := runtimeinbound.DeterministicEventID(request.PublicationID, ordinal)
		if err != nil {
			return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, err
		}
		envelope := events.EventEnvelope{}
		if output.Kind == providertriggers.OutputKindRaw {
			envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{EntityID: request.EntityID, FlowInstance: target.FlowInstance})
		}
		event, err := events.NewExistingRunRootIngressEvent(events.ExistingRunRootIngressEventInput{Facts: events.EventFacts{
			ID: eventID, Type: output.Name, Producer: events.ProducerClaim{Type: events.EventProducerExternal, ID: "inbound-gateway"},
			Payload: mustJSON(output.Payload), Envelope: envelope, RoutingSource: routingSource,
			CreatedAt: now, ExecutionMode: executionmode.Live,
		}, RunID: request.ResolvedRunID})
		if err != nil {
			return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, err
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
				Summary:     output.AuthorSummary,
			}
		}
	}
	evidencePayload, err := runtimeinbound.BuildEvidencePayload(request, eventIDs, eventNames)
	if err != nil {
		return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, err
	}
	evidence, err := events.NewRunScopedDiagnosticDirectEvent(events.RunScopedRuntimeEventInput{Facts: events.EventFacts{
		ID: request.MarkerEventID, Type: events.EventTypePlatformInboundRecord,
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"}, Payload: evidencePayload,
		Envelope: events.EnvelopeForEntityID(events.EventEnvelope{}, request.EntityID), CreatedAt: now, ExecutionMode: executionmode.Live,
	}, RunID: request.ResolvedRunID})
	if err != nil {
		return nil, noEvidence, runtimeauthoractivity.InboundProjection{}, err
	}
	return published, evidence, authorProjection, nil
}

func inboundPublicationResponse(status string, record runtimeinbound.Record, providerEventType, entityID, entitySlug string) map[string]any {
	return map[string]any{
		"status": status, "entity_id": entityID, "entity_slug": entitySlug,
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
	entityID = strings.TrimSpace(parts[1])
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
