package runtime

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestInboundGatewayStandingServiceAdmissionClosesDrainsAndReopens(t *testing.T) {
	gateway := NewInboundGateway(nil, nil, nil)
	activeCtx, release, admitted := gateway.beginStandingServiceAdmission(context.Background(), "service-1")
	if !admitted || activeCtx == nil || release == nil {
		t.Fatal("initial standing service admission was rejected")
	}
	if err := gateway.ReopenStandingServiceAdmission("service-1"); err != nil {
		t.Fatalf("ReopenStandingServiceAdmission while already open: %v", err)
	}
	if err := gateway.CloseStandingServiceAdmission("service-1"); err != nil {
		t.Fatalf("CloseStandingServiceAdmission: %v", err)
	}
	if _, _, admitted := gateway.beginStandingServiceAdmission(context.Background(), "service-1"); admitted {
		t.Fatal("closed standing service admitted a new request")
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := gateway.WaitForStandingServiceAdmission(waitCtx, "service-1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForStandingServiceAdmission while active = %v, want deadline", err)
	}
	if err := gateway.ReopenStandingServiceAdmission("service-1"); err == nil {
		t.Fatal("ReopenStandingServiceAdmission succeeded before drain")
	}
	release()
	if err := gateway.WaitForStandingServiceAdmission(context.Background(), "service-1"); err != nil {
		t.Fatalf("WaitForStandingServiceAdmission after release: %v", err)
	}
	if err := gateway.ReopenStandingServiceAdmission("service-1"); err != nil {
		t.Fatalf("ReopenStandingServiceAdmission: %v", err)
	}
	_, reopenedRelease, admitted := gateway.beginStandingServiceAdmission(context.Background(), "service-1")
	if !admitted {
		t.Fatal("reopened standing service rejected admission")
	}
	reopenedRelease()
}

type testInboundTargetResolver interface {
	ResolveInboundTarget(context.Context, string, string) (InboundTarget, error)
}

type testInboundGateway struct {
	*InboundGateway
	resolver testInboundTargetResolver
	catalog  *providertriggers.CatalogSnapshot
}

func (g *testInboundGateway) HandleResolvedWebhook(w http.ResponseWriter, r *http.Request, target InboundTarget, source semanticview.Source) {
	if target.Provider == "" || target.Alias == "" {
		alias, provider, _ := parseWebhookPath(r.URL.Path)
		if target.Alias == "" {
			target.Alias = alias
		}
		if target.Provider == "" {
			target.Provider = provider
		}
	}
	if !target.AdmissionPlan.Valid() && g.catalog != nil {
		declaration := providertriggers.AdmissionDeclaration{}
		if _, installed := g.catalog.EntryByProvider(target.Provider); !installed && target.SigningSecret == "" {
			declaration = providertriggers.AdmissionDeclaration{
				Kind: "raw", Acknowledge: providertriggers.UnsignedWebhookAcknowledgement,
				Authentication: providertriggers.RawAuthenticationDeclaration{Kind: "none"},
				Event:          "inbound." + providertriggers.NormalizeProviderName(target.Provider),
				DeliveryID:     providertriggers.RawDeliveryIDDeclaration{Source: "json_path", JSONPath: "$.id"}, Payload: "json",
			}
		}
		plan, err := g.catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
			Alias: target.Alias, Provider: target.Provider, SigningSecret: target.SigningSecret, Declaration: declaration,
		})
		if err == nil {
			target.AdmissionPlan = plan
		}
	}
	g.InboundGateway.HandleResolvedWebhook(w, r, target, source)
}

func (g *testInboundGateway) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		alias, provider, ok := parseWebhookPath(r.URL.Path)
		if !ok {
			http.Error(w, "expected /webhooks/{alias}/{provider}", http.StatusBadRequest)
			return
		}
		if g == nil || g.resolver == nil {
			http.Error(w, fmt.Sprintf("no ingress target %q is declared; add ingress to a standing singleton flow", alias), http.StatusNotFound)
			return
		}
		target, err := g.resolver.ResolveInboundTarget(r.Context(), alias, provider)
		if err != nil {
			http.Error(w, fmt.Sprintf("no ingress target %q is declared; add ingress to a standing singleton flow", alias), http.StatusNotFound)
			return
		}
		g.HandleResolvedWebhook(w, r, target, nil)
	})
}

func newTestInboundGateway(t *testing.T, bus *runtimebus.EventBus, logger *RuntimeLogger, shutdownAdmissionClosed func() bool, stores ...InboundPersistence) *testInboundGateway {
	t.Helper()
	root := filepath.Join("..", "..", "packs", "provider-triggers")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read provider trigger pack root: %v", err)
	}
	dirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, filepath.Join(root, entry.Name()))
		}
	}
	sort.Strings(dirs)
	registry, _, err := providertriggers.NewCatalogSnapshotFromPackDirs("0.7.0", dirs, nil)
	if err != nil {
		t.Fatalf("load provider trigger registry: %v", err)
	}
	if bus != nil {
		bus.SetProviderOutputAuthorizationVerifier(registry)
	}
	gateway := NewInboundGateway(bus, logger, shutdownAdmissionClosed, stores...)
	if len(stores) > 0 && bus != nil {
		if store, ok := stores[0].(interface{ bindTestInboundEventStore(runtimebus.EventStore) }); ok {
			store.bindTestInboundEventStore(bus.Store())
		}
	}
	gateway.SetCredentialStore(identityInboundCredentialStore{})
	var resolver testInboundTargetResolver
	if len(stores) > 0 {
		resolver, _ = any(stores[0]).(testInboundTargetResolver)
	}
	return &testInboundGateway{InboundGateway: gateway, resolver: resolver, catalog: registry}
}

type identityInboundCredentialStore struct{}

func (identityInboundCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	return key, strings.TrimSpace(key) != "", nil
}
func (identityInboundCredentialStore) Set(context.Context, string, string) error { return nil }
func (identityInboundCredentialStore) List(context.Context) ([]string, error)    { return nil, nil }
func (identityInboundCredentialStore) Delete(context.Context, string) error      { return nil }

type failingInboundEventStore struct{}

func (failingInboundEventStore) AppendEvent(context.Context, events.Event) error {
	return errors.New("append failed")
}

func (failingInboundEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

func (failingInboundEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}

func (s failingInboundEventStore) RunEventMutation(ctx context.Context, fn func(runtimebus.EventMutation) error) error {
	return runInboundTestMutation(ctx, s.AppendEvent, nil, fn)
}

type capturingInboundEventStore struct {
	events          []events.Event
	seen            map[string]struct{}
	duplicate       bool
	recorded        bool
	providerEventID string
	entityID        string
	provider        string
}

func (s *capturingInboundEventStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.events = append(s.events, evt)
	s.recorded = true
	return nil
}

func (*capturingInboundEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

func (*capturingInboundEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}

func (s *capturingInboundEventStore) RunEventMutation(ctx context.Context, fn func(runtimebus.EventMutation) error) error {
	if s.seen == nil {
		s.seen = map[string]struct{}{}
	}
	return runInboundTestMutation(ctx, s.AppendEvent, s, fn)
}

type inboundTestMutation struct {
	ctx    context.Context
	append func(context.Context, events.Event) error
	sink   *capturingInboundEventStore
}

func runInboundTestMutation(ctx context.Context, appendEvent func(context.Context, events.Event) error, sink *capturingInboundEventStore, fn func(runtimebus.EventMutation) error) error {
	postCommit := make([]func(), 0, 4)
	mutation := &inboundTestMutation{append: appendEvent, sink: sink}
	mutation.ctx = runtimebus.WithEventMutationContext(runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommit), mutation)
	if err := fn(mutation); err != nil {
		return err
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	return nil
}

func (m *inboundTestMutation) Context() context.Context { return m.ctx }
func (m *inboundTestMutation) AppendEvent(ctx context.Context, event events.Event) error {
	return m.append(ctx, event)
}
func (m *inboundTestMutation) AppendEventOutcome(ctx context.Context, event events.Event) (runtimebus.EventAppendOutcome, error) {
	if err := m.append(ctx, event); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return runtimebus.EventAppendInserted, nil
}
func (*inboundTestMutation) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (*inboundTestMutation) InsertEventDeliveriesWithTargets(context.Context, string, []string, map[string]events.RouteIdentity) error {
	return nil
}
func (*inboundTestMutation) InsertEventDeliveryRoutes(context.Context, string, []events.DeliveryRoute) error {
	return nil
}
func (*inboundTestMutation) UpsertCommittedReplayScope(context.Context, string, runtimereplayclaim.CommittedReplayScope) error {
	return nil
}
func (*inboundTestMutation) UpsertPipelineReceipt(context.Context, string, string, *runtimefailures.Envelope) error {
	return nil
}
func (*inboundTestMutation) RecordDeadLetter(context.Context, runtimedeadletters.Record) error {
	return nil
}
func TestInboundGatewayResolvedTargetPreservesStandingAuthority(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{inserted: true}
	gateway := newTestInboundGateway(t, bus, nil, nil, store)
	body := []byte(`{"update_id":123,"message":{"message_id":7,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"hello"}}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/chat/telegram", strings.NewReader(string(body)))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
	rec := httptest.NewRecorder()
	gateway.HandleResolvedWebhook(rec, req, InboundTarget{
		BundleHash: "bundle-v1:sha256:" + strings.Repeat("a", 64), FlowID: "chat-flow",
		RunID: "41000000-0000-0000-0000-000000000001", FlowInstance: "chat-flow/a",
		EntityID: "41000000-0000-0000-0000-000000000002", Alias: "chat", Provider: "telegram",
		SigningSecret: "telegram-secret",
	}, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s, want 202", rec.Code, rec.Body.String())
	}
	if len(eventStore.events) != 2 {
		t.Fatalf("persisted events = %d, want raw plus normalized", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.RunID() != "41000000-0000-0000-0000-000000000001" || evt.FlowInstance() != "chat-flow/a" || evt.EntityID() != "41000000-0000-0000-0000-000000000002" {
		t.Fatalf("event authority = run=%s flow_instance=%s entity=%s", evt.RunID(), evt.FlowInstance(), evt.EntityID())
	}
}

type rollbackTrackingInboundStore struct {
	recorded bool
	rolled   bool
	store    runtimebus.EventStore
}

func (s *rollbackTrackingInboundStore) bindTestInboundEventStore(store runtimebus.EventStore) {
	s.store = store
}

func (s *rollbackTrackingInboundStore) ResolveInboundTarget(context.Context, string, string) (InboundTarget, error) {
	return InboundTarget{EntityID: "entity-1", EntitySlug: "entity-1"}, nil
}

type recordingInboundStore struct {
	target          InboundTarget
	resolveErr      error
	inserted        bool
	recorded        bool
	providerEventID string
	entityID        string
	provider        string
	store           runtimebus.EventStore
	record          runtimeinbound.Record
	integrityErr    error
	integrityCalls  atomic.Int32
}

type concurrentInboundStore struct {
	mu               sync.Mutex
	store            runtimebus.EventStore
	record           runtimeinbound.Record
	inFlight         bool
	firstRunEntered  chan struct{}
	contenderEntered chan struct{}
	releaseFirst     chan struct{}
	committed        chan struct{}
	callbackCalls    atomic.Int32
}

func newConcurrentInboundStore() *concurrentInboundStore {
	return &concurrentInboundStore{
		firstRunEntered: make(chan struct{}), contenderEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}), committed: make(chan struct{}),
	}
}

func (s *concurrentInboundStore) bindTestInboundEventStore(store runtimebus.EventStore) {
	s.store = store
}

func (s *concurrentInboundStore) RunInboundPublicationMutation(ctx context.Context, request runtimeinbound.Request, fn func(runtimeinbound.Mutation) error) (runtimeinbound.Record, error) {
	s.mu.Lock()
	if s.record.State == "committed" {
		record := s.record
		s.mu.Unlock()
		if err := validateConcurrentInboundRetry(request, record); err != nil {
			return runtimeinbound.Record{}, err
		}
		record.Created = false
		return record, nil
	}
	if s.inFlight {
		committed := s.committed
		select {
		case <-s.contenderEntered:
		default:
			close(s.contenderEntered)
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return runtimeinbound.Record{}, ctx.Err()
		case <-committed:
		}
		s.mu.Lock()
		record := s.record
		s.mu.Unlock()
		if err := validateConcurrentInboundRetry(request, record); err != nil {
			return runtimeinbound.Record{}, err
		}
		record.Created = false
		return record, nil
	}
	s.inFlight = true
	close(s.firstRunEntered)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		return runtimeinbound.Record{}, ctx.Err()
	case <-s.releaseFirst:
	}
	s.callbackCalls.Add(1)
	record, err := runTestInboundPublication(ctx, s.store, request, true, fn)
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	s.mu.Lock()
	s.record = record
	s.inFlight = false
	close(s.committed)
	s.mu.Unlock()
	return record, nil
}

func (s *concurrentInboundStore) LoadInboundPublicationByIdentity(context.Context, string, string, string) (runtimeinbound.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.record
	record.Created = false
	return record, record.State == "committed", nil
}

func (*concurrentInboundStore) ValidateInboundPublicationIntegrity(context.Context) error { return nil }

func validateConcurrentInboundRetry(request runtimeinbound.Request, record runtimeinbound.Record) error {
	if request.RequestProjectionVersion != record.RequestProjectionVersion || request.RequestFingerprint != record.RequestFingerprint {
		return runtimeinbound.ErrRequestIdentityConflict
	}
	return nil
}

func (s *recordingInboundStore) bindTestInboundEventStore(store runtimebus.EventStore) {
	s.store = store
}

func (s *recordingInboundStore) ResolveInboundTarget(context.Context, string, string) (InboundTarget, error) {
	return s.target, s.resolveErr
}

type testInboundPublicationMutation struct {
	ctx          context.Context
	store        runtimebus.EventStore
	finalization runtimeinbound.Finalization
	finalized    bool
}

func newTestInboundPublicationMutation(ctx context.Context, store runtimebus.EventStore) *testInboundPublicationMutation {
	mutation := &testInboundPublicationMutation{store: store}
	mutation.ctx = runtimebus.WithEventMutationContext(ctx, mutation)
	return mutation
}

func (m *testInboundPublicationMutation) Context() context.Context { return m.ctx }

func (m *testInboundPublicationMutation) AppendEvent(ctx context.Context, evt events.Event) error {
	if m.store == nil {
		return nil
	}
	return m.store.AppendEvent(ctx, evt)
}

func (m *testInboundPublicationMutation) AppendEventOutcome(ctx context.Context, evt events.Event) (runtimebus.EventAppendOutcome, error) {
	if m.store == nil {
		return runtimebus.EventAppendInserted, nil
	}
	if owner, ok := m.store.(runtimebus.EventAppendOutcomePersistence); ok {
		return owner.AppendEventOutcome(ctx, evt)
	}
	if err := m.store.AppendEvent(ctx, evt); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return runtimebus.EventAppendInserted, nil
}

func (m *testInboundPublicationMutation) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	if m.store == nil {
		return nil
	}
	return m.store.InsertEventDeliveries(ctx, eventID, agentIDs)
}

func (m *testInboundPublicationMutation) InsertEventDeliveriesWithTargets(ctx context.Context, eventID string, agentIDs []string, _ map[string]events.RouteIdentity) error {
	return m.InsertEventDeliveries(ctx, eventID, agentIDs)
}

func (*testInboundPublicationMutation) InsertEventDeliveryRoutes(context.Context, string, []events.DeliveryRoute) error {
	return nil
}

func (*testInboundPublicationMutation) UpsertCommittedReplayScope(context.Context, string, runtimereplayclaim.CommittedReplayScope) error {
	return nil
}

func (*testInboundPublicationMutation) UpsertPipelineReceipt(context.Context, string, string, *runtimefailures.Envelope) error {
	return nil
}

func (*testInboundPublicationMutation) RecordDeadLetter(context.Context, runtimedeadletters.Record) error {
	return nil
}

func (m *testInboundPublicationMutation) FinalizeInboundPublication(_ context.Context, finalization runtimeinbound.Finalization) error {
	m.finalization = finalization
	m.finalized = true
	return nil
}

func runTestInboundPublication(ctx context.Context, eventStore runtimebus.EventStore, request runtimeinbound.Request, inserted bool, fn func(runtimeinbound.Mutation) error) (runtimeinbound.Record, error) {
	if !inserted {
		request.AcknowledgementMode = runtimeinbound.AcknowledgementDurableBeforeDispatch
		eventID, _ := runtimeinbound.DeterministicEventID(request.PublicationID, 0)
		return runtimeinbound.Record{
			Request: request, State: "committed", OutputCount: 1,
			Events: []runtimeinbound.EventRecord{{Ordinal: 0, EventID: eventID, EventName: "inbound." + request.Provider}},
		}, nil
	}
	mutation := newTestInboundPublicationMutation(ctx, eventStore)
	if err := fn(mutation); err != nil {
		return runtimeinbound.Record{}, err
	}
	if !mutation.finalized {
		return runtimeinbound.Record{}, errors.New("test inbound publication was not finalized")
	}
	eventRecords := make([]runtimeinbound.EventRecord, len(mutation.finalization.Events))
	for index, child := range mutation.finalization.Events {
		var routes []events.DeliveryRoute
		if err := json.Unmarshal(child.RecipientManifest, &routes); err != nil {
			return runtimeinbound.Record{}, fmt.Errorf("decode test inbound recipient manifest: %w", err)
		}
		_, recipientFingerprint, count, err := runtimeinbound.CanonicalRecipientManifest(routes)
		if err != nil {
			return runtimeinbound.Record{}, err
		}
		eventFingerprint, err := runtimeinbound.EventIntegrityFingerprint(child.Event, child.Kind, child.Authorization)
		if err != nil {
			return runtimeinbound.Record{}, err
		}
		eventRecords[index] = runtimeinbound.EventRecord{
			Ordinal: child.Ordinal, EventID: child.Event.ID(), EventName: string(child.Event.Type()), Kind: child.Kind,
			Authorization: child.Authorization, EventIntegrityFingerprint: eventFingerprint,
			RecipientManifestFingerprint: recipientFingerprint, RecipientCount: count, Event: child.Event,
		}
	}
	return runtimeinbound.Record{
		Request: request, State: "committed", OutputCount: len(eventRecords), Events: eventRecords, Created: true,
	}, nil
}

func (s *recordingInboundStore) RunInboundPublicationMutation(ctx context.Context, request runtimeinbound.Request, fn func(runtimeinbound.Mutation) error) (runtimeinbound.Record, error) {
	s.providerEventID = request.ProviderEventID
	s.entityID = request.EntityID
	s.provider = request.Provider
	record, err := runTestInboundPublication(ctx, s.store, request, s.inserted, fn)
	if err == nil {
		s.recorded = true
		s.record = record
		if sink, ok := s.store.(*capturingInboundEventStore); ok {
			sink.providerEventID = request.ProviderEventID
			sink.entityID = request.EntityID
			sink.provider = request.Provider
		}
	}
	return record, err
}

func (s *recordingInboundStore) LoadInboundPublicationByIdentity(context.Context, string, string, string) (runtimeinbound.Record, bool, error) {
	return s.record, s.record.State == "committed", nil
}

func (s *recordingInboundStore) ValidateInboundPublicationIntegrity(context.Context) error {
	s.integrityCalls.Add(1)
	return s.integrityErr
}

func (s *rollbackTrackingInboundStore) RunInboundPublicationMutation(ctx context.Context, request runtimeinbound.Request, fn func(runtimeinbound.Mutation) error) (runtimeinbound.Record, error) {
	s.recorded = true
	record, err := runTestInboundPublication(ctx, s.store, request, true, fn)
	if err != nil {
		s.rolled = true
	}
	return record, err
}

func (*rollbackTrackingInboundStore) LoadInboundPublicationByIdentity(context.Context, string, string, string) (runtimeinbound.Record, bool, error) {
	return runtimeinbound.Record{}, false, nil
}

func (*rollbackTrackingInboundStore) ValidateInboundPublicationIntegrity(context.Context) error {
	return nil
}

func TestInboundGateway_Returns503WithoutCompensatingMarkerPathWhenBatchFails(t *testing.T) {
	bus, err := runtimebus.NewEventBus(failingInboundEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &rollbackTrackingInboundStore{}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/custom", strings.NewReader(`{"id":"evt-1","type":"push"}`))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if !store.recorded || !store.rolled {
		t.Fatal("publication runner did not own and roll back the failed mutation")
	}
}

func TestInboundGateway_Returns503WhenRuntimeShutdownAdmissionClosed(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &rollbackTrackingInboundStore{}
	g := newTestInboundGateway(t, bus, nil, func() bool { return true }, store)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/custom", strings.NewReader(`{"id":"evt-1","type":"push"}`))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "runtime shutting down") {
		t.Fatalf("body = %q, want runtime shutting down", rec.Body.String())
	}
	if store.recorded {
		t.Fatal("did not expect inbound event recording after shutdown admission closed")
	}
}

func TestInboundGateway_UnknownTargetFailsBeforeProviderAdmission(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{resolveErr: errors.New("target not found")}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	// A GitHub request without a signature would fail provider admission with 401
	// if target resolution did not gate the provider interpreter first.
	req := httptest.NewRequest(http.MethodPost, "/webhooks/unknown/github", strings.NewReader(`{"id":"evt-1"}`))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "no ingress target \"unknown\" is declared") {
		t.Fatalf("response = %d %q, want target-gate 404", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("unknown target reached provider marker persistence")
	}
}

func TestInboundGateway_PausedRuntimeUsesIngressOwnerAndAcceptsQueueableWebhook(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(nil, bus, runtimeingress.Options{})
	bus.SetRuntimeIngressDispatchGate(controller)
	if _, err := controller.Pause(testAuthorActivityContext(context.Background()), runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	store := &rollbackTrackingInboundStore{}
	g := newTestInboundGateway(t, bus, nil, nil, store)
	g.SetRuntimeIngress(controller)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/entity-1/custom", strings.NewReader(`{"id":"evt-1","type":"push"}`))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !eventStore.recorded {
		t.Fatal("expected inbound event to be recorded while paused")
	}
}

func TestInboundGateway_GitHubPausedRuntimeUsesIngressOwnerAndAcceptsQueueableWebhook(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	controller := runtimeingress.NewController(nil, bus, runtimeingress.Options{})
	bus.SetRuntimeIngressDispatchGate(controller)
	if _, err := controller.Pause(testAuthorActivityContext(context.Background()), runtimeingress.TransitionRequest{
		Reason:       "test_pause",
		ControlledBy: "test",
	}); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	t.Cleanup(runtimebus.ResumeRuntimeIngress)
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "github-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)
	g.SetRuntimeIngress(controller)

	body := []byte(`{"zen":"Keep it logically awesome."}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubWebhookSignature("github-secret", body))
	req.Header.Set("X-GitHub-Delivery", "delivery-123")
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !eventStore.recorded {
		t.Fatal("expected GitHub delivery to record inbound marker while paused")
	}
	if eventStore.providerEventID != "delivery-123" {
		t.Fatalf("providerEventID = %q, want delivery-123", eventStore.providerEventID)
	}
}

func TestInboundGateway_GitHubAdapterOwnsSignatureDeliveryIDAndEventMapping(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "github-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"zen":"Keep it logically awesome."}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubWebhookSignature("github-secret", body))
	req.Header.Set("X-GitHub-Delivery", "delivery-123")
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !eventStore.recorded {
		t.Fatal("expected GitHub delivery to record inbound marker")
	}
	if eventStore.providerEventID != "delivery-123" {
		t.Fatalf("providerEventID = %q, want delivery-123", eventStore.providerEventID)
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.Type() != events.EventType("inbound.github.push") {
		t.Fatalf("event type = %q, want inbound.github.push", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["provider_event_id"] != "delivery-123" || payload["event_type"] != "push" || payload["provider"] != "github" {
		t.Fatalf("payload = %+v, want GitHub delivery identity", payload)
	}
	if strings.Contains(rec.Body.String(), "github-secret") || strings.Contains(string(evt.Payload()), "github-secret") {
		t.Fatal("GitHub signing secret leaked into readback or event payload")
	}
}

func TestInboundGateway_GitHubAdapterRejectsInvalidSignatureBeforeMarkerAndPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "github-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"zen":"Keep it logically awesome."}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubWebhookSignature("wrong-secret", body))
	req.Header.Set("X-GitHub-Delivery", "delivery-123")
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("invalid GitHub signature recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_GitHubAdapterDuplicateDeliveryDoesNotPublishAgain(t *testing.T) {
	eventStore := &capturingInboundEventStore{duplicate: true}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "github-secret",
		},
		inserted: false,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"zen":"Keep it logically awesome."}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubWebhookSignature("github-secret", body))
	req.Header.Set("X-GitHub-Delivery", "delivery-123")
	req.Header.Set("X-GitHub-Event", "push")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"duplicate"`) {
		t.Fatalf("duplicate response = %s", rec.Body.String())
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGatewayExactRetryBypassesCurrentProjectionAndConflictsOnChangedRedactedSemantics(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{inserted: true}
	gateway := newTestInboundGateway(t, bus, nil, nil, store)
	firstPlan, firstCatalog := compiledRedactedNormalizedPlan(t, "1.0.0", "text")
	bus.SetProviderOutputAuthorizationVerifier(firstCatalog)
	target := InboundTarget{
		ServiceID: "9f733ec3-f834-47ff-bd55-3ea9038187ef", PackageKey: "retry-proof", FlowID: "ingress",
		RunID: "85fe8f5a-40dd-4ff2-8785-9f5450e42687", PublicationSequence: 1,
		InstanceID: "instance-1", FlowInstance: "ingress/instance-1",
		EntityID: "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a", EntitySlug: "customer-a",
		Alias: "chat", Provider: "telegram", AdmissionPlan: firstPlan,
	}

	first := httptest.NewRecorder()
	gateway.HandleResolvedWebhook(first, normalizedRetryRequest(`{"message":{"text":"hello"}}`), target, nil)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202 body=%s", first.Code, first.Body.String())
	}
	if !strings.Contains(first.Body.String(), `"event_names":["inbound.telegram","inbound.telegram.text_message"]`) {
		t.Fatalf("first response does not expose ordered committed children: %s", first.Body.String())
	}
	committedEventCount := len(eventStore.events)

	projectionFailingPlan, _ := compiledRedactedNormalizedPlan(t, "2.0.0", "integer")
	target.AdmissionPlan = projectionFailingPlan
	duplicate := httptest.NewRecorder()
	gateway.HandleResolvedWebhook(duplicate, normalizedRetryRequest(`{"message":{"text":"hello"}}`), target, nil)
	if duplicate.Code != http.StatusOK || !strings.Contains(duplicate.Body.String(), `"status":"duplicate"`) {
		t.Fatalf("duplicate status = %d body=%s", duplicate.Code, duplicate.Body.String())
	}
	if !strings.Contains(duplicate.Body.String(), `"event_names":["inbound.telegram","inbound.telegram.text_message"]`) {
		t.Fatalf("duplicate response did not return original ordered children: %s", duplicate.Body.String())
	}
	if len(eventStore.events) != committedEventCount {
		t.Fatalf("duplicate published %d additional events", len(eventStore.events)-committedEventCount)
	}

	target.AdmissionPlan = firstPlan
	conflict := httptest.NewRecorder()
	gateway.HandleResolvedWebhook(conflict, normalizedRetryRequest(`{"message":{"text":"changed"}}`), target, nil)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("changed redacted semantic status = %d, want 409 body=%s", conflict.Code, conflict.Body.String())
	}
	if len(eventStore.events) != committedEventCount {
		t.Fatalf("conflicting retry published %d additional events", len(eventStore.events)-committedEventCount)
	}
}

func TestInboundGatewayConcurrentLoserReturnsCommittedBatchDespiteCurrentProjectionFailure(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := newConcurrentInboundStore()
	gateway := newTestInboundGateway(t, bus, nil, nil, store)
	firstPlan, firstCatalog := compiledRedactedNormalizedPlan(t, "1.0.0", "text")
	projectionFailingPlan, _ := compiledRedactedNormalizedPlan(t, "2.0.0", "integer")
	bus.SetProviderOutputAuthorizationVerifier(firstCatalog)
	target := InboundTarget{
		ServiceID: "9f733ec3-f834-47ff-bd55-3ea9038187ef", PackageKey: "retry-proof", FlowID: "ingress",
		RunID: "85fe8f5a-40dd-4ff2-8785-9f5450e42687", PublicationSequence: 1,
		InstanceID: "instance-1", FlowInstance: "ingress/instance-1",
		EntityID: "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a", EntitySlug: "customer-a",
		Alias: "chat", Provider: "telegram", AdmissionPlan: firstPlan,
	}

	first := httptest.NewRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		gateway.HandleResolvedWebhook(first, normalizedRetryRequest(`{"message":{"text":"hello"}}`), target, nil)
	}()
	select {
	case <-store.firstRunEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first publication did not enter the identity owner")
	}

	contenderTarget := target
	contenderTarget.AdmissionPlan = projectionFailingPlan
	contender := httptest.NewRecorder()
	contenderDone := make(chan struct{})
	go func() {
		defer close(contenderDone)
		gateway.HandleResolvedWebhook(contender, normalizedRetryRequest(`{"message":{"text":"hello"}}`), contenderTarget, nil)
	}()
	select {
	case <-store.contenderEntered:
	case <-time.After(2 * time.Second):
		close(store.releaseFirst)
		<-firstDone
		<-contenderDone
		t.Fatalf("concurrent loser did not reach identity serialization; status=%d body=%s", contender.Code, contender.Body.String())
	}
	close(store.releaseFirst)
	<-firstDone
	<-contenderDone

	if first.Code != http.StatusAccepted {
		t.Fatalf("winner status = %d, want 202 body=%s", first.Code, first.Body.String())
	}
	if contender.Code != http.StatusOK || !strings.Contains(contender.Body.String(), `"status":"duplicate"`) {
		t.Fatalf("concurrent loser status = %d, want duplicate 200 body=%s", contender.Code, contender.Body.String())
	}
	if got := store.callbackCalls.Load(); got != 1 {
		t.Fatalf("publication callback calls = %d, want 1", got)
	}
	if !strings.Contains(contender.Body.String(), `"event_names":["inbound.telegram","inbound.telegram.text_message"]`) {
		t.Fatalf("concurrent loser did not return winner batch: %s", contender.Body.String())
	}
}

func compiledRedactedNormalizedPlan(t *testing.T, version, projectedType string) (providertriggers.InboundAdmissionPlan, *providertriggers.CatalogSnapshot) {
	t.Helper()
	manifest := providertriggers.Manifest{
		Provider: "telegram", PayloadObjectRequired: true,
		DeliveryID: providertriggers.ValueSource{Literal: "delivery-1", Required: true},
		EventType:  providertriggers.ValueSource{Literal: "message", Required: true},
		EventName:  providertriggers.EventNameManifest{Literal: "inbound.telegram"},
		RedactKeys: []string{"text"},
		NormalizedEvents: []providertriggers.NormalizedEventManifest{{
			Event: "inbound.telegram.text_message",
			Fields: map[string]providertriggers.NormalizedEventFieldProjection{
				"text": {From: "message.text", Schema: runtimecontracts.ToolInputSchema{Type: mapProjectedSchemaType(projectedType)}},
			},
		}},
	}
	catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{
		Manifest: manifest,
		Identity: providertriggers.PackIdentity{
			ID: "provider.telegram", Version: version,
			ManifestHash: "sha256:" + strings.Repeat(version[:1], 64), Provenance: "platform",
		},
		Source: "test",
	})
	if err != nil {
		t.Fatalf("NewCatalogSnapshot: %v", err)
	}
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "chat", Provider: "telegram",
		Declaration: providertriggers.AdmissionDeclaration{Acknowledge: providertriggers.UnsignedWebhookAcknowledgement},
	})
	if err != nil {
		t.Fatalf("CompileAdmission: %v", err)
	}
	return plan, catalog
}

func mapProjectedSchemaType(projectedType string) string {
	if projectedType == "text" {
		return "string"
	}
	return projectedType
}

func normalizedRetryRequest(body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/webhooks/chat/telegram", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	return request
}

func TestInboundGateway_SlackURLVerificationReturnsChallengeWithoutMarkerOrPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "slack-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"type":"url_verification","challenge":"challenge-value","token":"deprecated-token"}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "challenge-value" {
		t.Fatalf("body = %q, want challenge-value", rec.Body.String())
	}
	if store.recorded {
		t.Fatal("Slack url_verification recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
	if strings.Contains(rec.Body.String(), "slack-secret") || strings.Contains(rec.Body.String(), "deprecated-token") {
		t.Fatal("Slack secret material leaked into challenge response")
	}
}

func TestInboundGateway_SlackURLVerificationRequiresChallengeBeforeMarkerAndPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "slack-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"type":"url_verification","token":"deprecated-token"}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("Slack url_verification without challenge recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_SlackRejectsMissingSecretBeforeMarkerAndPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:   "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug: "customer-a",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"type":"event_callback","event_id":"Ev123","event":{"type":"message"}}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("Slack request without configured signing secret recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_SlackRejectsMissingOrInvalidSignatureBeforeMarkerAndPublish(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*http.Request, []byte)
	}{
		{
			name: "missing signature",
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("X-Slack-Request-Timestamp", strconv.FormatInt(time.Now().UTC().Unix(), 10))
			},
		},
		{
			name: "invalid signature",
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("X-Slack-Request-Timestamp", timestamp)
				req.Header.Set("X-Slack-Signature", slackWebhookSignature("wrong-secret", timestamp, body))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			store := &recordingInboundStore{
				target: InboundTarget{
					EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
					EntitySlug:    "customer-a",
					SigningSecret: "slack-secret",
				},
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)

			body := []byte(`{"type":"event_callback","event_id":"Ev123","event":{"type":"message"}}`)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/slack", strings.NewReader(string(body)))
			tc.configure(req, body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
			}
			if store.recorded {
				t.Fatal("Slack request with invalid signature recorded inbound marker")
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_SlackRejectsStaleTimestampBeforeMarkerAndPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "slack-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"type":"event_callback","event_id":"Ev123","event":{"type":"message"}}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, "1")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("Slack request with stale timestamp recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_SlackEventCallbackOwnsEventIDAndInnerEventMapping(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "slack-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"type":"event_callback","token":"deprecated-token","event_id":"Ev123ABC456","event":{"type":"message.channels","text":"hello"}}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !eventStore.recorded {
		t.Fatal("expected Slack callback to record inbound marker")
	}
	if eventStore.providerEventID != "Ev123ABC456" {
		t.Fatalf("providerEventID = %q, want Ev123ABC456", eventStore.providerEventID)
	}
	if eventStore.provider != "slack" {
		t.Fatalf("provider = %q, want slack", eventStore.provider)
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.Type() != events.EventType("inbound.slack.message_channels") {
		t.Fatalf("event type = %q, want inbound.slack.message_channels", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["provider_event_id"] != "Ev123ABC456" || payload["event_type"] != "message_channels" || payload["provider"] != "slack" {
		t.Fatalf("payload = %+v, want Slack delivery identity", payload)
	}
	payloadJSON := string(evt.Payload())
	if strings.Contains(rec.Body.String(), "slack-secret") || strings.Contains(payloadJSON, "slack-secret") {
		t.Fatal("Slack signing secret leaked into readback or event payload")
	}
	if strings.Contains(payloadJSON, "deprecated-token") {
		t.Fatal("Slack deprecated verification token leaked into event payload")
	}
}

func TestInboundGateway_SlackEventCallbackAcknowledgesBeforePostCommitDispatchCompletes(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseDispatch := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	t.Cleanup(releaseDispatch)
	bus, err := runtimebus.NewEventBusWithOptions(eventStore, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{
			blockingInboundInterceptor{started: started, release: release},
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "slack-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"type":"event_callback","event_id":"Ev123ABC456","event":{"type":"message","text":"hello"}}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	responseDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		g.Handler().ServeHTTP(rec, req)
		responseDone <- rec
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Slack callback post-commit dispatch did not start")
	}
	select {
	case rec := <-responseDone:
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Slack callback response waited for post-commit dispatch completion")
	}
	if !eventStore.recorded {
		t.Fatal("expected Slack callback to record inbound marker before acknowledgement")
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1 before dispatch release", len(eventStore.events))
	}

	releaseDispatch()
	waitCtx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after dispatch release: %v", err)
	}
}

func TestInboundGateway_SlackEventCallbackRequiresEventIDBeforeMarkerAndPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "slack-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"type":"event_callback","event":{"type":"message"}}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("Slack callback without event_id recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_SlackDuplicateEventDoesNotPublishAgain(t *testing.T) {
	eventStore := &capturingInboundEventStore{duplicate: true}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "slack-secret",
		},
		inserted: false,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"type":"event_callback","event_id":"Ev123ABC456","event":{"type":"message"}}`)
	req := newSignedSlackRequest("/webhooks/customer-a/slack", "slack-secret", body, strconv.FormatInt(time.Now().UTC().Unix(), 10))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"duplicate"`) {
		t.Fatalf("duplicate response = %s", rec.Body.String())
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_StripeManifestOwnsSignatureReplayIDTypeAndAck(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseDispatch := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	t.Cleanup(releaseDispatch)
	bus, err := runtimebus.NewEventBusWithOptions(eventStore, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{
			blockingInboundInterceptor{started: started, release: release},
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "stripe-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"id":"evt_123","type":"invoice.paid","data":{"object":{"id":"in_123"}}}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(body)))
	req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
	responseDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		g.Handler().ServeHTTP(rec, req)
		responseDone <- rec
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Stripe callback post-commit dispatch did not start")
	}
	select {
	case rec := <-responseDone:
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stripe callback response waited for post-commit dispatch completion")
	}
	if !eventStore.recorded {
		t.Fatal("expected Stripe callback to record inbound marker")
	}
	if eventStore.providerEventID != "evt_123" {
		t.Fatalf("providerEventID = %q, want evt_123", eventStore.providerEventID)
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1 before dispatch release", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.Type() != events.EventType("inbound.stripe") {
		t.Fatalf("event type = %q, want inbound.stripe", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["provider_event_id"] != "evt_123" || payload["provider_event_type"] != "invoice_paid" || payload["provider"] != "stripe" {
		t.Fatalf("payload = %+v, want Stripe delivery identity", payload)
	}
	if strings.Contains(string(evt.Payload()), "stripe-secret") {
		t.Fatal("Stripe signing secret leaked into event payload")
	}

	releaseDispatch()
	waitCtx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after dispatch release: %v", err)
	}
}

func TestInboundGateway_StripeRejectsInvalidInputsBeforeMarkerAndPublish(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       []byte
		configure  func(*http.Request, []byte)
		wantStatus int
	}{
		{
			name:       "missing signature",
			body:       []byte(`{"id":"evt_123","type":"invoice.paid"}`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "wrong signature version param",
			body: []byte(`{"id":"evt_123","type":"invoice.paid"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", "t="+timestamp+",v0="+stripeSignatureHex("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "malformed signature component with otherwise valid signature",
			body: []byte(`{"id":"evt_123","type":"invoice.paid"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", "t="+timestamp+",broken,v1="+stripeSignatureHex("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "duplicate timestamp params with otherwise valid signature",
			body: []byte(`{"id":"evt_123","type":"invoice.paid"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", "t="+timestamp+",t="+timestamp+",v1="+stripeSignatureHex("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "stale timestamp",
			body: []byte(`{"id":"evt_123","type":"invoice.paid"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Add(-10*time.Minute).Unix(), 10)
				req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "missing event id",
			body: []byte(`{"type":"invoice.paid"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "object event id",
			body: []byte(`{"id":{"nested":"evt_123"},"type":"invoice.paid"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing event type",
			body: []byte(`{"id":"evt_123"}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "bool event type",
			body: []byte(`{"id":"evt_123","type":true}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "list event type",
			body: []byte(`{"id":"evt_123","type":["invoice.paid"]}`),
			configure: func(req *http.Request, body []byte) {
				timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
				req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
			},
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			store := &recordingInboundStore{
				target: InboundTarget{
					EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
					EntitySlug:    "customer-a",
					SigningSecret: "stripe-secret",
				},
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)

			req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(tc.body)))
			tc.configure(req, tc.body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if store.recorded {
				t.Fatal("invalid Stripe request recorded inbound marker")
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_StripeDuplicateEventDoesNotPublishAgain(t *testing.T) {
	eventStore := &capturingInboundEventStore{duplicate: true}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "stripe-secret",
		},
		inserted: false,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"id":"evt_123","type":"invoice.paid"}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/stripe", strings.NewReader(string(body)))
	req.Header.Set("Stripe-Signature", stripeWebhookSignature("stripe-secret", timestamp, body))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"duplicate"`) {
		t.Fatalf("duplicate response = %s", rec.Body.String())
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_TwilioManifestOwnsURLFormSignatureAndLiteralEvent(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "twilio-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=alpha"
	form := url.Values{
		"Body":          {"hello from twilio"},
		"From":          {"+15551234567"},
		"MessageSid":    {"SM1234567890abcdef"},
		"To":            {"+15557654321"},
		"UnexpectedNew": {"still signed"},
	}
	req := newSignedTwilioRequest(requestURL, "twilio-secret", form)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !eventStore.recorded {
		t.Fatal("expected Twilio delivery to record inbound marker")
	}
	if eventStore.providerEventID != "SM1234567890abcdef" {
		t.Fatalf("providerEventID = %q, want MessageSid", eventStore.providerEventID)
	}
	if eventStore.provider != "twilio" {
		t.Fatalf("provider = %q, want twilio", eventStore.provider)
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.Type() != events.EventType("inbound.twilio") {
		t.Fatalf("event type = %q, want inbound.twilio", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["provider_event_id"] != "SM1234567890abcdef" ||
		payload["provider_event_type"] != "message_received" ||
		payload["provider"] != "twilio" {
		t.Fatalf("payload = %+v, want Twilio manifest delivery identity", payload)
	}
	formPayload, ok := payload["payload"].(map[string]any)
	if !ok {
		t.Fatalf("payload.payload = %T, want form payload map", payload["payload"])
	}
	if formPayload["Body"] != "hello from twilio" || formPayload["UnexpectedNew"] != "still signed" {
		t.Fatalf("form payload = %+v, want evolving Twilio form parameters preserved", formPayload)
	}
	if strings.Contains(rec.Body.String(), "twilio-secret") || strings.Contains(string(evt.Payload()), "twilio-secret") {
		t.Fatal("Twilio signing secret leaked into readback or event payload")
	}
}

func TestInboundGateway_TwilioRejectsInvalidInputsBeforeMarkerAndPublish(t *testing.T) {
	for _, tc := range []struct {
		name       string
		requestURL string
		form       url.Values
		configure  func(*http.Request, url.Values, string)
		wantStatus int
	}{
		{
			name:       "missing signature",
			requestURL: "https://example.com/webhooks/customer-a/twilio?tenant=alpha",
			form:       url.Values{"MessageSid": {"SM1234567890abcdef"}, "Body": {"hello"}},
			configure: func(req *http.Request, form url.Values, requestURL string) {
				req.Header.Del("X-Twilio-Signature")
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "url mismatch",
			requestURL: "https://example.com/webhooks/customer-a/twilio?tenant=beta",
			form:       url.Values{"MessageSid": {"SM1234567890abcdef"}, "Body": {"hello"}},
			configure: func(req *http.Request, form url.Values, requestURL string) {
				req.Header.Set("X-Twilio-Signature", twilioWebhookSignature("twilio-secret", "https://example.com/webhooks/customer-a/twilio?tenant=alpha", form))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "duplicate query params",
			requestURL: "https://example.com/webhooks/customer-a/twilio?tenant=alpha&tenant=beta",
			form:       url.Values{"MessageSid": {"SM1234567890abcdef"}, "Body": {"hello"}},
			configure:  func(*http.Request, url.Values, string) {},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "duplicate form params",
			requestURL: "https://example.com/webhooks/customer-a/twilio?tenant=alpha",
			form:       url.Values{"MessageSid": {"SM1234567890abcdef"}, "Body": {"hello", "tampered"}},
			configure:  func(*http.Request, url.Values, string) {},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing message sid",
			requestURL: "https://example.com/webhooks/customer-a/twilio?tenant=alpha",
			form:       url.Values{"Body": {"hello"}},
			configure:  func(*http.Request, url.Values, string) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "json body sha256 mode unsupported in this slice",
			requestURL: "https://example.com/webhooks/customer-a/twilio?bodySHA256=abc123",
			form:       url.Values{"MessageSid": {"SM1234567890abcdef"}, "Body": {"hello"}},
			configure: func(req *http.Request, form url.Values, requestURL string) {
				req.Header.Set("Content-Type", "application/json")
				req.Body = io.NopCloser(strings.NewReader(`{"MessageSid":"SM1234567890abcdef"}`))
				req.ContentLength = int64(len(`{"MessageSid":"SM1234567890abcdef"}`))
			},
			wantStatus: http.StatusUnauthorized,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			store := &recordingInboundStore{
				target: InboundTarget{
					EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
					EntitySlug:    "customer-a",
					SigningSecret: "twilio-secret",
				},
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)
			req := newSignedTwilioRequest(tc.requestURL, "twilio-secret", tc.form)
			tc.configure(req, tc.form, tc.requestURL)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if store.recorded {
				t.Fatal("invalid Twilio request recorded inbound marker")
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_TwilioDuplicateDeliveryDoesNotPublishAgain(t *testing.T) {
	eventStore := &capturingInboundEventStore{duplicate: true}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "twilio-secret",
		},
		inserted: false,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	requestURL := "https://example.com/webhooks/customer-a/twilio?tenant=alpha"
	form := url.Values{"MessageSid": {"SM1234567890abcdef"}, "Body": {"hello"}}
	req := newSignedTwilioRequest(requestURL, "twilio-secret", form)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"duplicate"`) {
		t.Fatalf("duplicate response = %s", rec.Body.String())
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_ShopifyManifestOwnsRawBodySignatureDeliveryIDAndTopic(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "shopify-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`)
	req := newSignedShopifyRequest("/webhooks/customer-a/shopify", "shopify-secret", body)
	req.Header.Set("X-Shopify-Webhook-Id", "webhook-123")
	req.Header.Set("X-Shopify-Topic", "orders/create")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
	}
	if !eventStore.recorded {
		t.Fatal("expected Shopify delivery to record inbound marker")
	}
	if eventStore.providerEventID != "webhook-123" {
		t.Fatalf("providerEventID = %q, want webhook-123", eventStore.providerEventID)
	}
	if eventStore.provider != "shopify" {
		t.Fatalf("provider = %q, want shopify", eventStore.provider)
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.Type() != events.EventType("inbound.shopify") {
		t.Fatalf("event type = %q, want inbound.shopify", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["provider_event_id"] != "webhook-123" ||
		payload["provider_event_type"] != "orders_create" ||
		payload["provider"] != "shopify" {
		t.Fatalf("payload = %+v, want Shopify manifest delivery identity", payload)
	}
	if strings.Contains(rec.Body.String(), "shopify-secret") || strings.Contains(string(evt.Payload()), "shopify-secret") {
		t.Fatal("Shopify signing secret leaked into readback or event payload")
	}
}

func TestInboundGateway_ShopifyRejectsInvalidInputsBeforeMarkerAndPublish(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       []byte
		configure  func(*http.Request, []byte)
		wantStatus int
	}{
		{
			name:       "missing signature",
			body:       []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "invalid signature",
			body: []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`),
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature("wrong-secret", body))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "raw body mutation",
			body: []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`),
			configure: func(req *http.Request, body []byte) {
				signedBody := []byte(`{"line_items":[{"sku":"abc"}],"id":123}`)
				req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature("shopify-secret", signedBody))
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "missing webhook id",
			body: []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`),
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature("shopify-secret", body))
				req.Header.Del("X-Shopify-Webhook-Id")
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing topic",
			body: []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`),
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature("shopify-secret", body))
				req.Header.Del("X-Shopify-Topic")
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "non object payload",
			body: []byte(`[{"id":123}]`),
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature("shopify-secret", body))
			},
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			store := &recordingInboundStore{
				target: InboundTarget{
					EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
					EntitySlug:    "customer-a",
					SigningSecret: "shopify-secret",
				},
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)
			req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/shopify", strings.NewReader(string(tc.body)))
			req.Header.Set("X-Shopify-Webhook-Id", "webhook-123")
			req.Header.Set("X-Shopify-Topic", "orders/create")
			tc.configure(req, tc.body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if store.recorded {
				t.Fatal("invalid Shopify request recorded inbound marker")
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_ShopifyDuplicateDeliveryDoesNotPublishAgain(t *testing.T) {
	eventStore := &capturingInboundEventStore{duplicate: true}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "shopify-secret",
		},
		inserted: false,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`)
	req := newSignedShopifyRequest("/webhooks/customer-a/shopify", "shopify-secret", body)
	req.Header.Set("X-Shopify-Webhook-Id", "webhook-123")
	req.Header.Set("X-Shopify-Topic", "orders/create")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"duplicate"`) {
		t.Fatalf("duplicate response = %s", rec.Body.String())
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_TypeformAndIntercomManifestsOwnRawBodySignatureDeliveryIDAndEventType(t *testing.T) {
	for _, tc := range []struct {
		name              string
		provider          string
		secret            string
		body              []byte
		newRequest        func(path string, secret string, body []byte) *http.Request
		wantProviderID    string
		wantProviderType  string
		wantEventName     events.EventType
		wantMetadataKeyID string
		wantMetadataKeyTy string
	}{
		{
			name:              "typeform",
			provider:          "typeform",
			secret:            "typeform-secret",
			body:              []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`),
			newRequest:        newSignedTypeformRequest,
			wantProviderID:    "tf-evt-123",
			wantProviderType:  "form_response",
			wantEventName:     events.EventType("inbound.typeform"),
			wantMetadataKeyID: "typeform_event_id",
			wantMetadataKeyTy: "typeform_event_type",
		},
		{
			name:              "intercom",
			provider:          "intercom",
			secret:            "intercom-secret",
			body:              []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
			newRequest:        newSignedIntercomRequest,
			wantProviderID:    "notif_123",
			wantProviderType:  "conversation_user_created",
			wantEventName:     events.EventType("inbound.intercom"),
			wantMetadataKeyID: "intercom_notification_id",
			wantMetadataKeyTy: "intercom_topic",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			store := &recordingInboundStore{
				target: InboundTarget{
					EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
					EntitySlug:    "customer-a",
					SigningSecret: tc.secret,
				},
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)

			req := tc.newRequest("/webhooks/customer-a/"+tc.provider, tc.secret, tc.body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
			}
			if !eventStore.recorded {
				t.Fatalf("expected %s delivery to record inbound marker", tc.provider)
			}
			if eventStore.providerEventID != tc.wantProviderID {
				t.Fatalf("providerEventID = %q, want %s", eventStore.providerEventID, tc.wantProviderID)
			}
			if eventStore.provider != tc.provider {
				t.Fatalf("provider = %q, want %s", eventStore.provider, tc.provider)
			}
			if len(eventStore.events) != 1 {
				t.Fatalf("published events = %d, want 1", len(eventStore.events))
			}
			evt := eventStore.events[0]
			if evt.Type() != tc.wantEventName {
				t.Fatalf("event type = %q, want %s", evt.Type(), tc.wantEventName)
			}
			var payload map[string]any
			if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
				t.Fatalf("unmarshal payload: %v", err)
			}
			headers, ok := payload["headers"].(map[string]any)
			if !ok {
				t.Fatalf("headers = %T, want metadata map", payload["headers"])
			}
			if payload["provider_event_id"] != tc.wantProviderID ||
				payload["provider_event_type"] != tc.wantProviderType ||
				payload["provider"] != tc.provider ||
				headers[tc.wantMetadataKeyID] != tc.wantProviderID ||
				headers[tc.wantMetadataKeyTy] != tc.wantProviderType {
				t.Fatalf("payload = %+v, want %s manifest delivery identity", payload, tc.provider)
			}
			if strings.Contains(rec.Body.String(), tc.secret) || strings.Contains(string(evt.Payload()), tc.secret) {
				t.Fatalf("%s signing secret leaked into readback or event payload", tc.provider)
			}
		})
	}
}

func TestInboundGateway_TypeformAndIntercomRejectInvalidInputsBeforeMarkerAndPublish(t *testing.T) {
	for _, providerCase := range []struct {
		provider   string
		secret     string
		validBody  []byte
		newRequest func(path string, secret string, body []byte) *http.Request
		cases      []struct {
			name       string
			body       []byte
			configure  func(*http.Request, []byte)
			wantStatus int
		}
	}{
		{
			provider:   "typeform",
			secret:     "typeform-secret",
			validBody:  []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`),
			newRequest: newSignedTypeformRequest,
			cases: []struct {
				name       string
				body       []byte
				configure  func(*http.Request, []byte)
				wantStatus int
			}{
				{
					name:       "missing signature",
					body:       []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`),
					configure:  func(req *http.Request, _ []byte) { req.Header.Del("Typeform-Signature") },
					wantStatus: http.StatusUnauthorized,
				},
				{
					name: "invalid signature",
					body: []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`),
					configure: func(req *http.Request, body []byte) {
						req.Header.Set("Typeform-Signature", typeformWebhookSignature("wrong-secret", body))
					},
					wantStatus: http.StatusUnauthorized,
				},
				{
					name: "raw body mutation",
					body: []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`),
					configure: func(req *http.Request, _ []byte) {
						signedBody := []byte(`{"event_type":"form_response","event_id":"tf-evt-123","form_response":{"token":"abc"}}`)
						req.Header.Set("Typeform-Signature", typeformWebhookSignature("typeform-secret", signedBody))
					},
					wantStatus: http.StatusUnauthorized,
				},
				{
					name:       "missing delivery id",
					body:       []byte(`{"event_type":"form_response","form_response":{"token":"abc"}}`),
					configure:  func(*http.Request, []byte) {},
					wantStatus: http.StatusBadRequest,
				},
				{
					name:       "missing event type",
					body:       []byte(`{"event_id":"tf-evt-123","form_response":{"token":"abc"}}`),
					configure:  func(*http.Request, []byte) {},
					wantStatus: http.StatusBadRequest,
				},
				{
					name:       "non object payload",
					body:       []byte(`[{"event_id":"tf-evt-123","event_type":"form_response"}]`),
					configure:  func(*http.Request, []byte) {},
					wantStatus: http.StatusBadRequest,
				},
			},
		},
		{
			provider:   "intercom",
			secret:     "intercom-secret",
			validBody:  []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
			newRequest: newSignedIntercomRequest,
			cases: []struct {
				name       string
				body       []byte
				configure  func(*http.Request, []byte)
				wantStatus int
			}{
				{
					name:       "missing signature",
					body:       []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
					configure:  func(req *http.Request, _ []byte) { req.Header.Del("X-Hub-Signature") },
					wantStatus: http.StatusUnauthorized,
				},
				{
					name: "invalid signature",
					body: []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
					configure: func(req *http.Request, body []byte) {
						req.Header.Set("X-Hub-Signature", intercomWebhookSignature("wrong-secret", body))
					},
					wantStatus: http.StatusUnauthorized,
				},
				{
					name: "raw body mutation",
					body: []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
					configure: func(req *http.Request, _ []byte) {
						signedBody := []byte(`{"topic":"conversation.user.created","id":"notif_123","data":{"item":{"id":"conv_1"}}}`)
						req.Header.Set("X-Hub-Signature", intercomWebhookSignature("intercom-secret", signedBody))
					},
					wantStatus: http.StatusUnauthorized,
				},
				{
					name:       "missing delivery id",
					body:       []byte(`{"topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
					configure:  func(*http.Request, []byte) {},
					wantStatus: http.StatusBadRequest,
				},
				{
					name:       "missing event type",
					body:       []byte(`{"id":"notif_123","data":{"item":{"id":"conv_1"}}}`),
					configure:  func(*http.Request, []byte) {},
					wantStatus: http.StatusBadRequest,
				},
				{
					name:       "non object payload",
					body:       []byte(`[{"id":"notif_123","topic":"conversation.user.created"}]`),
					configure:  func(*http.Request, []byte) {},
					wantStatus: http.StatusBadRequest,
				},
			},
		},
	} {
		for _, tc := range providerCase.cases {
			t.Run(providerCase.provider+"/"+tc.name, func(t *testing.T) {
				eventStore := &capturingInboundEventStore{}
				bus, err := runtimebus.NewEventBus(eventStore)
				if err != nil {
					t.Fatalf("NewEventBus: %v", err)
				}
				store := &recordingInboundStore{
					target: InboundTarget{
						EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
						EntitySlug:    "customer-a",
						SigningSecret: providerCase.secret,
					},
					inserted: true,
				}
				g := newTestInboundGateway(t, bus, nil, nil, store)
				body := tc.body
				if len(body) == 0 {
					body = providerCase.validBody
				}
				req := providerCase.newRequest("/webhooks/customer-a/"+providerCase.provider, providerCase.secret, body)
				tc.configure(req, body)
				rec := httptest.NewRecorder()
				g.Handler().ServeHTTP(rec, req)

				if rec.Code != tc.wantStatus {
					t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
				}
				if store.recorded {
					t.Fatalf("invalid %s request recorded inbound marker", providerCase.provider)
				}
				if len(eventStore.events) != 0 {
					t.Fatalf("published events = %d, want 0", len(eventStore.events))
				}
			})
		}
	}
}

func TestInboundGateway_TypeformAndIntercomDuplicateDeliveryDoesNotPublishAgain(t *testing.T) {
	for _, tc := range []struct {
		provider   string
		secret     string
		body       []byte
		newRequest func(path string, secret string, body []byte) *http.Request
	}{
		{
			provider:   "typeform",
			secret:     "typeform-secret",
			body:       []byte(`{"event_id":"tf-evt-123","event_type":"form_response","form_response":{"token":"abc"}}`),
			newRequest: newSignedTypeformRequest,
		},
		{
			provider:   "intercom",
			secret:     "intercom-secret",
			body:       []byte(`{"id":"notif_123","topic":"conversation.user.created","data":{"item":{"id":"conv_1"}}}`),
			newRequest: newSignedIntercomRequest,
		},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{duplicate: true}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			store := &recordingInboundStore{
				target: InboundTarget{
					EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
					EntitySlug:    "customer-a",
					SigningSecret: tc.secret,
				},
				inserted: false,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)

			req := tc.newRequest("/webhooks/customer-a/"+tc.provider, tc.secret, tc.body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), `"status":"duplicate"`) {
				t.Fatalf("duplicate response = %s", rec.Body.String())
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_TelegramManifestOwnsTokenDeliveryIDLiteralEventAndAck(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseDispatch := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	t.Cleanup(releaseDispatch)
	bus, err := runtimebus.NewEventBusWithOptions(eventStore, runtimebus.EventBusOptions{
		Interceptors: []runtimebus.EventInterceptor{
			blockingInboundInterceptor{started: started, release: release},
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "telegram-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"update_id":123456789,"message":{"message_id":7,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"hello"}}`)
	req := newSignedTelegramRequest("/webhooks/customer-a/telegram", "telegram-secret", body)
	responseDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		g.Handler().ServeHTTP(rec, req)
		responseDone <- rec
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Telegram callback post-commit dispatch did not start")
	}
	select {
	case rec := <-responseDone:
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 body=%s", rec.Code, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "telegram-secret") {
			t.Fatal("Telegram secret token leaked into readback")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Telegram callback response waited for post-commit dispatch completion")
	}
	if !eventStore.recorded {
		t.Fatal("expected Telegram delivery to record inbound marker")
	}
	if eventStore.providerEventID != "123456789" {
		t.Fatalf("providerEventID = %q, want 123456789", eventStore.providerEventID)
	}
	if eventStore.provider != "telegram" {
		t.Fatalf("provider = %q, want telegram", eventStore.provider)
	}
	if len(eventStore.events) != 2 {
		t.Fatalf("published events = %d, want atomic raw plus normalized before dispatch release", len(eventStore.events))
	}
	evt := eventStore.events[0]
	if evt.Type() != events.EventType("inbound.telegram") {
		t.Fatalf("event type = %q, want inbound.telegram", evt.Type())
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	headers, ok := payload["headers"].(map[string]any)
	if !ok {
		t.Fatalf("headers = %T, want metadata map", payload["headers"])
	}
	if payload["provider_event_id"] != "123456789" ||
		payload["provider_event_type"] != "update" ||
		payload["provider"] != "telegram" ||
		headers["telegram_update_id"] != "123456789" ||
		headers["telegram_update_type"] != "update" {
		t.Fatalf("payload = %+v, want Telegram manifest delivery identity", payload)
	}
	if strings.Contains(string(evt.Payload()), "telegram-secret") {
		t.Fatal("Telegram secret token leaked into event payload")
	}

	releaseDispatch()
	waitCtx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 5*time.Second)
	defer cancel()
	if err := bus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("WaitForQuiescence after dispatch release: %v", err)
	}
}

func TestInboundGateway_TelegramRejectsInvalidInputsBeforeMarkerAndPublish(t *testing.T) {
	for _, tc := range []struct {
		name          string
		body          []byte
		target        InboundTarget
		configure     func(*http.Request, []byte)
		wantStatus    int
		wantBodyParts []string
	}{
		{
			name:       "missing configured secret",
			body:       []byte(`{"update_id":123456789,"message":{"message_id":7,"text":"hello"}}`),
			target:     InboundTarget{EntityID: "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a", EntitySlug: "customer-a"},
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "missing token header",
			body:       []byte(`{"update_id":123456789,"message":{"message_id":7,"text":"hello"}}`),
			configure:  func(req *http.Request, _ []byte) { req.Header.Del("X-Telegram-Bot-Api-Secret-Token") },
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "duplicate token header",
			body: []byte(`{"update_id":123456789,"message":{"message_id":7,"text":"hello"}}`),
			configure: func(req *http.Request, _ []byte) {
				req.Header.Add("X-Telegram-Bot-Api-Secret-Token", "telegram-secret")
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "invalid token header",
			body: []byte(`{"update_id":123456789,"message":{"message_id":7,"text":"hello"}}`),
			configure: func(req *http.Request, _ []byte) {
				req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "wrong-secret")
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing update id",
			body:       []byte(`{"message":{"message_id":7,"text":"hello"}}`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "chat id conversion failure",
			body:       []byte(`{"update_id":123456789,"message":{"message_id":7,"from":{"id":42},"chat":{"id":"not-a-number","type":"private"},"text":"hello"}}`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusBadRequest,
			wantBodyParts: []string{
				"provider.telegram", "version=0.1.0", "manifest_hash=sha256:",
				`normalized event "inbound.telegram.text_message"`, `path "message.chat.id"`,
				"number_to_text requires a numeric value",
			},
		},
		{
			name:       "message id type failure",
			body:       []byte(`{"update_id":123456789,"message":{"message_id":"seven","from":{"id":42},"chat":{"id":42,"type":"private"},"text":"hello"}}`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusBadRequest,
			wantBodyParts: []string{
				"provider.telegram", "version=0.1.0", "manifest_hash=sha256:",
				`normalized event "inbound.telegram.text_message"`, `path "message.message_id"`,
				"projected value violates its declared output schema", "$ must be integer",
			},
		},
		{
			name:       "message text type failure",
			body:       []byte(`{"update_id":123456789,"message":{"message_id":7,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":99}}`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusBadRequest,
			wantBodyParts: []string{
				"provider.telegram", "version=0.1.0", "manifest_hash=sha256:",
				`normalized event "inbound.telegram.text_message"`, `path "message.text"`,
				"projected value violates its declared output schema", "$ must be string",
			},
		},
		{
			name:       "non object payload",
			body:       []byte(`[{"update_id":123456789}]`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "trailing junk after object",
			body:       []byte(`{"update_id":123456789}junk`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "second JSON value after object",
			body:       []byte(`{"update_id":123456789} {"update_id":2}`),
			configure:  func(*http.Request, []byte) {},
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			target := tc.target
			if target.EntityID == "" {
				target = InboundTarget{
					EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
					EntitySlug:    "customer-a",
					SigningSecret: "telegram-secret",
				}
			}
			store := &recordingInboundStore{
				target:   target,
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)

			req := newSignedTelegramRequest("/webhooks/customer-a/telegram", "telegram-secret", tc.body)
			tc.configure(req, tc.body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			for _, want := range tc.wantBodyParts {
				if !strings.Contains(rec.Body.String(), want) {
					t.Errorf("body = %q, want containing %q", rec.Body.String(), want)
				}
			}
			if store.recorded {
				t.Fatal("invalid Telegram request recorded inbound marker")
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_TelegramDuplicateDeliveryDoesNotPublishAgain(t *testing.T) {
	eventStore := &capturingInboundEventStore{duplicate: true}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "telegram-secret",
		},
		inserted: false,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"update_id":123456789,"message":{"message_id":7,"text":"hello"}}`)
	req := newSignedTelegramRequest("/webhooks/customer-a/telegram", "telegram-secret", body)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"duplicate"`) {
		t.Fatalf("duplicate response = %s", rec.Body.String())
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_NoPlanDoesNotInterpretTypeformOrIntercomSignatures(t *testing.T) {
	for _, tc := range []struct {
		name      string
		body      []byte
		configure func(*http.Request, []byte)
	}{
		{
			name: "typeform",
			body: []byte(`{"event_id":"tf-evt-123","event_type":"form_response"}`),
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("Typeform-Signature", typeformWebhookSignature("raw-secret", body))
			},
		},
		{
			name: "intercom",
			body: []byte(`{"id":"notif_123","topic":"conversation.user.created"}`),
			configure: func(req *http.Request, body []byte) {
				req.Header.Set("X-Hub-Signature", intercomWebhookSignature("raw-secret", body))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatalf("NewEventBus: %v", err)
			}
			store := &recordingInboundStore{
				target: InboundTarget{
					EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
					EntitySlug:    "customer-a",
					SigningSecret: "raw-secret",
				},
				inserted: true,
			}
			g := newTestInboundGateway(t, bus, nil, nil, store)

			req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/custom", strings.NewReader(string(tc.body)))
			tc.configure(req, tc.body)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503 body=%s", rec.Code, rec.Body.String())
			}
			if store.recorded {
				t.Fatalf("missing compiled plan accepted %s signature and recorded inbound marker", tc.name)
			}
			if len(eventStore.events) != 0 {
				t.Fatalf("published events = %d, want 0", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_NoPlanDoesNotInterpretTelegramSecretToken(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "raw-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"update_id":123456789}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/custom", strings.NewReader(string(body)))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "raw-secret")
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("missing compiled plan accepted Telegram secret token and recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_NoPlanDoesNotInterpretShopifySignature(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "raw-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"id":123,"line_items":[{"sku":"abc"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/custom", strings.NewReader(string(body)))
	req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature("raw-secret", body))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("missing compiled plan accepted Shopify signature and recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_NoPlanDoesNotInterpretStripeSignature(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "raw-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	body := []byte(`{"id":"evt_123","type":"invoice.paid"}`)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/custom", strings.NewReader(string(body)))
	req.Header.Set("Stripe-Signature", stripeWebhookSignature("raw-secret", timestamp, body))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("missing compiled plan accepted Stripe-Signature and recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func TestInboundGateway_ExecutesOnlyCompiledRawAdmissionPolicy(t *testing.T) {
	catalog, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "partner", Provider: "partner-events", SigningSecret: "partner-secret",
		Declaration: providertriggers.AdmissionDeclaration{
			Kind: "raw", Event: "inbound.partner", Payload: "json",
			Authentication: providertriggers.RawAuthenticationDeclaration{Kind: "hmac_sha256", Header: "X-Partner-Signature", Prefix: "sha256=", Encoding: "hex"},
			DeliveryID:     providertriggers.RawDeliveryIDDeclaration{Source: "header", Header: "X-Partner-Delivery"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"id":"payload-id-is-not-authority","value":1}`)
	mac := hmac.New(sha256.New, []byte("partner-secret"))
	_, _ = mac.Write(body)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	for _, tc := range []struct {
		name, signature string
		wantStatus      int
		wantRecorded    bool
	}{
		{name: "declared signature and delivery header", signature: signature, wantStatus: http.StatusAccepted, wantRecorded: true},
		{name: "provider-shaped but undeclared signature is rejected", signature: shopifyWebhookSignature("partner-secret", body), wantStatus: http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventStore := &capturingInboundEventStore{}
			bus, err := runtimebus.NewEventBus(eventStore)
			if err != nil {
				t.Fatal(err)
			}
			store := &recordingInboundStore{inserted: true, store: eventStore}
			gateway := NewInboundGateway(bus, nil, nil, store)
			gateway.SetCredentialStore(identityInboundCredentialStore{})
			req := httptest.NewRequest(http.MethodPost, "/webhooks/partner/partner-events", strings.NewReader(string(body)))
			req.Header.Set("X-Partner-Signature", tc.signature)
			req.Header.Set("X-Partner-Delivery", "declared-delivery")
			// These old heuristic candidates are deliberately irrelevant.
			req.Header.Set("Stripe-Signature", signature)
			req.Header.Set("X-GitHub-Event", "push")
			rec := httptest.NewRecorder()
			gateway.HandleResolvedWebhook(rec, req, InboundTarget{
				BundleHash: "bundle-v1:sha256:" + strings.Repeat("d", 64), FlowID: "partner-flow", RunID: "run-partner",
				FlowInstance: "partner-flow/standing", EntityID: "entity-partner", Alias: "partner", Provider: "partner-events",
				SigningSecret: "partner-secret", AdmissionPlan: plan,
			}, nil)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if eventStore.recorded != tc.wantRecorded {
				t.Fatalf("marker recorded=%t, want %t", eventStore.recorded, tc.wantRecorded)
			}
			if tc.wantRecorded {
				if eventStore.providerEventID != "declared-delivery" || len(eventStore.events) != 1 || eventStore.events[0].Type() != "inbound.partner" {
					t.Fatalf("accepted raw delivery marker=%q events=%#v", eventStore.providerEventID, eventStore.events)
				}
			} else if len(eventStore.events) != 0 {
				t.Fatalf("rejected raw delivery published %d events", len(eventStore.events))
			}
		})
	}
}

func TestInboundGateway_PreservesExactEmptyBodyForCompiledAdmission(t *testing.T) {
	catalog, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "partner", Provider: "partner-events", SigningSecret: "partner-secret",
		Declaration: providertriggers.AdmissionDeclaration{
			Kind: "raw", Event: "inbound.partner", Payload: "raw",
			Authentication: providertriggers.RawAuthenticationDeclaration{Kind: "hmac_sha256", Header: "X-Partner-Signature", Encoding: "hex"},
			DeliveryID:     providertriggers.RawDeliveryIDDeclaration{Source: "body_sha256"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(sha256.New, []byte("partner-secret"))
	signature := hex.EncodeToString(mac.Sum(nil))
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatal(err)
	}
	store := &recordingInboundStore{inserted: true, store: eventStore}
	gateway := NewInboundGateway(bus, nil, nil, store)
	gateway.SetCredentialStore(identityInboundCredentialStore{})
	req := httptest.NewRequest(http.MethodPost, "/webhooks/partner/partner-events", nil)
	req.Header.Set("X-Partner-Signature", signature)
	rec := httptest.NewRecorder()
	gateway.HandleResolvedWebhook(rec, req, InboundTarget{
		BundleHash: "bundle-v1:sha256:" + strings.Repeat("e", 64), FlowID: "partner-flow", RunID: "run-partner",
		FlowInstance: "partner-flow/standing", EntityID: "entity-partner", Alias: "partner", Provider: "partner-events",
		SigningSecret: "partner-secret", AdmissionPlan: plan,
	}, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	wantID := hex.EncodeToString(sha256.New().Sum(nil))
	if eventStore.providerEventID != wantID {
		t.Fatalf("body_sha256 delivery id = %q, want exact empty-body hash %q", eventStore.providerEventID, wantID)
	}
	if len(eventStore.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(eventStore.events))
	}

	jsonPlan, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: "json", Provider: "json-events",
		Declaration: providertriggers.AdmissionDeclaration{
			Kind: "raw", Acknowledge: providertriggers.UnsignedWebhookAcknowledgement,
			Event: "inbound.json", Payload: "json",
			Authentication: providertriggers.RawAuthenticationDeclaration{Kind: "none"},
			DeliveryID:     providertriggers.RawDeliveryIDDeclaration{Source: "body_sha256"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	jsonStore := &recordingInboundStore{inserted: true, store: eventStore}
	jsonGateway := NewInboundGateway(bus, nil, nil, jsonStore)
	jsonReq := httptest.NewRequest(http.MethodPost, "/webhooks/json/json-events", nil)
	jsonRec := httptest.NewRecorder()
	jsonGateway.HandleResolvedWebhook(jsonRec, jsonReq, InboundTarget{
		BundleHash: "bundle-v1:sha256:" + strings.Repeat("f", 64), FlowID: "json-flow", RunID: "run-json",
		FlowInstance: "json-flow/standing", EntityID: "entity-json", Alias: "json", Provider: "json-events",
		AdmissionPlan: jsonPlan,
	}, nil)
	if jsonRec.Code != http.StatusBadRequest || !strings.Contains(jsonRec.Body.String(), "must be valid JSON") {
		t.Fatalf("empty JSON body response = %d %q", jsonRec.Code, jsonRec.Body.String())
	}
	if eventStore.providerEventID != wantID || len(eventStore.events) != 1 {
		t.Fatal("empty JSON body mutated the previously recorded marker or events")
	}
}

func TestInboundGateway_RejectsOversizedBodyBeforeMarkerAndPublish(t *testing.T) {
	eventStore := &capturingInboundEventStore{}
	bus, err := runtimebus.NewEventBus(eventStore)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &recordingInboundStore{
		target: InboundTarget{
			EntityID:      "3fd8fc37-6d02-4d50-8bb7-14c6cb0fed0a",
			EntitySlug:    "customer-a",
			SigningSecret: "github-secret",
		},
		inserted: true,
	}
	g := newTestInboundGateway(t, bus, nil, nil, store)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/customer-a/github", strings.NewReader(strings.Repeat("a", inboundWebhookMaxBodyBytes+1)))
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 body=%s", rec.Code, rec.Body.String())
	}
	if store.recorded {
		t.Fatal("oversized webhook body recorded inbound marker")
	}
	if len(eventStore.events) != 0 {
		t.Fatalf("published events = %d, want 0", len(eventStore.events))
	}
}

func githubWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func newSignedSlackRequest(path string, secret string, body []byte, timestamp string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("X-Slack-Request-Timestamp", timestamp)
	req.Header.Set("X-Slack-Signature", slackWebhookSignature(secret, timestamp, body))
	return req
}

func slackWebhookSignature(secret string, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + timestamp + ":" + string(body)))
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}

func stripeWebhookSignature(secret string, timestamp string, body []byte) string {
	return "t=" + timestamp + ",v1=" + stripeSignatureHex(secret, timestamp, body)
}

func stripeSignatureHex(secret string, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "." + string(body)))
	return hex.EncodeToString(mac.Sum(nil))
}

func newSignedTwilioRequest(requestURL string, secret string, form url.Values) *http.Request {
	req := httptest.NewRequest(http.MethodPost, requestURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Twilio-Signature", twilioWebhookSignature(secret, requestURL, form))
	return req
}

func twilioWebhookSignature(secret, requestURL string, form url.Values) string {
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write([]byte(twilioSignedPayload(requestURL, form)))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func twilioSignedPayload(requestURL string, form url.Values) string {
	keys := make([]string, 0, len(form))
	for key := range form {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(requestURL)
	for _, key := range keys {
		b.WriteString(key)
		b.WriteString(form.Get(key))
	}
	return b.String()
}

func newSignedShopifyRequest(path string, secret string, body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("X-Shopify-Hmac-Sha256", shopifyWebhookSignature(secret, body))
	return req
}

func shopifyWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func newSignedTelegramRequest(path string, secret string, body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
	return req
}

func newSignedTypeformRequest(path string, secret string, body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("Typeform-Signature", typeformWebhookSignature(secret, body))
	return req
}

func typeformWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func newSignedIntercomRequest(path string, secret string, body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature", intercomWebhookSignature(secret, body))
	return req
}

func intercomWebhookSignature(secret string, body []byte) string {
	mac := hmac.New(sha1.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha1=" + hex.EncodeToString(mac.Sum(nil))
}

type blockingInboundInterceptor struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (i blockingInboundInterceptor) Intercept(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	select {
	case i.started <- struct{}{}:
	default:
	}
	select {
	case <-i.release:
		return true, nil, nil
	case <-ctx.Done():
		return false, nil, ctx.Err()
	}
}
