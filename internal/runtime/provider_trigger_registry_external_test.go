package runtime_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
)

type boundedProviderCredentialStore struct{}

func (boundedProviderCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	return key, key != "", nil
}
func (boundedProviderCredentialStore) Set(context.Context, string, string) error { return nil }
func (boundedProviderCredentialStore) List(context.Context) ([]string, error)    { return nil, nil }
func (boundedProviderCredentialStore) Delete(context.Context, string) error      { return nil }

func testProviderTriggerCatalog(t *testing.T) *providertriggers.CatalogSnapshot {
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
	return registry
}

func newTestInboundGateway(t *testing.T, bus *runtimebus.EventBus, logger *runtimepkg.RuntimeLogger, shutdownAdmissionClosed func() bool, stores ...runtimepkg.InboundPersistence) *runtimepkg.InboundGateway {
	t.Helper()
	gateway := runtimepkg.NewInboundGateway(bus, logger, shutdownAdmissionClosed)
	gateway.SetCredentialStore(boundedProviderCredentialStore{})
	return gateway
}

func handleBoundedProviderDelivery(t *testing.T, gateway *runtimepkg.InboundGateway, bus *runtimebus.EventBus, store runtimepkg.InboundPersistence, w http.ResponseWriter, r *http.Request, runID, entityID, provider, signingSecret string) {
	t.Helper()
	_ = gateway
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		payload = map[string]any{"raw": string(body)}
	}
	query := r.URL.Query()
	form := make(url.Values)
	formParsed := false
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type"))), "application/x-www-form-urlencoded") {
		parsed, parseErr := url.ParseQuery(string(body))
		if parseErr == nil {
			form = parsed
			formParsed = true
		}
	}
	plan, err := testProviderTriggerCatalog(t).CompileAdmission(providertriggers.CompileAdmissionRequest{
		Alias: entityID, Provider: provider, SigningSecret: "test.signing_secret",
	})
	if err != nil {
		t.Fatalf("compile provider admission: %v", err)
	}
	delivery, err := plan.Accept(providertriggers.Request{
		Provider: provider,
		Target:   providertriggers.Target{EntityID: entityID, EntitySlug: entityID, WebhookSecret: signingSecret},
		Method:   r.Method, URL: r.URL.String(), Body: body, Headers: r.Header, Payload: payload,
		ContentType: r.Header.Get("Content-Type"), Query: query, Form: form, FormParsed: formParsed,
		Received: time.Now().UTC(), UserAgent: r.UserAgent(),
	})
	if err != nil {
		status := http.StatusBadRequest
		if providerErr, ok := err.(providertriggers.Error); ok {
			status = providerErr.Status
		}
		http.Error(w, err.Error(), status)
		return
	}
	if delivery.Response != nil {
		w.WriteHeader(delivery.Response.Status)
		_, _ = w.Write(delivery.Response.Body)
		return
	}
	_ = store
	batchEvents := make([]runtimebus.InboundDeliveryEvent, 0, len(delivery.Events))
	for _, output := range delivery.Events {
		envelope := events.EventEnvelope{}
		if output.Kind == providertriggers.OutputKindRaw {
			envelope = events.EventEnvelope{EntityID: entityID}
		}
		event := eventtest.RootIngress(
			"", output.Name, "bounded-provider-integration", "",
			mustBoundedJSON(t, output.Payload), 0, runID, "", envelope, time.Now().UTC(),
		)
		batchEvents = append(batchEvents, runtimebus.InboundDeliveryEvent{
			Event: event, Kind: runtimeprovideroutput.Kind(output.Kind), Authorization: output.Authorization,
		})
	}
	result, err := bus.PublishInboundDelivery(r.Context(), runtimebus.InboundDeliveryBatch{
		Claim: runtimebus.InboundDeliveryClaim{
			ProviderEventID: delivery.ProviderEventID,
			EntityID:        entityID,
			Provider:        provider,
		},
		Events:                    batchEvents,
		AcknowledgeBeforeDispatch: delivery.AcknowledgeBeforeDispatch,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if result.Duplicate {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func mustBoundedJSON(t testing.TB, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal bounded provider delivery: %v", err)
	}
	return body
}
