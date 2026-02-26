package runtime

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type inboundStoreStub struct {
	seen map[string]struct{}
}

func (s *inboundStoreStub) RecordInboundEvent(_ context.Context, providerEventID, verticalID, provider string) (bool, error) {
	if s.seen == nil {
		s.seen = map[string]struct{}{}
	}
	key := providerEventID + "|" + verticalID + "|" + provider
	if _, ok := s.seen[key]; ok {
		return false, nil
	}
	s.seen[key] = struct{}{}
	return true, nil
}

func (s *inboundStoreStub) ResolveInboundTarget(_ context.Context, verticalKey, _ string) (InboundTarget, error) {
	return InboundTarget{
		VerticalID:    verticalKey,
		VerticalSlug:  verticalKey,
		WebhookSecret: "secret",
	}, nil
}

func (s *inboundStoreStub) PurgeInboundEventsBefore(context.Context, time.Time, int) (int, error) {
	return 0, nil
}

func TestInboundGatewayPublishesEvent(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	store := &inboundStoreStub{}
	g := NewInboundGateway(bus, store)

	_ = bus.SetRoutingTable("v1", &RoutingTable{
		VerticalID: "v1",
		Routes: []Route{
			{EventPattern: "inbound.v1.whatsapp_message", SubscriberID: "test-agent", Status: "active"},
		},
	})
	ch := bus.Subscribe("test-agent")
	body := `{"id":"evt-1","text":"hello"}`
	req := httptest.NewRequest(http.MethodPost, "/webhooks/v1/whatsapp", strings.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", whatsappSig("secret", []byte(body)))
	rec := httptest.NewRecorder()

	g.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}

	select {
	case evt := <-ch:
		if evt.VerticalID != "v1" {
			t.Fatalf("unexpected vertical: %s", evt.VerticalID)
		}
	case <-time.After(600 * time.Millisecond):
		t.Fatal("expected inbound event publish")
	}
}

func TestInboundGatewayDeduplicates(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	store := &inboundStoreStub{}
	g := NewInboundGateway(bus, store)
	_ = bus.SetRoutingTable("v1", &RoutingTable{
		VerticalID: "v1",
		Routes: []Route{
			{EventPattern: "inbound.v1.whatsapp_message", SubscriberID: "test-agent", Status: "active"},
		},
	})
	ch := bus.Subscribe("test-agent")

	for i := 0; i < 2; i++ {
		body := `{"id":"evt-2","text":"hello"}`
		req := httptest.NewRequest(http.MethodPost, "/webhooks/v1/whatsapp", strings.NewReader(body))
		req.Header.Set("X-Hub-Signature-256", whatsappSig("secret", []byte(body)))
		rec := httptest.NewRecorder()
		g.Handler().ServeHTTP(rec, req)
		if i == 0 && rec.Code != http.StatusAccepted {
			t.Fatalf("expected first call accepted, got %d", rec.Code)
		}
		if i == 1 && rec.Code != http.StatusOK {
			t.Fatalf("expected duplicate call ok, got %d", rec.Code)
		}
	}

	select {
	case <-ch:
	case <-time.After(600 * time.Millisecond):
		t.Fatal("expected first inbound event")
	}
	select {
	case <-ch:
		t.Fatal("unexpected duplicate publish")
	case <-time.After(300 * time.Millisecond):
	}
}

func whatsappSig(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
