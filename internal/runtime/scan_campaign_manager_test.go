package runtime

import (
	"context"
	"empireai/internal/events"
	"encoding/json"
	"testing"
	"time"
)

type scanStoreStub struct {
	pauseCalls   int
	resumeCalls  int
	markCalls    int
	requeueCalls int
	claimCalls   int
	lookupCalls  int

	nextClaimOk bool
	nextClaim   ScanCampaign
}

func (s *scanStoreStub) CreateScanCampaign(context.Context, CreateScanCampaignInput) (ScanCampaign, error) {
	return ScanCampaign{}, nil
}
func (s *scanStoreStub) ListScanCampaigns(context.Context, ScanCampaignFilter) ([]ScanCampaign, error) {
	return nil, nil
}
func (s *scanStoreStub) ClaimNextDueScanCampaign(context.Context) (ScanCampaign, bool, error) {
	s.claimCalls++
	if !s.nextClaimOk {
		return ScanCampaign{}, false, nil
	}
	s.nextClaimOk = false
	return s.nextClaim, true, nil
}
func (s *scanStoreStub) LookupGeographyLabel(_ context.Context, _ string) (string, error) {
	s.lookupCalls++
	return "US", nil
}
func (s *scanStoreStub) MarkScanCampaignCompleted(_ context.Context, _ string, _ int) error {
	s.markCalls++
	return nil
}
func (s *scanStoreStub) RequeueDueRescans(_ context.Context, _ time.Time) (int, error) {
	s.requeueCalls++
	return 1, nil
}
func (s *scanStoreStub) PauseQueuedScanCampaigns(context.Context) (int, error) {
	s.pauseCalls++
	return 1, nil
}
func (s *scanStoreStub) ResumePausedScanCampaigns(context.Context) (int, error) {
	s.resumeCalls++
	return 1, nil
}

func TestScanCampaignManager_Tick_ClaimsAndEmitsScanRequested(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ch := bus.Subscribe("watch", events.EventType("scan.requested"))

	store := &scanStoreStub{
		nextClaimOk: true,
		nextClaim: ScanCampaign{
			ID:          "c1",
			GeographyID: "geo1",
			Mode:        "default",
			Categories:  []string{"a", "b"},
			Priority:    "high",
			StrategicContext: mustJSON(map[string]any{
				"directive_text": "US, corpus",
				"corpus_path":    "/data/test-signals-25.jsonl",
			}),
		},
	}
	mgr := NewScanCampaignManager(bus, store)
	mgr.tick(context.Background())

	select {
	case evt := <-ch:
		if string(evt.Type) != "scan.requested" {
			t.Fatalf("unexpected type: %s", evt.Type)
		}
		var payload map[string]any
		if err := json.Unmarshal(evt.Payload, &payload); err != nil {
			t.Fatalf("payload: %v", err)
		}
		if payload["campaign_id"] != "c1" {
			t.Fatalf("expected c1, got %#v", payload["campaign_id"])
		}
		if got := asString(payload["corpus_path"]); got != "/data/test-signals-25.jsonl" {
			t.Fatalf("expected corpus_path propagated, got %q", got)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected scan.requested event")
	}

	if store.requeueCalls != 1 || store.claimCalls != 1 || store.lookupCalls != 1 {
		t.Fatalf("unexpected store calls: requeue=%d claim=%d lookup=%d", store.requeueCalls, store.claimCalls, store.lookupCalls)
	}
}

func TestScanCampaignManager_OnEvent_ThrottleResumeAndCompleted(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ch := bus.Subscribe("watch", events.EventType("scan.requested"))

	store := &scanStoreStub{
		nextClaimOk: true,
		nextClaim: ScanCampaign{
			ID:          "c2",
			GeographyID: "geo1",
			Mode:        "default",
			Categories:  []string{"a"},
			Priority:    "low",
		},
	}
	mgr := NewScanCampaignManager(bus, store)

	mgr.onEvent(context.Background(), events.Event{Type: events.EventType("budget.throttle")})
	mgr.onEvent(context.Background(), events.Event{Type: events.EventType("budget.resumed")})

	mgr.onEvent(context.Background(), events.Event{
		Type:    events.EventType("scan.completed"),
		Payload: mustJSON(map[string]any{"campaign_id": "c1", "discoveries_count": "3"}),
	})

	select {
	case <-ch:
		// tick should fire after completed.
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected scan.requested after scan.completed")
	}

	if store.pauseCalls != 1 || store.resumeCalls != 1 {
		t.Fatalf("expected pause/resume calls, got pause=%d resume=%d", store.pauseCalls, store.resumeCalls)
	}
	if store.markCalls != 1 {
		t.Fatalf("expected MarkScanCampaignCompleted called once, got %d", store.markCalls)
	}
}

func TestScanCampaignManager_Run_KicksOnceAndStops(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	ch := bus.Subscribe("watch", events.EventType("scan.requested"))
	store := &scanStoreStub{
		nextClaimOk: true,
		nextClaim: ScanCampaign{
			ID:          "c-run",
			GeographyID: "geo1",
			Mode:        "default",
			Categories:  []string{"a"},
			Priority:    "low",
		},
	}
	mgr := NewScanCampaignManager(bus, store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Run(ctx)

	select {
	case <-ch:
		// startup tick emits
	case <-time.After(300 * time.Millisecond):
		t.Fatal("expected startup tick to emit scan.requested")
	}
	cancel()
}

func TestAsInt(t *testing.T) {
	if asInt(" 42 ") != 42 {
		t.Fatal("expected 42")
	}
	if asInt("x") != 0 {
		t.Fatal("expected 0")
	}
	if asInt(float64(3)) != 3 {
		t.Fatal("expected 3")
	}
}

func TestParseDirectiveMode(t *testing.T) {
	mode, explicit := parseDirectiveMode("SaaS in Paraguay")
	if mode != "saas_gap" || explicit {
		t.Fatalf("expected default open campaign mode saas_gap, got mode=%s explicit=%v", mode, explicit)
	}

	mode, explicit = parseDirectiveMode("run saas_trend in Paraguay")
	if mode != "saas_trend" || !explicit {
		t.Fatalf("expected explicit saas_trend mode, got mode=%s explicit=%v", mode, explicit)
	}

	mode, explicit = parseDirectiveMode("run automation micro in Paraguay")
	if mode != "saas_gap" || !explicit {
		t.Fatalf("expected explicit automation_micro alias to saas_gap, got mode=%s explicit=%v", mode, explicit)
	}

	mode, explicit = parseDirectiveMode("US, corpus, corpus_path=/data/test-signals-25.jsonl")
	if mode != "corpus" || !explicit {
		t.Fatalf("expected explicit corpus mode, got mode=%s explicit=%v", mode, explicit)
	}
}

func TestRemainingCampaignModes(t *testing.T) {
	out := remainingCampaignModes("saas_gap")
	if len(out) != 2 || out[0] != "saas_trend" || out[1] != "local_services" {
		t.Fatalf("unexpected campaign remainder for saas_gap: %+v", out)
	}
	out = remainingCampaignModes("automation_micro")
	if len(out) != 2 || out[0] != "saas_trend" || out[1] != "local_services" {
		t.Fatalf("unexpected campaign remainder for automation_micro alias: %+v", out)
	}
	out = remainingCampaignModes("local_services")
	if len(out) != 0 {
		t.Fatalf("local_services should have no follow-on modes, got %+v", out)
	}
}

func TestCampaignModesForDirective(t *testing.T) {
	got := campaignModesForDirective("saas_gap", false)
	if len(got) != 3 || got[0] != "saas_gap" || got[1] != "saas_trend" || got[2] != "local_services" {
		t.Fatalf("unexpected full-cycle modes: %+v", got)
	}

	single := campaignModesForDirective("saas_trend", true)
	if len(single) != 1 || single[0] != "saas_trend" {
		t.Fatalf("unexpected explicit single mode: %+v", single)
	}

	corpus := campaignModesForDirective("corpus", true)
	if len(corpus) != 1 || corpus[0] != "corpus" {
		t.Fatalf("unexpected corpus explicit mode: %+v", corpus)
	}
}

func TestIsComplexDirectiveText(t *testing.T) {
	if !isComplexDirectiveText("Focus on compliance-driven opportunities in LATAM countries with over 80 percent internet penetration") {
		t.Fatal("expected complex directive to be detected")
	}
	if isComplexDirectiveText("SaaS in Uruguay") {
		t.Fatal("expected simple directive to stay deterministic-runtime path")
	}
}

func TestParseDirectiveMode_Corpus(t *testing.T) {
	mode, explicit := parseDirectiveMode("US, corpus, corpus_path=/data/test-signals-25.jsonl")
	if mode != "corpus" || !explicit {
		t.Fatalf("expected corpus explicit mode, got mode=%q explicit=%v", mode, explicit)
	}
}

func TestParseDirectiveGeography_USAbbrev(t *testing.T) {
	name, country, _ := parseDirectiveGeography("US, corpus, corpus_path=/data/test-signals-25.jsonl")
	if name != "United States" || country != "United States" {
		t.Fatalf("expected US abbreviation to map to United States, got name=%q country=%q", name, country)
	}
}
