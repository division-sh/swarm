package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/testutil"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type prefilterContractVectors struct {
	Cases []prefilterContractCase `yaml:"cases"`
}

type prefilterContractCase struct {
	Name           string `yaml:"name"`
	Expected       string `yaml:"expected"`
	ExpectedReason string `yaml:"expected_reason"`
	Input          struct {
		SignalStrength      float64  `yaml:"signal_strength"`
		RedFlags            []string `yaml:"red_flags"`
		EvidenceURLs        int      `yaml:"evidence_urls"`
		RetentionPrimitives []string `yaml:"retention_primitives"`
	} `yaml:"input"`
}

func TestPipelineCoordinatorContractSemantics_PrefilterVectors(t *testing.T) {
	repoRoot := contractComplianceRepoRoot(t)
	runPrefilterContractVectorChecks(t, repoRoot)
}

func runPrefilterContractVectorChecks(t *testing.T, repoRoot string) {
	t.Helper()
	path := filepath.Join(repoRoot, "contracts", "test-vectors", "prefilter.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read prefilter vectors: %v", err)
	}
	var vectors prefilterContractVectors
	if err := yaml.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("parse prefilter vectors: %v", err)
	}
	if len(vectors.Cases) == 0 {
		t.Fatal("prefilter vectors empty")
	}

	for _, tc := range vectors.Cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			payload := buildPrefilterFixturePayload(tc)
			ok, _, reason := evaluateDiscoveryPreFilter(payload, tc.Input.SignalStrength)
			switch strings.ToLower(strings.TrimSpace(tc.Expected)) {
			case "pass":
				if !ok {
					t.Fatalf("expected pass, got reject reason=%q", reason)
				}
			case "reject":
				if ok {
					t.Fatalf("expected reject, got pass")
				}
				if want := strings.TrimSpace(tc.ExpectedReason); want != "" && reason != want {
					t.Fatalf("reject reason mismatch: got=%q want=%q", reason, want)
				}
			default:
				t.Fatalf("unsupported expected value %q", tc.Expected)
			}
		})
	}
}

func TestPipelineCoordinatorContractSemantics_PrefilterSkipLogging(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	bus := NewEventBus(InMemoryEventStore{})
	bus.SetRuntimeLogger(NewRuntimeLogger(db))
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	scanID := uuid.NewString()
	campaignID := uuid.NewString()
	pc.handleScanRequested(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("scan.requested"),
		Payload: mustJSON(map[string]any{
			"scan_id":     scanID,
			"campaign_id": campaignID,
			"mode":        "corpus",
			"geography":   "us",
		}),
	})

	reportID := uuid.NewString()
	pc.handleDiscoveryReport(ctx, events.Event{
		ID:          reportID,
		Type:        events.EventType("category.assessed"),
		SourceAgent: "market-research-agent",
		Payload: mustJSON(map[string]any{
			"scan_id":                scanID,
			"signal_strength":        75,
			"opportunity_name":       "Contract logging fixture",
			"preliminary_icp":        "Owner at salon schedule desk",
			"opportunity_hypothesis": "Simple booking helper",
			"build_sketch": map[string]any{
				"red_flags": []any{
					map[string]any{"type": "phone_led_sales"},
				},
			},
			"retention_primitives": []any{"recurring_data"},
			"evidence": map[string]any{
				"competitors": []any{
					map[string]any{"name": "X", "pricing": "$99", "source_url": "https://example.com/c"},
				},
				"pain_signals": []any{
					map[string]any{"signal": "pain", "source_url": "https://example.com/p"},
				},
				"buyer_communities": []any{
					map[string]any{"name": "buyers", "source_url": "https://example.com/b"},
				},
			},
		}),
	})

	var gotReason, gotSignal, gotRetention string
	err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(detail->>'skip_reason',''),
			COALESCE(detail->>'signal_strength',''),
			COALESCE(detail->>'retention_primitive','')
		FROM runtime_log
		WHERE component='prefilter'
		  AND action='skipped'
		  AND event_id=$1::uuid
		ORDER BY ts DESC
		LIMIT 1
	`, reportID).Scan(&gotReason, &gotSignal, &gotRetention)
	if err != nil {
		t.Fatalf("load prefilter runtime_log: %v", err)
	}
	if gotReason != "blocking_red_flag" {
		t.Fatalf("unexpected skip_reason: %q", gotReason)
	}
	if strings.TrimSpace(gotSignal) == "" {
		t.Fatalf("expected signal_strength in runtime_log detail")
	}
	if !strings.Contains(gotRetention, "recurring_data") {
		t.Fatalf("expected retention_primitive in runtime_log detail, got %q", gotRetention)
	}
}

func TestPipelineCoordinatorContractSemantics_ScanCompletion(t *testing.T) {
	ctx := context.Background()

	t.Run("single_agent_modes_complete_on_first_completion", func(t *testing.T) {
		bus := NewEventBus(InMemoryEventStore{})
		pc := NewFactoryPipelineCoordinator(bus, nil)
		done := bus.Subscribe("scan-done-corpus", events.EventType("scan.completed"))

		pc.handleScanRequested(ctx, events.Event{
			ID:   uuid.NewString(),
			Type: events.EventType("scan.requested"),
			Payload: mustJSON(map[string]any{
				"scan_id":     "scan-contract-corpus",
				"campaign_id": uuid.NewString(),
				"mode":        "corpus",
				"geography":   "us",
			}),
		})
		pc.handleScanCompletion(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("market_research.scan_complete"),
			SourceAgent: "market-research-agent",
			Payload:     mustJSON(map[string]any{"scan_id": "scan-contract-corpus"}),
		})

		evt := waitForEventType(t, done, "scan.completed")
		payload := parsePayloadMap(evt.Payload)
		if got, want := asInt(payload["agents_expected"]), 1; got != want {
			t.Fatalf("agents_expected mismatch: got=%d want=%d payload=%v", got, want, payload)
		}
		if got, want := asInt(payload["agents_complete"]), 1; got != want {
			t.Fatalf("agents_complete mismatch: got=%d want=%d payload=%v", got, want, payload)
		}
		if got := len(pc.SnapshotScans()); got != 0 {
			t.Fatalf("expected scan accumulator cleared, got=%d", got)
		}
	})

	t.Run("local_services_waits_for_all_scanners", func(t *testing.T) {
		bus := NewEventBus(InMemoryEventStore{})
		pc := NewFactoryPipelineCoordinator(bus, nil)
		done := bus.Subscribe("scan-done-local-services", events.EventType("scan.completed"))

		scanID := "scan-contract-local-services"
		pc.handleScanRequested(ctx, events.Event{
			ID:   uuid.NewString(),
			Type: events.EventType("scan.requested"),
			Payload: mustJSON(map[string]any{
				"scan_id":     scanID,
				"campaign_id": uuid.NewString(),
				"mode":        "local_services",
				"geography":   "us",
			}),
		})

		completions := []events.EventType{
			events.EventType("scanner.google_maps.scan_complete"),
			events.EventType("scanner.instagram.scan_complete"),
			events.EventType("scanner.reviews.scan_complete"),
			events.EventType("scanner.directories.scan_complete"),
			events.EventType("scanner.yelp.scan_complete"),
		}
		for i, evtType := range completions {
			pc.handleScanCompletion(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        evtType,
				SourceAgent: "scanner-agent",
				Payload:     mustJSON(map[string]any{"scan_id": scanID}),
			})
			if i < len(completions)-1 {
				select {
				case <-done:
					t.Fatalf("scan.completed emitted early at completion #%d", i+1)
				case <-time.After(60 * time.Millisecond):
				}
			}
		}

		evt := waitForEventType(t, done, "scan.completed")
		payload := parsePayloadMap(evt.Payload)
		if got, want := asInt(payload["agents_expected"]), localServicesScannerExpected; got != want {
			t.Fatalf("agents_expected mismatch: got=%d want=%d payload=%v", got, want, payload)
		}
		if got, want := asInt(payload["agents_complete"]), localServicesScannerExpected; got != want {
			t.Fatalf("agents_complete mismatch: got=%d want=%d payload=%v", got, want, payload)
		}
	})
}

func TestPipelineCoordinatorContractSemantics_ValidationGateAndInterceptPolicy(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)

	verticalID := uuid.NewString()
	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 81}),
	})
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("research.completed"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g1")
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("spec.approved"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g2")
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("cto.spec_approved"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g3")
	pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("brand.candidates_ready"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"ok": true})}, "g4")

	pc.mu.Lock()
	st := pc.validations[verticalID]
	pc.mu.Unlock()
	if st == nil || !st.G1Research || !st.G2Spec || !st.G3CTO || !st.G4Brand || !st.PackagingRequested {
		t.Fatalf("expected all validation gates completed and packaging requested, state=%+v", st)
	}

	pc.handleValidationPackaged(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("vertical.ready_for_review"), VerticalID: verticalID})
	if reason := pc.interceptStateDropReason("research.completed", events.Event{VerticalID: verticalID}); reason == "" {
		t.Fatal("expected packaged pipeline to drop stale research.completed")
	}

	consume, handled := pc.interceptPolicy("vertical.scored", events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.scored"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"result": "marginal"}),
	})
	if !handled || !consume {
		t.Fatalf("expected marginal vertical.scored to be consumed, handled=%v consume=%v", handled, consume)
	}
	consume, handled = pc.interceptPolicy("vertical.scored", events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.scored"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"result": "shortlisted"}),
	})
	if !handled || consume {
		t.Fatalf("expected shortlisted vertical.scored to pass through, handled=%v consume=%v", handled, consume)
	}
}

func TestPipelineCoordinatorContractSemantics_DedupQueueDrains(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	ch := bus.Subscribe("dedup-contract", events.EventType("dedup.ambiguous"), events.EventType("vertical.discovered"))

	existingID := uuid.NewString()
	insertTestVertical(t, db, existingID, "Pet Grooming Scheduling", "argentina")

	scanID := "scan-contract-dedup"
	pc.handleScanRequested(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("scan.requested"),
		Payload: mustJSON(map[string]any{
			"scan_id":     scanID,
			"campaign_id": uuid.NewString(),
			"mode":        "saas_gap",
			"geography":   "argentina",
		}),
	})
	pc.handleDiscoveryReport(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("category.assessed"),
		SourceAgent: "market-research-agent",
		Payload: mustJSON(map[string]any{
			"scan_id":         scanID,
			"vertical_name":   "Pet Grooming Scheduling SaaS",
			"signal_strength": 81,
			"geography":       "argentina",
			"mode":            "saas_gap",
		}),
	})

	dedupEvt := waitForEventType(t, ch, "dedup.ambiguous")
	dedupPayload := parsePayloadMap(dedupEvt.Payload)
	dedupEventID := strings.TrimSpace(asString(dedupPayload["dedup_event_id"]))
	if dedupEventID == "" {
		t.Fatal("expected dedup_event_id in dedup.ambiguous")
	}

	pc.handleDedupResolved(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("dedup.resolved"),
		SourceAgent: "discovery-coordinator",
		Payload: mustJSON(map[string]any{
			"dedup_event_id": dedupEventID,
			"action":         "keep_both",
		}),
	})
	waitForEventType(t, ch, "vertical.discovered")

	pc.mu.Lock()
	_, stillPending := pc.pendingDedup[dedupEventID]
	pc.mu.Unlock()
	if stillPending {
		t.Fatal("expected pending dedup queue to drain after dedup.resolved")
	}
}

func TestPipelineCoordinatorContractSemantics_CampaignCycleOrder(t *testing.T) {
	cases := []struct {
		mode string
		want []string
	}{
		{mode: "saas_gap", want: []string{"saas_trend", "local_services"}},
		{mode: "saas_trend", want: []string{"local_services"}},
		{mode: "local_services", want: nil},
		{mode: "automation_micro", want: []string{"saas_trend", "local_services"}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.mode, func(t *testing.T) {
			got := remainingCampaignModes(tc.mode)
			if len(got) != len(tc.want) {
				t.Fatalf("remainingCampaignModes(%q) len mismatch got=%v want=%v", tc.mode, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("remainingCampaignModes(%q) mismatch got=%v want=%v", tc.mode, got, tc.want)
				}
			}
		})
	}
}

func buildPrefilterFixturePayload(tc prefilterContractCase) map[string]any {
	urlCount := tc.Input.EvidenceURLs
	if urlCount <= 0 {
		urlCount = 1
	}
	urls := make([]string, 0, urlCount)
	for i := 0; i < urlCount; i++ {
		urls = append(urls, "https://example.com/vector/"+tc.Name+"/"+strings.TrimSpace(strings.ReplaceAll(uuid.NewString(), "-", "")))
	}
	urlAt := func(idx int) string {
		if len(urls) == 0 {
			return "https://example.com/vector/default"
		}
		return urls[idx%len(urls)]
	}

	redFlags := make([]any, 0, len(tc.Input.RedFlags))
	for _, flag := range tc.Input.RedFlags {
		flag = strings.TrimSpace(flag)
		if flag == "" {
			continue
		}
		redFlags = append(redFlags, map[string]any{"type": flag})
	}
	retention := make([]any, 0, len(tc.Input.RetentionPrimitives))
	for _, primitive := range tc.Input.RetentionPrimitives {
		primitive = strings.TrimSpace(primitive)
		if primitive == "" {
			continue
		}
		retention = append(retention, primitive)
	}

	return map[string]any{
		"signal_strength":        tc.Input.SignalStrength,
		"opportunity_name":       "Fixture Opportunity " + tc.Name,
		"preliminary_icp":        "Owner at salon schedule desk",
		"opportunity_hypothesis": "Simple booking helper",
		"retention_primitives":   retention,
		"opportunity_pattern":    "ai_wrapper",
		"build_sketch": map[string]any{
			"core_features":    []any{"Booking page"},
			"key_integrations": []any{},
			"red_flags":        redFlags,
		},
		"evidence": map[string]any{
			"competitors": []any{
				map[string]any{"name": "Comp", "pricing": "$99", "source_url": urlAt(0)},
			},
			"pain_signals": []any{
				map[string]any{"signal": "Manual scheduling pain", "source_url": urlAt(1)},
			},
			"buyer_communities": []any{
				map[string]any{"name": "Salon owners", "source_url": urlAt(2)},
			},
			"regulatory": []any{
				map[string]any{"detail": "Local requirements", "source_url": urlAt(3)},
			},
		},
	}
}
