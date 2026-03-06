package pipeline

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

type scoringCompositeVectors struct {
	Cases []scoringCompositeCase `yaml:"cases"`
}

type scoringCompositeCase struct {
	Name              string         `yaml:"name"`
	Dimensions        map[string]int `yaml:"dimensions"`
	ExpectedResult    string         `yaml:"expected_result"`
	ExpectedReason    string         `yaml:"expected_reason"`
	ExpectedComposite float64        `yaml:"expected_composite"`
}

type validationGateVectors struct {
	Cases []validationGateCase `yaml:"cases"`
}

type validationGateCase struct {
	Name     string   `yaml:"name"`
	Steps    []string `yaml:"steps"`
	Expected struct {
		G1                 bool   `yaml:"g1"`
		G2                 bool   `yaml:"g2"`
		G3                 bool   `yaml:"g3"`
		G4                 bool   `yaml:"g4"`
		PackagingRequested bool   `yaml:"packaging_requested"`
		Status             string `yaml:"status"`
		StaleResearchDrop  bool   `yaml:"stale_research_drop"`
	} `yaml:"expected"`
}

type campaignCyclingVectors struct {
	ModeCases           []campaignModeCase           `yaml:"mode_cases"`
	ScanCompletionCases []campaignScanCompletionCase `yaml:"scan_completion_cases"`
}

type campaignModeCase struct {
	Name           string   `yaml:"name"`
	Mode           string   `yaml:"mode"`
	RemainingModes []string `yaml:"remaining_modes"`
	ExpectedAgents int      `yaml:"expected_agents"`
}

type campaignScanCompletionCase struct {
	Name                string   `yaml:"name"`
	Mode                string   `yaml:"mode"`
	CompletionEvents    []string `yaml:"completion_events"`
	ExpectScanCompleted bool     `yaml:"expect_scan_completed"`
}

type derivationConstraintVectors struct {
	Cases []derivationConstraintCase `yaml:"cases"`
}

type derivationConstraintCase struct {
	Name             string `yaml:"name"`
	GenerationDepth  int    `yaml:"generation_depth"`
	ExistingChildren int    `yaml:"existing_children"`
	ExpectDiscovered bool   `yaml:"expect_discovered"`
}

func TestPipelineCoordinatorContractSemantics_PrefilterVectors(t *testing.T) {
	repoRoot := contractComplianceRepoRoot(t)
	runPrefilterContractVectorChecks(t, repoRoot)
}

func TestPipelineCoordinatorContractSemantics_ScoringCompositeVectors(t *testing.T) {
	repoRoot := contractComplianceRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(repoRoot, "contracts", "test-vectors", "scoring-composite-rejection.yaml"))
	if err != nil {
		t.Fatalf("read scoring vectors: %v", err)
	}
	var vectors scoringCompositeVectors
	if err := yaml.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("parse scoring vectors: %v", err)
	}
	if len(vectors.Cases) == 0 {
		t.Fatal("scoring vectors empty")
	}

	for _, tc := range vectors.Cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)
			acc := newUniversalAccumulator(uuid.NewString(), "Vector Vertical", "argentina", "saas_gap")
			setScores(acc, tc.Dimensions)
			result := pc.computeComposite(acc, false)

			if got, want := strings.TrimSpace(result.Result), strings.TrimSpace(tc.ExpectedResult); got != want {
				t.Fatalf("result mismatch: got=%q want=%q", got, want)
			}
			if want := strings.TrimSpace(tc.ExpectedReason); want != "" && strings.TrimSpace(result.Reason) != want {
				t.Fatalf("reason mismatch: got=%q want=%q", result.Reason, want)
			}
			if tc.ExpectedComposite > 0 {
				if diff := result.CompositeScore - tc.ExpectedComposite; diff > 0.01 || diff < -0.01 {
					t.Fatalf("composite mismatch: got=%v want=%v", result.CompositeScore, tc.ExpectedComposite)
				}
			}
		})
	}
}

func TestPipelineCoordinatorContractSemantics_ValidationGateVectors(t *testing.T) {
	repoRoot := contractComplianceRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(repoRoot, "contracts", "test-vectors", "validation-gates.yaml"))
	if err != nil {
		t.Fatalf("read validation gate vectors: %v", err)
	}
	var vectors validationGateVectors
	if err := yaml.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("parse validation gate vectors: %v", err)
	}
	if len(vectors.Cases) == 0 {
		t.Fatal("validation gate vectors empty")
	}

	for _, tc := range vectors.Cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
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
			for _, step := range tc.Steps {
				switch strings.ToLower(strings.TrimSpace(step)) {
				case "g1":
					pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("research.completed"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"business_brief": map[string]any{"ok": true}})}, "g1")
				case "g2":
					pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("spec.approved"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"final_spec": "ok"})}, "g2")
				case "g3":
					pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("cto.spec_approved"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"cto_notes": "ok"})}, "g3")
				case "g4":
					pc.handleValidationGate(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("brand.candidates_ready"), VerticalID: verticalID, Payload: mustJSON(map[string]any{"candidates": []string{"A"}})}, "g4")
				case "packaged":
					pc.handleValidationPackaged(ctx, events.Event{ID: uuid.NewString(), Type: events.EventType("vertical.ready_for_review"), VerticalID: verticalID})
				default:
					t.Fatalf("unsupported step %q", step)
				}
			}
			pc.mu.Lock()
			st := pc.validations[verticalID]
			pc.mu.Unlock()
			if st == nil {
				t.Fatal("missing validation state")
			}
			if st.G1Research != tc.Expected.G1 || st.G2Spec != tc.Expected.G2 || st.G3CTO != tc.Expected.G3 || st.G4Brand != tc.Expected.G4 {
				t.Fatalf("gate mismatch: got g1=%v g2=%v g3=%v g4=%v want g1=%v g2=%v g3=%v g4=%v", st.G1Research, st.G2Spec, st.G3CTO, st.G4Brand, tc.Expected.G1, tc.Expected.G2, tc.Expected.G3, tc.Expected.G4)
			}
			if st.PackagingRequested != tc.Expected.PackagingRequested {
				t.Fatalf("packaging_requested mismatch: got=%v want=%v", st.PackagingRequested, tc.Expected.PackagingRequested)
			}
			if got, want := strings.TrimSpace(st.Status), strings.TrimSpace(tc.Expected.Status); got != want {
				t.Fatalf("status mismatch: got=%q want=%q", got, want)
			}
			dropReason := pc.interceptStateDropReason("research.completed", events.Event{VerticalID: verticalID})
			if tc.Expected.StaleResearchDrop && strings.TrimSpace(dropReason) == "" {
				t.Fatalf("expected stale research drop reason, got empty")
			}
			if !tc.Expected.StaleResearchDrop && strings.TrimSpace(dropReason) != "" {
				t.Fatalf("expected no stale research drop reason, got %q", dropReason)
			}
		})
	}
}

func TestPipelineCoordinatorContractSemantics_CampaignCyclingVectors(t *testing.T) {
	repoRoot := contractComplianceRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(repoRoot, "contracts", "test-vectors", "campaign-cycling.yaml"))
	if err != nil {
		t.Fatalf("read campaign vectors: %v", err)
	}
	var vectors campaignCyclingVectors
	if err := yaml.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("parse campaign vectors: %v", err)
	}
	if len(vectors.ModeCases) == 0 && len(vectors.ScanCompletionCases) == 0 {
		t.Fatal("campaign vectors empty")
	}

	for _, tc := range vectors.ModeCases {
		tc := tc
		t.Run("mode_"+tc.Name, func(t *testing.T) {
			gotModes := remainingCampaignModes(tc.Mode)
			if len(gotModes) != len(tc.RemainingModes) {
				t.Fatalf("remaining modes len mismatch: got=%v want=%v", gotModes, tc.RemainingModes)
			}
			for i := range gotModes {
				if gotModes[i] != tc.RemainingModes[i] {
					t.Fatalf("remaining modes mismatch: got=%v want=%v", gotModes, tc.RemainingModes)
				}
			}
			if got, want := expectedAgents(tc.Mode), tc.ExpectedAgents; got != want {
				t.Fatalf("expectedAgents mismatch for mode=%s got=%d want=%d", tc.Mode, got, want)
			}
		})
	}

	for _, tc := range vectors.ScanCompletionCases {
		tc := tc
		t.Run("completion_"+tc.Name, func(t *testing.T) {
			ctx := context.Background()
			bus := NewEventBus(InMemoryEventStore{})
			pc := NewFactoryPipelineCoordinator(bus, nil)
			scanID := "scan-" + strings.TrimSpace(strings.ReplaceAll(uuid.NewString(), "-", ""))
			done := bus.Subscribe("scan-done-"+tc.Name, events.EventType("scan.completed"))
			pc.handleScanRequested(ctx, events.Event{
				ID:   uuid.NewString(),
				Type: events.EventType("scan.requested"),
				Payload: mustJSON(map[string]any{
					"scan_id":     scanID,
					"campaign_id": uuid.NewString(),
					"mode":        tc.Mode,
					"geography":   "us",
				}),
			})

			for _, eventType := range tc.CompletionEvents {
				pc.handleScanCompletion(ctx, events.Event{
					ID:          uuid.NewString(),
					Type:        events.EventType(strings.TrimSpace(eventType)),
					SourceAgent: "vector-agent",
					Payload:     mustJSON(map[string]any{"scan_id": scanID}),
				})
			}
			gotCompleted := false
			select {
			case <-done:
				gotCompleted = true
			case <-time.After(100 * time.Millisecond):
			}
			if gotCompleted != tc.ExpectScanCompleted {
				t.Fatalf("scan completion mismatch: got=%v want=%v", gotCompleted, tc.ExpectScanCompleted)
			}
		})
	}
}

func TestPipelineCoordinatorContractSemantics_DerivationConstraintVectors(t *testing.T) {
	repoRoot := contractComplianceRepoRoot(t)
	raw, err := os.ReadFile(filepath.Join(repoRoot, "contracts", "test-vectors", "derivation-constraints.yaml"))
	if err != nil {
		t.Fatalf("read derivation vectors: %v", err)
	}
	var vectors derivationConstraintVectors
	if err := yaml.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("parse derivation vectors: %v", err)
	}
	if len(vectors.Cases) == 0 {
		t.Fatal("derivation vectors empty")
	}

	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	for _, tc := range vectors.Cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			bus := NewEventBus(InMemoryEventStore{})
			pc := NewFactoryPipelineCoordinator(bus, db)

			parentID := uuid.NewString()
			insertTestVertical(t, db, parentID, "Parent Vertical "+tc.Name, "argentina")
			for i := 0; i < tc.ExistingChildren; i++ {
				childID := uuid.NewString()
				if _, err := db.ExecContext(ctx, `
					INSERT INTO verticals (id, name, slug, geography, stage, mode, parent_id, created_at, updated_at)
					VALUES ($1::uuid, $2, $3, 'argentina', 'discovered', 'factory', $4::uuid, now(), now())
				`, childID, "Child "+tc.Name+" "+uuid.NewString(), "child-"+strings.ToLower(strings.ReplaceAll(uuid.NewString(), "-", "")), parentID); err != nil {
					t.Fatalf("seed child vertical: %v", err)
				}
			}

			discoveredCh := bus.Subscribe("vector-derived-"+tc.Name, events.EventType("vertical.discovered"))
			pc.handleVerticalDerived(ctx, events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType("vertical.derived"),
				SourceAgent: "analysis-agent",
				Payload: mustJSON(map[string]any{
					"parent_id":              parentID,
					"generation_depth":       tc.GenerationDepth,
					"generator_agent_id":     "analysis-agent",
					"derivation_rationale":   map[string]any{"why": "vector test"},
					"opportunity_name":       "Derived Opportunity " + tc.Name,
					"signal_strength":        72,
					"geography":              "argentina",
					"discovery_context":      map[string]any{"source": "vector"},
					"preliminary_icp":        "Billing manager at SMB clinic chains handling invoice reporting",
					"opportunity_hypothesis": "Automate insurer invoice reconciliation with reporting queue",
					"retention_primitives":   []any{"workflow_embedding"},
					"build_sketch": map[string]any{
						"core_features":    []any{"parser", "exception queue"},
						"key_integrations": []any{"quickbooks"},
						"red_flags":        []any{},
					},
					"evidence": map[string]any{
						"competitors":       []any{map[string]any{"name": "Comp", "pricing": "$99", "source_url": "https://example.com/c1"}},
						"pain_signals":      []any{map[string]any{"signal": "manual denials", "source_url": "https://example.com/p1"}},
						"buyer_communities": []any{map[string]any{"name": "community", "source_url": "https://example.com/b1"}},
						"regulatory":        []any{map[string]any{"detail": "billing code checks", "source_url": "https://example.com/r1"}},
					},
				}),
			})

			gotDiscovered := false
			select {
			case <-discoveredCh:
				gotDiscovered = true
			case <-time.After(120 * time.Millisecond):
			}
			if gotDiscovered != tc.ExpectDiscovered {
				t.Fatalf("derived discovery mismatch: got=%v want=%v", gotDiscovered, tc.ExpectDiscovered)
			}
		})
	}
}

func TestPipelineCoordinatorContractSemantics_DerivedEventMaterializesChildVertical(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	parentID := uuid.NewString()
	insertTestVertical(t, db, parentID, "Parent Vertical Derived Integration", "argentina")

	discoveredCh := bus.Subscribe("derived-integration-watch", events.EventType("vertical.discovered"))
	derivedEventID := uuid.NewString()
	if err := bus.Publish(ctx, events.Event{
		ID:          derivedEventID,
		Type:        events.EventType("vertical.derived"),
		SourceAgent: "analysis-agent",
		VerticalID:  parentID,
		Payload: mustJSON(map[string]any{
			"parent_id":              parentID,
			"generation_depth":       1,
			"generator_agent_id":     "analysis-agent",
			"derivation_rationale":   map[string]any{"summary": "Narrow ICP to micro-firms to remove direct incumbent collision"},
			"opportunity_name":       "Derived Vertical Integration Test",
			"signal_strength":        74,
			"geography":              "argentina",
			"discovery_context":      map[string]any{"source": "integration-test"},
			"preliminary_icp":        "Operations manager at small clinic chains handling invoice compliance workflow",
			"opportunity_hypothesis": "Automate compliance reporting queue for clinic operators with recurring billing workflows",
			"retention_primitives":   []any{"workflow_embedding"},
			"build_sketch": map[string]any{
				"core_features":        []any{"parser", "exception queue"},
				"key_integrations":     []any{"quickbooks"},
				"red_flags":            []any{},
				"retention_primitives": []any{"workflow_embedding"},
				"workflow_embedding":   true,
				"integration_lock_in":  true,
				"compliance_cadence":   true,
				"team_collaboration":   true,
				"recurring_data":       true,
			},
			"evidence": map[string]any{
				"competitors": []any{
					map[string]any{"name": "CompA", "pricing": "$99", "source_url": "https://example.com/compa"},
				},
				"pain_signals": []any{
					map[string]any{"signal": "manual rework", "source_url": "https://example.com/pain"},
				},
				"buyer_communities": []any{
					map[string]any{"name": "Community", "source_url": "https://example.com/community"},
				},
				"regulatory": []any{
					map[string]any{"detail": "compliance cadence", "source_url": "https://example.com/reg"},
				},
			},
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish vertical.derived: %v", err)
	}

	discovered := waitForEventType(t, discoveredCh, "vertical.discovered")
	discoveredPayload := parsePayloadMap(discovered.Payload)
	childID := strings.TrimSpace(discovered.VerticalID)
	if childID == "" {
		childID = strings.TrimSpace(asString(discoveredPayload["vertical_id"]))
	}
	if childID == "" {
		t.Fatal("expected vertical.discovered with child vertical_id")
	}
	if got := strings.TrimSpace(asString(discoveredPayload["mode"])); got != "derived" {
		t.Fatalf("expected vertical.discovered mode=derived, got=%q payload=%v", got, discoveredPayload)
	}

	var (
		gotParent, gotGenerator, gotStage string
		gotDepth                          int
	)
	if err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(parent_id::text, ''),
			COALESCE(generator_agent_id, ''),
			COALESCE(stage, ''),
			COALESCE(generation_depth, 0)
		FROM verticals
		WHERE id = $1::uuid
	`, childID).Scan(&gotParent, &gotGenerator, &gotStage, &gotDepth); err != nil {
		t.Fatalf("load derived child vertical: %v", err)
	}
	if gotParent != parentID {
		t.Fatalf("expected child parent_id=%s got=%s", parentID, gotParent)
	}
	if gotGenerator != "analysis-agent" {
		t.Fatalf("expected child generator_agent_id=analysis-agent got=%q", gotGenerator)
	}
	if gotDepth != 1 {
		t.Fatalf("expected child generation_depth=1 got=%d", gotDepth)
	}
	if gotStage != "discovered" {
		t.Fatalf("expected child stage=discovered got=%q", gotStage)
	}

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
