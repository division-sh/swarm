package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimepipeline "empireai/internal/runtime/pipeline"
	empirepipeline "empireai/internal/runtime/pipeline/empire"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

func assertNoEventType(t *testing.T, ch <-chan events.Event, typ string, d time.Duration) {
	t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case evt := <-ch:
			if string(evt.Type) == typ {
				t.Fatalf("unexpected event type %s", typ)
			}
		case <-timer.C:
			return
		}
	}
}

func extractSystemPromptForTest(cfg models.AgentConfig) string {
	if len(cfg.Config) == 0 || !json.Valid(cfg.Config) {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(cfg.Config, &obj); err != nil {
		return ""
	}
	if v, ok := obj["system_prompt"].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

type captureStore struct {
	events     []events.Event
	deliveries map[string][]string
}

func (s *captureStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.events = append(s.events, evt)
	return nil
}

func (s *captureStore) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	if s.deliveries == nil {
		s.deliveries = make(map[string][]string)
	}
	s.deliveries[eventID] = append([]string(nil), agentIDs...)
	return nil
}

type failingDeliveryStore struct{}

func (failingDeliveryStore) AppendEvent(_ context.Context, _ events.Event) error { return nil }
func (failingDeliveryStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return nil
}

func expectedScoringDimensions(rubric string) []string {
	return empirepipeline.NewModule().ScoringPolicy().ExpectedScoringDimensions(rubric)
}

func evaluateDiscoveryPreFilter(payload map[string]any, rawSignal float64) (bool, float64, string) {
	return empirepipeline.NewModule().DiscoveryPolicy().EvaluateDiscoveryPreFilter(payload, rawSignal)
}

func buildPrefilterSkipDetail(payload map[string]any, rawSignal, adjustedSignal float64, reason, mode string) map[string]any {
	return empirepipeline.NewModule().DiscoveryPolicy().BuildPrefilterSkipDetail(payload, rawSignal, adjustedSignal, reason, mode)
}

func cloneMap(in map[string]any) map[string]any {
	return runtimepipeline.CloneMapForTest(in)
}

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

func newScanCampaignHooksForTest() runtimepipeline.ScanCampaignHooks {
	return runtimepipeline.ScanCampaignHooks{
		Warnf: runtimeWarn,
		RecordTransition: func(ctx context.Context, db *sql.DB, in runtimepipeline.ScanCampaignTransitionInput) error {
			return RecordPipelineTransition(ctx, db, PipelineTransitionInput{
				EventID:       in.EventID,
				EventType:     in.EventType,
				Handler:       in.Handler,
				PipelineType:  in.PipelineType,
				PipelineID:    in.PipelineID,
				Action:        in.Action,
				StateBefore:   in.StateBefore,
				StateAfter:    in.StateAfter,
				EventsEmitted: in.EventsEmitted,
				DropReason:    in.DropReason,
				Error:         in.Error,
				Duration:      in.Duration,
			})
		},
		EnsureDirectiveGeography: runtimepipeline.EnsureDirectiveGeography,
	}
}
