package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"empireai/internal/events"
)

const scoringNodeID = ScoringNodeID

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

func inferDiscoveryMode(text string) string {
	t := strings.ToLower(strings.TrimSpace(text))
	switch {
	case strings.Contains(t, "automation_micro"),
		(strings.Contains(t, "automation") && strings.Contains(t, "micro")):
		return "saas_gap"
	case strings.Contains(t, "local service"), strings.Contains(t, "local_services"):
		return "local_services"
	case strings.Contains(t, "trend"), strings.Contains(t, "saas_trend"):
		return "saas_trend"
	default:
		return "saas_gap"
	}
}

func inferGeographyHint(text string) string {
	t := strings.TrimSpace(text)
	if t == "" {
		return ""
	}
	low := strings.ToLower(t)
	for _, geo := range []string{"paraguay", "argentina", "brazil", "mexico", "chile", "peru", "colombia", "uruguay"} {
		if strings.Contains(low, geo) {
			return geo
		}
	}
	return t
}

func budgetEventTypeFromThresholdPayload(raw []byte) events.EventType {
	state := strings.ToLower(strings.TrimSpace(fieldStringFromJSON(raw, "state")))
	switch state {
	case "emergency":
		return events.EventType("budget.emergency")
	case "throttle":
		return events.EventType("budget.throttle")
	case "warning":
		return events.EventType("budget.warning")
	case "ok", "resumed":
		return events.EventType("budget.resumed")
	}
	return events.EventType("")
}

func fieldStringFromJSON(raw []byte, key string) string {
	if len(raw) == 0 || strings.TrimSpace(key) == "" {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil || obj == nil {
		return ""
	}
	return strings.TrimSpace(asString(obj[key]))
}

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

func (s *scanStoreStub) LookupGeographyLabel(context.Context, string) (string, error) {
	s.lookupCalls++
	return "US", nil
}

func (s *scanStoreStub) MarkScanCampaignCompleted(context.Context, string, int) error {
	s.markCalls++
	return nil
}

func (s *scanStoreStub) RequeueDueRescans(context.Context, time.Time) (int, error) {
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

func sanitizeGeographyPhrase(v string) string {
	return SanitizeGeographyPhrase(v)
}

func (m *ScanCampaignManager) setBudgetPausedForTest(v bool) {
	m.SetBudgetPausedForTest(v)
}

func (m *ScanCampaignManager) budgetPausedForTest() bool {
	return m.BudgetPausedForTest()
}

func (m *ScanCampaignManager) backpressurePausedForTest() bool {
	return m.BackpressurePausedForTest()
}
