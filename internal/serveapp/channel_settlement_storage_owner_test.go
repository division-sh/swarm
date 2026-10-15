package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func channelSettlementStorageObservation(t *testing.T, h *channelOnboardingE2EHarness) storetest.ChannelSettlementObservation {
	t.Helper()
	channelAnchorStorageObservation(t, h)
	return h.anchorObservation
}

type channelSettlementDiagnosticControl struct {
	storetest.ChannelSettlementObservation
	operations   []map[string]string
	attemptReads int
}

func (*channelSettlementDiagnosticControl) ReadChannelPlanSourceDefaultStorage(context.Context, string) ([]map[string]string, error) {
	return []map[string]string{{"delivery_id": "exact"}}, nil
}
func (*channelSettlementDiagnosticControl) ReadChannelActivationDiagnosticStorage(context.Context, string) ([]map[string]string, error) {
	return nil, nil
}
func (*channelSettlementDiagnosticControl) ReadChannelRenderDiagnosticStorage(context.Context, string) ([]map[string]string, error) {
	return nil, nil
}
func (*channelSettlementDiagnosticControl) ReadChannelReceiptDiagnosticStorage(context.Context, string) ([]map[string]string, error) {
	return nil, nil
}
func (p *channelSettlementDiagnosticControl) ReadChannelOperationDiagnosticStorage(context.Context) ([]map[string]string, error) {
	return p.operations, nil
}
func (p *channelSettlementDiagnosticControl) ReadChannelAttemptDiagnosticStorage(context.Context, string) ([]map[string]string, error) {
	p.attemptReads++
	return []map[string]string{{"attempt_id": "known-attempt"}}, nil
}

func TestChannelSettlementDiagnosticPreservesBoundsIdentityAndRedaction(t *testing.T) {
	probe := &channelSettlementDiagnosticControl{}
	for index := 0; index < 1025; index++ {
		id := "exact"
		if index >= 25 {
			id = "foreign"
		}
		raw, err := json.Marshal(map[string]string{"delivery_id": id, "render_id": "render", "secret": "must-not-appear", "provider_reference": "must-not-appear"})
		if err != nil {
			t.Fatal(err)
		}
		probe.operations = append(probe.operations, map[string]string{"operation_id": fmt.Sprint(index), "effect_kind": "channel", "state": "launched", "authority_evidence": string(raw)})
	}
	evidence, err := readSelectedChannelDeliverySettlementDiagnostic(context.Background(), probe, "exact")
	if err != nil {
		t.Fatal(err)
	}
	operations := evidence["operations"].([]map[string]any)
	if evidence["operation_scan_truncated"] != true || evidence["matching_operations_truncated"] != true || len(operations) != 20 || probe.attemptReads != 20 {
		t.Fatalf("bounded diagnostic changed: operations=%d reads=%d evidence=%v", len(operations), probe.attemptReads, evidence)
	}
	raw, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "must-not-appear") || strings.Contains(string(raw), "foreign") {
		t.Fatalf("private/foreign evidence escaped redaction: %s", raw)
	}
	probe.operations = []map[string]string{{"operation_id": "broken", "authority_evidence": "{"}}
	if _, err := readSelectedChannelDeliverySettlementDiagnostic(context.Background(), probe, "exact"); err == nil {
		t.Fatal("corrupt diagnostic authority was accepted")
	}
}

func requireHistoricalObjectReceipt(t *testing.T, h *channelOnboardingE2EHarness, card, want string) {
	t.Helper()
	raw, err := channelAnchorStorageObservation(t, h).ReadFirstSentSourceReceiptStorage(context.Background(), card)
	if err != nil {
		t.Fatal(err)
	}
	var receipt struct {
		Reference struct {
			ID string `json:"id"`
		} `json:"delivery_reference"`
	}
	if err := json.Unmarshal([]byte(raw), &receipt); err != nil || receipt.Reference.ID != want {
		t.Fatalf("historical receipt was replaced by current evidence: got=%s want=%s err=%v", raw, want, err)
	}
}

func TestChannelSettlementStorageReaderRetainsAbsenceCancellationAndLifetimeBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			var reader storetest.ChannelSettlementObservation
			missing := uuid.NewString()
			t.Run("harness", func(t *testing.T) {
				h := newChannelOnboardingE2EHarness(t, backend, true)
				h.start(t)
				defer h.stop(t)
				reader = channelSettlementStorageObservation(t, h)
				if raw, err := reader.ReadLatestCardSentReceiptStorage(context.Background(), missing); raw != "" || err != sql.ErrNoRows {
					t.Fatalf("exact polling absence lost: %q %v", raw, err)
				}
				if _, err := reader.ReadAcknowledgedCardRenderStorage(context.Background(), missing); err != sql.ErrNoRows {
					t.Fatalf("render absence changed: %v", err)
				}
				if _, err := reader.ReadChannelDraftCardPhysicalStorage(context.Background(), missing); err != sql.ErrNoRows {
					t.Fatalf("draft absence changed: %v", err)
				}
				if _, err := reader.ReadIntentSentResponseStorage(context.Background(), render.IntentKind("foreign"), missing); err == nil || errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("invalid kind hidden as absence: %v", err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := reader.ReadLatestCardSentReceiptStorage(ctx, missing); !errors.Is(err, context.Canceled) || errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("cancellation hidden as absence: %v", err)
				}
			})
			if _, err := reader.ReadLatestCardSentReceiptStorage(context.Background(), missing); err == nil || errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("closed owner accepted/hidden as absence: %v", err)
			}
		})
	}
}
