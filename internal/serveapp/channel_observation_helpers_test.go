package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func openChannelFixtureObservation(t *testing.T, backend, location string) storetest.ChannelObservation {
	t.Helper()
	switch backend {
	case "default_sqlite":
		backend = "sqlite"
	case "explicit_postgres":
		backend = "postgres"
	}
	reader, err := storetest.OpenChannelObservation(backend, location)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
	})
	return reader
}

func observedChannelLanding(t *testing.T, h *channelOnboardingE2EHarness, flow string) runlifecycle.LandingRun {
	t.Helper()
	reader, ok := servedTestProcessRuntime(t, h.process).Bus.Store().(runlifecycle.LandingRunReader)
	if !ok {
		t.Fatal("channel fixture lacks its existing landing-run reader")
	}
	run, found, err := reader.LoadLandingRun(t.Context(), runlifecycle.LiveRunScope{FlowPath: flow})
	if err != nil || !found || run.RunID == "" {
		t.Fatalf("channel fixture lost exact landing run %s: found=%t err=%v", flow, found, err)
	}
	return run
}

func waitObservedChannelReceipt(t *testing.T, reader storetest.ChannelObservation, card string) int {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		raw, err := reader.HistoricalReceipt(t.Context(), card)
		if err == nil {
			var reference struct {
				Delivery struct {
					ID int `json:"id"`
				} `json:"delivery_reference"`
			}
			if err := json.Unmarshal([]byte(raw), &reference); err != nil || reference.Delivery.ID < 1 {
				t.Fatalf("receipt has no real provider message: %s %v", raw, err)
			}
			return reference.Delivery.ID
		}
		if err != sql.ErrNoRows || time.Now().After(deadline) {
			t.Fatalf("real card was not delivered: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitObservedChannelDecision(t *testing.T, h *channelOnboardingE2EHarness, card string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		state := readRegisteredChannelCard(t, h, card)
		if state.Status == decisioncard.StatusDecided && state.Verdict == "approve" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("authenticated channel action did not decide actual card: %s/%s", state.Status, state.Verdict)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitObservedChannelRejectedCallback(t *testing.T, reader storetest.ChannelObservation, token string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		counts, err := reader.ControlCounts(t.Context(), token)
		if err != nil {
			t.Fatal(err)
		}
		if counts.Nonmutating > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("stale callback has no durable non-mutation disposition")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitObservedChannelSendsSettled(t *testing.T, reader storetest.ChannelObservation) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		count, err := reader.CountUnsettledResponsibilities(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("initial public deliveries never settled: responsibilities=%d", count)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitObservedChannelEntryCount(t *testing.T, reader storetest.ChannelObservation, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		count, err := reader.CountEntryIntents(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if count == want {
			return
		}
		if count > want || time.Now().After(deadline) {
			t.Fatalf("native intent disposition=entry count=%d want=%d", count, want)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitObservedChannelCardMessage(t *testing.T, h *channelOnboardingE2EHarness, observer storetest.ChannelObservation, flow string) int {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	run := observedChannelLanding(t, h, flow)
	instance := flow
	if flow == "." {
		instance = run.RunID
	}
	cards := servedTestProcessRuntime(t, h.process).Bus.Store().(interface {
		ListDecisionCards(context.Context, decisioncard.ListOptions) ([]decisioncard.ListItem, string, error)
		InspectSnapshot(context.Context, func(context.Context) error) error
	})
	cardDeadline := time.Now().Add(20 * time.Second)
	for {
		var match string
		err := cards.InspectSnapshot(t.Context(), func(ctx context.Context) error {
			cursor := ""
			for {
				items, next, err := cards.ListDecisionCards(ctx, decisioncard.ListOptions{RunID: run.RunID, AnchorKind: string(decisioncard.AnchorKindStageGate), Limit: 200, Cursor: cursor})
				if err != nil {
					return err
				}
				for _, item := range items {
					if item.Scope.FlowInstance == instance {
						if match != "" {
							t.Fatal("multiple standing cards for exact run/flow")
						}
						match = item.CardID
					}
				}
				if next == "" {
					return nil
				}
				if next == cursor {
					t.Fatal("standing card cursor did not advance")
				}
				cursor = next
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if match != "" {
			message := waitObservedChannelReceipt(t, observer, match)
			if time.Now().After(deadline) {
				t.Fatalf("exact standing card %s/%s exceeded its delivery deadline", run.RunID, instance)
			}
			return message
		}
		if time.Now().After(cardDeadline) {
			t.Fatal("real standing producer did not commit its exact card")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
