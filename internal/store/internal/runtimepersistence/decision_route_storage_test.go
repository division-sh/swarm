package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestDecisionRouteStoragePreservesExactEventAndOriginalReadBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			cards, runID := decisionCardTestStore(t, backend)
			now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
			card := newDecisionCardTestCard(t, runID, now)
			if err := cards.CreateDecisionCard(ctx, card); err != nil {
				t.Fatal(err)
			}
			eventID := uuid.NewString()
			if _, err := DecisionCardDomainForTest(cards).ApplyDecisionForTest(ctx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "accept", PrincipalID: "operator", ObservedContentHash: card.CardContentHash,
				DecisionEventID: eventID, Now: now.Add(time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
			probe, restore, err := InstallTransactionProbeForTest(cards, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			if status, err := ReadDecisionRouteStatusStorageForTest(ctx, cards, eventID); err != nil || status != "pending" {
				t.Fatalf("exact route=%q, err=%v; want pending", status, err)
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("route read escaped original coordinator: %+v", counts)
			}
			if status, err := ReadDecisionRouteStatusStorageForTest(ctx, cards, uuid.NewString()); !errors.Is(err, sql.ErrNoRows) || status != "" {
				t.Fatalf("foreign event borrowed route evidence: %q, %v", status, err)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if status, err := ReadDecisionRouteStatusStorageForTest(cancelled, cards, eventID); !errors.Is(err, context.Canceled) || status != "" {
				t.Fatalf("cancelled route read returned evidence: %q, %v", status, err)
			}
			if status, err := ReadDecisionRouteStatusStorageForTest(ctx, cards, "invalid"); err == nil || status != "" {
				t.Fatalf("invalid key returned evidence: %q, %v", status, err)
			}
			switch selected := cards.(type) {
			case *PostgresStore:
				if err := selected.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := selected.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if status, err := ReadDecisionRouteStatusStorageForTest(context.Background(), cards, eventID); err == nil || status != "" {
				t.Fatalf("closed owner returned route evidence: %q, %v", status, err)
			}
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, struct{}{}} {
		if status, err := ReadDecisionRouteStatusStorageForTest(context.Background(), invalid, uuid.NewString()); err == nil || status != "" {
			t.Fatalf("missing original owner returned route evidence: %q, %v", status, err)
		}
	}
}
