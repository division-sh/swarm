package serveapp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/decisioncardtest"
	"github.com/google/uuid"
)

// Ready releases maintenance; it does not join its first expiry commit.
// #2672/6191459119 and6191468991 keep this observation in the original startup budget.
func waitHumanTaskExpiryCommit(ctx context.Context, original decisioncard.Card, read func(context.Context) (decisioncard.Card, error)) (decisioncard.Card, error) {
	deadline, bounded := ctx.Deadline()
	if !bounded {
		return decisioncard.Card{}, fmt.Errorf("human-task expiry observation requires its existing startup deadline")
	}
	var last decisioncard.Card
	checkBudget := func() error {
		err := ctx.Err()
		if err == nil && !time.Now().Before(deadline) {
			err = context.DeadlineExceeded
		}
		if err != nil {
			return fmt.Errorf("await human-task expiry commit, last status=%q: %w", last.Status, err)
		}
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := checkBudget(); err != nil {
			return last, err
		}
		var err error
		last, err = read(ctx)
		if err != nil {
			return last, fmt.Errorf("read canonical human-task expiry card: %w", err)
		}
		if last.CardID != original.CardID || last.RunID != original.RunID || !reflect.DeepEqual(last.Anchor, original.Anchor) {
			return last, fmt.Errorf("human-task expiry changed card/run identity or anchor: %+v", last)
		}
		if err := checkBudget(); err != nil {
			return last, err
		}
		switch last.Status {
		case decisioncard.StatusExpired:
			return last, nil
		case decisioncard.StatusPending:
		default:
			return last, fmt.Errorf("human-task expiry reached unexpected status=%q", last.Status)
		}
		select {
		case <-ctx.Done():
			return last, fmt.Errorf("await human-task expiry commit, last status=%q: %w", last.Status, ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestHumanTaskExpiryWaitPreservesStartupBudgetAndExactCard(t *testing.T) {
	runID, eventID := uuid.NewString(), uuid.NewString()
	anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
		RequesterAgentID: "reviewer", OperationID: decisioncardtest.HumanOperation(t, t.Name(), eventID), Category: "review",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: runID, EntityID: runID}, Source: eventtest.RootRoutingSource(runID),
	})
	if err != nil {
		t.Fatal(err)
	}
	original := decisioncard.Card{CardID: uuid.NewString(), RunID: runID, Anchor: anchor, Status: decisioncard.StatusPending}
	expired := original
	expired.Status = decisioncard.StatusExpired
	t.Run("pending_is_not_success", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		calls := 0
		got, err := waitHumanTaskExpiryCommit(ctx, original, func(readCtx context.Context) (decisioncard.Card, error) {
			if readCtx != ctx {
				t.Fatal("read lost the original bounded context")
			}
			calls++
			if calls == 1 {
				return original, nil
			}
			return expired, nil
		})
		if err != nil || !reflect.DeepEqual(got, expired) || calls != 2 {
			t.Fatalf("pending admitted as success or expiry changed: calls=%d got=%+v err=%v", calls, got, err)
		}
	})
	t.Run("restart_consumed_budget", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()
		got, err := waitHumanTaskExpiryCommit(ctx, original, func(context.Context) (decisioncard.Card, error) {
			t.Fatal("expired startup budget started another read")
			return expired, nil
		})
		if !errors.Is(err, context.DeadlineExceeded) || got.Status != "" {
			t.Fatalf("consumed startup budget accepted: got=%+v err=%v", got, err)
		}
	})
	t.Run("pending_cancellation_keeps_last_state", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		got, err := waitHumanTaskExpiryCommit(ctx, original, func(context.Context) (decisioncard.Card, error) {
			cancel()
			return original, nil
		})
		if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(got, original) || !strings.Contains(err.Error(), `last status="pending"`) {
			t.Fatalf("pending cancellation accepted or lost last state: got=%+v err=%v", got, err)
		}
	})
	t.Run("pending_deadline_keeps_last_state", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		got, err := waitHumanTaskExpiryCommit(ctx, original, func(readCtx context.Context) (decisioncard.Card, error) {
			<-readCtx.Done()
			return original, nil
		})
		if !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(got, original) || !strings.Contains(err.Error(), `last status="pending"`) {
			t.Fatalf("pending deadline accepted or lost last state: got=%+v err=%v", got, err)
		}
	})
	t.Run("unbounded_observation_refused", func(t *testing.T) {
		_, err := waitHumanTaskExpiryCommit(context.Background(), original, func(context.Context) (decisioncard.Card, error) {
			t.Fatal("unbounded expiry observation started a read")
			return expired, nil
		})
		if err == nil {
			t.Fatal("expiry observation accepted without its original deadline")
		}
	})
	t.Run("read_error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		readErr := errors.New("canonical read failed")
		calls := 0
		_, err := waitHumanTaskExpiryCommit(ctx, original, func(context.Context) (decisioncard.Card, error) {
			calls++
			return decisioncard.Card{}, readErr
		})
		if !errors.Is(err, readErr) || calls != 1 {
			t.Fatalf("read error retried or accepted: calls=%d err=%v", calls, err)
		}
	})
	for _, field := range []string{"card", "run", "anchor", "decided", "superseded"} {
		t.Run(field, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			changed := expired
			switch field {
			case "card":
				changed.CardID = uuid.NewString()
			case "run":
				changed.RunID = uuid.NewString()
			case "anchor":
				changed.Anchor = decisioncard.Anchor{}
			case "decided":
				changed.Status = decisioncard.StatusDecided
			case "superseded":
				changed.Status = decisioncard.StatusSuperseded
			}
			calls := 0
			got, err := waitHumanTaskExpiryCommit(ctx, original, func(context.Context) (decisioncard.Card, error) {
				calls++
				return changed, nil
			})
			if err == nil || calls != 1 || !reflect.DeepEqual(got, changed) {
				t.Fatalf("changed card accepted or retried: field=%s calls=%d got=%+v err=%v", field, calls, got, err)
			}
		})
	}
}
