package deliverylifecycle_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestCommittedRetryDecisionRequiresExactSettlementAndLaterDue(t *testing.T) {
	settled := time.Date(2026, 10, 15, 12, 0, 0, 123456000, time.UTC)
	valid := deliverylifecycle.CommittedRetryDecision{ClaimVersion: 3, SettledAt: settled, EligibleAt: settled.Add(17 * time.Millisecond)}
	for _, test := range []struct {
		name string
		edit func(*deliverylifecycle.CommittedRetryDecision)
	}{
		{"valid", func(*deliverylifecycle.CommittedRetryDecision) {}},
		{"no_generation", func(d *deliverylifecycle.CommittedRetryDecision) { d.ClaimVersion = 0 }},
		{"negative_generation", func(d *deliverylifecycle.CommittedRetryDecision) { d.ClaimVersion = -1 }},
		{"no_settlement", func(d *deliverylifecycle.CommittedRetryDecision) { d.SettledAt = time.Time{} }},
		{"no_due", func(d *deliverylifecycle.CommittedRetryDecision) { d.EligibleAt = time.Time{} }},
		{"equal_due", func(d *deliverylifecycle.CommittedRetryDecision) { d.EligibleAt = settled }},
		{"earlier_due", func(d *deliverylifecycle.CommittedRetryDecision) { d.EligibleAt = settled.Add(-time.Microsecond) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			decision := valid
			test.edit(&decision)
			err := decision.Validate()
			if test.name == "valid" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, deliverylifecycle.ErrConflict) {
				t.Fatalf("invalid retry evidence admitted: %+v err=%v", decision, err)
			}
		})
	}
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	var decoded deliverylifecycle.CommittedRetryDecision
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != valid {
		t.Fatalf("committed retry coordinate changed: got=%+v want=%+v err=%v", decoded, valid, err)
	}
}
