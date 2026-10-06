package timerobligation

import (
	"testing"
	"time"

	runtimeobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
)

func TestParkedTimerObligationRequiresDeclaredClock(t *testing.T) {
	at := time.Now().UTC()
	if err := validateRow(runtimeobligation.FamilyScheduledTask, "run", "parked", "instance", at); err != nil {
		t.Fatal(err)
	}
	for _, family := range runtimeobligation.AllFamilies() {
		for _, owner := range []string{"agent", "system", "instance"} {
			if family == runtimeobligation.FamilyScheduledTask && owner == "instance" {
				continue
			}
			if err := validateRow(family, "run", "parked", owner, at); err == nil {
				t.Fatalf("non-clock acquired parking: %s/%s", family, owner)
			}
		}
	}
	if err := validateRow(runtimeobligation.FamilyScheduledTask, "", "parked", "instance", at); err == nil {
		t.Fatal("global clock acquired parking")
	}
}
