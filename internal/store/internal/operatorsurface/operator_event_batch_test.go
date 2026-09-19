package operatorsurface

import (
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

func TestOperatorEventBatchNeverConsumesBeyondMatchingLookahead(t *testing.T) {
	for limit := 1; limit <= 1000; limit++ {
		for collected := 0; collected <= limit; collected++ {
			n := operatorEventBatchSize(limit, collected)
			if n < 1 || n > 128 || collected+n > limit+1 {
				t.Fatalf("limit=%d collected=%d admits %d candidates", limit, collected, n)
			}
			if n != min(128, limit+1-collected) {
				t.Fatalf("limit=%d collected=%d truncated lawful batch to %d", limit, collected, n)
			}
		}
	}
}

func TestOperatorEventBatchPreservesTypedReadErrors(t *testing.T) {
	missing := fmt.Errorf("selected candidate: %w", eventrecord.Missing("event"))
	if got := operatorEventBatchError(missing); !errors.Is(got, operatorread.ErrEventNotFound) {
		t.Fatalf("missing candidate changed public error identity: %v", got)
	}
	cause := errors.New("malformed settlement")
	corrupt := eventrecord.Corrupt("event", cause)
	if got := operatorEventBatchError(corrupt); !errors.Is(got, eventrecord.ErrCorrupt) || !errors.Is(got, cause) || errors.Is(got, operatorread.ErrEventNotFound) {
		t.Fatalf("corruption changed error identity: %v", got)
	}
}
