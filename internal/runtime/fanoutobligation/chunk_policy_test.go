package fanoutobligation

import (
	"fmt"
	"testing"
)

func TestFanOutChunkBudgetStartsAtCap(t *testing.T) {
	if InitialChunkSize != 32 {
		t.Fatalf("initial budget = %d, want existing cap 32", InitialChunkSize)
	}
}

func TestFanOutChunkRangeAndRetryBudget(t *testing.T) {
	for _, cardinality := range []int{0, 1, 25, 32, 33, 500} {
		t.Run(fmt.Sprint(cardinality), func(t *testing.T) {
			for cursor := 0; cursor <= cardinality; cursor++ {
				for budget := MinChunkSize; budget <= MaxChunkSize; budget++ {
					intent := Intent{Request: IntentRequest{Cardinality: cardinality}, Cursor: cursor, NextChunkSize: budget}
					end := intent.ChunkEndOrdinal()
					if end-cursor != min(budget, cardinality-cursor) || end > cardinality {
						t.Fatalf("range cardinality=%d cursor=%d budget=%d end=%d", cardinality, cursor, budget, end)
					}
				}
			}
		})
	}
	budget := InitialChunkSize
	for _, want := range []int{16, 8, 4, 2, 1, 1} {
		budget = RetryChunkSize(budget)
		if budget != want {
			t.Fatalf("retry budget = %d, want %d", budget, want)
		}
	}
	if got := RetryChunkSize(5); got != 3 {
		t.Fatalf("odd retry budget = %d, want 3", got)
	}
}
