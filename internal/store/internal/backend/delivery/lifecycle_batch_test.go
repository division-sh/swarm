package delivery

import (
	"fmt"
	"testing"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestContinuationObservationBatchAdmission(t *testing.T) {
	tooLarge := make([]string, runtimedelivery.MaxContinuationObservationBatch+1)
	for i := range tooLarge {
		tooLarge[i] = fmt.Sprintf("delivery-%d", i)
	}
	for _, tc := range []struct {
		name string
		ids  []string
		want bool
	}{
		{name: "one", ids: []string{"one"}, want: true},
		{name: "maximum", ids: tooLarge[:runtimedelivery.MaxContinuationObservationBatch], want: true},
		{name: "empty"},
		{name: "too_large", ids: tooLarge},
		{name: "empty_identity", ids: []string{"one", ""}},
		{name: "duplicate", ids: []string{"one", "one"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateContinuationObservationBatch(tc.ids); (err == nil) != tc.want {
				t.Fatalf("batch admission err=%v, want valid=%v", err, tc.want)
			}
		})
	}
}
