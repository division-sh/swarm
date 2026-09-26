package fanoutobligation

import (
	"testing"

	"github.com/google/uuid"
)

func TestFanOutSummaryOriginIsDisjoint(t *testing.T) {
	delivery := uuid.NewString()
	feed := uuid.NewString()
	for _, tc := range []struct {
		name               string
		delivery, feed     string
		flow, family, path string
		valid              bool
	}{
		{name: "handler", delivery: delivery, flow: ".", family: "fan_out", path: "node.scatter", valid: true},
		{name: "deployment", feed: feed, valid: true},
		{name: "mixed identity", delivery: delivery, feed: feed},
		{name: "mixed grammar", feed: feed, family: "fan_out"},
		{name: "missing identity"},
		{name: "invalid feed", feed: "not-a-uuid"},
		{name: "missing handler declaration", delivery: delivery, family: "fan_out"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateFanOutSummaryOrigin(tc.delivery, tc.feed, tc.flow, tc.family, tc.path)
			if (err == nil) != tc.valid {
				t.Fatalf("summary origin validation: err=%v want valid=%v", err, tc.valid)
			}
		})
	}
}
