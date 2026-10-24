package mutationlog

import (
	"reflect"
	"testing"
)

func Test2293FanOutSourceEvidenceDoesNotChangeEntityProjection(t *testing.T) {
	for _, source := range []any{[]any{"original"}, map[string]any{"original": int64(1)}, []any{}, map[string]any{}} {
		for _, final := range []any{[]any{"replacement"}, nil} {
			write := ProjectionMutation{Domain: DomainAuthoredField, Path: "items", NewValue: final}
			observation := ProjectionMutation{Domain: DomainFanOutSource, Path: "items", NewValue: source}
			want, err := ReconstructEntityStateProjection([]ProjectionMutation{write})
			if err != nil {
				t.Fatal(err)
			}
			for _, records := range [][]ProjectionMutation{{observation, write}, {write, observation}, {observation, write, observation}} {
				got, err := ReconstructEntityStateProjection(records)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("source order changed state: got=%+v want=%+v err=%v", got, want, err)
				}
			}
		}
	}
}

func Test2293FanOutSourceClassificationRefusesInvalidEvidence(t *testing.T) {
	for _, record := range []ProjectionMutation{
		{Domain: DomainFanOutSource, NewValue: []any{}},
		{Domain: DomainFanOutSource, Path: "items"},
		{Domain: DomainFanOutSource, Path: "items", NewValue: "not a collection"},
		{Domain: DomainFanOutSource, Path: "items", NewValue: ([]any)(nil)},
		{Domain: DomainFanOutSource, Path: "items", NewValue: (map[string]any)(nil)},
		{Domain: "unknown_source", Path: "items", NewValue: []any{}},
	} {
		if _, err := record.ProjectsEntityState(); err == nil {
			t.Fatalf("invalid source classification accepted: %+v", record)
		}
		if _, err := ReconstructEntityStateProjection([]ProjectionMutation{record}); err == nil {
			t.Fatalf("invalid history accepted: %+v", record)
		}
	}
}
