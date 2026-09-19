package runtimepersistence

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

func TestCompletionOutcomeObservationMatchesOnlyAgentTurnInsert(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  bool
	}{
		{"\nINSERT INTO agent_turns (turn_id) VALUES ($1) RETURNING turn_id, run_id", true},
		{"insert into agent_turns\n(turn_id) values (?)", true},
		{"SELECT 'INSERT INTO agent_turns (turn_id)'", false},
		{"SELECT turn_id FROM agent_turns", false},
		{"INSERT INTO agent_turns_archive (turn_id) VALUES ($1) RETURNING turn_id", false},
		{"UPDATE agent_turns SET turn_id=$1 RETURNING turn_id", false},
		{"INSERT INTO agent_turn_outputs (turn_id) VALUES ($1)", false},
	} {
		if got := completionOutcomeAgentTurnInsert(tc.query); got != tc.want {
			t.Errorf("match %q=%t, want %t", tc.query, got, tc.want)
		}
	}
}

type completionOutcomeTestRows struct {
	driver.Rows
	remaining int
	err       error
}

func (r *completionOutcomeTestRows) Next(dest []driver.Value) error {
	if r.err != nil {
		return r.err
	}
	if r.remaining == 0 {
		return io.EOF
	}
	r.remaining--
	dest[0], dest[1] = "stored-turn", "stored-run"
	return nil
}

func TestCompletionOutcomeObservationRequiresSuccessfulRowOnce(t *testing.T) {
	queryErr := errors.New("native query failure")
	for _, tc := range []struct {
		name      string
		remaining int
		err       error
		enabled   bool
		phase     string
		writes    int32
		cancels   int
	}{
		{"successful-row", 1, nil, true, "callback_cancel", 1, 1},
		{"multiple-row-observation", 2, nil, true, "callback_cancel", 1, 1},
		{"no-row", 0, nil, true, "callback_cancel", 0, 0},
		{"native-error", 1, queryErr, true, "callback_cancel", 0, 0},
		{"disabled-setup", 1, nil, false, "callback_cancel", 0, 0},
		{"healthy-no-cancel", 1, nil, true, "healthy", 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cancels := 0
			control := &completionOutcomeConnector{phase: tc.phase, cancel: func() { cancels++ }}
			control.enabled.Store(tc.enabled)
			rows := &completionOutcomeRows{Rows: &completionOutcomeTestRows{remaining: tc.remaining, err: tc.err}, control: control}
			dest := make([]driver.Value, 2)
			for i := 0; i <= tc.remaining; i++ {
				err := rows.Next(dest)
				wantErr := tc.err
				if wantErr == nil && i == tc.remaining {
					wantErr = io.EOF
				}
				if !errors.Is(err, wantErr) {
					t.Fatalf("Next error=%v, want %v", err, wantErr)
				}
				if err == nil && (dest[0] != "stored-turn" || dest[1] != "stored-run") {
					t.Fatalf("returned coordinates changed: %v", dest)
				}
			}
			if got := control.writes.Load(); got != tc.writes || cancels != tc.cancels {
				t.Fatalf("writes=%d cancels=%d, want %d/%d", got, cancels, tc.writes, tc.cancels)
			}
		})
	}
}
