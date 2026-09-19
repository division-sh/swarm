package runtimepersistence

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

func TestLifecycleDiagnosticInsertBarrierMatching(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  bool
	}{
		{"\n INSERT INTO events (event_id) VALUES ($1)\n RETURNING event_id", true},
		{"SELECT 'insert into events ( returning event_id'", false},
		{"SELECT event_id FROM events", false},
		{"INSERT INTO events_archive (event_id) VALUES ($1) RETURNING event_id", false},
		{"UPDATE events SET event_id = $1 RETURNING event_id", false},
		{"INSERT INTO events (event_id) VALUES ($1)", false},
	} {
		if got := diagnosticEventInsertReturning(tc.query); got != tc.want {
			t.Errorf("match %q = %t, want %t", tc.query, got, tc.want)
		}
	}
}

type diagnosticBarrierTestRows struct {
	driver.Rows
	remaining int
	err       error
}

func (r *diagnosticBarrierTestRows) Next(dest []driver.Value) error {
	if r.err != nil {
		return r.err
	}
	if r.remaining == 0 {
		return io.EOF
	}
	r.remaining--
	dest[0] = "stored-event-id"
	return nil
}

func TestLifecycleDiagnosticInsertBarrierReturnedRow(t *testing.T) {
	queryErr := errors.New("query failed")
	hookErr := errors.New("barrier canceled")
	for _, tc := range []struct {
		name      string
		remaining int
		queryErr  error
		hookErr   error
		wantErr   error
		wantHits  int
	}{
		{"inserted", 2, nil, nil, nil, 1},
		{"conflict-no-row", 0, nil, nil, io.EOF, 0},
		{"query-error", 0, queryErr, nil, queryErr, 0},
		{"barrier-error", 1, nil, hookErr, hookErr, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hits := 0
			rows := &diagnosticInsertRows{
				Rows:        &diagnosticBarrierTestRows{remaining: tc.remaining, err: tc.queryErr},
				afterInsert: func() error { hits++; return tc.hookErr },
			}
			dest := make([]driver.Value, 1)
			if err := rows.Next(dest); !errors.Is(err, tc.wantErr) {
				t.Fatalf("Next error = %v, want %v", err, tc.wantErr)
			}
			if tc.name == "inserted" {
				if dest[0] != "stored-event-id" {
					t.Fatalf("returned value changed: %v", dest[0])
				}
				if err := rows.Next(dest); err != nil {
					t.Fatal(err)
				}
			}
			if hits != tc.wantHits {
				t.Fatalf("barrier hits = %d, want %d", hits, tc.wantHits)
			}
		})
	}
}
