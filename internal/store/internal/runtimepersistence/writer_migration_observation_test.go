package runtimepersistence

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

func TestWriterMigrationObservationMatchesReturnedWrite(t *testing.T) {
	for _, tc := range []struct {
		at, query string
		want      bool
	}{
		{"query_row_returned", "\nINSERT INTO agent_sessions\n(session_id) VALUES ($1) RETURNING session_id", true},
		{"exec", "INSERT INTO agent_sessions (session_id) VALUES ($1)", true},
		{"query", "INSERT INTO agent_sessions (session_id) VALUES ($1) RETURNING session_id", false},
		{"query_row_returned", "SELECT 'insert into agent_sessions (session_id)'", false},
		{"query_row_returned", "INSERT INTO agent_sessions_archive (session_id) VALUES ($1)", false},
		{"query_row_returned", "UPDATE agent_sessions SET session_id=$1 RETURNING session_id", false},
		{"commit_returned", "INSERT INTO agent_sessions (session_id) VALUES ($1)", false},
	} {
		if got := writerMigrationObservedWrite(tc.at, tc.query, "insert into agent_sessions"); got != tc.want {
			t.Errorf("observation %q %q=%t, want %t", tc.at, tc.query, got, tc.want)
		}
	}
}

type writerMigrationTestRows struct {
	driver.Rows
	remaining int
	err       error
}

func (r *writerMigrationTestRows) Next(dest []driver.Value) error {
	if r.err != nil {
		return r.err
	}
	if r.remaining == 0 {
		return io.EOF
	}
	r.remaining--
	dest[0], dest[1] = "stored-session", "stored-run"
	return nil
}

func TestWriterMigrationObservationRequiresSuccessfulRowOnce(t *testing.T) {
	nativeErr, hookErr := errors.New("native row error"), errors.New("row hook error")
	for _, tc := range []struct {
		name      string
		remaining int
		nativeErr error
		hookErr   error
		wantCalls int
	}{
		{"one-row", 1, nil, nil, 1},
		{"multiple-rows", 2, nil, nil, 1},
		{"no-row", 0, nil, nil, 0},
		{"native-error", 1, nativeErr, nil, 0},
		{"hook-error", 2, nil, hookErr, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			const query = "INSERT INTO agent_sessions (session_id) VALUES ($1) RETURNING session_id, run_id"
			probe := &pipelineGracefulProbe{}
			probe.set(func(at, gotQuery string) error {
				calls++
				if at != "query_row_returned" || gotQuery != query {
					t.Fatalf("unexpected observation %q %q", at, gotQuery)
				}
				return tc.hookErr
			})
			rows := &pipelineGracefulRows{Rows: &writerMigrationTestRows{remaining: tc.remaining, err: tc.nativeErr}, probe: probe, query: query}
			dest := make([]driver.Value, 2)
			for i := 0; i <= tc.remaining; i++ {
				err := rows.Next(dest)
				wantErr := tc.nativeErr
				if wantErr == nil {
					if i == tc.remaining {
						wantErr = io.EOF
					} else if i == 0 {
						wantErr = tc.hookErr
					}
				}
				if !errors.Is(err, wantErr) {
					t.Fatalf("Next error=%v, want %v", err, wantErr)
				}
				if tc.nativeErr == nil && i < tc.remaining && (dest[0] != "stored-session" || dest[1] != "stored-run") {
					t.Fatalf("native coordinates changed: %v", dest)
				}
			}
			if calls != tc.wantCalls {
				t.Fatalf("observations=%d, want %d", calls, tc.wantCalls)
			}
		})
	}
}
