package runtimepersistence

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

type groupProofObservationRows struct {
	driver.Rows
	remaining int
	err       error
}

func (r *groupProofObservationRows) Next(dest []driver.Value) error {
	if r.err != nil {
		return r.err
	}
	if r.remaining == 0 {
		return io.EOF
	}
	r.remaining--
	dest[0], dest[1] = "stored-event", "stored-run"
	return nil
}

func TestFanOutGroupFaultObservationRequiresSuccessfulReturnedRowOnce(t *testing.T) {
	nativeErr, injected := errors.New("native rows failed"), errors.New("post-write fault")
	for _, tc := range []struct {
		name               string
		rows               int
		nativeErr, hookErr error
		want               int
	}{
		{"one", 1, nil, nil, 1},
		{"multiple", 2, nil, nil, 1},
		{"empty", 0, nil, nil, 0},
		{"native-failure", 1, nativeErr, nil, 0},
		{"injected-failure", 2, nil, injected, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const query = "native returned statement"
			probe := &groupProofConnector{}
			calls := 0
			probe.set(func(phase, observed string) error {
				calls++
				if phase != "after_query_row" || observed != query {
					t.Fatalf("wrong boundary: %q %q", phase, observed)
				}
				return tc.hookErr
			})
			rows := &groupProofRows{Rows: &groupProofObservationRows{remaining: tc.rows, err: tc.nativeErr}, owner: probe, query: query}
			dest := make([]driver.Value, 2)
			for i := 0; i <= tc.rows; i++ {
				err := rows.Next(dest)
				wantErr := tc.nativeErr
				if wantErr == nil {
					if i == tc.rows {
						wantErr = io.EOF
					} else if i == 0 {
						wantErr = tc.hookErr
					}
				}
				if !errors.Is(err, wantErr) {
					t.Fatalf("Next=%v, want %v", err, wantErr)
				}
				if tc.nativeErr == nil && i < tc.rows && (dest[0] != "stored-event" || dest[1] != "stored-run") {
					t.Fatalf("native stored coordinates changed: %v", dest)
				}
			}
			if calls != tc.want {
				t.Fatalf("calls=%d, want %d", calls, tc.want)
			}
		})
	}
}
