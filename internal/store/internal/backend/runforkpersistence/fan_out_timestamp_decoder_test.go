package runforkpersistence

import (
	"testing"
	"time"
)

// Decoder validity is not SQL presence. In particular, an empty string or a
// native zero value must not let the caller erase forbidden non-NULL state.
func TestFanOutTimestampDecoderDriverRepresentations(t *testing.T) {
	want := time.Date(2026, 9, 8, 10, 11, 12, 123456789, time.UTC)
	zoned := want.In(time.FixedZone("fixture", -7*3600))
	for _, cell := range []struct {
		name    string
		raw     any
		want    time.Time
		valid   bool
		wantErr bool
	}{
		{"native_offset", zoned, want, true, false},
		{"text_offset", zoned.Format(time.RFC3339Nano), want, true, false},
		{"bytes_offset", []byte(zoned.Format(time.RFC3339Nano)), want, true, false},
		{"sql_null", nil, time.Time{}, false, false},
		{"empty_text", "", time.Time{}, false, false},
		{"empty_bytes", []byte{}, time.Time{}, false, false},
		{"whitespace", " \t", time.Time{}, false, false},
		{"native_zero", time.Time{}, time.Time{}, false, false},
		{"text_zero", time.Time{}.Format(time.RFC3339Nano), time.Time{}, true, false},
		{"bytes_zero", []byte(time.Time{}.Format(time.RFC3339Nano)), time.Time{}, true, false},
		{"malformed_text", "not-a-timestamp", time.Time{}, false, true},
		{"malformed_bytes", []byte("not-a-timestamp"), time.Time{}, false, true},
		{"unsupported_driver_value", int64(1), time.Time{}, false, true},
	} {
		t.Run(cell.name, func(t *testing.T) {
			got, valid, err := sqliteTimeValue(cell.raw)
			if (err != nil) != cell.wantErr || valid != cell.valid || !got.Equal(cell.want) {
				t.Fatalf("decode %T %#v = %s valid=%v err=%v; want %s valid=%v error=%v", cell.raw, cell.raw, got.Format(time.RFC3339Nano), valid, err, cell.want.Format(time.RFC3339Nano), cell.valid, cell.wantErr)
			}
			if !got.IsZero() && (got.Location() != time.UTC || got.Nanosecond() != 123456789) {
				t.Fatalf("timestamp lost UTC normalization or nanosecond precision: %s", got.Format(time.RFC3339Nano))
			}
		})
	}
}
