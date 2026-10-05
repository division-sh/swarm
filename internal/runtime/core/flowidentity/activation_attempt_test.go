package flowidentity

import "testing"

func TestParseActivationAttemptID(t *testing.T) {
	for _, id := range []string{"1", "2", "9223372036854775807"} {
		if value, err := ParseActivationAttemptID(id); err != nil || value == 0 {
			t.Fatalf("valid ordinal %q: value=%d err=%v", id, value, err)
		}
	}
	for _, id := range []string{"", "0", "01", " 1", "1 ", "+1", "-1", "1.0", "9223372036854775808", "3e74d116-5c31-4a0b-973f-71ec9bca85c2"} {
		if value, err := ParseActivationAttemptID(id); err == nil || value != 0 {
			t.Fatalf("invalid ordinal %q: value=%d err=%v", id, value, err)
		}
	}
}
