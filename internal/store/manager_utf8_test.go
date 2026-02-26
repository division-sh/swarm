package store

import (
	"testing"
	"unicode/utf8"
)

func TestRedactText_NormalizesInvalidUTF8(t *testing.T) {
	raw := string([]byte{0xf0, 0x9f, 0x2e, 0x2e})
	out := redactText(raw)
	if !utf8.ValidString(out) {
		t.Fatalf("expected valid UTF-8 output, got bytes=%v", []byte(out))
	}
}
