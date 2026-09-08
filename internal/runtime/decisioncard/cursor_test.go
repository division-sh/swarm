package decisioncard

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"
)

func TestDecisionCardCursorCodec(t *testing.T) {
	now := time.Date(2026, 9, 8, 10, 11, 12, 123, time.FixedZone("offset", 3600))
	cursor, err := DecodeCursor(EncodeCursor(now, "opaque/id"))
	if err != nil || !cursor.CreatedAt.Equal(now) || cursor.CardID != "opaque/id" {
		t.Fatalf("round trip = %+v, %v", cursor, err)
	}
	if _, err := DecodeCursor(""); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`null`, `[]`, `{}`, `{"created_at":"2026-09-08T10:11:12Z"}`,
		`{"created_at":"2026-09-08T10:11:12Z","card_id":null}`,
		`{"created_at":"2026-09-08T10:11:12Z","card_id":" "}`,
		`{"created_at":"2026-09-08T10:11:12Z","mailbox_id":"card"}`,
		`{"created_at":"2026-09-08T10:11:12Z","card_id":"notice","mailbox_id":"card"}`,
		`{"created_at":"2026-09-08T10:11:12Z","card_id":"notice","extra":1}`,
		`{"created_at":"2026-09-08T10:11:12Z","card_id":"notice","card_id":"other"}`,
		`{"created_at":null,"card_id":"notice"}`, `{"created_at":"bad","card_id":"notice"}`,
		`{"created_at":"2026-09-08T10:11:12Z","card_id":1}`, `{`,
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := DecodeCursor(base64.RawURLEncoding.EncodeToString([]byte(raw))); !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("admitted %s: %v", raw, err)
			}
		})
	}
	if _, err := DecodeCursor("!"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatal(err)
	}
}
