package mailbox

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"
)

func TestNoticeCursorCodec(t *testing.T) {
	now := time.Date(2026, 9, 8, 10, 11, 12, 123, time.FixedZone("offset", 3600))
	cursor, err := DecodeV1Cursor(EncodeV1Cursor(now, "opaque/id"))
	if err != nil || !cursor.CreatedAt.Equal(now) || cursor.MailboxID != "opaque/id" {
		t.Fatalf("round trip = %+v, %v", cursor, err)
	}
	if _, err := DecodeV1Cursor(""); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		`null`, `[]`, `{}`, `{"created_at":"2026-09-08T10:11:12Z"}`,
		`{"created_at":"2026-09-08T10:11:12Z","mailbox_id":null}`,
		`{"created_at":"2026-09-08T10:11:12Z","mailbox_id":" "}`,
		`{"created_at":"2026-09-08T10:11:12Z","card_id":"card"}`,
		`{"created_at":"2026-09-08T10:11:12Z","mailbox_id":"notice","card_id":"card"}`,
		`{"created_at":"2026-09-08T10:11:12Z","mailbox_id":"notice","extra":1}`,
		`{"created_at":"2026-09-08T10:11:12Z","mailbox_id":"notice","mailbox_id":"other"}`,
		`{"created_at":null,"mailbox_id":"notice"}`, `{"created_at":"bad","mailbox_id":"notice"}`,
		`{"created_at":"2026-09-08T10:11:12Z","mailbox_id":1}`, `{`,
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := DecodeV1Cursor(base64.RawURLEncoding.EncodeToString([]byte(raw))); !errors.Is(err, ErrV1InvalidCursor) {
				t.Fatalf("admitted %s: %v", raw, err)
			}
		})
	}
	if _, err := DecodeV1Cursor("!"); !errors.Is(err, ErrV1InvalidCursor) {
		t.Fatal(err)
	}
}
