package decisioncard

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

// Cursor is a creation-order position, not a decision-card change sequence.
type Cursor struct {
	CreatedAt time.Time `json:"created_at"`
	CardID    string    `json:"card_id"`
}

func DecodeCursor(raw string) (Cursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Cursor{}, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	value, err := canonicaljson.Decode(decoded)
	if err != nil || value.Kind() != semanticvalue.KindObject || value.Len() != 2 {
		return Cursor{}, ErrInvalidCursor
	}
	for _, member := range value.Members() {
		if member.Name != "created_at" && member.Name != "card_id" || member.Value.Kind() != semanticvalue.KindString {
			return Cursor{}, ErrInvalidCursor
		}
	}
	var cursor Cursor
	if err := canonicaljson.ValueInto(value, &cursor); err != nil || cursor.CreatedAt.IsZero() || strings.TrimSpace(cursor.CardID) == "" {
		return Cursor{}, ErrInvalidCursor
	}
	return cursor, nil
}

func EncodeCursor(createdAt time.Time, cardID string) string {
	raw, _ := json.Marshal(Cursor{CreatedAt: createdAt.UTC(), CardID: strings.TrimSpace(cardID)})
	return base64.RawURLEncoding.EncodeToString(raw)
}
