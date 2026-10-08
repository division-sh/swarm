package mutationprotocol

import (
	"strings"

	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

// physicalRunKey changes only bookkeeping identity, never payload or SQL spelling.
// An unrecognized key remains exact but cannot mint an active-run admission.
func physicalRunKey(dialect privateactivity.Dialect, runID string) (string, bool) {
	runID = strings.TrimSpace(runID)
	switch dialect {
	case privateactivity.DialectSQLite:
		return runID, runID != ""
	case privateactivity.DialectPostgres:
	default:
		return runID, false
	}
	body := runID
	if strings.HasPrefix(body, "{") && strings.HasSuffix(body, "}") {
		body = body[1 : len(body)-1]
	}
	// PG accepts an optional hyphen after each four hex digits, with optional
	// braces. uuid.Parse alone both misses PG forms and accepts non-PG forms.
	var hex [32]byte
	for i := 0; i < len(hex); i += 4 {
		if len(body) < 4 {
			return runID, false
		}
		copy(hex[i:i+4], body[:4])
		body = body[4:]
		if i < len(hex)-4 {
			body = strings.TrimPrefix(body, "-")
		}
	}
	if body != "" {
		return runID, false
	}
	id, err := uuid.ParseBytes(hex[:])
	if err != nil {
		return runID, false
	}
	return id.String(), true
}
