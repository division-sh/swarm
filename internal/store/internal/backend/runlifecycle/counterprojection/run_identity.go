package counterprojection

import (
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/google/uuid"
)

// PhysicalRunKey changes bookkeeping identity, never payload or SQL spelling.
func PhysicalRunKey(dialect authoractivity.Dialect, runID string) (string, bool) {
	runID = strings.TrimSpace(runID)
	switch dialect {
	case authoractivity.DialectSQLite:
		return runID, runID != ""
	case authoractivity.DialectPostgres:
	default:
		return runID, false
	}
	body := runID
	if strings.HasPrefix(body, "{") && strings.HasSuffix(body, "}") {
		body = body[1 : len(body)-1]
	}
	// PostgreSQL permits an optional hyphen after each four hex digits.
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
