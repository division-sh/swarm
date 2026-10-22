package replycontextstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/replycontext"
)

// InsertConstraintFixtureForTest exercises physical row constraints only. It
// cannot grant run admission, claim a reply, or declare a history revision.
func InsertConstraintFixtureForTest(ctx context.Context, tx *sql.Tx, postgres bool, record replycontext.Record) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("reply constraint fixture requires the selected transaction")
	}
	origin, err := record.EncodeOrigin()
	if err != nil {
		return false, err
	}
	result, err := insertReplyContextRow(ctx, tx, record, origin, postgres)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}
