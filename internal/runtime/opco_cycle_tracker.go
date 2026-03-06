package runtime

import (
	"database/sql"
	"time"

	runtimebus "empireai/internal/runtime/bus"
)

const (
	defaultOpCoCycleLimit      = 5
	defaultOpCoCycleWindow     = 4 * time.Hour
	spendNeededCycleLimit      = 3
	spendNeededCycleWindow     = 1 * time.Hour
	defaultCycleEscalationRole = "opco_cto"
)

type OpCoCycleTracker = runtimebus.OpCoCycleTracker

func NewOpCoCycleTracker(db *sql.DB) *OpCoCycleTracker {
	return runtimebus.NewOpCoCycleTracker(db)
}
