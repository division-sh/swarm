package pipelinepersistence

import "time"

type HistoricalReplayScopeStorage struct {
	RunID, Scope         string
	CreatedAt, UpdatedAt time.Time
}
