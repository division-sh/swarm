package runtimepersistence

import (
	"database/sql"

	storeentity "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
)

func scanToolEntityRows(rows *sql.Rows) ([]map[string]any, error) {
	return storeentity.ScanToolEntityRows(rows)
}

func toolDecodeJSONMap(raw any) (map[string]any, error) {
	return storeentity.DecodeJSONMap(raw)
}
