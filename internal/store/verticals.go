package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimemanager "empireai/internal/runtime/manager"
)

// GetVerticalInfo is a legacy compatibility read against the pre-spec
// verticals table. New platform state must use workflow_instances instead.
func (s *PostgresStore) GetVerticalInfo(ctx context.Context, verticalID string) (runtimemanager.VerticalInfo, bool, error) {
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return runtimemanager.VerticalInfo{}, false, fmt.Errorf("vertical_id is required")
	}
	var v runtimemanager.VerticalInfo
	v.ID = verticalID
	if err := s.DB.QueryRowContext(ctx, `
		SELECT name, COALESCE(slug,''), COALESCE(geography,''), COALESCE(stage,'')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&v.Name, &v.Slug, &v.Geography, &v.Stage); err != nil {
		if err == sql.ErrNoRows {
			return runtimemanager.VerticalInfo{}, false, nil
		}
		return runtimemanager.VerticalInfo{}, false, fmt.Errorf("get vertical info: %w", err)
	}
	return v, true, nil
}
