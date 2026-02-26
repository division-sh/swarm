package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"empireai/internal/runtime"
)

func (s *PostgresStore) GetVerticalInfo(ctx context.Context, verticalID string) (runtime.VerticalInfo, bool, error) {
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return runtime.VerticalInfo{}, false, fmt.Errorf("vertical_id is required")
	}
	var v runtime.VerticalInfo
	v.ID = verticalID
	if err := s.DB.QueryRowContext(ctx, `
		SELECT name, COALESCE(slug,''), COALESCE(geography,''), COALESCE(stage,'')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&v.Name, &v.Slug, &v.Geography, &v.Stage); err != nil {
		if err == sql.ErrNoRows {
			return runtime.VerticalInfo{}, false, nil
		}
		return runtime.VerticalInfo{}, false, fmt.Errorf("get vertical info: %w", err)
	}
	return v, true, nil
}
