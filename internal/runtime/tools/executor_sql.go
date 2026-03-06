package tools

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"empireai/internal/models"
)

func (e *Executor) execSQLExecute(ctx context.Context, actor models.AgentConfig, input any) (any, error) {
	e.mu.RLock()
	db := e.sqlDB
	e.mu.RUnlock()
	if db == nil {
		return nil, errors.New("sql db is not configured")
	}
	if strings.TrimSpace(actor.VerticalID) == "" {
		return nil, errors.New("sql_execute requires actor vertical_id")
	}
	var in struct {
		Query string `json:"query"`
	}
	if err := decodeToolInput(input, &in); err != nil {
		return nil, err
	}
	query := strings.TrimSpace(in.Query)
	if query == "" {
		return nil, errors.New("query is required")
	}
	normalizedQuery, err := sanitizeSQLReadQuery(query)
	if err != nil {
		return nil, err
	}

	schema := actor.VerticalID
	if slug, err := lookupVerticalSlug(ctx, db, actor.VerticalID); err == nil && strings.TrimSpace(slug) != "" {
		schema = slug + "_schema"
	}
	schema = sanitizeIdentifier(schema)
	if schema == "" {
		return nil, errors.New("failed to derive sql schema for actor")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin sql_execute tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, "SET LOCAL search_path = "+quoteIdent(schema)); err != nil {
		return nil, fmt.Errorf("set search_path: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
		return nil, fmt.Errorf("set read only: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '15s'"); err != nil {
		return nil, fmt.Errorf("set statement timeout: %w", err)
	}

	rows, err := tx.QueryContext(ctx, normalizedQuery)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read columns: %w", err)
	}
	out := make([]map[string]any, 0, 64)
	for rows.Next() {
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		rowObj := make(map[string]any, len(cols))
		for i, c := range cols {
			rowObj[c] = normalizeSQLValue(values[i])
		}
		out = append(out, rowObj)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit read tx: %w", err)
	}
	return map[string]any{
		"rows":      out,
		"schema":    schema,
		"query":     normalizedQuery,
		"read_only": true,
	}, nil
}

func lookupVerticalSlug(ctx context.Context, db *sql.DB, verticalID string) (string, error) {
	var slug string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(NULLIF(slug, ''), '')
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&slug); err != nil {
		return "", err
	}
	return strings.TrimSpace(slug), nil
}

func isSelectQuery(query string) bool {
	q := strings.ToLower(strings.TrimSpace(query))
	return strings.HasPrefix(q, "select ") || strings.HasPrefix(q, "with ")
}

const (
	maxSQLQueryLength = 8000
	maxSQLResultRows  = 200
)

var (
	sqlForbiddenTokenPattern        = regexp.MustCompile(`(?i)\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|set|reset|call|do|copy|vacuum|analyze|comment)\b`)
	sqlCommentPattern               = regexp.MustCompile(`--|/\*|\*/`)
	sqlSchemaQualifiedFromJoinRegex = regexp.MustCompile(`(?is)\b(from|join)\s+((\"[^\"]+\"|[a-z_][a-z0-9_]*)\s*\.)`)
	sqlRestrictedSchemaPattern      = regexp.MustCompile(`(?is)(\"?(pg_catalog|information_schema|public)\"?\s*\.)`)
	sqlLimitPattern                 = regexp.MustCompile(`(?i)\blimit\s+([0-9]+)\b`)
)

func sanitizeSQLReadQuery(query string) (string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return "", errors.New("query is required")
	}
	if len(query) > maxSQLQueryLength {
		return "", fmt.Errorf("query too long (max %d chars)", maxSQLQueryLength)
	}
	if strings.Contains(query, ";") {
		return "", errors.New("multi-statement SQL is not allowed")
	}
	if sqlCommentPattern.MatchString(query) {
		return "", errors.New("SQL comments are not allowed")
	}
	if !isSelectQuery(query) {
		return "", errors.New("only read-only SELECT queries are allowed")
	}
	if sqlForbiddenTokenPattern.MatchString(query) {
		return "", errors.New("query contains non-read-only SQL")
	}
	if sqlRestrictedSchemaPattern.MatchString(query) {
		return "", errors.New("access to system/shared schemas is not allowed")
	}
	if sqlSchemaQualifiedFromJoinRegex.MatchString(query) {
		return "", errors.New("schema-qualified table references are not allowed")
	}
	if matches := sqlLimitPattern.FindStringSubmatch(query); len(matches) == 2 {
		limitVal := strings.TrimSpace(matches[1])
		if limitVal != "" {
			if n, convErr := strconv.Atoi(limitVal); convErr == nil && n > maxSQLResultRows {
				return "", fmt.Errorf("LIMIT exceeds maximum of %d", maxSQLResultRows)
			}
		}
		return query, nil
	}
	return query + fmt.Sprintf(" LIMIT %d", maxSQLResultRows), nil
}

func SanitizeSQLReadQueryForTest(query string) (string, error) {
	return sanitizeSQLReadQuery(query)
}

func sanitizeIdentifier(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func quoteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

func normalizeSQLValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return t
	}
}

func NormalizeSQLValueForTest(v any) any {
	return normalizeSQLValue(v)
}
