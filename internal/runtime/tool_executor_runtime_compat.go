package runtime

import (
	"fmt"
	"strings"

	runtimepipeline "empireai/internal/runtime/pipeline"
)

func (e *RuntimeToolExecutor) validateRuntimeToolInput(name string, input any) error {
	if e == nil || e.inner == nil {
		return nil
	}
	return e.inner.ValidateRuntimeToolInputForTest(name, input)
}

func normalizeScanModeCompat(raw string) string {
	if mode := runtimepipeline.NormalizeScanMode(raw); mode != "" {
		return mode
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "discovery", "scan", "default", "automation", "micro", "automation-micro", "automation_micro":
		return "saas_gap"
	case "saas":
		return "saas_gap"
	case "trend", "trend_scan", "saas-trend":
		return "saas_trend"
	case "local", "local_service", "local-services", "services":
		return "local_services"
	case "corpus_mode", "signal_corpus", "corpus":
		return "corpus"
	case "derived":
		return "derived"
	default:
		return ""
	}
}

func normalizeScanPriorityCompat(raw string) string {
	if priority := runtimepipeline.NormalizeScanPriority(raw); priority != "" {
		return priority
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "med", "medium", "default":
		return "normal"
	case "urgent":
		return "critical"
	default:
		return ""
	}
}

func sanitizeSQLReadQuery(query string) (string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return "", fmt.Errorf("query is required")
	}
	const maxSQLQueryLength = 8000
	const maxSQLResultRows = 200
	if len(query) > maxSQLQueryLength {
		return "", fmt.Errorf("query too long (max %d chars)", maxSQLQueryLength)
	}
	if strings.Contains(query, ";") {
		return "", fmt.Errorf("multi-statement SQL is not allowed")
	}
	if strings.Contains(query, "--") || strings.Contains(query, "/*") || strings.Contains(query, "*/") {
		return "", fmt.Errorf("SQL comments are not allowed")
	}
	q := strings.ToLower(strings.TrimSpace(query))
	if !strings.HasPrefix(q, "select ") && !strings.HasPrefix(q, "with ") {
		return "", fmt.Errorf("only read-only SELECT queries are allowed")
	}
	for _, token := range []string{"insert", "update", "delete", "drop", "alter", "truncate", "create", "grant", "revoke", "set ", "reset", "call", "do ", "copy", "vacuum", "analyze", "comment"} {
		if strings.Contains(q, token) {
			return "", fmt.Errorf("query contains non-read-only SQL")
		}
	}
	for _, bad := range []string{"pg_catalog.", "information_schema.", "public.", `"pg_catalog".`, `"information_schema".`, `"public".`} {
		if strings.Contains(q, bad) {
			return "", fmt.Errorf("access to system/shared schemas is not allowed")
		}
	}
	if strings.Contains(q, ".") && (strings.Contains(q, " from ") || strings.Contains(q, " join ")) {
		return "", fmt.Errorf("schema-qualified table references are not allowed")
	}
	if idx := strings.LastIndex(q, "limit "); idx >= 0 {
		var n int
		if _, err := fmt.Sscanf(q[idx:], "limit %d", &n); err == nil && n > maxSQLResultRows {
			return "", fmt.Errorf("LIMIT exceeds maximum of %d", maxSQLResultRows)
		}
		return query, nil
	}
	return query + fmt.Sprintf(" LIMIT %d", maxSQLResultRows), nil
}
