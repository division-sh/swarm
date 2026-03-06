package runtime

import (
	"regexp"
	"strings"

	runtimetools "empireai/internal/runtime/tools"
)

// MigrationClassification captures whether a SQL migration can be auto-executed.
// Safe means additive-only operations; any destructive or unknown statement
// requires explicit human approval.
type MigrationClassification struct {
	Safe             bool
	DestructiveOps   []string
	RequiresApproval bool
}

var (
	sqlLineCommentPattern   = regexp.MustCompile(`(?m)--.*$`)
	sqlBlockCommentPattern  = regexp.MustCompile(`(?s)/\*.*?\*/`)
	sqlWhitespacePattern    = regexp.MustCompile(`\s+`)
	sqlAdditiveStmtPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^create\s+table\b`),
		regexp.MustCompile(`(?i)^create\s+(unique\s+)?index\b`),
		regexp.MustCompile(`(?i)^create\s+type\b`),
		regexp.MustCompile(`(?i)^alter\s+table\b.*\badd\s+column\b`),
		regexp.MustCompile(`(?i)^insert\s+into\b`),
	}
	sqlDestructivePatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\bdrop\s+table\b`),
		regexp.MustCompile(`(?i)\bdrop\s+column\b`),
		regexp.MustCompile(`(?i)\bdrop\s+index\b`),
		regexp.MustCompile(`(?i)\bdrop\s+type\b`),
		regexp.MustCompile(`(?i)\btruncate\b`),
		regexp.MustCompile(`(?i)\balter\s+table\b.*\balter\s+column\b.*\btype\b`),
		regexp.MustCompile(`(?i)\bdelete\s+from\b`),
		regexp.MustCompile(`(?i)\balter\s+table\b.*\bdrop\s+constraint\b`),
	}
)

// ClassifyMigration classifies migration SQL as additive-only vs requiring review.
// Conservative behavior is intentional: unknown statements require approval.
func ClassifyMigration(sqlText string) MigrationClassification {
	stmts := splitSQLStatements(sqlText)
	destructive := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		if stmt == "" {
			continue
		}
		if op, ok := classifyDestructiveStatement(stmt); ok {
			destructive = append(destructive, op)
			continue
		}
		if isAdditiveStatement(stmt) {
			continue
		}
		destructive = append(destructive, "UNCLASSIFIED "+compactSQL(stmt))
	}
	destructive = runtimetools.UniqueNonEmpty(destructive)
	return MigrationClassification{
		Safe:             len(destructive) == 0,
		DestructiveOps:   destructive,
		RequiresApproval: len(destructive) > 0,
	}
}

func classifyDestructiveStatement(stmt string) (string, bool) {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return "", false
	}
	for _, pattern := range sqlDestructivePatterns {
		if pattern.MatchString(stmt) {
			return compactSQL(stmt), true
		}
	}
	return "", false
}

func isAdditiveStatement(stmt string) bool {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return true
	}
	for _, pattern := range sqlAdditiveStmtPatterns {
		if pattern.MatchString(stmt) {
			return true
		}
	}
	return false
}

func splitSQLStatements(sqlText string) []string {
	sqlText = sqlLineCommentPattern.ReplaceAllString(sqlText, "")
	sqlText = sqlBlockCommentPattern.ReplaceAllString(sqlText, "")
	parts := strings.Split(sqlText, ";")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func compactSQL(stmt string) string {
	stmt = strings.TrimSpace(stmt)
	stmt = sqlWhitespacePattern.ReplaceAllString(stmt, " ")
	return strings.TrimSpace(stmt)
}
