package store

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	sqliteCastPattern              = regexp.MustCompile(`(?i)::[a-z_][a-z0-9_]*(?:\[\])?`)
	sqliteTextArrayDefaultPattern  = regexp.MustCompile(`(?i)\bTEXT\[\](\s+NOT\s+NULL\s+DEFAULT\s+)'{}'`)
	sqliteTextArrayPattern         = regexp.MustCompile(`(?i)\bTEXT\[\]\b`)
	sqliteDoublePrecisionPattern   = regexp.MustCompile(`(?i)\bDOUBLE\s+PRECISION\b`)
	sqliteTimestampPattern         = regexp.MustCompile(`(?i)\bTIMESTAMPTZ\b`)
	sqliteJSONBPattern             = regexp.MustCompile(`(?i)\bJSONB\b`)
	sqliteUUIDPattern              = regexp.MustCompile(`(?i)\bUUID\b`)
	sqliteByteaPattern             = regexp.MustCompile(`(?i)\bBYTEA\b`)
	sqliteBooleanPattern           = regexp.MustCompile(`(?i)\bBOOLEAN\b`)
	sqliteBigIntPattern            = regexp.MustCompile(`(?i)\bBIGINT\b`)
	sqliteDefaultNowPattern        = regexp.MustCompile(`(?i)\bDEFAULT\s+NOW\(\)`)
	sqliteDefaultGeneratedUUID     = regexp.MustCompile(`(?i)\s+DEFAULT\s+gen_random_uuid\(\)`)
	sqliteDefaultFalsePattern      = regexp.MustCompile(`(?i)\bDEFAULT\s+FALSE\b`)
	sqliteDefaultTruePattern       = regexp.MustCompile(`(?i)\bDEFAULT\s+TRUE\b`)
	sqliteJSONTextArrowExprPattern = regexp.MustCompile(`\(([a-z_][a-z0-9_]*)->>'([a-z_][a-z0-9_]*)'\)`)
)

func StatementsForSchemaDialect(plan SchemaTableDDL, dialect SchemaDialect) ([]string, error) {
	switch dialect {
	case SchemaDialectPostgres:
		return FlattenSchemaTableDDLs([]SchemaTableDDL{plan}), nil
	case SchemaDialectSQLite:
		return SQLiteStatementsForPlan(plan)
	default:
		return nil, fmt.Errorf("schema dialect %q is unsupported", strings.TrimSpace(string(dialect)))
	}
}

func SQLiteStatementsForPlan(plan SchemaTableDDL) ([]string, error) {
	out := make([]string, 0, len(plan.Statements))
	for _, statement := range plan.Statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		translated, err := sqliteTranslateDDLStatement(statement)
		if err != nil {
			return nil, fmt.Errorf("sqlite ddl for %s table %s: %w", strings.TrimSpace(plan.SchemaKind), strings.TrimSpace(plan.TableName), err)
		}
		out = append(out, translated)
	}
	if len(out) == 0 && len(plan.Statements) > 0 {
		return nil, fmt.Errorf("sqlite ddl for %s table %s produced no executable statements", strings.TrimSpace(plan.SchemaKind), strings.TrimSpace(plan.TableName))
	}
	return out, nil
}

func sqliteTranslateDDLStatement(statement string) (string, error) {
	upper := strings.ToUpper(strings.TrimSpace(statement))
	switch {
	case strings.HasPrefix(upper, "CREATE TABLE "):
	case strings.HasPrefix(upper, "CREATE TABLE IF NOT EXISTS "):
	case strings.HasPrefix(upper, "CREATE INDEX "):
	case strings.HasPrefix(upper, "CREATE INDEX IF NOT EXISTS "):
	case strings.HasPrefix(upper, "CREATE UNIQUE INDEX "):
	case strings.HasPrefix(upper, "CREATE UNIQUE INDEX IF NOT EXISTS "):
	default:
		return "", fmt.Errorf("unsupported schema statement %q", statement)
	}

	translated := statement
	translated = strings.ReplaceAll(translated, "bundle_hash ~ '^bundle-v1:sha256:[0-9a-f]{64}$'", sqliteBundleHashPredicate("bundle_hash"))
	translated = strings.ReplaceAll(translated, "source_route <> '{}'::jsonb", "source_route <> '{}'")
	translated = strings.ReplaceAll(translated, "target_route <> '{}'::jsonb", "target_route <> '{}'")
	translated = sqliteJSONTextArrowExprPattern.ReplaceAllString(translated, "(json_extract($1, '$.$2'))")
	translated = sqliteTextArrayDefaultPattern.ReplaceAllString(translated, "TEXT${1}'[]'")
	translated = sqliteTextArrayPattern.ReplaceAllString(translated, "TEXT")
	translated = sqliteDefaultGeneratedUUID.ReplaceAllString(translated, "")
	translated = sqliteDefaultNowPattern.ReplaceAllString(translated, "DEFAULT CURRENT_TIMESTAMP")
	translated = sqliteDefaultFalsePattern.ReplaceAllString(translated, "DEFAULT 0")
	translated = sqliteDefaultTruePattern.ReplaceAllString(translated, "DEFAULT 1")
	translated = sqliteCastPattern.ReplaceAllString(translated, "")
	translated = sqliteDoublePrecisionPattern.ReplaceAllString(translated, "REAL")
	translated = sqliteTimestampPattern.ReplaceAllString(translated, "TEXT")
	translated = sqliteJSONBPattern.ReplaceAllString(translated, "TEXT")
	translated = sqliteUUIDPattern.ReplaceAllString(translated, "TEXT")
	translated = sqliteByteaPattern.ReplaceAllString(translated, "BLOB")
	translated = sqliteBooleanPattern.ReplaceAllString(translated, "INTEGER")
	translated = sqliteBigIntPattern.ReplaceAllString(translated, "INTEGER")

	if err := sqliteRejectUnsupportedDDL(translated); err != nil {
		return "", err
	}
	return translated, nil
}

func sqliteBundleHashPredicate(column string) string {
	return fmt.Sprintf("(length(%s) = 81 AND substr(%s, 1, 17) = 'bundle-v1:sha256:' AND substr(%s, 18) GLOB '%s')",
		column, column, column, strings.Repeat("[0-9a-f]", 64))
}

func sqliteRejectUnsupportedDDL(statement string) error {
	checks := []struct {
		pattern *regexp.Regexp
		label   string
	}{
		{regexp.MustCompile(`(?i)::[a-z_][a-z0-9_]*(?:\[\])?`), "Postgres cast"},
		{regexp.MustCompile(`(?i)\bJSONB\b`), "JSONB type"},
		{regexp.MustCompile(`(?i)\bTIMESTAMPTZ\b`), "TIMESTAMPTZ type"},
		{regexp.MustCompile(`(?i)\bUUID\b`), "UUID type"},
		{regexp.MustCompile(`(?i)\bBYTEA\b`), "BYTEA type"},
		{regexp.MustCompile(`(?i)\bTEXT\[\]\b`), "Postgres array type"},
		{regexp.MustCompile(`(?i)\bgen_random_uuid\(\)`), "Postgres UUID default"},
		{regexp.MustCompile(`(?i)\bNOW\(\)`), "Postgres NOW() default"},
		{regexp.MustCompile(`(?i)\bCREATE\s+EXTENSION\b`), "Postgres extension"},
		{regexp.MustCompile(`(?i)\bDO\s+\$\$`), "Postgres DO block"},
		{regexp.MustCompile(`(?i)\bFOR\s+UPDATE\b`), "Postgres row locking"},
		{regexp.MustCompile(`(?i)\binformation_schema\b`), "Postgres information_schema"},
		{regexp.MustCompile(`(?i)\bpg_[a-z0-9_]+\b`), "Postgres catalog/function"},
		{regexp.MustCompile(`~\s*'`), "Postgres regex operator"},
	}
	for _, check := range checks {
		if check.pattern.MatchString(statement) {
			return fmt.Errorf("unsupported SQLite schema construct: %s in %q", check.label, strings.TrimSpace(statement))
		}
	}
	return nil
}
