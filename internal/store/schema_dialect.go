package store

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	sqliteCreateTablePattern         = regexp.MustCompile(`(?is)^create\s+table\s+if\s+not\s+exists\s+("?[a-z_][a-z0-9_]*"?)\s*\((.*)\)\s*$`)
	sqliteCreateIndexPattern         = regexp.MustCompile(`(?is)^create\s+(unique\s+)?index\s+if\s+not\s+exists\s+("?[a-z_][a-z0-9_]*"?)\s+on\s+("?[a-z_][a-z0-9_]*"?)\s*\(`)
	sqliteForeignKeyPattern          = regexp.MustCompile(`(?is)^foreign\s+key\s*\(([^)]+)\)\s+references\s+("?[a-z_][a-z0-9_]*"?)\s*\(([^)]+)\)(?:\s+on\s+delete\s+(cascade|restrict|set\s+null|no\s+action))?\s*$`)
	sqliteUnsupportedConstructChecks = []struct {
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
		{regexp.MustCompile(`->>`), "Postgres JSON text extraction"},
	}
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
		translated, err := sqliteRenderSchemaPlanStatement(statement)
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

func sqliteRenderSchemaPlanStatement(statement string) (string, error) {
	upper := strings.ToUpper(strings.TrimSpace(statement))
	switch {
	case strings.HasPrefix(upper, "CREATE TABLE IF NOT EXISTS "):
		return sqliteRenderCreateTable(statement)
	case strings.HasPrefix(upper, "CREATE INDEX IF NOT EXISTS "), strings.HasPrefix(upper, "CREATE UNIQUE INDEX IF NOT EXISTS "):
		return sqliteRenderCreateIndex(statement)
	default:
		return "", fmt.Errorf("unsupported schema statement %q", statement)
	}
}

func sqliteRenderCreateTable(statement string) (string, error) {
	matches := sqliteCreateTablePattern.FindStringSubmatch(strings.TrimSpace(statement))
	if len(matches) != 3 {
		return "", fmt.Errorf("unsupported CREATE TABLE shape %q", statement)
	}
	tableName, err := sqliteRenderIdent(matches[1], "table")
	if err != nil {
		return "", err
	}
	lines, err := splitSchemaDefinitionLines(matches[2])
	if err != nil {
		return "", err
	}
	if len(lines) == 0 {
		return "", fmt.Errorf("CREATE TABLE %s has no schema lines", tableName)
	}
	renderedLines := make([]string, 0, len(lines))
	for _, line := range lines {
		rendered, err := sqliteRenderTableLine(line)
		if err != nil {
			return "", fmt.Errorf("CREATE TABLE %s line %q: %w", tableName, line, err)
		}
		renderedLines = append(renderedLines, rendered)
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n    %s\n)", tableName, strings.Join(renderedLines, ",\n    ")), nil
}

func sqliteRenderTableLine(line string) (string, error) {
	line = strings.TrimSpace(strings.TrimSuffix(line, ","))
	upper := strings.ToUpper(line)
	switch {
	case strings.HasPrefix(upper, "PRIMARY KEY "):
		return sqliteRenderPrimaryKeyConstraint(line)
	case strings.HasPrefix(upper, "UNIQUE "):
		return sqliteRenderUniqueConstraint(line)
	case strings.HasPrefix(upper, "CHECK "):
		return sqliteRenderCheckConstraint(line)
	case strings.HasPrefix(upper, "FOREIGN KEY "):
		return sqliteRenderForeignKeyConstraint(line)
	case strings.HasPrefix(upper, "CONSTRAINT "):
		return "", fmt.Errorf("unsupported SQLite table constraint")
	}
	return sqliteRenderColumnDefinition(line)
}

func sqliteRenderForeignKeyConstraint(line string) (string, error) {
	matches := sqliteForeignKeyPattern.FindStringSubmatch(strings.TrimSpace(line))
	if len(matches) != 5 {
		return "", fmt.Errorf("unsupported SQLite foreign key constraint %q", line)
	}
	columns, err := sqliteRenderIdentifierList(matches[1])
	if err != nil {
		return "", err
	}
	tableName, err := sqliteRenderIdent(matches[2], "foreign key table")
	if err != nil {
		return "", err
	}
	references, err := sqliteRenderIdentifierList(matches[3])
	if err != nil {
		return "", err
	}
	if len(columns) != len(references) {
		return "", fmt.Errorf("SQLite foreign key column count does not match referenced column count")
	}
	rendered := fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s(%s)", strings.Join(columns, ", "), tableName, strings.Join(references, ", "))
	if action := strings.ToUpper(strings.Join(strings.Fields(matches[4]), " ")); action != "" {
		rendered += " ON DELETE " + action
	}
	return rendered, nil
}

func sqliteRenderIdentifierList(raw string) ([]string, error) {
	parts, err := splitTopLevelComma(raw)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		ident, rest, err := parseSchemaLeadingIdent(strings.TrimSpace(part))
		if err != nil || strings.TrimSpace(rest) != "" {
			return nil, fmt.Errorf("unsupported SQLite identifier list %q", raw)
		}
		out = append(out, quoteIdent(ident))
	}
	return out, nil
}

func sqliteRenderColumnDefinition(line string) (string, error) {
	columnName, rest, err := parseSchemaLeadingIdent(line)
	if err != nil {
		return "", err
	}
	typeSpec, clauses := splitColumnTypeAndClauses(rest)
	if strings.TrimSpace(typeSpec) == "" {
		return "", fmt.Errorf("column %s type is required", columnName)
	}
	sqliteType, err := sqliteRenderType(typeSpec)
	if err != nil {
		return "", fmt.Errorf("column %s: %w", columnName, err)
	}
	parts := []string{quoteIdent(columnName), sqliteType}
	for strings.TrimSpace(clauses) != "" {
		clauses = strings.TrimSpace(clauses)
		upper := strings.ToUpper(clauses)
		switch {
		case strings.HasPrefix(upper, "PRIMARY KEY"):
			parts = append(parts, "PRIMARY KEY")
			clauses = strings.TrimSpace(clauses[len("PRIMARY KEY"):])
		case strings.HasPrefix(upper, "NOT NULL"):
			parts = append(parts, "NOT NULL")
			clauses = strings.TrimSpace(clauses[len("NOT NULL"):])
		case strings.HasPrefix(upper, "UNIQUE"):
			parts = append(parts, "UNIQUE")
			clauses = strings.TrimSpace(clauses[len("UNIQUE"):])
		case strings.HasPrefix(upper, "DEFAULT "):
			rawDefault, remaining := splitColumnClauseValue(clauses[len("DEFAULT "):])
			renderedDefault, err := sqliteRenderDefault(typeSpec, rawDefault)
			if err != nil {
				return "", fmt.Errorf("column %s default: %w", columnName, err)
			}
			parts = append(parts, "DEFAULT "+renderedDefault)
			clauses = remaining
		case strings.HasPrefix(upper, "REFERENCES "):
			referenceTarget, remaining := splitColumnClauseValue(clauses[len("REFERENCES "):])
			references := "REFERENCES " + referenceTarget
			if err := rejectSQLiteUnsupportedConstructs(references); err != nil {
				return "", fmt.Errorf("column %s references: %w", columnName, err)
			}
			parts = append(parts, references)
			clauses = remaining
		case strings.HasPrefix(upper, "CHECK "):
			condition, err := unwrapSchemaClause("CHECK", clauses)
			if err != nil {
				return "", err
			}
			renderedCondition, err := sqliteRenderPredicate(condition)
			if err != nil {
				return "", fmt.Errorf("column %s check: %w", columnName, err)
			}
			parts = append(parts, fmt.Sprintf("CHECK (%s)", renderedCondition))
			clauses = ""
		default:
			return "", fmt.Errorf("unsupported SQLite column clause %q", clauses)
		}
	}
	return strings.Join(parts, " "), nil
}

func sqliteRenderCreateIndex(statement string) (string, error) {
	statement = strings.TrimSpace(statement)
	matches := sqliteCreateIndexPattern.FindStringSubmatch(statement)
	if len(matches) != 4 {
		return "", fmt.Errorf("unsupported CREATE INDEX shape %q", statement)
	}
	unique := strings.TrimSpace(matches[1]) != ""
	indexName, err := sqliteRenderIdent(matches[2], "index")
	if err != nil {
		return "", err
	}
	tableName, err := sqliteRenderIdent(matches[3], "index table")
	if err != nil {
		return "", err
	}
	start := len(matches[0])
	end, err := findMatchingParen(statement, start-1)
	if err != nil {
		return "", err
	}
	rawTerms := strings.TrimSpace(statement[start:end])
	terms, err := sqliteRenderIndexTerms(rawTerms)
	if err != nil {
		return "", err
	}
	remaining := strings.TrimSpace(statement[end+1:])
	whereClause := ""
	if remaining != "" {
		if !strings.HasPrefix(strings.ToUpper(remaining), "WHERE ") {
			return "", fmt.Errorf("unsupported CREATE INDEX suffix %q", remaining)
		}
		predicate, err := sqliteRenderPredicate(strings.TrimSpace(remaining[len("WHERE "):]))
		if err != nil {
			return "", fmt.Errorf("index %s predicate: %w", strings.Trim(matches[2], `"`), err)
		}
		whereClause = " WHERE " + predicate
	}
	prefix := "CREATE INDEX IF NOT EXISTS"
	if unique {
		prefix = "CREATE UNIQUE INDEX IF NOT EXISTS"
	}
	return fmt.Sprintf("%s %s ON %s(%s)%s", prefix, indexName, tableName, strings.Join(terms, ", "), whereClause), nil
}

func sqliteRenderIndexTerms(raw string) ([]string, error) {
	parts, err := splitTopLevelComma(raw)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		rendered, err := sqliteRenderIndexTerm(part)
		if err != nil {
			return nil, err
		}
		out = append(out, rendered)
	}
	return out, nil
}

func sqliteRenderIndexTerm(term string) (string, error) {
	if rendered, ok := sqliteRenderJSONExtractTerm(term); ok {
		return rendered, nil
	}
	upper := strings.ToUpper(term)
	desc := false
	if strings.HasSuffix(upper, " DESC") {
		desc = true
		term = strings.TrimSpace(term[:len(term)-len(" DESC")])
	}
	ident, rest, err := parseSchemaLeadingIdent(term)
	if err != nil {
		return "", fmt.Errorf("unsupported SQLite index term %q", term)
	}
	if strings.TrimSpace(rest) != "" {
		return "", fmt.Errorf("unsupported SQLite index term %q", term)
	}
	rendered := quoteIdent(ident)
	if desc {
		rendered += " DESC"
	}
	return rendered, nil
}

func sqliteRenderJSONExtractTerm(term string) (string, bool) {
	trimmed := strings.TrimSpace(term)
	if !strings.HasPrefix(trimmed, "(") || !strings.HasSuffix(trimmed, ")") {
		return "", false
	}
	inner := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	parts := strings.Split(inner, "->>")
	if len(parts) != 2 {
		return "", false
	}
	column := strings.TrimSpace(parts[0])
	field := strings.Trim(strings.TrimSpace(parts[1]), "'")
	if _, err := validateSchemaDDLIdentifier(column, "sqlite json index column"); err != nil {
		return "", false
	}
	if _, err := validateSchemaDDLIdentifier(field, "sqlite json index field"); err != nil {
		return "", false
	}
	return fmt.Sprintf("json_extract(%s, '$.%s')", quoteIdent(column), field), true
}

func sqliteRenderType(raw string) (string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(raw))
	switch normalized {
	case "TEXT":
		return "TEXT", nil
	case "UUID":
		return "TEXT", nil
	case "JSONB":
		return "TEXT", nil
	case "BYTEA":
		return "BLOB", nil
	case "TIMESTAMPTZ":
		return "TEXT", nil
	case "INTEGER":
		return "INTEGER", nil
	case "BIGINT":
		return "INTEGER", nil
	case "BIGSERIAL":
		// INTEGER PRIMARY KEY is SQLite's rowid-backed monotonic sequence owner.
		return "INTEGER", nil
	case "DOUBLE PRECISION":
		return "REAL", nil
	case "BOOLEAN":
		return "INTEGER", nil
	case "NUMERIC":
		return "NUMERIC", nil
	case "TEXT[]":
		return "TEXT", nil
	}
	if schemaDDLNumericPattern.MatchString(strings.ToLower(strings.TrimSpace(raw))) {
		return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(raw), " ", "")), nil
	}
	return "", fmt.Errorf("unsupported SQLite column type %q", strings.TrimSpace(raw))
}

func sqliteRenderDefault(typeSpec, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	normalized := strings.ToUpper(raw)
	switch {
	case strings.EqualFold(raw, "gen_random_uuid()"):
		return sqliteUUIDDefaultExpression(), nil
	case strings.EqualFold(raw, "NOW()"):
		return "CURRENT_TIMESTAMP", nil
	case normalized == "FALSE":
		return "0", nil
	case normalized == "TRUE":
		return "1", nil
	case strings.EqualFold(strings.TrimSpace(typeSpec), "TEXT[]") && raw == "'{}'":
		return "'[]'", nil
	case strings.HasPrefix(raw, "'") && strings.HasSuffix(raw, "'"):
		return raw, nil
	case strings.HasSuffix(strings.ToLower(raw), "::jsonb"):
		literal := strings.TrimSpace(raw[:len(raw)-len("::jsonb")])
		if strings.HasPrefix(literal, "'") && strings.HasSuffix(literal, "'") {
			return literal, nil
		}
	case isIntegerLiteral(raw), isNumericLiteral(raw):
		return raw, nil
	}
	return "", fmt.Errorf("unsupported SQLite default %q", raw)
}

func sqliteUUIDDefaultExpression() string {
	return "(lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))), 2) || '-' || substr('89ab', 1 + (abs(random()) % 4), 1) || substr(lower(hex(randomblob(2))), 2) || '-' || lower(hex(randomblob(6))))"
}

func sqliteRenderPrimaryKeyConstraint(line string) (string, error) {
	columns, err := unwrapSchemaClause("PRIMARY KEY", line)
	if err != nil {
		return "", err
	}
	rendered, err := sqliteRenderIdentList(columns)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("PRIMARY KEY (%s)", rendered), nil
}

func sqliteRenderUniqueConstraint(line string) (string, error) {
	columns, err := unwrapSchemaClause("UNIQUE", line)
	if err != nil {
		return "", err
	}
	rendered, err := sqliteRenderIdentList(columns)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("UNIQUE (%s)", rendered), nil
}

func sqliteRenderCheckConstraint(line string) (string, error) {
	condition, err := unwrapSchemaClause("CHECK", line)
	if err != nil {
		return "", err
	}
	rendered, err := sqliteRenderPredicate(condition)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("CHECK (%s)", rendered), nil
}

func sqliteRenderPredicate(raw string) (string, error) {
	predicate := strings.TrimSpace(raw)
	if predicate == "" {
		return "", fmt.Errorf("predicate is required")
	}
	predicate = strings.ReplaceAll(predicate,
		"current_bundle_hash ~ '^bundle-v1:sha256:[0-9a-f]{64}$'",
		sqliteBundleHashPredicate("current_bundle_hash"),
	)
	predicate = strings.ReplaceAll(predicate,
		"created_bundle_hash ~ '^bundle-v1:sha256:[0-9a-f]{64}$'",
		sqliteBundleHashPredicate("created_bundle_hash"),
	)
	predicate = strings.ReplaceAll(predicate,
		"bundle_hash ~ '^bundle-v1:sha256:[0-9a-f]{64}$'",
		sqliteBundleHashPredicate("bundle_hash"),
	)
	predicate = strings.ReplaceAll(predicate, "source_route <> '{}'::jsonb", "source_route <> '{}'")
	predicate = strings.ReplaceAll(predicate, "target_route <> '{}'::jsonb", "target_route <> '{}'")
	if err := rejectSQLiteUnsupportedConstructs(predicate); err != nil {
		return "", err
	}
	return predicate, nil
}

func sqliteBundleHashPredicate(column string) string {
	return fmt.Sprintf("(length(%s) = 81 AND substr(%s, 1, 17) = 'bundle-v1:sha256:' AND substr(%s, 18) GLOB '%s')",
		column, column, column, strings.Repeat("[0-9a-f]", 64))
}

func rejectSQLiteUnsupportedConstructs(statement string) error {
	for _, check := range sqliteUnsupportedConstructChecks {
		if check.pattern.MatchString(statement) {
			return fmt.Errorf("unsupported SQLite schema construct: %s in %q", check.label, strings.TrimSpace(statement))
		}
	}
	return nil
}

func splitSchemaDefinitionLines(body string) ([]string, error) {
	return splitTopLevelComma(body)
}

func splitColumnTypeAndClauses(rest string) (string, string) {
	rest = strings.TrimSpace(rest)
	depth := 0
	inString := false
	for i := 0; i < len(rest); i++ {
		ch := rest[i]
		switch ch {
		case '\'':
			inString = !inString
		case '(':
			if !inString {
				depth++
			}
		case ')':
			if !inString && depth > 0 {
				depth--
			}
		}
		if inString || depth != 0 || (i > 0 && !isSpace(rest[i-1])) {
			continue
		}
		tail := strings.ToUpper(strings.TrimSpace(rest[i:]))
		for _, keyword := range []string{"PRIMARY KEY", "NOT NULL", "DEFAULT ", "REFERENCES ", "CHECK ", "UNIQUE"} {
			if strings.HasPrefix(tail, keyword) {
				return strings.TrimSpace(rest[:i]), strings.TrimSpace(rest[i:])
			}
		}
	}
	return rest, ""
}

func splitColumnClauseValue(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	depth := 0
	inString := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		switch ch {
		case '\'':
			inString = !inString
		case '(':
			if !inString {
				depth++
			}
		case ')':
			if !inString && depth > 0 {
				depth--
			}
		}
		if inString || depth != 0 || (i > 0 && !isSpace(raw[i-1])) {
			continue
		}
		tail := strings.ToUpper(strings.TrimSpace(raw[i:]))
		for _, keyword := range []string{"PRIMARY KEY", "NOT NULL", "DEFAULT ", "REFERENCES ", "CHECK ", "UNIQUE"} {
			if strings.HasPrefix(tail, keyword) {
				return strings.TrimSpace(raw[:i]), strings.TrimSpace(raw[i:])
			}
		}
	}
	return raw, ""
}

func parseSchemaLeadingIdent(raw string) (string, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", fmt.Errorf("identifier is required")
	}
	if raw[0] == '"' {
		end := strings.Index(raw[1:], `"`)
		if end < 0 {
			return "", "", fmt.Errorf("unterminated quoted identifier")
		}
		ident := raw[1 : end+1]
		if _, err := validateSchemaDDLIdentifier(ident, "sqlite schema"); err != nil {
			return "", "", err
		}
		return ident, strings.TrimSpace(raw[end+2:]), nil
	}
	end := 0
	for end < len(raw) && (isIdentChar(raw[end]) || (end == 0 && raw[end] >= 'a' && raw[end] <= 'z')) {
		end++
	}
	ident := raw[:end]
	if _, err := validateSchemaDDLIdentifier(ident, "sqlite schema"); err != nil {
		return "", "", err
	}
	return ident, strings.TrimSpace(raw[end:]), nil
}

func sqliteRenderIdent(raw, context string) (string, error) {
	ident := strings.Trim(strings.TrimSpace(raw), `"`)
	if _, err := validateSchemaDDLIdentifier(ident, "sqlite "+context); err != nil {
		return "", err
	}
	return quoteIdent(ident), nil
}

func sqliteRenderIdentList(raw string) (string, error) {
	parts, err := splitTopLevelComma(raw)
	if err != nil {
		return "", err
	}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		ident, rest, err := parseSchemaLeadingIdent(part)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(rest) != "" {
			return "", fmt.Errorf("unsupported identifier list item %q", part)
		}
		out = append(out, quoteIdent(ident))
	}
	return strings.Join(out, ", "), nil
}

func splitTopLevelComma(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("comma list is required")
	}
	out := make([]string, 0)
	start := 0
	depth := 0
	inString := false
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '\'':
			inString = !inString
		case '(':
			if !inString {
				depth++
			}
		case ')':
			if !inString {
				depth--
				if depth < 0 {
					return nil, fmt.Errorf("unbalanced parentheses in %q", raw)
				}
			}
		case ',':
			if !inString && depth == 0 {
				out = append(out, strings.TrimSpace(raw[start:i]))
				start = i + 1
			}
		}
	}
	if depth != 0 || inString {
		return nil, fmt.Errorf("unterminated comma list %q", raw)
	}
	out = append(out, strings.TrimSpace(raw[start:]))
	return out, nil
}

func unwrapSchemaClause(prefix, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(strings.ToUpper(raw), strings.ToUpper(prefix)) {
		return "", fmt.Errorf("expected %s clause", prefix)
	}
	raw = strings.TrimSpace(raw[len(prefix):])
	if !strings.HasPrefix(raw, "(") || !strings.HasSuffix(raw, ")") {
		return "", fmt.Errorf("%s clause must be parenthesized", prefix)
	}
	return strings.TrimSpace(raw[1 : len(raw)-1]), nil
}

func findMatchingParen(raw string, open int) (int, error) {
	if open < 0 || open >= len(raw) || raw[open] != '(' {
		return -1, fmt.Errorf("opening parenthesis not found")
	}
	depth := 0
	inString := false
	for i := open; i < len(raw); i++ {
		switch raw[i] {
		case '\'':
			inString = !inString
		case '(':
			if !inString {
				depth++
			}
		case ')':
			if !inString {
				depth--
				if depth == 0 {
					return i, nil
				}
			}
		}
	}
	return -1, fmt.Errorf("matching parenthesis not found")
}

func isIntegerLiteral(raw string) bool {
	if raw == "" {
		return false
	}
	_, err := strconv.ParseInt(raw, 10, 64)
	return err == nil
}

func isNumericLiteral(raw string) bool {
	if raw == "" {
		return false
	}
	_, err := strconv.ParseFloat(raw, 64)
	return err == nil && strings.Contains(raw, ".")
}

func isIdentChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_'
}

func isSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}
