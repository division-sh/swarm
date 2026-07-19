package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/store/platformschema"
)

func retiredPlatformTableDrift(objects map[string]struct{}) []string {
	drift := make([]string, 0)
	for _, table := range platformschema.RetiredPlatformTables() {
		if _, exists := objects[string(table)]; exists {
			drift = append(drift, fmt.Sprintf("retired platform table %s exists", table))
		}
	}
	return drift
}

type schemaCompatibilityState string

const (
	schemaStateFresh        schemaCompatibilityState = "fresh"
	schemaStateCompatible   schemaCompatibilityState = "compatible"
	schemaStateIncompatible schemaCompatibilityState = "incompatible"
)

type schemaCompatibilityReport struct {
	State  schemaCompatibilityState
	Origin *RuntimeStoreOrigin
	Drift  []string
	Target string
}

type SchemaCompatibilityError struct {
	Backend SchemaDialect
	Target  string
	Current RuntimeStoreOrigin
	Origin  *RuntimeStoreOrigin
	Drift   []string
}

type schemaCompatibilityDiagnostic struct {
	Backend SchemaDialect
	Target  string
	Current RuntimeStoreOrigin
	Origin  *RuntimeStoreOrigin
}

func (d schemaCompatibilityDiagnostic) failure(drift []string) *SchemaCompatibilityError {
	return &SchemaCompatibilityError{
		Backend: d.Backend,
		Target:  d.Target,
		Current: d.Current,
		Origin:  d.Origin,
		Drift:   append([]string(nil), drift...),
	}
}

func generatedStateDrift(tableName string, drift []string) []string {
	prefixed := make([]string, len(drift))
	for i, item := range drift {
		prefixed[i] = fmt.Sprintf("generated state table %s: %s", tableName, item)
	}
	return prefixed
}

func (e *SchemaCompatibilityError) Error() string {
	target := strings.TrimSpace(e.Target)
	if target == "" {
		target = "selected store"
	}
	origin := "unknown (the store is not stamped)"
	if e.Origin != nil {
		origin = fmt.Sprintf("Swarm %s / platform %s created %s", e.Origin.SwarmVersion, e.Origin.PlatformVersion, e.Origin.CreatedAt.UTC().Format(time.RFC3339))
	}
	drift := strings.Join(e.Drift, "; ")
	if drift == "" {
		drift = "the selected namespace is not a compatible Swarm runtime store"
	}
	remediation := "create and select a fresh PostgreSQL database (for example: createdb swarm_fresh, then set database.name to swarm_fresh)"
	if e.Backend == SchemaDialectSQLite {
		remediation = fmt.Sprintf("stop Swarm and remove the incompatible local store with: rm -f -- %s %s %s", shellQuote(target), shellQuote(target+"-wal"), shellQuote(target+"-shm"))
	}
	return fmt.Sprintf("%s %s is incompatible with Swarm %s / platform %s: %s; stored origin: %s; remediation: %s", e.Backend, target, e.Current.SwarmVersion, e.Current.PlatformVersion, drift, origin, remediation)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

type schemaShape struct {
	Tables map[string]schemaTableShape
}

type schemaTableShape struct {
	Columns     map[string]schemaColumnShape
	Constraints []string
	Indexes     map[string]string
}

type schemaColumnShape struct {
	Type       string
	NotNull    bool
	Default    string
	PrimaryKey bool
	Unique     bool
	Reference  string
	Checks     []string
}

var (
	compatCreateTablePattern = regexp.MustCompile(`(?is)^create\s+table(?:\s+if\s+not\s+exists)?\s+("?[a-z_][a-z0-9_]*"?)\s*\((.*)\)\s*$`)
	compatCreateIndexPattern = regexp.MustCompile(`(?is)^create\s+(unique\s+)?index(?:\s+if\s+not\s+exists)?\s+("?[a-z_][a-z0-9_]*"?)\s+on\s+("?[a-z_][a-z0-9_]*"?)\s*\((.*)\)\s*(where\s+.+)?$`)
	compatSQLSpacePattern    = regexp.MustCompile(`\s+`)
	compatSQLCastPattern     = regexp.MustCompile(`::[a-z_][a-z0-9_]*(?:\[\])?`)
)

func expectedSchemaShape(plans []SchemaTableDDL, dialect SchemaDialect) (schemaShape, error) {
	shape := schemaShape{Tables: make(map[string]schemaTableShape, len(plans))}
	for _, plan := range plans {
		statements, err := StatementsForSchemaDialect(plan, dialect)
		if err != nil {
			return schemaShape{}, err
		}
		for _, statement := range statements {
			if err := addStatementToShape(&shape, statement); err != nil {
				return schemaShape{}, fmt.Errorf("derive expected %s shape for %s: %w", dialect, plan.TableName, err)
			}
		}
	}
	if dialect == SchemaDialectPostgres {
		normalizeExpectedPostgresSerialColumns(&shape)
	}
	return shape, nil
}

func normalizeExpectedPostgresSerialColumns(shape *schemaShape) {
	if shape == nil {
		return
	}
	for tableName, table := range shape.Tables {
		for columnName, column := range table.Columns {
			var storageType string
			switch column.Type {
			case "smallserial":
				storageType = "smallint"
			case "serial":
				storageType = "integer"
			case "bigserial":
				storageType = "bigint"
			default:
				continue
			}
			column.Type = storageType
			column.Default = normalizeDefault(fmt.Sprintf("nextval('%s_%s_seq')", tableName, columnName))
			table.Columns[columnName] = column
		}
		shape.Tables[tableName] = table
	}
}

func addStatementToShape(shape *schemaShape, statement string) error {
	statement = strings.TrimSpace(strings.TrimSuffix(statement, ";"))
	if matches := compatCreateTablePattern.FindStringSubmatch(statement); len(matches) == 3 {
		tableName := strings.Trim(matches[1], `"`)
		table := schemaTableShape{Columns: map[string]schemaColumnShape{}, Indexes: map[string]string{}}
		definitions, err := splitSchemaDefinitionLines(matches[2])
		if err != nil {
			return err
		}
		for _, line := range definitions {
			line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(strings.TrimSuffix(line, ",")), ","))
			if line == "" {
				continue
			}
			upper := strings.ToUpper(line)
			if strings.HasPrefix(upper, "PRIMARY KEY ") || strings.HasPrefix(upper, "UNIQUE ") || strings.HasPrefix(upper, "CHECK ") || strings.HasPrefix(upper, "FOREIGN KEY ") || strings.HasPrefix(upper, "CONSTRAINT ") {
				table.Constraints = append(table.Constraints, normalizeConstraint(line))
				continue
			}
			name, rest, err := parseSchemaLeadingIdent(line)
			if err != nil {
				return err
			}
			typeSpec, clauses := splitColumnTypeAndClauses(rest)
			column, err := parseColumnShape(typeSpec, clauses)
			if err != nil {
				return fmt.Errorf("column %s: %w", name, err)
			}
			table.Columns[name] = column
			if column.PrimaryKey {
				table.Constraints = append(table.Constraints, normalizeConstraint(fmt.Sprintf("PRIMARY KEY (%s)", name)))
			}
			if column.Unique {
				table.Constraints = append(table.Constraints, normalizeConstraint(fmt.Sprintf("UNIQUE (%s)", name)))
			}
			if column.Reference != "" {
				table.Constraints = append(table.Constraints, normalizeConstraint(fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s", name, column.Reference)))
			}
			for _, check := range column.Checks {
				table.Constraints = append(table.Constraints, normalizeConstraint("CHECK ("+check+")"))
			}
		}
		sort.Strings(table.Constraints)
		shape.Tables[tableName] = table
		return nil
	}
	if matches := compatCreateIndexPattern.FindStringSubmatch(statement); len(matches) == 6 {
		indexName := strings.Trim(matches[2], `"`)
		tableName := strings.Trim(matches[3], `"`)
		table, ok := shape.Tables[tableName]
		if !ok {
			return fmt.Errorf("index %s refers to unknown table %s", indexName, tableName)
		}
		table.Indexes[indexName] = normalizeIndexDefinition(matches[1] != "", tableName, matches[4], matches[5])
		shape.Tables[tableName] = table
		return nil
	}
	return fmt.Errorf("unsupported statement %q", statement)
}

func parseColumnShape(typeSpec, clauses string) (schemaColumnShape, error) {
	column := schemaColumnShape{Type: normalizeSchemaType(typeSpec)}
	for strings.TrimSpace(clauses) != "" {
		clauses = strings.TrimSpace(clauses)
		upper := strings.ToUpper(clauses)
		switch {
		case strings.HasPrefix(upper, "PRIMARY KEY"):
			column.PrimaryKey, column.NotNull = true, true
			clauses = strings.TrimSpace(clauses[len("PRIMARY KEY"):])
		case strings.HasPrefix(upper, "NOT NULL"):
			column.NotNull = true
			clauses = strings.TrimSpace(clauses[len("NOT NULL"):])
		case strings.HasPrefix(upper, "UNIQUE"):
			column.Unique = true
			clauses = strings.TrimSpace(clauses[len("UNIQUE"):])
		case strings.HasPrefix(upper, "DEFAULT "):
			value, remaining := splitColumnClauseValue(clauses[len("DEFAULT "):])
			column.Default = normalizeDefault(value)
			clauses = remaining
		case strings.HasPrefix(upper, "REFERENCES "):
			value, remaining := splitColumnClauseValue(clauses[len("REFERENCES "):])
			column.Reference = normalizeReference(value)
			clauses = remaining
		case strings.HasPrefix(upper, "CHECK "):
			condition, err := unwrapSchemaClause("CHECK", clauses)
			if err != nil {
				return schemaColumnShape{}, err
			}
			column.Checks = append(column.Checks, normalizeExpression(condition))
			clauses = ""
		default:
			return schemaColumnShape{}, fmt.Errorf("unsupported column clause %q", clauses)
		}
	}
	return column, nil
}

func normalizeSchemaType(value string) string {
	v := strings.ToLower(compatSQLSpacePattern.ReplaceAllString(strings.TrimSpace(value), " "))
	switch v {
	case "timestamp with time zone":
		return "timestamptz"
	case "double precision":
		return "double precision"
	case "integer":
		return "integer"
	}
	return v
}

func normalizeDefault(value string) string {
	v := normalizeExpression(value)
	v = compatSQLCastPattern.ReplaceAllString(v, "")
	for strings.HasPrefix(v, "(") && strings.HasSuffix(v, ")") {
		v = strings.TrimSpace(v[1 : len(v)-1])
	}
	switch v {
	case "current_timestamp":
		return "now()"
	}
	return v
}

func normalizeReference(value string) string {
	return normalizeExpression(value)
}

func normalizeConstraint(value string) string {
	v := normalizeExpression(value)
	v = compatSQLCastPattern.ReplaceAllString(v, "")
	v = strings.ReplaceAll(v, " = any (array[", " in (")
	v = strings.ReplaceAll(v, " <> all (array[", " not in (")
	v = strings.ReplaceAll(v, "])", ")")
	if strings.HasPrefix(v, "check (") && strings.HasSuffix(v, ")") {
		return "check " + canonicalBooleanExpression(strings.TrimSpace(v[len("check "):]))
	}
	return v
}

func normalizeExpression(value string) string {
	v := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), `"`, ""))
	v = compatSQLSpacePattern.ReplaceAllString(v, " ")
	v = strings.ReplaceAll(v, "( ", "(")
	v = strings.ReplaceAll(v, " )", ")")
	v = strings.ReplaceAll(v, ", ", ",")
	v = strings.ReplaceAll(v, " ->> ", "->>")
	return strings.TrimSpace(v)
}

var singleValueInPattern = regexp.MustCompile(`(?is)^(.+?)\s+in\s+\(([^,()]+)\)$`)

func canonicalBooleanExpression(value string) string {
	value = trimBalancedOuterParens(strings.TrimSpace(value))
	if parts := splitTopLevelSQLKeyword(value, "or"); len(parts) > 1 {
		canonical := make([]string, 0, len(parts))
		for _, part := range parts {
			canonical = append(canonical, canonicalBooleanExpression(part))
		}
		return "or(" + strings.Join(canonical, ",") + ")"
	}
	if parts := splitTopLevelSQLKeyword(value, "and"); len(parts) > 1 {
		canonical := make([]string, 0, len(parts))
		for _, part := range parts {
			canonical = append(canonical, canonicalBooleanExpression(part))
		}
		return "and(" + strings.Join(canonical, ",") + ")"
	}
	value = trimBalancedOuterParens(value)
	value = trimRedundantComparisonOperandParens(value)
	value = strings.ReplaceAll(value, "trim(both from ", "trim(")
	if matches := singleValueInPattern.FindStringSubmatch(value); len(matches) == 3 {
		return strings.TrimSpace(matches[1]) + " = " + strings.TrimSpace(matches[2])
	}
	return value
}

func trimRedundantComparisonOperandParens(value string) string {
	if !strings.HasPrefix(value, "(") {
		return value
	}
	end, err := findMatchingParen(value, 0)
	if err != nil || end <= 0 || end >= len(value)-1 {
		return value
	}
	remainder := strings.TrimSpace(value[end+1:])
	for _, operator := range []string{"<=", ">=", "=", "<", ">"} {
		if strings.HasPrefix(remainder, operator) {
			return strings.TrimSpace(value[1:end]) + " " + remainder
		}
	}
	return value
}

func trimBalancedOuterParens(value string) string {
	for strings.HasPrefix(value, "(") && strings.HasSuffix(value, ")") {
		end, err := findMatchingParen(value, 0)
		if err != nil || end != len(value)-1 {
			break
		}
		value = strings.TrimSpace(value[1 : len(value)-1])
	}
	return value
}

func splitTopLevelSQLKeyword(value, keyword string) []string {
	depth := 0
	quote := byte(0)
	start := 0
	var parts []string
	lower := strings.ToLower(value)
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if quote != 0 {
			if ch == quote {
				if i+1 < len(value) && value[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"':
			quote = ch
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && strings.HasPrefix(lower[i:], keyword) && sqlKeywordBoundary(lower, i-1) && sqlKeywordBoundary(lower, i+len(keyword)) {
				parts = append(parts, strings.TrimSpace(value[start:i]))
				i += len(keyword) - 1
				start = i + 1
			}
		}
	}
	if len(parts) == 0 {
		return nil
	}
	parts = append(parts, strings.TrimSpace(value[start:]))
	return parts
}

func sqlKeywordBoundary(value string, index int) bool {
	if index < 0 || index >= len(value) {
		return true
	}
	ch := value[index]
	return !(ch == '_' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9')
}

func normalizeIndexDefinition(unique bool, table, terms, where string) string {
	prefix := "index"
	if unique {
		prefix = "unique index"
	}
	value := fmt.Sprintf("%s on %s (%s)", prefix, table, terms)
	if strings.TrimSpace(where) != "" {
		value += " " + strings.TrimSpace(where)
	}
	return normalizeConstraint(value)
}

func compareSchemaShapes(expected, actual schemaShape) []string {
	var drift []string
	for tableName, want := range expected.Tables {
		got, ok := actual.Tables[tableName]
		if !ok {
			drift = append(drift, "missing table "+tableName)
			continue
		}
		for columnName, wantColumn := range want.Columns {
			gotColumn, ok := got.Columns[columnName]
			if !ok {
				drift = append(drift, fmt.Sprintf("missing column %s.%s", tableName, columnName))
				continue
			}
			if wantColumn.Type != gotColumn.Type {
				drift = append(drift, fmt.Sprintf("column %s.%s type is %s, want %s", tableName, columnName, gotColumn.Type, wantColumn.Type))
			}
			if wantColumn.NotNull != gotColumn.NotNull {
				drift = append(drift, fmt.Sprintf("column %s.%s nullability differs", tableName, columnName))
			}
			if wantColumn.Default != gotColumn.Default {
				drift = append(drift, fmt.Sprintf("column %s.%s default is %q, want %q", tableName, columnName, gotColumn.Default, wantColumn.Default))
			}
		}
		for columnName := range got.Columns {
			if _, ok := want.Columns[columnName]; !ok {
				drift = append(drift, fmt.Sprintf("unexpected column %s.%s", tableName, columnName))
			}
		}
		drift = append(drift, compareNamedDefinitions(tableName+" constraint", want.Constraints, got.Constraints)...)
		for name, definition := range want.Indexes {
			actualDefinition, ok := got.Indexes[name]
			if !ok {
				drift = append(drift, fmt.Sprintf("missing index %s", name))
			} else if definition != actualDefinition {
				drift = append(drift, fmt.Sprintf("index %s is %q, want %q", name, actualDefinition, definition))
			}
		}
		for name := range got.Indexes {
			if _, ok := want.Indexes[name]; !ok {
				drift = append(drift, fmt.Sprintf("unexpected index %s", name))
			}
		}
	}
	sort.Strings(drift)
	return drift
}

func compareNamedDefinitions(label string, want, got []string) []string {
	w := append([]string(nil), want...)
	g := append([]string(nil), got...)
	sort.Strings(w)
	sort.Strings(g)
	if strings.Join(w, "\n") == strings.Join(g, "\n") {
		return nil
	}
	wantSet := make(map[string]struct{}, len(w))
	gotSet := make(map[string]struct{}, len(g))
	for _, value := range w {
		wantSet[value] = struct{}{}
	}
	for _, value := range g {
		gotSet[value] = struct{}{}
	}
	var drift []string
	for _, value := range w {
		if _, ok := gotSet[value]; !ok {
			drift = append(drift, fmt.Sprintf("missing %s %q", label, value))
		}
	}
	for _, value := range g {
		if _, ok := wantSet[value]; !ok {
			drift = append(drift, fmt.Sprintf("unexpected %s %q", label, value))
		}
	}
	return drift
}

func readRuntimeStoreOrigin(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (*RuntimeStoreOrigin, error) {
	var origin RuntimeStoreOrigin
	var createdAt any
	err := q.QueryRowContext(ctx, `SELECT swarm_version, platform_version, created_at FROM runtime_store_metadata WHERE id = 1`).Scan(&origin.SwarmVersion, &origin.PlatformVersion, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	switch value := createdAt.(type) {
	case time.Time:
		origin.CreatedAt = value
	case string:
		parsed, parseErr := parseRuntimeStoreTime(value)
		if parseErr != nil {
			return nil, fmt.Errorf("parse runtime store creation time %q: %w", value, parseErr)
		}
		origin.CreatedAt = parsed
	case []byte:
		parsed, parseErr := parseRuntimeStoreTime(string(value))
		if parseErr != nil {
			return nil, fmt.Errorf("parse runtime store creation time %q: %w", string(value), parseErr)
		}
		origin.CreatedAt = parsed
	default:
		return nil, fmt.Errorf("runtime store creation time has unsupported type %T", createdAt)
	}
	origin = origin.canonical()
	if err := origin.validateStored(); err != nil {
		return nil, err
	}
	return &origin, nil
}

func parseRuntimeStoreTime(value string) (time.Time, error) {
	var lastErr error
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST"} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}
