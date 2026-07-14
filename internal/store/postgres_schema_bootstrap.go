package store

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/store/platformschema"
)

const postgresSchemaBootstrapLock int64 = 0x535741524d534348

type schemaQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *PostgresStore) BootstrapSchema(ctx context.Context, request SchemaBootstrapRequest) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required for schema bootstrap")
	}
	request = request.canonical()
	if err := request.validate(); err != nil {
		return err
	}
	expected, err := expectedSchemaShape(request.PlatformPlans, SchemaDialectPostgres)
	if err != nil {
		return err
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin postgres schema bootstrap: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, postgresSchemaBootstrapLock); err != nil {
		return fmt.Errorf("serialize postgres schema bootstrap: %w", err)
	}
	if err := enforcePostgresDecisionCardLifecycleOutboxCutoff(ctx, tx); err != nil {
		return err
	}
	if err := migratePostgresCompletionAuthoritySchema(ctx, tx, request.PlatformPlans); err != nil {
		return err
	}
	target, report, err := inspectPostgresCompatibility(ctx, tx, expected)
	if err != nil {
		return err
	}
	report.Target = target
	diagnostic := schemaCompatibilityDiagnostic{Backend: SchemaDialectPostgres, Target: target, Current: request.Origin, Origin: report.Origin}
	switch report.State {
	case schemaStateFresh:
		if err := platformschema.BootstrapFreshPostgres(ctx, tx, request.PlatformPlans, request.Origin.SwarmVersion, request.Origin.PlatformVersion, request.Origin.CreatedAt); err != nil {
			return err
		}
	case schemaStateCompatible:
	case schemaStateIncompatible:
		return diagnostic.failure(report.Drift)
	default:
		return fmt.Errorf("unknown postgres schema compatibility state %q", report.State)
	}
	if err := ensurePostgresStatePlans(ctx, tx, request.StatePlans, diagnostic); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit postgres schema bootstrap: %w", err)
	}
	committed = true
	_, err = s.BindSchemaCapabilities(ctx)
	return err
}

func inspectPostgresCompatibility(ctx context.Context, q schemaQueryer, expected schemaShape) (string, schemaCompatibilityReport, error) {
	var target string
	if err := q.QueryRowContext(ctx, `SELECT current_database()`).Scan(&target); err != nil {
		return "", schemaCompatibilityReport{}, fmt.Errorf("read postgres database identity: %w", err)
	}
	tables, err := postgresPublicTables(ctx, q)
	if err != nil {
		return target, schemaCompatibilityReport{}, err
	}
	if len(tables) == 0 {
		return target, schemaCompatibilityReport{State: schemaStateFresh}, nil
	}
	var origin *RuntimeStoreOrigin
	var drift []string
	if _, ok := tables[RuntimeStoreMetadataTable]; !ok {
		drift = append(drift, "non-empty public schema has no runtime_store_metadata origin stamp")
	} else {
		origin, err = readRuntimeStoreOrigin(ctx, q)
		if err != nil {
			drift = append(drift, "runtime_store_metadata origin row is malformed: "+err.Error())
		} else if origin == nil {
			drift = append(drift, "runtime_store_metadata does not contain the required id=1 origin row")
		}
	}
	actual, err := loadPostgresSchemaShape(ctx, q, expected)
	if err != nil {
		return target, schemaCompatibilityReport{}, err
	}
	drift = append(drift, compareSchemaShapes(expected, actual)...)
	state := schemaStateCompatible
	if len(drift) > 0 {
		state = schemaStateIncompatible
	}
	return target, schemaCompatibilityReport{State: state, Origin: origin, Drift: drift}, nil
}

func postgresPublicTables(ctx context.Context, q schemaQueryer) (map[string]struct{}, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT c.relname
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
	`)
	if err != nil {
		return nil, fmt.Errorf("inspect postgres public tables: %w", err)
	}
	defer rows.Close()
	tables := map[string]struct{}{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables[strings.TrimSpace(name)] = struct{}{}
	}
	return tables, rows.Err()
}

func loadPostgresSchemaShape(ctx context.Context, q schemaQueryer, expected schemaShape) (schemaShape, error) {
	actual := schemaShape{Tables: map[string]schemaTableShape{}}
	rows, err := q.QueryContext(ctx, `
		SELECT c.relname, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull,
		       COALESCE(pg_get_expr(d.adbin, d.adrelid), '')
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
		LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
		WHERE n.nspname = 'public' AND c.relkind = 'r'
	`)
	if err != nil {
		return schemaShape{}, fmt.Errorf("inspect postgres columns: %w", err)
	}
	for rows.Next() {
		var tableName, columnName, typeName, defaultValue string
		var notNull bool
		if err := rows.Scan(&tableName, &columnName, &typeName, &notNull, &defaultValue); err != nil {
			rows.Close()
			return schemaShape{}, err
		}
		if _, required := expected.Tables[tableName]; !required {
			continue
		}
		table, ok := actual.Tables[tableName]
		if !ok {
			table = schemaTableShape{Columns: map[string]schemaColumnShape{}, Indexes: map[string]string{}}
		}
		table.Columns[columnName] = schemaColumnShape{Type: normalizeSchemaType(typeName), NotNull: notNull, Default: normalizeDefault(defaultValue)}
		actual.Tables[tableName] = table
	}
	if err := rows.Close(); err != nil {
		return schemaShape{}, err
	}
	rows, err = q.QueryContext(ctx, `
		SELECT c.relname, pg_get_constraintdef(con.oid, true)
		FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
	`)
	if err != nil {
		return schemaShape{}, fmt.Errorf("inspect postgres constraints: %w", err)
	}
	for rows.Next() {
		var tableName, definition string
		if err := rows.Scan(&tableName, &definition); err != nil {
			rows.Close()
			return schemaShape{}, err
		}
		table, ok := actual.Tables[tableName]
		if !ok {
			continue
		}
		table.Constraints = append(table.Constraints, normalizeConstraint(definition))
		actual.Tables[tableName] = table
	}
	if err := rows.Close(); err != nil {
		return schemaShape{}, err
	}
	rows, err = q.QueryContext(ctx, `
		SELECT t.relname, i.relname, ix.indisunique, pg_get_indexdef(ix.indexrelid, 0, true)
		FROM pg_catalog.pg_index ix
		JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
		JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
		LEFT JOIN pg_catalog.pg_constraint con ON con.conindid = ix.indexrelid
		WHERE n.nspname = 'public' AND con.oid IS NULL
	`)
	if err != nil {
		return schemaShape{}, fmt.Errorf("inspect postgres indexes: %w", err)
	}
	for rows.Next() {
		var tableName, indexName, definition string
		var unique bool
		if err := rows.Scan(&tableName, &indexName, &unique, &definition); err != nil {
			rows.Close()
			return schemaShape{}, err
		}
		table, ok := actual.Tables[tableName]
		if !ok {
			continue
		}
		table.Indexes[indexName] = normalizePostgresIndexDefinition(unique, tableName, definition)
		actual.Tables[tableName] = table
	}
	if err := rows.Close(); err != nil {
		return schemaShape{}, err
	}
	for name, table := range actual.Tables {
		sort.Strings(table.Constraints)
		actual.Tables[name] = table
	}
	return actual, nil
}

var postgresIndexBodyPattern = regexp.MustCompile(`(?is)^create\s+(?:unique\s+)?index\s+\S+\s+on\s+(?:public\.)?\S+\s+(?:using\s+btree\s+)?\((.*)\)\s*(where\s+.+)?$`)

func normalizePostgresIndexDefinition(unique bool, tableName, definition string) string {
	matches := postgresIndexBodyPattern.FindStringSubmatch(strings.TrimSpace(definition))
	if len(matches) != 3 {
		return normalizeConstraint(definition)
	}
	return normalizeIndexDefinition(unique, tableName, matches[1], matches[2])
}

func executePostgresPlans(ctx context.Context, tx *sql.Tx, plans []SchemaTableDDL) error {
	for _, plan := range plans {
		for _, statement := range plan.Statements {
			if _, err := tx.ExecContext(ctx, strings.TrimSpace(statement)); err != nil {
				return fmt.Errorf("create postgres %s table %s: %w", plan.SchemaKind, plan.TableName, err)
			}
		}
	}
	return nil
}

func ensurePostgresStatePlans(ctx context.Context, tx *sql.Tx, plans []SchemaTableDDL, diagnostic schemaCompatibilityDiagnostic) error {
	for _, plan := range plans {
		expected, err := expectedSchemaShape([]SchemaTableDDL{plan}, SchemaDialectPostgres)
		if err != nil {
			return err
		}
		tables, err := postgresPublicTables(ctx, tx)
		if err != nil {
			return err
		}
		if _, exists := tables[plan.TableName]; !exists {
			if err := executePostgresPlans(ctx, tx, []SchemaTableDDL{plan}); err != nil {
				return err
			}
			continue
		}
		actual, err := loadPostgresSchemaShape(ctx, tx, expected)
		if err != nil {
			return err
		}
		if drift := compareSchemaShapes(expected, actual); len(drift) > 0 {
			return diagnostic.failure(generatedStateDrift(plan.TableName, drift))
		}
	}
	return nil
}
