package schemastore

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
)

// SchemaInspection distinguishes a compatible read surface from preparation
// that normal boot still owns. Inspection never creates or alters tables.
type SchemaInspection struct {
	Target             string
	Fresh              bool
	Origin             *RuntimeStoreOrigin
	MissingStateTables []string
}

func (s *Postgres) InspectSchema(ctx context.Context, request SchemaBootstrapRequest) (SchemaInspection, error) {
	if s == nil || s.backend == nil {
		return SchemaInspection{}, NewMissingPostgresOwnerError()
	}
	request = request.canonical()
	if err := request.validate(); err != nil {
		return SchemaInspection{}, err
	}
	expected, err := postgresPlatformShape.expected(request.PlatformPlans)
	if err != nil {
		return SchemaInspection{}, err
	}
	var result SchemaInspection
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		target, report, err := inspectPostgresCompatibility(ctx, tx, expected)
		if err != nil {
			return err
		}
		result, err = inspectSchemaPreparation(ctx, tx, request, SchemaDialectPostgres, target, report)
		return err
	})
	if err == nil && !result.Fresh {
		s.schemaAdmission.markCurrent()
	}
	return result, err
}

func (s *SQLite) InspectSchema(ctx context.Context, request SchemaBootstrapRequest) (SchemaInspection, error) {
	if s == nil || s.backend == nil {
		return SchemaInspection{}, NewMissingSQLiteOwnerError()
	}
	request = request.canonical()
	if err := request.validate(); err != nil {
		return SchemaInspection{}, err
	}
	expected, err := sqlitePlatformShape.expected(request.PlatformPlans)
	if err != nil {
		return SchemaInspection{}, err
	}
	var result SchemaInspection
	err = s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		report, err := inspectSQLiteCompatibility(ctx, tx, expected)
		if err != nil {
			return err
		}
		result, err = inspectSchemaPreparation(ctx, tx, request, SchemaDialectSQLite, s.path, report)
		return err
	})
	if err == nil && !result.Fresh {
		s.schemaAdmission.markCurrent()
	}
	return result, err
}

func inspectSchemaPreparation(ctx context.Context, q schemaQueryer, request SchemaBootstrapRequest, dialect SchemaDialect, target string, report schemaCompatibilityReport) (SchemaInspection, error) {
	diagnostic := schemaCompatibilityDiagnostic{Backend: dialect, Target: target, Current: request.Origin, Origin: report.Origin}
	if report.State == schemaStateIncompatible {
		return SchemaInspection{}, diagnostic.failure(report.Drift)
	}
	if report.State != schemaStateFresh && report.State != schemaStateCompatible {
		return SchemaInspection{}, fmt.Errorf("unknown %s schema compatibility state %q", dialect, report.State)
	}
	missing, err := inspectStatePlans(ctx, q, request.StatePlans, dialect, diagnostic)
	if err != nil {
		return SchemaInspection{}, err
	}
	result := SchemaInspection{Target: target, Fresh: report.State == schemaStateFresh, Origin: report.Origin}
	for _, plan := range missing {
		result.MissingStateTables = append(result.MissingStateTables, plan.TableName)
	}
	sort.Strings(result.MissingStateTables)
	return result, nil
}

// Boot consumes this same read decision before executing only the missing DDL.
func inspectStatePlans(ctx context.Context, q schemaQueryer, plans []SchemaTableDDL, dialect SchemaDialect, diagnostic schemaCompatibilityDiagnostic) ([]SchemaTableDDL, error) {
	var tables map[string]struct{}
	var err error
	switch dialect {
	case SchemaDialectPostgres:
		tables, err = postgresPublicTables(ctx, q)
	case SchemaDialectSQLite:
		tables, err = sqliteUserTables(ctx, q)
	default:
		return nil, fmt.Errorf("unsupported schema inspection dialect %q", dialect)
	}
	if err != nil {
		return nil, err
	}
	var missing []SchemaTableDDL
	for _, plan := range plans {
		expected, err := expectedSchemaShape([]SchemaTableDDL{plan}, dialect)
		if err != nil {
			return nil, err
		}
		if _, exists := tables[plan.TableName]; !exists {
			missing = append(missing, plan)
			continue
		}
		var actual schemaShape
		if dialect == SchemaDialectPostgres {
			actual, err = loadPostgresSchemaShape(ctx, q, expected)
		} else {
			actual, err = loadSQLiteSchemaShape(ctx, q, expected)
		}
		if err != nil {
			return nil, err
		}
		if drift := compareSchemaShapes(expected, actual); len(drift) > 0 {
			return nil, diagnostic.failure(generatedStateDrift(plan.TableName, drift))
		}
	}
	return missing, nil
}
