package schemastore

import (
	"maps"
	"slices"
	"sync"
)

// Only the pure expected platform shape is reused. Every bootstrap still
// inspects the selected database, and generated state plans remain independent.
var postgresPlatformShape = platformShapeMemo{dialect: SchemaDialectPostgres}
var sqlitePlatformShape = platformShapeMemo{dialect: SchemaDialectSQLite}

type platformShapeMemo struct {
	mu      sync.Mutex
	dialect SchemaDialect
	plans   []SchemaTableDDL
	shape   schemaShape
}

func (m *platformShapeMemo) expected(plans []SchemaTableDDL) (schemaShape, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shape.Tables != nil && equalSchemaPlans(m.plans, plans) {
		return cloneSchemaShape(m.shape), nil
	}
	shape, err := expectedSchemaShape(plans, m.dialect)
	if err != nil {
		return schemaShape{}, err
	}
	// Retain at most one plan per dialect, with a bounded source size. Larger
	// valid plans are still derived and validated normally, just not retained.
	size := 0
	for _, plan := range plans {
		size += len(plan.TableName) + len(plan.SchemaKind) + 64
		for _, statement := range plan.Statements {
			size += len(statement) + 16
		}
	}
	if size <= 1<<20 {
		m.plans = slices.Clone(plans)
		for i := range m.plans {
			m.plans[i].Statements = slices.Clone(plans[i].Statements)
		}
		m.shape = cloneSchemaShape(shape)
	}
	return shape, nil
}

func equalSchemaPlans(left, right []SchemaTableDDL) bool {
	return slices.EqualFunc(left, right, func(a, b SchemaTableDDL) bool {
		return a.TableName == b.TableName && a.SchemaKind == b.SchemaKind &&
			a.ColumnCount == b.ColumnCount && slices.Equal(a.Statements, b.Statements)
	})
}

func cloneSchemaShape(shape schemaShape) schemaShape {
	out := schemaShape{Tables: make(map[string]schemaTableShape, len(shape.Tables))}
	for name, table := range shape.Tables {
		table.Columns = maps.Clone(table.Columns)
		for name, column := range table.Columns {
			column.Checks = slices.Clone(column.Checks)
			table.Columns[name] = column
		}
		table.Constraints = slices.Clone(table.Constraints)
		table.Indexes = maps.Clone(table.Indexes)
		out.Tables[name] = table
	}
	return out
}
