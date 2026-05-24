package store

import (
	"fmt"
	"strings"
)

type OutdatedSchemaError struct {
	SchemaKind     string
	TableName      string
	MissingColumns []string
	Cause          error
}

func (e *OutdatedSchemaError) Error() string {
	tableName := strings.TrimSpace(e.TableName)
	schemaKind := strings.TrimSpace(e.SchemaKind)
	if schemaKind == "" {
		schemaKind = "store"
	}
	missing := strings.Join(e.MissingColumns, ", ")
	if missing == "" {
		missing = "unknown"
	}
	if tableName == "" {
		return fmt.Sprintf("database schema is out of date for %s schema: missing required column(s): %s; this Swarm build cannot migrate the existing database schema automatically; use a fresh database or run an approved schema migration before starting Swarm", schemaKind, missing)
	}
	return fmt.Sprintf("database schema is out of date for %s table %s: missing required column(s): %s; this Swarm build cannot migrate the existing database schema automatically; use a fresh database or run an approved schema migration before starting Swarm", schemaKind, tableName, missing)
}

func (e *OutdatedSchemaError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
