package store

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store/platformschema"
)

type SchemaTableDDL = platformschema.TableDDL

var (
	schemaDDLIdentifierPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	schemaDDLNumericPattern    = regexp.MustCompile(`^numeric\(\s*(\d+)\s*,\s*(\d+)\s*\)$`)
)

func SchemaFieldTypeToDDL(schemaType string) (string, error) {
	schemaType = strings.TrimSpace(schemaType)
	if schemaType == "" {
		return "", fmt.Errorf("schema type is required")
	}
	normalized := strings.ToLower(schemaType)
	if matches := schemaDDLNumericPattern.FindStringSubmatch(normalized); len(matches) == 3 {
		return fmt.Sprintf("NUMERIC(%s,%s)", matches[1], matches[2]), nil
	}
	if strings.HasPrefix(normalized, "numeric ") {
		return "", fmt.Errorf("%w %q", ErrUnknownSchemaType, schemaType)
	}
	if idx := strings.IndexAny(normalized, " \t\r\n"); idx >= 0 {
		normalized = strings.TrimSpace(normalized[:idx])
	}
	if strings.HasSuffix(normalized, "[]") {
		baseDDL, err := SchemaFieldTypeToDDL(strings.TrimSpace(strings.TrimSuffix(normalized, "[]")))
		if err != nil {
			return "", err
		}
		return baseDDL + "[]", nil
	}
	switch normalized {
	case "text", "string":
		return "TEXT", nil
	case "integer", "int", "bigint":
		return "BIGINT", nil
	case "float", "double", "real":
		return "DOUBLE PRECISION", nil
	case "numeric":
		return "NUMERIC", nil
	case "boolean":
		return "BOOLEAN", nil
	case "jsonb", "json":
		return "JSONB", nil
	case "timestamp", "timestamptz":
		return "TIMESTAMPTZ", nil
	case "uuid":
		return "UUID", nil
	}
	return "", fmt.Errorf("%w %q", ErrUnknownSchemaType, schemaType)
}

func NodeStateFieldTypeToDDL(schemaType string) (string, error) {
	normalized, err := runtimecontracts.NormalizeNodeStateFieldType(schemaType)
	if err != nil {
		return "", err
	}
	if _, ok := runtimecontracts.NodeStateNamedTypeName(normalized); ok {
		return "JSONB", nil
	}
	return SchemaFieldTypeToDDL(normalized)
}

func GeneratePlatformTableDDLs(spec runtimecontracts.PlatformSpecDocument) ([]SchemaTableDDL, error) {
	return platformschema.GeneratePlatformTableDDLs(spec)
}

func GenerateEntityTableDDLs(schema runtimecontracts.EntitySchema) ([]SchemaTableDDL, error) {
	if schema.Empty() {
		return nil, nil
	}
	groups := append([]runtimecontracts.EntitySchemaGroup{}, schema.Groups...)
	sort.Slice(groups, func(i, j int) bool {
		return strings.TrimSpace(groups[i].Name) < strings.TrimSpace(groups[j].Name)
	})
	plans := make([]SchemaTableDDL, 0, len(groups))
	seenTables := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		tableName, err := validateSchemaDDLIdentifier(group.Name, "entity schema group")
		if err != nil {
			return nil, err
		}
		if _, exists := seenTables[tableName]; exists {
			return nil, fmt.Errorf("entity schema group %q declares duplicate table %q", strings.TrimSpace(group.Name), tableName)
		}
		seenTables[tableName] = struct{}{}

		columnDefs := []string{
			`"entity_id" UUID PRIMARY KEY`,
			`"created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
			`"updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		}
		seenColumns := map[string]struct{}{
			"entity_id":  {},
			"created_at": {},
			"updated_at": {},
		}
		indexStatements := make([]string, 0, len(group.Fields))
		for _, field := range group.Fields {
			columnName, err := validateSchemaDDLIdentifier(field.Name, fmt.Sprintf("entity schema group %s field", tableName))
			if err != nil {
				return nil, err
			}
			if _, exists := seenColumns[columnName]; exists {
				if schemaDDLIsManagedEntityColumn(columnName) {
					continue
				}
				return nil, fmt.Errorf("entity schema group %s declares duplicate column %s", tableName, columnName)
			}
			seenColumns[columnName] = struct{}{}
			columnType, err := SchemaFieldTypeToDDL(field.Type)
			if err != nil {
				return nil, fmt.Errorf("entity schema group %s field %s: %w", tableName, columnName, err)
			}
			columnDef := fmt.Sprintf("%s %s", quoteIdent(columnName), columnType)
			if !field.Nullable || field.Primary {
				columnDef += " NOT NULL"
			}
			if field.Primary {
				columnDef += " UNIQUE"
			}
			columnDefs = append(columnDefs, columnDef)
			if field.Indexed {
				indexName := schemaDDLIndexName("idx", tableName, columnName)
				indexStatements = append(indexStatements,
					fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(%s)", quoteIdent(indexName), quoteIdent(tableName), quoteIdent(columnName)),
				)
			}
		}
		statements := []string{
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n    %s\n)", quoteIdent(tableName), strings.Join(columnDefs, ",\n    ")),
		}
		statements = append(statements, indexStatements...)
		plans = append(plans, SchemaTableDDL{
			TableName:   tableName,
			SchemaKind:  "entity_schema",
			ColumnCount: len(columnDefs),
			Statements:  statements,
		})
	}
	return plans, nil
}

func schemaDDLIsManagedEntityColumn(columnName string) bool {
	switch strings.TrimSpace(columnName) {
	case "entity_id", "created_at", "updated_at":
		return true
	default:
		return false
	}
}

func GenerateNodeStateTableDDLs(nodes map[string]runtimecontracts.SystemNodeContract) ([]SchemaTableDDL, error) {
	if len(nodes) == 0 {
		return nil, nil
	}
	nodeIDs := make([]string, 0, len(nodes))
	for nodeID := range nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	plans := make([]SchemaTableDDL, 0, len(nodeIDs))
	tableOwners := map[string]string{}
	for _, nodeID := range nodeIDs {
		node := nodes[nodeID]
		if strings.TrimSpace(node.StateTable) == "" {
			continue
		}
		tableName, err := validateSchemaDDLIdentifier(node.StateTable, fmt.Sprintf("node %s state_table", strings.TrimSpace(nodeID)))
		if err != nil {
			return nil, err
		}
		if owner, exists := tableOwners[tableName]; exists {
			return nil, fmt.Errorf("state table %s declared by multiple nodes: %s, %s", tableName, owner, strings.TrimSpace(nodeID))
		}
		tableOwners[tableName] = strings.TrimSpace(nodeID)

		columnDefs := []string{
			`"entity_id" UUID NOT NULL`,
			`"node_id" TEXT NOT NULL`,
			`"updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		}
		seenColumns := map[string]struct{}{
			"entity_id":  {},
			"node_id":    {},
			"updated_at": {},
		}
		for _, field := range node.StateSchema.Fields {
			columnName, err := validateSchemaDDLIdentifier(field.Name, fmt.Sprintf("node %s state_schema field", strings.TrimSpace(nodeID)))
			if err != nil {
				return nil, err
			}
			if _, exists := seenColumns[columnName]; exists {
				return nil, fmt.Errorf("node %s state table %s declares duplicate column %s", strings.TrimSpace(nodeID), tableName, columnName)
			}
			seenColumns[columnName] = struct{}{}
			columnType, err := NodeStateFieldTypeToDDL(field.Type)
			if err != nil {
				return nil, fmt.Errorf("node %s state table %s field %s: %w", strings.TrimSpace(nodeID), tableName, columnName, err)
			}
			columnDefs = append(columnDefs, fmt.Sprintf("%s %s", quoteIdent(columnName), columnType))
		}
		columnDefs = append(columnDefs, `PRIMARY KEY ("entity_id", "node_id")`)
		plans = append(plans, SchemaTableDDL{
			TableName:   tableName,
			SchemaKind:  "state_schema",
			ColumnCount: len(columnDefs) - 1,
			Statements: []string{
				fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n    %s\n)", quoteIdent(tableName), strings.Join(columnDefs, ",\n    ")),
			},
		})
	}
	return plans, nil
}

func FlattenSchemaTableDDLs(plans []SchemaTableDDL) []string {
	return platformschema.Flatten(plans)
}

func validateSchemaDDLIdentifier(name, context string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("%s identifier is required", strings.TrimSpace(context))
	}
	if !schemaDDLIdentifierPattern.MatchString(name) {
		return "", fmt.Errorf("%s identifier %q must match %s", strings.TrimSpace(context), name, schemaDDLIdentifierPattern.String())
	}
	return name, nil
}

func schemaDDLIndexName(parts ...string) string {
	raw := strings.Join(parts, "_")
	name := sanitizeSchemaIdent(raw)
	if name == "" {
		return "idx_generated"
	}
	return name
}

func QuoteIdent(v string) string {
	return quoteIdent(v)
}

func schemaDDLExtractTableName(statement string) string {
	return platformschema.ExtractTableName(statement)
}

func schemaDDLPlanColumnNames(plan SchemaTableDDL) []string {
	return platformschema.PlanColumnNames(plan)
}
