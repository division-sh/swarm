package schemastore

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	storeerrors "github.com/division-sh/swarm/internal/store/internal/storeerrors"
	"github.com/division-sh/swarm/internal/store/platformschema"
)

type SchemaTableDDL = platformschema.TableDDL

var ErrUnknownSchemaType = storeerrors.ErrUnknownSchemaType

func sanitizeSchemaIdent(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func quoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

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

func GenerateNodeStateTableDDLs(records []runtimecontracts.ScopedNodeRecord) ([]SchemaTableDDL, error) {
	if len(records) == 0 {
		return nil, nil
	}
	sort.Slice(records, func(i, j int) bool {
		left, _ := records[i].Identity()
		right, _ := records[j].Identity()
		return left.Key() < right.Key()
	})
	plans := make([]SchemaTableDDL, 0, len(records))
	tableOwners := map[string]string{}
	for _, record := range records {
		identity, err := record.Identity()
		if err != nil {
			return nil, fmt.Errorf("state schema has invalid executable node identity: %w", err)
		}
		nodeID := identity.Key()
		node := record.Entry
		if strings.TrimSpace(node.StateTable) == "" {
			continue
		}
		tableName, err := validateSchemaDDLIdentifier(node.StateTable, fmt.Sprintf("node %s state_table", nodeID))
		if err != nil {
			return nil, err
		}
		if owner, exists := tableOwners[tableName]; exists {
			return nil, fmt.Errorf("state table %s declared by multiple nodes: %s, %s", tableName, owner, strings.TrimSpace(nodeID))
		}
		tableOwners[tableName] = nodeID

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
