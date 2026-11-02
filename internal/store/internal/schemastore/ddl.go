package schemastore

import (
	"fmt"
	"regexp"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	storeerrors "github.com/division-sh/swarm/internal/store/internal/storeerrors"
	"github.com/division-sh/swarm/internal/store/platformschema"
)

type SchemaTableDDL = platformschema.TableDDL

var ErrUnknownSchemaType = storeerrors.ErrUnknownSchemaType

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

func GeneratePlatformTableDDLs(spec runtimecontracts.PlatformSpecDocument) ([]SchemaTableDDL, error) {
	return platformschema.GeneratePlatformTableDDLs(spec)
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

func QuoteIdent(v string) string {
	return quoteIdent(v)
}
