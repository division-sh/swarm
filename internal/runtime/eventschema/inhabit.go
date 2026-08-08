package eventschema

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
	"github.com/google/uuid"
)

const (
	MaxInhabitationDepth        = 64
	MaxInhabitedCollectionItems = 1024
	MaxInhabitedStringRunes     = 64 * 1024
)

// InhabitationContext stamps deterministic witnesses without granting the
// generator any eligibility, routing, or mutation authority.
type InhabitationContext struct {
	Identity string
}

// InhabitDeterministically produces one bounded value from the schema
// vocabulary accepted by this package. Enum witness selection deliberately
// reads the admitted source order; canonical validation treats enum membership
// as order-insensitive and post-validates the selected value.
func InhabitDeterministically(schema map[string]any, context InhabitationContext) (any, error) {
	if schema == nil {
		return nil, fmt.Errorf("$: schema is required")
	}
	if strings.TrimSpace(context.Identity) == "" {
		return nil, fmt.Errorf("$: deterministic inhabitation identity is required")
	}
	accepted := CanonicalAcceptanceSchema(schema)
	value, err := inhabitSchemaValue(schema, accepted, context.Identity, "$", 0)
	if err != nil {
		return nil, err
	}
	if err := ValidateValueAgainstSchema(schema, value); err != nil {
		if hasPattern(accepted) {
			return nil, patternInhabitationError("$", accepted)
		}
		return nil, fmt.Errorf("$: generated value failed canonical validation: %w", err)
	}
	return value, nil
}

func inhabitSchemaValue(source, accepted map[string]any, identity, path string, depth int) (any, error) {
	if depth > MaxInhabitationDepth {
		return nil, fmt.Errorf("%s: schema exceeds maximum inhabitation depth %d", path, MaxInhabitationDepth)
	}
	if rawEnum, present := source["enum"]; present {
		values, ok := asArray(rawEnum)
		if !ok {
			return nil, fmt.Errorf("%s.enum: must be an array", path)
		}
		if len(values) == 0 {
			return nil, fmt.Errorf("%s.enum: must contain at least one value", path)
		}
		value, err := cloneSemanticJSON(values[0])
		if err != nil {
			return nil, fmt.Errorf("%s.enum[0]: %w", path, err)
		}
		if err := ValidateValueAgainstSchema(source, value); err != nil {
			if hasPattern(accepted) {
				return nil, patternInhabitationError(path, accepted)
			}
			return nil, fmt.Errorf("%s.enum[0]: first authored value is not accepted: %w", path, err)
		}
		return value, nil
	}

	typeName := strings.TrimSpace(asString(accepted["type"]))
	if typeName == "" {
		switch {
		case len(schemaProperties(accepted["properties"])) > 0 || len(exactRequiredList(accepted["required"])) > 0:
			typeName = "object"
		case accepted["items"] != nil:
			typeName = "array"
		default:
			return nil, fmt.Errorf("%s: schema has no deterministic inhabitant type", path)
		}
	}

	switch typeName {
	case "object":
		return inhabitObject(source, accepted, identity, path, depth)
	case "array":
		return inhabitArray(source, accepted, identity, path, depth)
	case "string":
		return inhabitString(accepted, identity, path)
	case "boolean":
		return false, nil
	case "number":
		return inhabitNumber(accepted, path, false)
	case "integer":
		return inhabitNumber(accepted, path, true)
	case "null":
		return nil, nil
	default:
		return nil, fmt.Errorf("%s: unsupported schema type %q", path, typeName)
	}
}

func inhabitObject(source, accepted map[string]any, identity, path string, depth int) (map[string]any, error) {
	sourceProperties := schemaProperties(source["properties"])
	acceptedProperties := schemaProperties(accepted["properties"])
	required := exactRequiredList(accepted["required"])
	sort.Strings(required)

	values := make(map[string]any, len(required))
	state := make(map[string]uint8, len(required))
	var generate func(string) error
	generate = func(name string) error {
		propertyPath := path + ".properties[" + name + "]"
		switch state[name] {
		case 1:
			return fmt.Errorf("%s: equality cycle detected", propertyPath)
		case 2:
			return nil
		}
		sourceProperty, sourceOK := sourceProperties[name]
		acceptedProperty, acceptedOK := acceptedProperties[name]
		if !sourceOK || !acceptedOK {
			return fmt.Errorf("%s: required property has no admitted schema", propertyPath)
		}
		state[name] = 1
		target := strings.TrimSpace(asString(acceptedProperty["x-swarm-equalTo"]))
		if target != "" {
			if _, ok := acceptedProperties[target]; !ok {
				return fmt.Errorf("%s: missing equality target %q", propertyPath, target)
			}
			if err := generate(target); err != nil {
				return err
			}
			copied, err := cloneSemanticJSON(values[target])
			if err != nil {
				return fmt.Errorf("%s: copy equality target %q: %w", propertyPath, target, err)
			}
			if err := ValidateValueAgainstSchema(sourceProperty, copied); err != nil {
				return fmt.Errorf("%s: equality target %q is incompatible: %w", propertyPath, target, err)
			}
			values[name] = copied
		} else {
			value, err := inhabitSchemaValue(sourceProperty, acceptedProperty, identity, propertyPath, depth+1)
			if err != nil {
				return err
			}
			values[name] = value
		}
		state[name] = 2
		return nil
	}

	for _, name := range required {
		if err := generate(name); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func inhabitArray(source, accepted map[string]any, identity, path string, depth int) ([]any, error) {
	minimum, err := nonNegativeIntegerConstraint(accepted, "minItems", path)
	if err != nil {
		return nil, err
	}
	maximum, maximumSet, err := optionalNonNegativeIntegerConstraint(accepted, "maxItems", path)
	if err != nil {
		return nil, err
	}
	if maximumSet && minimum > maximum {
		return nil, fmt.Errorf("%s: minItems %d exceeds maxItems %d", path, minimum, maximum)
	}
	if minimum > MaxInhabitedCollectionItems {
		return nil, fmt.Errorf("%s.minItems: %d exceeds generated-fixture limit %d; provide an explicit fixture", path, minimum, MaxInhabitedCollectionItems)
	}
	sourceItems, sourceOK := source["items"].(map[string]any)
	acceptedItems, acceptedOK := accepted["items"].(map[string]any)
	if !sourceOK || !acceptedOK {
		return nil, fmt.Errorf("%s.items: array item schema is required", path)
	}
	values := make([]any, 0, minimum)
	for index := 0; index < minimum; index++ {
		itemPath := fmt.Sprintf("%s.items[%d]", path, index)
		value, err := inhabitSchemaValue(sourceItems, acceptedItems, identity, itemPath, depth+1)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func inhabitString(schema map[string]any, identity, path string) (string, error) {
	minimum, err := nonNegativeIntegerConstraint(schema, "minLength", path)
	if err != nil {
		return "", err
	}
	maximum, maximumSet, err := optionalNonNegativeIntegerConstraint(schema, "maxLength", path)
	if err != nil {
		return "", err
	}
	if maximumSet && minimum > maximum {
		return "", fmt.Errorf("%s: minLength %d exceeds maxLength %d", path, minimum, maximum)
	}
	if minimum > MaxInhabitedStringRunes {
		return "", fmt.Errorf("%s.minLength: %d exceeds generated-fixture limit %d; provide an explicit fixture", path, minimum, MaxInhabitedStringRunes)
	}

	var value string
	switch strings.TrimSpace(asString(schema["format"])) {
	case "uuid":
		value = uuid.NewSHA1(uuid.NameSpaceURL, []byte(identity+"\x00"+path)).String()
	case "date-time":
		value = deterministicDateTime(identity, path, minimum)
	default:
		value = strings.Repeat("0", minimum)
	}
	length := utf8.RuneCountInString(value)
	if length < minimum || (maximumSet && length > maximum) {
		if hasPattern(schema) {
			return "", patternInhabitationError(path, schema)
		}
		return "", fmt.Errorf("%s: canonical string witness length %d does not satisfy [%d,%s]; provide an explicit fixture for this field", path, length, minimum, optionalBoundText(maximum, maximumSet))
	}
	if err := ValidateValueAgainstSchema(schema, value); err != nil {
		if hasPattern(schema) {
			return "", patternInhabitationError(path, schema)
		}
		return "", fmt.Errorf("%s: canonical string witness is not accepted: %w", path, err)
	}
	return value, nil
}

func inhabitNumber(schema map[string]any, path string, integer bool) (float64, error) {
	minimum, minimumSet, err := optionalNumberConstraint(schema, "minimum", path)
	if err != nil {
		return 0, err
	}
	maximum, maximumSet, err := optionalNumberConstraint(schema, "maximum", path)
	if err != nil {
		return 0, err
	}
	if minimumSet && maximumSet && minimum > maximum {
		return 0, fmt.Errorf("%s: minimum %v exceeds maximum %v", path, minimum, maximum)
	}
	value := float64(0)
	if minimumSet && value < minimum {
		value = minimum
	}
	if maximumSet && value > maximum {
		value = maximum
	}
	if integer {
		if minimumSet && value < math.Ceil(minimum) {
			value = math.Ceil(minimum)
		} else {
			value = math.Ceil(value)
		}
		if maximumSet && value > math.Floor(maximum) {
			value = math.Floor(maximum)
		}
		if (minimumSet && value < minimum) || (maximumSet && value > maximum) {
			return 0, fmt.Errorf("%s: numeric bounds contain no integer inhabitant", path)
		}
	}
	return value, nil
}

func deterministicDateTime(identity, path string, minimumLength int) string {
	sum := sha256.Sum256([]byte(identity + "\x00" + path))
	const thirtyYears = uint64(30 * 365 * 24 * 60 * 60)
	seconds := int64(binary.BigEndian.Uint64(sum[:8]) % thirtyYears)
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(seconds) * time.Second)
	const unqualifiedLength = len("2006-01-02T15:04:05Z")
	if minimumLength <= unqualifiedLength {
		return base.Format(time.RFC3339)
	}
	digits := minimumLength - unqualifiedLength - 1
	if digits < 1 {
		digits = 1
	}
	if digits > 9 {
		digits = 9
	}
	fraction := fmt.Sprintf("%09d", binary.BigEndian.Uint32(sum[8:12])%1_000_000_000)[:digits]
	return base.Format("2006-01-02T15:04:05") + "." + fraction + "Z"
}

func nonNegativeIntegerConstraint(schema map[string]any, key, path string) (int, error) {
	value, present, err := optionalNonNegativeIntegerConstraint(schema, key, path)
	if err != nil || !present {
		return value, err
	}
	return value, nil
}

func optionalNonNegativeIntegerConstraint(schema map[string]any, key, path string) (int, bool, error) {
	raw, present := schema[key]
	if !present {
		return 0, false, nil
	}
	value, ok := runtimesharedjson.AsFloat64(raw)
	if !ok || math.Trunc(value) != value || value < 0 || value > float64(math.MaxInt) {
		return 0, true, fmt.Errorf("%s.%s: must be a supported non-negative integer", path, key)
	}
	return int(value), true, nil
}

func optionalNumberConstraint(schema map[string]any, key, path string) (float64, bool, error) {
	raw, present := schema[key]
	if !present {
		return 0, false, nil
	}
	value, ok := runtimesharedjson.AsFloat64(raw)
	if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, true, fmt.Errorf("%s.%s: must be a finite supported JSON number", path, key)
	}
	return value, true, nil
}

func cloneSemanticJSON(value any) (any, error) {
	raw, err := canonicaljson.Bytes(value)
	if err != nil {
		return nil, fmt.Errorf("value is not semantic JSON: %w", err)
	}
	var cloned any
	if err := canonicaljson.DecodeInto(raw, &cloned); err != nil {
		return nil, fmt.Errorf("decode semantic JSON: %w", err)
	}
	return cloned, nil
}

func hasPattern(schema map[string]any) bool {
	pattern, ok := schema["pattern"].(string)
	return ok && pattern != ""
}

func patternInhabitationError(path string, schema map[string]any) error {
	return fmt.Errorf("%s: pattern %q has no canonical generated witness; provide an explicit fixture for this field", path, schema["pattern"])
}

func optionalBoundText(value int, present bool) string {
	if !present {
		return "unbounded"
	}
	return fmt.Sprint(value)
}
