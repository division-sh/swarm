package contracts

import (
	"fmt"
	"strings"
)

func catalogReferenceError(problem string) error {
	return NewExpectedShapeDiagnostic("contract_loader.invalid_type_reference", "", problem, "Declare a supported, nonrecursive catalog type.", ErrInvalidField)
}

func retiredCatalogListSpelling(typeRef string) error {
	typeRef = strings.TrimSpace(typeRef)
	if err := rejectAlternateListReference(typeRef, "catalog type"); err != nil {
		return err
	}
	var item string
	switch {
	case strings.HasSuffix(typeRef, "[]"):
		item = strings.TrimSpace(strings.TrimSuffix(typeRef, "[]"))
	case strings.HasPrefix(typeRef, "[]"):
		item = strings.TrimSpace(strings.TrimPrefix(typeRef, "[]"))
	default:
		return nil
	}
	if item == "" {
		item = "T"
	}
	if nodeStateNumericTypePattern.MatchString(item) {
		return catalogReferenceError(fmt.Sprintf("type reference %q is retired; use one plain block-sequence field value, for example amounts:\n  - %s (YAML [%s] has two items)", typeRef, item, item))
	}
	return catalogReferenceError(fmt.Sprintf("type reference %q is retired; use [%s]", typeRef, item))
}
