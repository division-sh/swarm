package contracts

import (
	"fmt"
	"math"
	"strings"
)

func validateValidationParam(context string, value any, policy PolicyDocument) error {
	switch typed := value.(type) {
	case nil:
		return fmt.Errorf("%w: %s must be a typed scalar or policy scalar reference", ErrInvalidField, context)
	case string:
		if ref, ok := strings.CutPrefix(strings.TrimSpace(typed), "policy."); ok {
			ref = strings.TrimSpace(ref)
			if ref == "" {
				return fmt.Errorf("%w: %s policy reference is missing a key", ErrInvalidField, context)
			}
			policyValue, exists := policy.Values[ref]
			if !exists {
				return fmt.Errorf("%w: %s references unknown policy scalar %q", ErrInvalidField, context, ref)
			}
			if !validationParamScalar(policyValue.Value) {
				return fmt.Errorf("%w: %s references non-scalar policy value %q", ErrInvalidField, context, ref)
			}
		}
		return nil
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		if !validationParamScalar(typed) {
			return fmt.Errorf("%w: %s must be finite", ErrInvalidField, context)
		}
		return nil
	default:
		return fmt.Errorf("%w: %s must be a typed scalar or policy scalar reference, got %T", ErrInvalidField, context, value)
	}
}

func validationParamScalar(value any) bool {
	switch typed := value.(type) {
	case float32:
		return !math.IsNaN(float64(typed)) && !math.IsInf(float64(typed), 0)
	case float64:
		return !math.IsNaN(typed) && !math.IsInf(typed, 0)
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	default:
		return false
	}
}
