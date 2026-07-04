package platform

import (
	"errors"
	"testing"
)

func TestValidateProductPlatformVersionAcceptsSupportedConstraintGrammar(t *testing.T) {
	t.Parallel()

	for _, declaredRange := range []string{
		">=0.7.0 <0.8.0",
		"0.7.0",
		"^0.7.0",
	} {
		if err := ValidateProductPlatformVersion(declaredRange, "0.7.0"); err != nil {
			t.Fatalf("ValidateProductPlatformVersion(%q, 0.7.0): %v", declaredRange, err)
		}
	}
}

func TestValidateProductPlatformVersionFailsClosed(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		declaredRange   string
		platformVersion string
		wantKind        CompatibilityErrorKind
	}{
		{
			name:            "missing range",
			platformVersion: "0.7.0",
			wantKind:        CompatibilityErrorMissingRange,
		},
		{
			name:            "invalid range",
			declaredRange:   "latest",
			platformVersion: "0.7.0",
			wantKind:        CompatibilityErrorInvalidRange,
		},
		{
			name:            "out of range",
			declaredRange:   ">=0.8.0",
			platformVersion: "0.7.0",
			wantKind:        CompatibilityErrorOutOfRange,
		},
		{
			name:          "missing platform",
			declaredRange: ">=0.7.0",
			wantKind:      CompatibilityErrorMissingPlatform,
		},
		{
			name:            "invalid platform",
			declaredRange:   ">=0.7.0",
			platformVersion: "dev",
			wantKind:        CompatibilityErrorInvalidPlatform,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateProductPlatformVersion(tc.declaredRange, tc.platformVersion)
			var compatibilityErr *CompatibilityError
			if !errors.As(err, &compatibilityErr) {
				t.Fatalf("error = %v, want CompatibilityError", err)
			}
			if compatibilityErr.Kind != tc.wantKind {
				t.Fatalf("CompatibilityError.Kind = %s, want %s", compatibilityErr.Kind, tc.wantKind)
			}
		})
	}
}
