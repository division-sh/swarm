package bundleidentity

import (
	"fmt"
	"regexp"
	"strings"
)

var bundleHashV1Pattern = regexp.MustCompile(`^bundle-v1:sha256:[0-9a-f]{64}$`)

func IsCanonicalHash(value string) bool {
	return value == strings.TrimSpace(value) && bundleHashV1Pattern.MatchString(value)
}

func ValidateCanonicalHash(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("bundle_hash must be non-empty")
	}
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("bundle_hash must not contain surrounding whitespace")
	}
	if !IsCanonicalHash(value) {
		return fmt.Errorf("bundle_hash must be bundle-v1:sha256:<64 lowercase hex>")
	}
	return nil
}
