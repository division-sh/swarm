package identity

import (
	"fmt"
	"regexp"
	"strings"
)

var bundleHashPattern = regexp.MustCompile(`^bundle-v2:sha256:[0-9a-f]{64}$`)

func IsCanonicalBundleHash(value string) bool {
	return value == strings.TrimSpace(value) && bundleHashPattern.MatchString(value)
}

func ValidateCanonicalBundleHash(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("bundle_hash must be non-empty")
	}
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("bundle_hash must not contain surrounding whitespace")
	}
	if !IsCanonicalBundleHash(value) {
		return fmt.Errorf("bundle_hash must be bundle-v2:sha256:<64 lowercase hex>")
	}
	return nil
}
