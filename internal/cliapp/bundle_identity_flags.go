package cliapp

import (
	"fmt"
	"regexp"
	"strings"
)

func validateBundleHashArg(label, value string) (string, error) {
	value = strings.TrimSpace(value)
	if !cliBundleHashPattern.MatchString(value) {
		return "", fmt.Errorf("%s must match bundle-v2:sha256:<64 lowercase hex>", label)
	}
	return value, nil
}

var (
	cliBundleHashPattern = regexp.MustCompile(`^bundle-v2:sha256:[a-f0-9]{64}$`)
)
