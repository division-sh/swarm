package main

import (
	"regexp"
	"strings"
)

const cliBundleHashPrefix = "bundle-v1:"

var (
	cliBundleHashPattern        = regexp.MustCompile(`^bundle-v1:sha256:[a-f0-9]{64}$`)
	cliBundleFingerprintPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)

func cliLegacyFingerprintFromBundleHash(bundleHash string) string {
	return strings.TrimPrefix(strings.TrimSpace(bundleHash), cliBundleHashPrefix)
}
