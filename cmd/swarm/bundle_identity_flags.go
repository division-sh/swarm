package main

import (
	"regexp"
)

var (
	cliBundleHashPattern        = regexp.MustCompile(`^bundle-v1:sha256:[a-f0-9]{64}$`)
	cliBundleFingerprintPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)
