package main

import (
	"strings"
)

func nativeObservabilityConsumerSignature(source string) string {
	return strings.ReplaceAll(source, "selected observabilityFixtureStore, db *sql.DB, dialect authoractivityfixture.Dialect", "selected observabilityFixtureStore")
}
