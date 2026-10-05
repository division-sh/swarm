package runtimepersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

func readinessPlanFixtureHash(t testing.TB, raw string) string {
	t.Helper()
	var value any
	if err := canonicaljson.DecodePreservingNumberLexemes([]byte(raw), &value); err != nil {
		t.Fatal(err)
	}
	encoded, err := canonicaljson.MarshalPreservingNumberKinds(value)
	if err != nil {
		t.Fatal(err)
	}
	return canonicaljson.HashBytes(encoded)
}
