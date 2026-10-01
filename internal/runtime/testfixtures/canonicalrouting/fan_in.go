package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// ArrivalJoinNegativeMutation modifies the finite, instance-scoped join rather
// than manufacturing a retired receiver-pin owner.
type ArrivalJoinNegativeMutation uint8

const (
	ArrivalJoinMissingContributor ArrivalJoinNegativeMutation = iota + 1
	ArrivalJoinUnknownContributor
	ArrivalJoinScalarMembers
	ArrivalJoinRetiredWindow
	ArrivalJoinRetiredTimeout
	ArrivalJoinWithAccumulate
)

func ApplyArrivalJoinNegativeMutation(t testing.TB, root string, mutation ArrivalJoinNegativeMutation) {
	t.Helper()
	nodes := filepath.Join(root, "portfolio", "period", "nodes.yaml")
	switch mutation {
	case ArrivalJoinMissingContributor:
		applyClosedReplacement(t, nodes, "          by: payload.operating_id\n", "")
	case ArrivalJoinUnknownContributor:
		applyClosedReplacement(t, nodes, "          by: payload.operating_id\n", "          by: payload.unknown_member\n")
	case ArrivalJoinScalarMembers:
		applyClosedReplacement(t, nodes, "          from: state.expected_operating_ids\n", "          from: state.period_id\n")
	case ArrivalJoinRetiredWindow:
		applyClosedReplacement(t, nodes, "        output: payload.revenue\n", "        window: {from: state.period_id}\n        output: payload.revenue\n")
	case ArrivalJoinRetiredTimeout:
		applyClosedReplacement(t, nodes, "        deadline:\n          after: 5m\n          from: stage_entry\n        on_deadline:\n          advances_to: failed\n", "        timeout: {after: 5m, advances_to: failed}\n")
	case ArrivalJoinWithAccumulate:
		applyClosedReplacement(t, nodes, "    period.reported:\n      join:\n", "    period.reported:\n      accumulate: {into: operating_reports, from: payload, key: payload.operating_id}\n      join:\n")
	default:
		t.Fatalf("unsupported arrival join negative mutation %d", mutation)
	}
}

// CopyRetiredFanInPin is an explicitly unsupported grammar specimen. Its
// source remains available to prove rejection, not to route a positive case.
func CopyRetiredFanInPin(t testing.TB) string {
	t.Helper()
	root := CopyExample(t, FanInStream)
	applyClosedReplacement(t, filepath.Join(root, "portfolio", "schema.yaml"),
		"      - operating.reported\n",
		"      - event: operating.reported\n        resolution: {mode: fan-in, from: payload.ignored, aggregation: stream, window: payload.period_id, dedup_by: [payload.operating_id], singleton: portfolio}\n")
	return root
}

type RetiredFanInGrammar uint8

const (
	RetiredFanInPinMode RetiredFanInGrammar = iota + 1
	RetiredFanInAggregate
	RetiredFanInConnectMode
)

// CopyRetiredFanInGrammar materializes only a closed loader-refusal specimen.
func CopyRetiredFanInGrammar(t testing.TB, variant RetiredFanInGrammar) string {
	t.Helper()
	var name string
	switch variant {
	case RetiredFanInPinMode:
		name = "retired-pin/{mode: fan-in}"
	case RetiredFanInAggregate:
		name = "retired-pin/{mode: fan-in, aggregation: stream, window: payload.period_id, dedup_by: [payload.operating_id], singleton: portfolio}"
	case RetiredFanInConnectMode:
		name = "resolution: fan-in"
	default:
		t.Fatalf("unsupported retired fan-in grammar %d", variant)
	}
	for _, specimen := range ConnectionAdmissionCases() {
		if specimen.Name == name {
			root := t.TempDir()
			writeClosedNegativeFile(t, root, "schema.yaml", "name: retired-fan-in\n"+specimen.Source)
			return root
		}
	}
	t.Fatalf("retired fan-in grammar specimen %q is missing", name)
	return ""
}
