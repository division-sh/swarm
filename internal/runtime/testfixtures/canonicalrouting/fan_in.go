package canonicalrouting

import (
	"path/filepath"
	"testing"
)

// FanInNegativeMutation is the closed malformed fan-in matrix. Positive
// fixtures always originate from FanInStream or FanInBarrier.
type FanInNegativeMutation uint8

const (
	FanInMissingDedup FanInNegativeMutation = iota + 1
	FanInDedupTuple
	FanInMissingWindow
	FanInMissingSingleton
	FanInWrongSingleton
	FanInAccumulateDedupRedeclaration
	FanInAccumulateWindowRedeclaration
	FanInLegacyConnectMap
	FanInEventIDDedup
	FanInNonSingletonReceiver
	FanInMissingReceiverHandler
	FanInMissingRuntimeOwner
	FanInAmbiguousReceiverInput
	FanInAuthoredMembersBy
	FanInAuthoredWindowBy
	FanInMissingJoinRow
	FanInMultipleJoinRows
	FanInBarrierNoWindow
	FanInBarrierReentrantNoWindow
	FanInBarrierWithAccumulate
	FanInHandlerOnComplete
)

func ApplyFanInNegativeMutation(t testing.TB, root string, mutation FanInNegativeMutation) {
	t.Helper()
	packageFile := filepath.Join(root, "package.yaml")
	receiverSchema := filepath.Join(root, "flows", "portfolio", "default", "schema.yaml")
	receiverNodes := filepath.Join(root, "flows", "portfolio", "default", "nodes.yaml")
	switch mutation {
	case FanInMissingDedup:
		applyClosedReplacement(t, receiverSchema, "          dedup_by: payload.operating_id\n", "")
	case FanInDedupTuple:
		applyClosedReplacement(t, receiverSchema, "          dedup_by: payload.operating_id\n", "          dedup_by: [payload.operating_id, payload.period_id]\n")
	case FanInMissingWindow:
		applyClosedReplacement(t, receiverSchema, "          window: payload.period_id\n", "")
	case FanInMissingSingleton:
		applyClosedReplacement(t, receiverSchema, "          singleton: portfolio\n", "")
	case FanInWrongSingleton:
		applyClosedReplacement(t, receiverSchema, "          singleton: portfolio\n", "          singleton: treasury/default\n")
	case FanInAccumulateDedupRedeclaration:
		applyClosedReplacement(t, receiverNodes, "        from: payload\n", "        from: payload\n        dedup_by: payload.period_id\n")
	case FanInAccumulateWindowRedeclaration:
		applyClosedReplacement(t, receiverNodes, "        from: payload\n", "        from: payload\n        window: payload.operating_id\n")
	case FanInLegacyConnectMap:
		applyClosedReplacement(t, packageFile, "  - from: operating.operating_reported\n    to: portfolio.operating_reported\n", "  - from: operating.operating_reported\n    to: portfolio.operating_reported\n    map:\n      operating_id:\n        source: payload.operating_id\n        target: entity.operating_id\n")
	case FanInEventIDDedup:
		applyClosedReplacement(t, receiverSchema, "          dedup_by: payload.operating_id\n", "          dedup_by: event.id\n")
	case FanInNonSingletonReceiver:
		applyClosedReplacement(t, packageFile, "    mode: singleton\n", "    mode: static\n")
		applyClosedReplacement(t, receiverSchema, "mode: singleton\n", "mode: static\n")
	case FanInMissingReceiverHandler:
		applyClosedReplacement(t, receiverNodes, "  subscribes_to: [operating.reported]\n", "  subscribes_to: []\n")
		applyClosedReplacement(t, receiverNodes, "  event_handlers:\n    operating.reported:\n      accumulate:\n        into: operating_reports\n        from: payload\n      data_accumulation:\n        writes:\n          - source_field: revenue\n            target_field: last_revenue\n", "  event_handlers: {}\n")
	case FanInMissingRuntimeOwner:
		applyClosedReplacement(t, receiverNodes, "      accumulate:\n        into: operating_reports\n        from: payload\n", "      advances_to: active\n")
	case FanInAmbiguousReceiverInput:
		applyClosedReplacement(t, receiverSchema,
			"      - name: operating_reported\n        event: operating.reported\n",
			"      - name: operating_reported_duplicate\n        event: operating.reported\n        resolution:\n          mode: fan-in\n          aggregation: stream\n          window: payload.period_id\n          dedup_by: payload.operating_id\n          singleton: portfolio\n      - name: operating_reported\n        event: operating.reported\n")
	case FanInAuthoredMembersBy:
		applyClosedReplacement(t, receiverNodes, "          from: entity.expected_operating_ids\n", "          from: entity.expected_operating_ids\n          by: payload.operating_id\n")
	case FanInAuthoredWindowBy:
		applyClosedReplacement(t, receiverNodes, "          from: entity.period_id\n", "          from: entity.period_id\n          by: payload.period_id\n")
	case FanInMissingJoinRow:
		applyClosedReplacement(t, receiverNodes,
			"    operating.reported:\n      join:\n        stage: awaiting\n        members:\n          from: entity.expected_operating_ids\n        window:\n          from: entity.period_id\n        output: payload.revenue\n        on_complete:\n          advances_to: complete\n        timeout:\n          after: 5m\n          advances_to: failed\n",
			"    operating.reported:\n      advances_to: awaiting\n")
	case FanInMultipleJoinRows:
		t.Fatal("FanInMultipleJoinRows is a post-load semantic mutation; use ApplyFanInMultipleJoinRows")
	case FanInBarrierNoWindow:
		applyClosedReplacement(t, receiverSchema, "          window: payload.period_id\n", "")
		applyClosedReplacement(t, receiverNodes, "        window:\n          from: entity.period_id\n", "")
		applyClosedReplacement(t, receiverNodes, "      select_entity:\n        by:\n          portfolio_id: payload.portfolio_id\n", "      create_entity: true\n")
	case FanInBarrierReentrantNoWindow:
		applyClosedReplacement(t, receiverSchema, "          window: payload.period_id\n", "")
		applyClosedReplacement(t, receiverNodes, "        window:\n          from: entity.period_id\n", "")
		applyClosedReplacement(t, receiverNodes, "          advances_to: complete\n", "          advances_to: awaiting\n")
	case FanInBarrierWithAccumulate:
		applyClosedReplacement(t, receiverNodes,
			"    operating.reported:\n      join:\n",
			"    operating.reported:\n      accumulate:\n        into: operating_reports\n        from: payload\n      join:\n")
	case FanInHandlerOnComplete:
		applyClosedReplacement(t, receiverNodes,
			"      accumulate:\n        into: operating_reports\n        from: payload\n",
			"      accumulate:\n        into: operating_reports\n        from: payload\n      on_complete:\n        - advances_to: active\n")
	default:
		t.Fatalf("unsupported fan-in negative mutation %d", mutation)
	}
}
