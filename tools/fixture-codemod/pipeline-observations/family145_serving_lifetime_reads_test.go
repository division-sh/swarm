package main

import (
	"fmt"
	"reflect"
	"strings"
)

var family145ServingLifetimeHelpers = map[string][]string{
	"readServingLifetimeState": {
		"selected notifyAllChildrenStore",
		"metadata, err := storetest.ReadServingLifetimeMetadataStorage(context.Background(), selected, runID)",
		"state.cursor, state.status, state.generation, state.retry = metadata.Cursor, metadata.Status, metadata.Generation, metadata.Retry",
		"storetest.CountNestedRunFanOutOutcomes(context.Background(), selected, runID)",
		"&state.outcomes",
		"storetest.CountPhysicalRunEvents(context.Background(), selected, runID)",
		"&state.events",
		"storetest.CountServingLifetimeHistoryStorage(context.Background(), selected, runID)",
		"&state.history",
		"count, err := query.read()",
		"*query.out = count",
		"return state",
	},
	"assertServingLifetimeClaimOwner": {
		"selected notifyAllChildrenStore",
		"owner, err := storetest.ReadServingLifetimeClaimOwnerStorage(context.Background(), selected, runID)",
		"if err != nil || owner != want",
		"t.Fatalf(\"exact claim cleanup authority: owner=%q want=%q err=%v\", owner, want, err)",
	},
	"waitServingMatrixTriggerReceipt": {
		"selected notifyAllChildrenStore",
		"deadline := time.Now().Add(5 * time.Second)",
		"outcome, err := storetest.ReadNestedPipelineReceiptOutcome(context.Background(), selected, eventID)",
		"if outcome != \"success\"",
		"if !errors.Is(err, sql.ErrNoRows) || time.Now().After(deadline)",
		"time.Sleep(time.Millisecond)",
	},
}

func family145ServingLifetimeReadProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("serving lifetime read lost exact claim/receipt/retirement/error/deadline assertions: %s %v", function, err)
	}
	cursor := -1
	for _, cut := range family145ServingLifetimeHelpers[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("serving read changed original independent cut/owner/run/event/destination: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "QueryRow") || strings.Contains(after, "*sql.DB") {
		return fmt.Errorf("shared serving observation still transports raw authority")
	}
	return nil
}
