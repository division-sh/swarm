package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145PublicationHistoryCuts = []string{
	"beforeReplay := []pipeline.WorkflowInstance{load(0), load(1)}",
	"beforeStorage, err := storetest.ReadRunPublicationHistoryStorage(ctx, selected.events, runID)",
	"eventsBefore, revisionsBefore := beforeStorage.Events, beforeStorage.Revisions",
	"for _, event := range replay",
	"bus.PublishAcknowledged(ctx, event)",
	"waitForGateRecoveryQuiescence(t, bus, ctx)",
	"afterStorage, err := storetest.ReadRunPublicationHistoryStorage(ctx, selected.events, runID)",
	"eventsAfter, revisionsAfter := afterStorage.Events, afterStorage.Revisions",
	"if eventsAfter != eventsBefore || revisionsAfter != revisionsBefore",
}

func family145PublicationHistoryProofError(before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("multi-until snapshot lost original replay/later-arm/history/receipt assertions: %v", err)
	}
	// The earlier journey also joins work; locate this exact replay window first.
	window := strings.Index(after, family145PublicationHistoryCuts[0])
	if window < 0 {
		return fmt.Errorf("original replay window disappeared")
	}
	after = after[window:]
	cursor := -1
	for _, cut := range family145PublicationHistoryCuts {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("multi-until snapshot changed original cut, selected owner, run, context or destination: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "QueryRow") || strings.Contains(after, "run_fork_revisions") {
		return fmt.Errorf("replay footprint retains its raw history interpreter")
	}
	return nil
}
