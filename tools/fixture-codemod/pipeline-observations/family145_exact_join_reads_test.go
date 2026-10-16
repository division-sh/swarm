package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145ExactJoinReadCuts = map[string][]string{
	"assertExactJoinDeliveryStatus": {
		"deadline := time.Now().Add(5 * time.Second)",
		"storage, err = storetest.ReadExactJoinNodeDeliveryStorage(ctx, selected.events, eventID, subscriberKey)",
		"if err == nil {",
		"status, failure = storage.Status, storage.Failure",
		"if err == nil && status == want",
		"time.Sleep(10 * time.Millisecond)",
		"runtimeLog, _ := storetest.ReadLatestWorkflowTimerRuntimeLogText(ctx, selected.events)",
	},
	"assertExactJoinDeliveryCount": {
		"count, err := storetest.CountTemplateNodeDelivery(ctx, selected.events, eventID, subscriberKey)",
	},
	"exactJoinOccurrenceEventID": {
		"deadline := time.Now().Add(5 * time.Second)",
		"if eventID, err := storetest.ReadWorkflowTimerEventIdentity(ctx, selected.events, runID, eventName); err == nil",
		"return eventID",
		"time.Sleep(10 * time.Millisecond)",
		`t.Fatalf("durable join occurrence event was not published")`,
	},
}

func family145ExactJoinReadProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("join helper changed exact status/count/error assertions: %s: %v", function, err)
	}
	cursor := -1
	for _, cut := range family145ExactJoinReadCuts[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("join helper changed original context, scope, deadline, poll or diagnostic cut: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "selected.db") || strings.Contains(after, "selected.postgres") || strings.Contains(after, "QueryRowContext") {
		return fmt.Errorf("join helper retained SQL/dialect authority: %s", function)
	}
	return nil
}
