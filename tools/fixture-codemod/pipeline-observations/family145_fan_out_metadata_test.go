package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145FanOutMetadataCuts = []string{
	"capsuleRaw, err := storetest.ReadFanOutRunCapsuleWire(p.ctx, p.selected.events, p.runID)",
	"history, err := storetest.ReadA2MapHistoryFootprintStorage(p.ctx, p.selected.events, p.runID)",
	"mutations, facts, revisions := history.Mutations, history.Facts, history.Revisions",
	"metadata, err := storetest.ReadFanOutRunClaimMetadataStorage(p.ctx, p.selected.events, p.runID)",
	"cursor, cardinality := metadata.Cursor, metadata.Cardinality",
	"status, owner, generation, lease, retained := metadata.Status, metadata.Owner, metadata.Generation, metadata.Lease, metadata.Capsule",
	"count, err := storetest.SetRetainedFanOutCapsuleWire(p.ctx, p.selected.events, p.runID, raw)",
	"t.Cleanup(func() {\n\t\t\t\t\t\tsetCapsule(t, capsuleRaw)\n\t\t\t\t\t})",
	"setCapsule(t, raw)",
	"storetest.SetRetainedFanOutCardinality(p.ctx, p.selected.events, p.runID, 3)",
	"storetest.SetRetainedFanOutCardinality(p.ctx, p.selected.events, p.runID, 4)",
}

func family145FanOutMetadataProofError(before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("retained metadata migration lost original projection/range, claim, no-mutation or refusal assertions: %v", err)
	}
	cursor := -1
	for _, cut := range family145FanOutMetadataCuts {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("metadata migration lost exact wire/claim destinations, read order or pre-fault restoration: %s", cut)
		}
		cursor = at
	}
	for _, old := range []string{"UPDATE fan_out_intents SET capsule", "UPDATE fan_out_intents SET cardinality", "SELECT capsule FROM fan_out_intents", "CAST(claim_generation AS TEXT)"} {
		if strings.Contains(after, old) {
			return fmt.Errorf("consumer retains a non-authoritative metadata interpreter: %s", old)
		}
	}
	return nil
}
