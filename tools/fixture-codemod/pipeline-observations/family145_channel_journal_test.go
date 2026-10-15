package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var family145ChannelJournalCalls = map[string][]string{
	"TestConfiguredChannelRuntimeDispatchesImportedAgentDurablyAcrossSelectedStores": {"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)", "assertConfiguredChannelJournal(t, ctx, eventStore,", "dispatchFence.armed.Store(true)", "fencedOwner.Replace(", "close(dispatchFence.release)", "channelAckLossExecutor"},
	"assertConfiguredChannelJournal":                                                 {"storetest.CountRunNamedEventStorage(ctx, selected, runID, \"platform.activity_requested\")", "storetest.CountChannelSucceededToolAttempts(ctx, selected, runID, privateTool)", "storetest.ReadChannelFirstActivitySourceRoute(ctx, selected, runID)", "requests != wantRequests || attempts != wantAttempts", "route.Normalized()", "got != wantRoute", "json.Unmarshal(rawRoute, &route)"},
}

func family145ChannelJournalProofError(function, before, after string) error {
	if err := family145ConnectorProofError(function, before, after); err != nil {
		return err
	}
	for _, cut := range family145ChannelJournalCalls[function] {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("channel lost owner/lifetime: %s", cut)
		}
	}
	return nil
}

func TestFamily145ChannelJournalChainPreservesCountsRoutingAndLifetime(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.File != "internal/runtime/channel_runtime_supported_surface_test.go" {
			continue
		}
		cuts, ok := family145ChannelJournalCalls[row.Function]
		if !ok {
			continue
		}
		seen[row.Function] = true
		// Reuse the exact original behavioral-assertion owner. Channel-specific
		// required cuts are independently controlled below, not substituted for it.
		if err := family145ChannelJournalProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range cuts {
			if !strings.Contains(row.After, cut) {
				t.Fatalf("channel lost owner/lifetime: %s/%s", row.Function, cut)
			}
			if family145ChannelJournalProofError(row.Function, row.Before, strings.ReplaceAll(row.After, cut, "foreignCut(")) == nil {
				t.Fatalf("lost channel cut accepted: %s", cut)
			}
		}
		want, err := family145DescriptorBusinessAssertions(row.Before)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range want {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145ChannelJournalProofError(row.Function, row.Before, strings.ReplaceAll(row.After, condition, "false")) == nil {
				t.Fatalf("lost channel assertion accepted: %s/%s", row.Function, condition)
			}
		}
	}
	if len(seen) != len(family145ChannelJournalCalls) {
		t.Fatalf("channel chain incomplete: %d/%d", len(seen), len(family145ChannelJournalCalls))
	}
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "runtime", "channel_runtime_supported_surface_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := family145DescriptorReadinessEscape(data); err != nil {
		t.Fatal(err)
	}
}
