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
