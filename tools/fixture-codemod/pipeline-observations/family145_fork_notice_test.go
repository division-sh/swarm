package main

import (
	_ "embed"
	"os"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/forknotice"
)

//go:embed family145-fork-notice-preimages.json
var family145ForkNoticeBytes []byte

func forkNoticePins(t *testing.T) []forknotice.Pin {
	t.Helper()
	pins, err := forknotice.Decode(family145ForkNoticeBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145ForkNoticeCallerInverse(function, current string) string {
	pins, err := forknotice.Decode(family145ForkNoticeBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return forknotice.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckForkNoticeWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range forkNoticePins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("fork notice escaped complete owner/caller rule:", file, function)
			}
			return true
		}
	}
	return false
}

func requireForkNoticePhysicalOwner(t *testing.T, pin forknotice.Pin) {
	t.Helper()
	const helper = `func readForkReceiverNoticeDomain(t *testing.T, reader receiverProofStateReader, runID string) []string {
	t.Helper()
	out, err := storetest.ReadForkReceiverNoticeStorage(context.Background(), reader, runID)
	if err != nil {
		t.Fatal(err)
	}
	return out
}`
	if family145ServedDebugNormalize(t, helper) != pin.After {
		t.Fatal("notice fixture restored independent SQL/decoding/construction")
	}
	raw, err := os.ReadFile("../../../internal/store/internal/mailboxpersistence/fork_notice_observation.go")
	if err != nil {
		t.Fatal(err)
	}
	owner := family145PublicA2CurrentDeclaration(t, raw, "ReadForkReceiverNoticeStorageTx")
	for _, cut := range []string{"`SELECT m.* FROM mailbox m JOIN events e ON e.event_id=m.source_event_id WHERE e.run_id=$1`", "rows.Columns()", "rows.Scan(pointers...)", "values[i] = string(raw)", "json.Marshal(values)", "sort.Strings(out)"} {
		if !strings.Contains(pin.Before, cut) || !strings.Contains(owner, cut) {
			t.Fatal("native notice owner lost original join/full row/bytes/order:", cut)
		}
	}
	for _, cut := range []string{"out := []string{}", "rows.Err()", "return nil, err", "defer rows.Close()"} {
		if !strings.Contains(owner, cut) {
			t.Fatal("native notice owner lost empty/refusal/row cleanup contract:", cut)
		}
	}
	bridge, err := os.ReadFile("../../../internal/store/internal/runtimepersistence/test_fork_notice_observation.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"validateSelectedForkStorageIdentity", "validateChannelObservationOwner", "readServedDeliveryObservation(ctx, selected,", "mailboxpersistence.ReadForkReceiverNoticeStorageTx"} {
		if !strings.Contains(string(bridge), cut) {
			t.Fatal("notice bypassed exact selected read boundary:", cut)
		}
	}
}

func forkNoticeHostiles(t *testing.T, pin forknotice.Pin) {
	t.Helper()
	pins := forkNoticePins(t)
	if forknotice.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || forknotice.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After || forknotice.Previous(nil, pin.File, pin.Function, pin.After) != pin.After {
		t.Fatal("notice inverse lost exact source or admitted foreign/missing rule")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"ReceiverStateReader", "reader", "ReadForkReceiverNoticeStorage", "runID", "reflect.DeepEqual", "seed.RunID", "siblingSeed.RunID", "frontier", "fork.ForkRunID", "release", "t.Fatal", "sourceNotices", "childNotices"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "foreignNoticeCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || forknotice.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("lost notice reader/identity/refusal/replay/lifetime cut accepted:", pin.Function, cut)
		}
	}
}
