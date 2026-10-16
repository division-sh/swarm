package main

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelanchor"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channeldecision"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelintent"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/deliverystatus"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/eventname"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/workflowstate"
)

func TestFamily145ImmutableInverseDecodingKeepsAllSourceAndCallerRefusals(t *testing.T) {
	family145ImmutableInverseDecode(t, "channel-anchor", family145ChannelAnchorBytes, channelanchor.Decode)
	family145ImmutableInverseDecode(t, "event-name", family145EventNameBytes, eventname.Decode)
	family145ImmutableInverseDecode(t, "delivery-status", family145DeliveryStatusBytes, deliverystatus.Decode)
	family145ImmutableInverseDecode(t, "request-key", family145RequestKeyBytes, requestkey.Decode)
	family145ImmutableInverseDecode(t, "channel-intent", family145ChannelIntentBytes, channelintent.Decode)
	family145ImmutableInverseDecode(t, "channel-decision", family145ChannelDecisionBytes, channeldecision.Decode)
	family145ImmutableInverseDecode(t, "workflow-state", family145WorkflowStateBytes, workflowstate.Decode)
}

func family145ImmutableInverseDecode[P any](t *testing.T, name string, raw []byte, decode func([]byte) ([]P, error)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		pins, err := decode(raw)
		if err != nil || len(pins) == 0 {
			t.Fatal("authentic immutable input refused:", err)
		}
		original, err := json.Marshal(pins)
		if err != nil {
			t.Fatal(err)
		}
		reflect.ValueOf(pins).Index(0).FieldByName("After").SetString("foreign caller mutation")
		again, err := decode(bytes.Clone(raw))
		encoded, encodeErr := json.Marshal(again)
		if err != nil || encodeErr != nil || !bytes.Equal(original, encoded) {
			t.Fatal("caller mutated immutable decoded evidence:", err, encodeErr)
		}
		changed := bytes.Clone(raw)
		changed[len(changed)/2] ^= 1
		for _, mutant := range [][]byte{nil, changed, append(bytes.Clone(raw), ' ')} {
			if _, err := decode(mutant); err == nil {
				t.Fatal("warm decoding cache accepted missing or changed manifest bytes")
			}
		}
	})
}
