package eventrecord

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/google/uuid"
)

func inheritedFanOutRecord(t *testing.T) Record {
	t.Helper()
	record := validRecord(t)
	node, err := identity.AdmitExecutableNodeDeclaration(".", "scatter")
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := identity.AdmitDeclarationIdentity(".", "fan_out", "scatter/handlers/items.ready/fan_out")
	if err != nil {
		t.Fatal(err)
	}
	origin, err := events.NewInheritedFanOutOrigin(record.RunID, uuid.NewString(), uuid.NewString(), uuid.NewString(), declaration, "bundle-exact", "digest-exact", 0)
	if err != nil {
		t.Fatal(err)
	}
	record.Class, record.ProducedByType, record.ProducedBy = events.EventAdmissionInheritedFanOut, events.EventProducerNode, node.Key()
	record.InheritedFanOutOrigin, err = json.Marshal(origin)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestInheritedFanOutRecordExactRoundTripAndIdentity(t *testing.T) {
	record := inheritedFanOutRecord(t)
	decoded, err := record.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Event().ParentEventID() != "" {
		t.Fatal("readback invented ordinary causality")
	}
	origin, found := decoded.Event().InheritedFanOutOrigin()
	if !found || origin.RunID() != record.RunID || origin.Ordinal() != 0 {
		t.Fatal("readback lost exact ordinal-zero origin")
	}
	settlement, err := record.DecodeSettlement()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := FromAdmitted(decoded, settlement)
	if err != nil || !record.Equal(encoded) {
		t.Fatalf("origin changed through codec: %v", err)
	}
	other := record.Clone()
	other.InheritedFanOutOrigin = bytes.Replace(other.InheritedFanOutOrigin, []byte(`"ordinal":0`), []byte(`"ordinal":1`), 1)
	if record.Equal(other) || bytes.Equal(record.InheritedFanOutOrigin, other.InheritedFanOutOrigin) {
		t.Fatal("ordinal identity was omitted or aliased")
	}
}

func TestInheritedFanOutRecordRejectsHostileCodecFacts(t *testing.T) {
	for _, variant := range []string{"absent", "null", "missing_ordinal", "negative_ordinal", "unknown_field", "trailing", "wrong_run", "parent", "root_with_origin", "selected_with_origin", "non_node"} {
		t.Run(variant, func(t *testing.T) {
			record := inheritedFanOutRecord(t)
			switch variant {
			case "absent":
				record.InheritedFanOutOrigin = nil
			case "null":
				record.InheritedFanOutOrigin = []byte(`null`)
			case "missing_ordinal":
				record.InheritedFanOutOrigin = bytes.Replace(record.InheritedFanOutOrigin, []byte(`,"ordinal":0`), nil, 1)
			case "negative_ordinal":
				record.InheritedFanOutOrigin = bytes.Replace(record.InheritedFanOutOrigin, []byte(`"ordinal":0`), []byte(`"ordinal":-1`), 1)
			case "unknown_field":
				record.InheritedFanOutOrigin = append([]byte(`{"permission":true,`), record.InheritedFanOutOrigin[1:]...)
			case "trailing":
				record.InheritedFanOutOrigin = append(record.InheritedFanOutOrigin, []byte(` {}`)...)
			case "wrong_run":
				record.RunID = uuid.NewString()
			case "parent":
				record.SourceEventID = uuid.NewString()
			case "root_with_origin":
				record.Class, record.ProducedByType, record.ProducedBy = events.EventAdmissionRootIngress, events.EventProducerExternal, "ingress"
			case "selected_with_origin":
				record.Class = events.EventAdmissionSelectedForkReplay
			case "non_node":
				record.ProducedByType = events.EventProducerAgent
			}
			if _, err := record.Decode(); err == nil {
				t.Fatal("hostile origin record decoded")
			}
		})
	}
}
