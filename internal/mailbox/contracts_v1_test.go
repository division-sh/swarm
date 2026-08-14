package mailbox

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
)

func TestV1DecisionSheetPreservesAuthoredFieldsAndBookkeepingAsSiblingMaps(t *testing.T) {
	detail := V1ItemDetail{
		DecisionSheet: &V1DecisionSheet{
			EntityContext: V1EntityContext{
				Available: true,
				Entity: &operatorread.OperatorEntityFull{
					Fields:      map[string]any{"activation": "manual"},
					Bookkeeping: map[string]any{"activation": "standing"},
					Gates:       map[string]bool{},
					Accumulated: map[string]any{},
				},
			},
		},
	}

	raw, err := json.Marshal(detail)
	if err != nil {
		t.Fatalf("marshal decision sheet: %v", err)
	}
	var decoded V1ItemDetail
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal decision sheet: %v", err)
	}
	if decoded.DecisionSheet == nil || decoded.DecisionSheet.EntityContext.Entity == nil {
		t.Fatalf("decision sheet entity context = %#v", decoded.DecisionSheet)
	}
	entity := decoded.DecisionSheet.EntityContext.Entity
	if entity.Fields["activation"] != "manual" || entity.Bookkeeping["activation"] != "standing" {
		t.Fatalf("decision sheet entity maps = fields:%#v bookkeeping:%#v", entity.Fields, entity.Bookkeeping)
	}
}
