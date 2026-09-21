package runforkreadiness

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSelectedContractReceiverConfigRejectsInvalidBusinessEvidence(t *testing.T) {
	for _, business := range []string{
		`{}`,
		`{"vertical_id":7}`,
		`{"vertical_id":null}`,
		`{"vertical_id":"recorded-business-key","undeclared":true}`,
	} {
		t.Run(business, func(t *testing.T) {
			req := templateAdmissionRequest(t)
			req.Plan.Entities[0].MaterializationMetadata.FlowConfig = json.RawMessage(`{"instance_id":"item","storage_ref":"consumer/item","flow_path":"consumer/item","config":` + business + `}`)
			admitted, err := Admit(req)
			if err == nil || !strings.Contains(err.Error(), "receiver configuration") {
				t.Fatalf("invalid business evidence acquired admission: %v", err)
			}
			if _, err := admitted.Projection(); err == nil {
				t.Fatal("rejected configuration retained a sealed projection")
			}
			if err := admitted.ValidateAgainst(req.Binding); err == nil {
				t.Fatal("rejected configuration retained admission authority")
			}
		})
	}
}
