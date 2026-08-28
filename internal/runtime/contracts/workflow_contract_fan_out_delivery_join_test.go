package contracts

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const fanOutDeliveryElementID = "a1111111-1111-4111-8111-111111111111"

func TestFanOutDeliveryJoinStrictClosedGrammar(t *testing.T) {
	canonical := `
id: all-items-delivered
members:
  from_fan_out: ` + fanOutDeliveryElementID + `
on_complete:
  emit:
    event: batch.completed
`
	var admitted JoinSpec
	if err := yaml.Unmarshal([]byte(canonical), &admitted); err != nil {
		t.Fatalf("decode canonical fan-out delivery join: %v", err)
	}
	if admitted.Mode() != WorkflowJoinModeFanOutDelivery || admitted.Members.FromFanOut.String() != fanOutDeliveryElementID {
		t.Fatalf("canonical fan-out delivery join = %#v", admitted)
	}

	hostile := map[string]string{
		"missing explicit id":  strings.Replace(canonical, "id: all-items-delivered\n", "", 1),
		"missing on_complete":  strings.Replace(canonical, "on_complete:\n  emit:\n    event: batch.completed\n", "", 1),
		"empty on_complete":    strings.Replace(canonical, "on_complete:\n  emit:\n    event: batch.completed", "on_complete: {}", 1),
		"invalid fan-out id":   strings.Replace(canonical, fanOutDeliveryElementID, "fan-out-local-name", 1),
		"zero fan-out id":      strings.Replace(canonical, fanOutDeliveryElementID, "00000000-0000-0000-0000-000000000000", 1),
		"uppercase fan-out id": strings.Replace(canonical, fanOutDeliveryElementID, strings.ToUpper(fanOutDeliveryElementID), 1),
		"fake stage":           canonical + "stage: __fan_out_delivery__\n",
		"members from":         strings.Replace(canonical, "  from_fan_out: "+fanOutDeliveryElementID, "  from_fan_out: "+fanOutDeliveryElementID+"\n  from: entity.items", 1),
		"members by":           strings.Replace(canonical, "  from_fan_out: "+fanOutDeliveryElementID, "  from_fan_out: "+fanOutDeliveryElementID+"\n  by: payload.id", 1),
		"window":               canonical + "window:\n  from: entity.batch\n  by: payload.batch\n",
		"output":               canonical + "output: item\n",
		"complete_when":        canonical + "complete_when: join.completed == join.expected\n",
		"remaining":            canonical + "remaining: ignore\n",
		"timeout":              canonical + "timeout:\n  after: 1m\n  emit:\n    event: batch.timed_out\n",
	}
	for name, document := range hostile {
		t.Run(name, func(t *testing.T) {
			var spec JoinSpec
			if err := yaml.Unmarshal([]byte(document), &spec); err == nil {
				t.Fatalf("hostile fan-out delivery join decoded as %#v", spec)
			}
		})
	}
}

func TestFanOutDeliveryJoinHandlerIsolationRequiresExactPairedElement(t *testing.T) {
	canonical := `
fan_out:
  element_id: ` + fanOutDeliveryElementID + `
  items_from: payload.items
  as: candidate
  emit:
    event: item.requested
join:
  id: all-items-delivered
  members:
    from_fan_out: ` + fanOutDeliveryElementID + `
  on_complete:
    emit:
      event: batch.completed
`
	var handler SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(canonical), &handler); err != nil {
		t.Fatalf("decode paired fan-out delivery handler: %v", err)
	}
	if err := ValidateJoinHandlerIsolation(handler); err != nil {
		t.Fatalf("validate paired fan-out delivery handler: %v", err)
	}

	for name, document := range map[string]string{
		"mismatched element":        strings.Replace(canonical, "from_fan_out: "+fanOutDeliveryElementID, "from_fan_out: 22222222-2222-4222-8222-222222222222", 1),
		"missing fan-out":           strings.Replace(canonical, "fan_out:\n  element_id: "+fanOutDeliveryElementID+"\n  items_from: payload.items\n  as: candidate\n  emit:\n    event: item.requested\n", "", 1),
		"additional handler effect": canonical + "advances_to: complete\n",
	} {
		t.Run(name, func(t *testing.T) {
			var candidate SystemNodeEventHandler
			if err := yaml.Unmarshal([]byte(document), &candidate); err != nil {
				return
			}
			if err := ValidateJoinHandlerIsolation(candidate); err == nil {
				t.Fatalf("hostile paired handler passed isolation: %#v", candidate)
			}
		})
	}
}
