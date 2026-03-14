package commgraph

import (
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/semanticview"
)

func TestRegistryExportHelpers_ReturnCopies(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"reviewer": {
				Role:       "reviewer",
				ToolsTier2: []string{"message_domain", "human_task_decide"},
				EmitEvents: []string{"review.completed"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"board.review_requested": {EmitterType: "board"},
			"review.completed":       {EmitterType: "agent"},
		},
	})
	SetContractSource(source)
	SetDefaultPolicyFactory(func() Policy {
		return NewSourcePolicy(source)
	})
	t.Cleanup(func() {
		SetContractSource(nil)
		SetDefaultPolicyFactory(func() Policy {
			return NewGenericTestPolicy()
		})
	})

	runtimeEvents := RuntimeEvents()
	humanEvents := HumanEvents()
	if len(runtimeEvents) == 0 || len(humanEvents) == 0 {
		t.Fatalf("expected non-empty event registries")
	}
	runtimeEvents[0] = "mutated.runtime"
	humanEvents[0] = "mutated.human"
	if RuntimeEvents()[0] == "mutated.runtime" {
		t.Fatal("RuntimeEvents should return a defensive copy")
	}
	if HumanEvents()[0] == "mutated.human" {
		t.Fatal("HumanEvents should return a defensive copy")
	}

	auth := MessageAuthorities()
	if len(auth) == 0 {
		t.Fatal("expected message authority registry entries")
	}
	origSender := auth[0].SenderRole
	auth[0].SenderRole = "mutated"
	if MessageAuthorities()[0].SenderRole != origSender {
		t.Fatal("MessageAuthorities should return a defensive copy")
	}

	roundTrips := MailboxRoundTrips()
	if len(roundTrips) > 0 {
		origType := roundTrips[0].MailboxType
		roundTrips[0].MailboxType = "mutated"
		if MailboxRoundTrips()[0].MailboxType != origType {
			t.Fatal("MailboxRoundTrips should return a defensive copy")
		}
	}

	humanTaskRoles := HumanTaskDecisionRoles()
	if len(humanTaskRoles) > 0 {
		origRole := humanTaskRoles[0]
		humanTaskRoles[0] = "mutated"
		if HumanTaskDecisionRoles()[0] != origRole {
			t.Fatal("HumanTaskDecisionRoles should return a defensive copy")
		}
	}

	routing := RoutingAuthorities()
	if len(routing) > 0 {
		origRoutingRole := routing[0].ActorRole
		routing[0].ActorRole = "mutated"
		if RoutingAuthorities()[0].ActorRole != origRoutingRole {
			t.Fatal("RoutingAuthorities should return a defensive copy")
		}
	}

	management := ManagementAuthorities()
	if len(management) > 0 {
		origManagementRole := management[0].ActorRole
		management[0].ActorRole = "mutated"
		if ManagementAuthorities()[0].ActorRole != origManagementRole {
			t.Fatal("ManagementAuthorities should return a defensive copy")
		}
	}

	mailboxRoles := MailboxSendRoles()
	if len(mailboxRoles) > 0 {
		origMailboxRole := mailboxRoles[0]
		mailboxRoles[0] = "mutated"
		if MailboxSendRoles()[0] != origMailboxRole {
			t.Fatal("MailboxSendRoles should return a defensive copy")
		}
	}
}
