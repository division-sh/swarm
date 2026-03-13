package testcases

import (
	"testing"

	"empireai/internal/commgraph"
	models "empireai/internal/runtime/core/actors"
)

func TestGenericBundle_AuthorizationMatrix(t *testing.T) {
	commgraph.SetDefaultPolicyFactory(func() commgraph.Policy {
		return commgraph.NewGenericTestPolicy()
	})

	controlPlane := models.AgentConfig{ID: "control-plane", Role: "control-plane"}
	reviewer := models.AgentConfig{ID: "reviewer", Role: "reviewer", EntityID: "item-123"}
	worker := models.AgentConfig{ID: "worker-a", Role: "worker", EntityID: "item-123"}
	otherWorker := models.AgentConfig{ID: "worker-b", Role: "worker", EntityID: "item-999"}

	if !commgraph.HasMessageAuthority(controlPlane, reviewer) {
		t.Fatal("expected control-plane to message reviewer")
	}
	if commgraph.HasMessageAuthority(worker, reviewer) {
		t.Fatal("expected worker messaging to be denied by default policy")
	}
	if err := commgraph.AuthorizeRouting(controlPlane, worker, "active"); err != nil {
		t.Fatalf("expected control-plane routing authority: %v", err)
	}
	if err := commgraph.AuthorizeRouting(reviewer, worker, "active"); err == nil {
		t.Fatal("expected reviewer routing to be constrained by status")
	}
	if err := commgraph.AuthorizeManagement(controlPlane, worker.Role, otherWorker.EntityID); err != nil {
		t.Fatalf("expected control-plane cross-scope management: %v", err)
	}
	if err := commgraph.AuthorizeManagement(reviewer, worker.Role, otherWorker.EntityID); err == nil {
		t.Fatal("expected reviewer cross-scope management denial")
	}
	if err := commgraph.AuthorizeMailboxSend(reviewer); err != nil {
		t.Fatalf("expected reviewer mailbox permission: %v", err)
	}
	if err := commgraph.AuthorizeMailboxSend(worker); err == nil {
		t.Fatal("expected worker mailbox permission to be denied")
	}
}
