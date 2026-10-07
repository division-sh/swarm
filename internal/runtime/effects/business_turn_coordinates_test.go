package effects

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/google/uuid"
)

func TestBusinessTurnCoordinatesPreserveExactAuthority(t *testing.T) {
	for _, mode := range []string{"flow", "root", "foreign_actor", "selected", "selected_root", "selected_foreign_run", "probe"} {
		t.Run(mode, func(t *testing.T) {
			token := effectLifecycleToken(t, 1, "worker", 2)
			if mode == "root" || mode == "selected_root" {
				token.Identity.Route = agentidentity.RootRoute()
			}
			authority := NormalAgentAuthority(token, "owner", time.Now().UTC().Add(time.Minute))
			authority.Target = UsageTarget{Kind: UsageTargetAgentTurn, ID: uuid.NewString(), RunID: token.Identity.RunID, AgentID: token.AgentID,
				AgentIdentity: token.Identity, SessionID: uuid.NewString(), FlowInstance: token.Identity.FlowInstance()}
			if mode == "foreign_actor" {
				authority.Target.AgentIdentity.Name.AgentID = "other"
				authority.Target.AgentID = "other"
			}
			if mode == "selected" || mode == "selected_root" || mode == "selected_foreign_run" {
				authority.Kind = AuthoritySelectedContractFork
				authority.ID = uuid.NewString()
				authority.SelectedFork = SelectedContractForkAuthority{ExecutionID: authority.ID, ForkRunID: token.Identity.RunID, Generation: 1,
					AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container", ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config"}
				if mode == "selected_foreign_run" {
					authority.SelectedFork.ForkRunID = uuid.NewString()
				}
			}
			if mode == "probe" {
				authority.Kind = AuthorityStartupProbe
			}
			before := authority
			scope, instance, path, err := authority.BusinessTurnCoordinates()
			if mode == "foreign_actor" || mode == "selected_foreign_run" || mode == "probe" {
				if err == nil || scope != "" || instance != "" || path != "" {
					t.Fatalf("foreign authority projected business ownership: %s %s %s err=%v", scope, instance, path, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "root" || mode == "selected_root" {
				if scope != "." || instance != token.Identity.RunID || path != token.Identity.RunID {
					t.Fatal("root projection lost its constructed run coordinate")
				}
			} else if path != token.Identity.FlowInstance() {
				t.Fatal("projection changed the concrete child coordinate")
			}
			if authority.Target != before.Target || authority.Normal != before.Normal || authority.SelectedFork != before.SelectedFork {
				t.Fatal("projection rewrote execution or declaration evidence")
			}
		})
	}
}
