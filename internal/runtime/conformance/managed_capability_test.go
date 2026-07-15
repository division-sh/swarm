package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/google/uuid"
)

func managedConformanceExecutionContext(t testing.TB, ctx context.Context, authorityID string) context.Context {
	t.Helper()
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		authorityID,
		1,
		"",
		"conformance-actors",
		"conformance-bundle",
		nil,
	)
	if err != nil {
		t.Fatalf("build conformance managed execution admission: %v", err)
	}
	return managedexecution.WithAdmission(ctx, admission)
}

func managedConformanceTurnRecord(t testing.TB, rec runtimellm.AgentTurnRecord) runtimellm.AgentTurnRecord {
	t.Helper()
	runtimeMode := "task"
	if rec.Memory.Enabled {
		runtimeMode = "session"
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorID: rec.AgentID, RuntimeMode: runtimeMode, Provider: "conformance", Transport: "api", ProviderContract: "conformance-provider-contract",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: uuid.NewString(), ExecutionKind: managedcapabilities.ExecutionNormalAgent,
			ExecutionAuthorityID: "conformance-persistence", RunID: rec.RunID, SessionID: rec.SessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build conformance managed capability surface: %v", err)
	}
	rec.CapabilitySurface = &surface
	return rec
}
