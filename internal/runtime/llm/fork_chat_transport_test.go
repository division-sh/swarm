package llm

import (
	"context"
	"testing"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/effects"
)

func TestForkChatTransportFreezesDefinitionsAndExactAuthority(t *testing.T) {
	authority := testConversationForkAuthority()
	actor := models.AgentConfig{ID: "fork-agent"}
	ctx := models.WithActor(effects.WithAuthority(context.Background(), authority), actor)
	tools := []ToolDefinition{{Name: "inspect", Description: "snapshot only", Schema: map[string]any{"type": "object"}}}
	dispatch, err := newForkChatToolDispatch(ctx, &fakeToolExec{}, tools)
	if err != nil {
		t.Fatal(err)
	}
	tools[0].Description = "mutated"
	tools[0].Schema.(map[string]any)["type"] = "string"
	copy := dispatch.ToolDefinitionsForActor(actor)
	copy[0].Schema.(map[string]any)["type"] = "array"
	if got := dispatch.ToolDefinitionsForActor(actor)[0]; got.Description != "snapshot only" || got.Schema.(map[string]any)["type"] != "object" {
		t.Fatalf("sandbox definitions became mutable: %+v", got)
	}
	if dispatch.Validate(ctx) != nil {
		t.Fatal("exact sandbox authority refused")
	}
	for _, test := range []struct {
		name   string
		mutate func(*effects.Authority)
	}{
		{"fork", func(a *effects.Authority) { a.ForkChat.ForkID = "foreign" }},
		{"source", func(a *effects.Authority) { a.ForkChat.SourceRunID = "foreign" }},
		{"occurrence", func(a *effects.Authority) { a.ForkChat.RequestOccurrenceID = "foreign" }},
		{"owner", func(a *effects.Authority) { a.ExecutionOwner += "-foreign" }},
		{"fence", func(a *effects.Authority) { a.FenceGeneration++ }},
		{"lease", func(a *effects.Authority) { a.LeaseExpiresAt = a.LeaseExpiresAt.Add(time.Minute) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			foreign := authority
			test.mutate(&foreign)
			if dispatch.Validate(models.WithActor(effects.WithAuthority(context.Background(), foreign), actor)) == nil {
				t.Fatal("foreign sandbox authority admitted")
			}
		})
	}
	if dispatch.Validate(models.WithActor(effects.WithAuthority(context.Background(), authority), models.AgentConfig{ID: "foreign"})) == nil {
		t.Fatal("foreign actor admitted")
	}
	if _, err := dispatch.Execute(ctx, "unplanned", nil); err == nil {
		t.Fatal("unplanned tool admitted")
	}
}
