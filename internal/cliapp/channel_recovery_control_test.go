package cliapp

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/store/selected"
)

// Invariant: local resource lifetime refuses closed/canceled/foreign owners before store or SDK access.
func TestDeploymentRecoveryControlLifetimeRefusals(t *testing.T) {
	for _, kind := range []string{"closed", "canceled", "foreign"} {
		t.Run(kind, func(t *testing.T) {
			process := worklifetime.NewProcess()
			c := &DeploymentChannelControl{process: process, store: &selected.Owner{}, provider: selectedChannelFacts(channelRegistrationFixture{}, "whatsapp"), borrowed: true}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch kind {
			case "closed":
				if err := c.Close(ctx); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				cancel()
			case "foreign":
				ctx = worklifetime.WithProcess(ctx, worklifetime.NewProcess())
			}
			if err := c.ResumeRegisteredAccount(ctx, channelonboarding.AccountReference{Provider: "whatsapp", Account: "+100000000001"}, channelonboarding.Operation{}); err == nil {
				t.Fatal("invalid lifetime reached resume")
			}
			process.Retire()
			if _, err := process.Join(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
