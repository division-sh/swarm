package runtime

import (
	"testing"
	"time"

	"empireai/internal/models"
)

func TestBudgetExecutionScopeKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		actor models.AgentConfig
		want  string
	}{
		{
			name: "vertical scope wins",
			actor: models.AgentConfig{
				ID:         "agent-1",
				Mode:       "factory",
				VerticalID: "v-123",
			},
			want: "v-123",
		},
		{
			name: "factory agent gets per agent scope",
			actor: models.AgentConfig{
				ID:   "market-research-agent-shard-0",
				Mode: "factory",
			},
			want: "__factory_agent__:market-research-agent-shard-0",
		},
		{
			name: "factory mode normalized",
			actor: models.AgentConfig{
				ID:   "market-research-agent-shard-1",
				Mode: "Factory",
			},
			want: "__factory_agent__:market-research-agent-shard-1",
		},
		{
			name: "non factory empty scope",
			actor: models.AgentConfig{
				ID:   "empire-coordinator",
				Mode: "holding",
			},
			want: "",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := budgetExecutionScopeKey(tc.actor); got != tc.want {
				t.Fatalf("budgetExecutionScopeKey() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBudgetLockExecutionScopeFactoryAgentsDoNotSerialize(t *testing.T) {
	t.Parallel()

	tracker := &BudgetTracker{}
	lockA := budgetExecutionScopeKey(models.AgentConfig{
		ID:   "market-research-agent-shard-0",
		Mode: "factory",
	})
	lockB := budgetExecutionScopeKey(models.AgentConfig{
		ID:   "market-research-agent-shard-1",
		Mode: "factory",
	})

	unlockA := tracker.LockExecutionScope(lockA)

	// Different factory shard should acquire immediately.
	diffAcquired := make(chan struct{})
	go func() {
		unlock := tracker.LockExecutionScope(lockB)
		close(diffAcquired)
		unlock()
	}()
	select {
	case <-diffAcquired:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("different factory shard lock should not block")
	}

	// Same shard key should block until the original lock is released.
	sameAcquired := make(chan struct{})
	go func() {
		unlock := tracker.LockExecutionScope(lockA)
		close(sameAcquired)
		unlock()
	}()
	select {
	case <-sameAcquired:
		t.Fatal("same shard lock should block while held")
	case <-time.After(100 * time.Millisecond):
	}

	unlockA()

	select {
	case <-sameAcquired:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("same shard lock should acquire after release")
	}
}
