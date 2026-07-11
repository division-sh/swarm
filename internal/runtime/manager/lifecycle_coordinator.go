package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type runtimeLifecyclePhase string

const (
	runtimeLifecycleStopped      runtimeLifecyclePhase = "stopped"
	runtimeLifecycleRunning      runtimeLifecyclePhase = "running"
	runtimeLifecycleShuttingDown runtimeLifecyclePhase = "shutting_down"
	runtimeLifecycleResetting    runtimeLifecyclePhase = "resetting"
)

type agentLifecycleCell struct {
	opMu           sync.Mutex
	epoch          int64
	generation     uint64
	phase          AgentLifecyclePhase
	configRevision string
	runMode        AgentRunMode
	cancel         context.CancelFunc
	done           chan struct{}
}

type agentLifecycleCoordinator struct {
	mu        sync.Mutex
	store     AgentLifecyclePersistence
	phase     runtimeLifecyclePhase
	runMode   AgentRunMode
	runCtx    context.Context
	cancelRun context.CancelFunc
	cells     map[string]*agentLifecycleCell
}

func newAgentLifecycleCoordinator(store AgentLifecyclePersistence) *agentLifecycleCoordinator {
	return &agentLifecycleCoordinator{
		store: store, phase: runtimeLifecycleStopped, runMode: AgentRunModeStopped,
		cells: map[string]*agentLifecycleCell{},
	}
}

func lifecycleConfigRevision(rec PersistedAgent) (string, error) {
	raw, err := json.Marshal(rec.Config)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func lifecycleRequestHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func (c *agentLifecycleCoordinator) register(ctx context.Context, rec PersistedAgent, persist bool) error {
	if c == nil {
		return fmt.Errorf("agent lifecycle coordinator is required")
	}
	agentID := strings.TrimSpace(rec.Config.ID)
	revision, err := lifecycleConfigRevision(rec)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.cells[agentID]; exists {
		return fmt.Errorf("%w: %s", ErrAgentAlreadyExists, agentID)
	}
	epoch := rec.LifecycleEpoch
	generation := rec.LifecycleGeneration
	phase := rec.LifecyclePhase
	mode := rec.LifecycleRunMode
	if epoch <= 0 {
		epoch = runtimebus.CurrentRuntimeEpoch()
	}
	if generation == 0 {
		generation = 1
	}
	if phase == "" {
		phase = AgentLifecycleRegistered
	}
	if mode == "" {
		mode = AgentRunModeStopped
	}
	if persist && c.store != nil {
		now := time.Now().UTC()
		_, err := c.store.CommitAgentLifecycleTransition(ctx, AgentLifecycleTransition{
			OperationID: uuid.NewString(), OperationKind: "spawn", AgentID: agentID, Trigger: "spawn",
			RequestHash: lifecycleRequestHash("spawn", agentID, revision), TargetEpoch: epoch,
			TargetGeneration: generation, TargetPhase: AgentLifecycleRegistered,
			ConfigRevision: revision, RunMode: AgentRunModeStopped, Agent: &rec, Now: now,
		})
		if err != nil {
			return err
		}
		phase = AgentLifecycleRegistered
		mode = AgentRunModeStopped
	}
	c.cells[agentID] = &agentLifecycleCell{epoch: epoch, generation: generation, phase: phase, configRevision: revision, runMode: mode}
	return nil
}

func (c *agentLifecycleCoordinator) unregisterLocal(agentID string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.cells, strings.TrimSpace(agentID))
	c.mu.Unlock()
}

func (c *agentLifecycleCoordinator) beginRun(parent context.Context, mode AgentRunMode) (context.Context, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase != runtimeLifecycleStopped {
		return c.runCtx, false
	}
	root := runtimebus.WithRuntimeEpoch(parent, runtimebus.CurrentRuntimeEpoch())
	c.runCtx, c.cancelRun = context.WithCancel(root)
	c.phase = runtimeLifecycleRunning
	c.runMode = mode
	return c.runCtx, true
}

func (c *agentLifecycleCoordinator) runSnapshot() (context.Context, AgentRunMode, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runCtx, c.runMode, c.phase == runtimeLifecycleRunning
}

func (c *agentLifecycleCoordinator) phaseSnapshot() runtimeLifecyclePhase {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase
}

func (c *agentLifecycleCoordinator) beginShutdownAdmission() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == runtimeLifecycleShuttingDown || c.phase == runtimeLifecycleResetting {
		return false
	}
	c.phase = runtimeLifecycleShuttingDown
	return true
}

func (c *agentLifecycleCoordinator) cancelShutdownWork() (context.Context, []<-chan struct{}) {
	c.mu.Lock()
	if c.cancelRun != nil {
		c.cancelRun()
		c.cancelRun = nil
	}
	done := make([]<-chan struct{}, 0, len(c.cells))
	for _, cell := range c.cells {
		if cell.cancel != nil {
			cell.cancel()
		}
		if cell.done != nil {
			done = append(done, cell.done)
		}
	}
	ctx := c.runCtx
	c.mu.Unlock()
	return ctx, done
}

func (c *agentLifecycleCoordinator) finishShutdown() {
	c.mu.Lock()
	c.phase = runtimeLifecycleStopped
	c.runMode = AgentRunModeStopped
	c.runCtx = nil
	c.cancelRun = nil
	c.mu.Unlock()
}

func (c *agentLifecycleCoordinator) beginReset() {
	c.mu.Lock()
	c.phase = runtimeLifecycleResetting
	c.mu.Unlock()
}

func (c *agentLifecycleCoordinator) finishReset() {
	c.mu.Lock()
	c.cells = map[string]*agentLifecycleCell{}
	c.phase = runtimeLifecycleStopped
	c.runMode = AgentRunModeStopped
	c.runCtx = nil
	c.cancelRun = nil
	c.mu.Unlock()
}

func (c *agentLifecycleCoordinator) replaceLoop(ctx context.Context, agentID, trigger, operationID string, rec *PersistedAgent) (context.Context, runtimeeffects.LifecycleToken, chan struct{}, error) {
	agentID = strings.TrimSpace(agentID)
	if operationID == "" {
		operationID = uuid.NewString()
	}
	c.mu.Lock()
	cell := c.cells[agentID]
	if cell == nil || cell.phase == AgentLifecycleTerminated || cell.phase == AgentLifecycleFailed {
		c.mu.Unlock()
		return nil, runtimeeffects.LifecycleToken{}, nil, fmt.Errorf("%w: %s", ErrAgentNotFound, agentID)
	}
	c.mu.Unlock()
	cell.opMu.Lock()
	defer cell.opMu.Unlock()

	c.mu.Lock()
	cell = c.cells[agentID]
	if cell == nil || cell.phase == AgentLifecycleTerminated || cell.phase == AgentLifecycleFailed {
		c.mu.Unlock()
		return nil, runtimeeffects.LifecycleToken{}, nil, fmt.Errorf("%w: %s", ErrAgentNotFound, agentID)
	}
	if c.phase == runtimeLifecycleShuttingDown || c.phase == runtimeLifecycleResetting {
		c.mu.Unlock()
		return nil, runtimeeffects.LifecycleToken{}, nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "agent-lifecycle", trigger, map[string]any{"agent_id": agentID})
	}
	previousEpoch, previousGeneration, previousPhase := cell.epoch, cell.generation, cell.phase
	previousDone, previousCancel := cell.done, cell.cancel
	runCtx, mode, running := c.runCtx, c.runMode, c.phase == runtimeLifecycleRunning
	nextEpoch := runtimebus.CurrentRuntimeEpoch()
	nextGeneration := previousGeneration + 1
	revision := cell.configRevision
	if rec != nil {
		var err error
		revision, err = lifecycleConfigRevision(*rec)
		if err != nil {
			return nil, runtimeeffects.LifecycleToken{}, nil, err
		}
	}
	targetPhase := AgentLifecycleRunning
	targetMode := mode
	if !running || runCtx == nil {
		targetPhase = AgentLifecycleRegistered
		targetMode = AgentRunModeStopped
	}
	result := AgentLifecycleTransitionResult{
		OperationID: operationID, AgentID: agentID,
		PreviousEpoch: previousEpoch, RuntimeEpoch: nextEpoch,
		PreviousGeneration: previousGeneration, Generation: nextGeneration,
		PreviousPhase: previousPhase, Phase: targetPhase, ConfigRevision: revision, RunMode: targetMode,
	}
	if c.store != nil {
		var err error
		result, err = c.store.CommitAgentLifecycleTransition(context.WithoutCancel(ctx), AgentLifecycleTransition{
			OperationID: operationID, OperationKind: trigger, RequestHash: lifecycleRequestHash(trigger, agentID, revision),
			AgentID: agentID, Trigger: trigger, ExpectedEpoch: previousEpoch, ExpectedGeneration: previousGeneration,
			ExpectedPhase: previousPhase, TargetEpoch: nextEpoch, TargetGeneration: nextGeneration,
			TargetPhase: targetPhase, ConfigRevision: revision, RunMode: targetMode, Agent: rec, Now: time.Now().UTC(),
		})
		if err != nil {
			c.mu.Unlock()
			return nil, runtimeeffects.LifecycleToken{}, nil, err
		}
	}
	if result.Replayed {
		if result.RuntimeEpoch != cell.epoch || result.Generation != cell.generation || result.Phase != cell.phase {
			c.mu.Unlock()
			return nil, runtimeeffects.LifecycleToken{}, nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_replay_projection_conflict", "agent-lifecycle", trigger, map[string]any{"agent_id": agentID, "operation_id": operationID})
		}
		token := runtimeeffects.LifecycleToken{RuntimeEpoch: cell.epoch, AgentID: agentID, Generation: cell.generation}
		c.mu.Unlock()
		return nil, token, nil, nil
	}
	cell.epoch, cell.generation, cell.phase, cell.configRevision, cell.runMode = result.RuntimeEpoch, result.Generation, result.Phase, result.ConfigRevision, result.RunMode
	cell.cancel, cell.done = nil, nil
	if previousCancel != nil {
		previousCancel()
	}
	c.mu.Unlock()
	if previousDone != nil {
		<-previousDone
	}
	if result.Phase != AgentLifecycleRunning {
		return nil, runtimeeffects.LifecycleToken{}, nil, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cell = c.cells[agentID]
	if cell == nil || cell.epoch != result.RuntimeEpoch || cell.generation != result.Generation || cell.phase != result.Phase {
		return nil, runtimeeffects.LifecycleToken{}, nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_transition_conflict", "agent-lifecycle", trigger, map[string]any{"agent_id": agentID})
	}
	loopCtx, cancel := context.WithCancel(runCtx)
	token := runtimeeffects.LifecycleToken{RuntimeEpoch: result.RuntimeEpoch, AgentID: agentID, Generation: result.Generation}
	loopCtx = runtimeeffects.WithLifecycleToken(loopCtx, token)
	if store, ok := c.store.(runtimeeffects.Store); ok && store != nil {
		loopCtx = runtimeeffects.WithController(loopCtx, runtimeeffects.NewController(store))
	}
	done := make(chan struct{})
	cell.cancel, cell.done = cancel, done
	return loopCtx, token, done, nil
}

func (c *agentLifecycleCoordinator) releaseLoop(token runtimeeffects.LifecycleToken, done chan struct{}) error {
	if c == nil {
		return nil
	}
	close(done)
	c.mu.Lock()
	cell := c.cells[token.AgentID]
	c.mu.Unlock()
	if cell == nil {
		return nil
	}
	cell.opMu.Lock()
	defer cell.opMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	cell = c.cells[token.AgentID]
	if cell == nil || cell.epoch != token.RuntimeEpoch || cell.generation != token.Generation || cell.done != done {
		return nil
	}
	if cell.phase == AgentLifecycleRunning && c.store != nil {
		_, err := c.store.CommitAgentLifecycleTransition(context.Background(), AgentLifecycleTransition{
			OperationID: uuid.NewString(), OperationKind: "self_release", RequestHash: lifecycleRequestHash("self_release", token.AgentID, cell.configRevision),
			AgentID: token.AgentID, Trigger: "self_release", ExpectedEpoch: cell.epoch, ExpectedGeneration: cell.generation,
			ExpectedPhase: cell.phase, TargetEpoch: cell.epoch, TargetGeneration: cell.generation,
			TargetPhase: AgentLifecycleRegistered, ConfigRevision: cell.configRevision, RunMode: AgentRunModeStopped, Now: time.Now().UTC(),
		})
		if err != nil {
			cell.phase = AgentLifecycleFailed
			cell.runMode = AgentRunModeStopped
			cell.cancel = nil
			cell.done = nil
			return fmt.Errorf("persist agent loop self-release: %w", err)
		}
		cell.phase = AgentLifecycleRegistered
		cell.runMode = AgentRunModeStopped
	} else if cell.phase == AgentLifecycleRunning {
		cell.phase = AgentLifecycleRegistered
		cell.runMode = AgentRunModeStopped
	}
	cell.cancel = nil
	cell.done = nil
	return nil
}

func (c *agentLifecycleCoordinator) terminate(ctx context.Context, agentID, trigger string, target AgentLifecyclePhase) error {
	agentID = strings.TrimSpace(agentID)
	c.mu.Lock()
	cell := c.cells[agentID]
	if cell == nil {
		c.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrAgentNotFound, agentID)
	}
	c.mu.Unlock()
	cell.opMu.Lock()
	defer cell.opMu.Unlock()
	c.mu.Lock()
	cell = c.cells[agentID]
	if cell == nil {
		c.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrAgentNotFound, agentID)
	}
	epoch, generation, phase, revision := cell.epoch, cell.generation, cell.phase, cell.configRevision
	done, cancel := cell.done, cell.cancel
	nextEpoch, nextGeneration := runtimebus.CurrentRuntimeEpoch(), generation+1
	if c.store != nil {
		result, err := c.store.CommitAgentLifecycleTransition(context.WithoutCancel(ctx), AgentLifecycleTransition{
			OperationID: uuid.NewString(), OperationKind: trigger, RequestHash: lifecycleRequestHash(trigger, agentID, revision),
			AgentID: agentID, Trigger: trigger, ExpectedEpoch: epoch, ExpectedGeneration: generation, ExpectedPhase: phase,
			TargetEpoch: nextEpoch, TargetGeneration: nextGeneration, TargetPhase: target,
			ConfigRevision: revision, RunMode: AgentRunModeStopped, Now: time.Now().UTC(),
		})
		if err != nil {
			c.mu.Unlock()
			return err
		}
		nextEpoch, nextGeneration = result.RuntimeEpoch, result.Generation
	}
	cell.epoch, cell.generation, cell.phase, cell.runMode = nextEpoch, nextGeneration, target, AgentRunModeStopped
	cell.cancel, cell.done = nil, nil
	if cancel != nil {
		cancel()
	}
	c.mu.Unlock()
	if done != nil {
		<-done
	}
	return nil
}

func (c *agentLifecycleCoordinator) token(agentID string) (runtimeeffects.LifecycleToken, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cell := c.cells[strings.TrimSpace(agentID)]
	if cell == nil || cell.phase != AgentLifecycleRunning {
		return runtimeeffects.LifecycleToken{}, false
	}
	return runtimeeffects.LifecycleToken{RuntimeEpoch: cell.epoch, AgentID: strings.TrimSpace(agentID), Generation: cell.generation}, true
}

func (c *agentLifecycleCoordinator) effectContext(ctx context.Context, agentID string) (context.Context, error) {
	// Lightweight managers used by isolated adapters do not own runtime loops.
	// Served runtimes always provide the selected lifecycle store.
	if c == nil || c.store == nil {
		return ctx, nil
	}
	token, ok := c.token(agentID)
	if !ok {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_generation_not_running", "agent-lifecycle", "bind_effect_context", map[string]any{"agent_id": strings.TrimSpace(agentID)})
	}
	store, ok := c.store.(runtimeeffects.Store)
	if !ok || store == nil {
		return nil, runtimefailures.New(runtimefailures.ClassLifecycleConflict, "lifecycle_effect_controller_missing", "agent-lifecycle", "bind_effect_context", map[string]any{"agent_id": strings.TrimSpace(agentID)})
	}
	ctx = runtimeeffects.WithLifecycleToken(ctx, token)
	return runtimeeffects.WithController(ctx, runtimeeffects.NewController(store)), nil
}
