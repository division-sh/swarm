package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimemcp "swarm/internal/runtime/mcp"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/runtime/sessions"
	workspace "swarm/internal/runtime/workspace"
)

type traceRunCanceller interface {
	CancelActiveTraceWorkByProducer(ctx context.Context, producerID string) ([]string, error)
}

func (am *AgentManager) RestartAgent(agentID string) error {
	am.mu.RLock()
	agent, ok := am.agents[agentID]
	am.mu.RUnlock()
	if !ok {
		return fmt.Errorf("agent not found: %s", agentID)
	}

	am.runMu.Lock()
	if cancel, ok := am.loopCancel[agentID]; ok {
		cancel()
		delete(am.loopCancel, agentID)
	}
	ctx := am.runCtx
	running := am.running
	am.runMu.Unlock()

	if running {
		am.startAgentLoop(ctx, agent)
	}
	return nil
}

func (am *AgentManager) Shutdown() error {
	am.runMu.Lock()
	if am.cancelRun != nil {
		am.cancelRun()
		am.cancelRun = nil
	}
	for id, cancel := range am.loopCancel {
		cancel()
		delete(am.loopCancel, id)
	}
	am.running = false
	am.runMu.Unlock()

	done := make(chan struct{})
	go func() {
		am.runWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(managerShutdownTimeout):
		return fmt.Errorf("agent manager shutdown timed out after %s", managerShutdownTimeout)
	}
}

func (am *AgentManager) Count() int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return len(am.agents)
}

func (am *AgentManager) IsRunning() bool {
	am.runMu.Lock()
	defer am.runMu.Unlock()
	return am.running
}

func (am *AgentManager) GetAgentConfig(agentID string) (runtimeactors.AgentConfig, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	cfg, ok := am.agentCfg[agentID]
	return cfg, ok
}

func (am *AgentManager) poisonKey(agentID, eventID string) string {
	return strings.TrimSpace(agentID) + "|" + strings.TrimSpace(eventID)
}

func (am *AgentManager) incrementPoisonPanicCount(agentID, eventID string) int {
	key := am.poisonKey(agentID, eventID)
	am.poisonMu.Lock()
	defer am.poisonMu.Unlock()
	am.poisonPanicCounts[key]++
	return am.poisonPanicCounts[key]
}

func (am *AgentManager) clearPoisonPanicCount(agentID, eventID string) {
	key := am.poisonKey(agentID, eventID)
	am.poisonMu.Lock()
	defer am.poisonMu.Unlock()
	delete(am.poisonPanicCounts, key)
}

func (am *AgentManager) quarantinePoisonEvent(ctx context.Context, agentID string, evt events.Event, count int, panicText string) {
	am.writeReceipt(ctx, evt.ID, agentID, ReceiptStatusProcessed, fmt.Sprintf("quarantined poison event after %d panics: %s", count, strings.TrimSpace(panicText)))
	affectedCount, shouldEmit := am.recordPoisonQuarantine(strings.TrimSpace(string(evt.Type)), evt.EntityID())
	if !shouldEmit {
		return
	}
	payload := map[string]any{
		"event_name":            strings.TrimSpace(string(evt.Type)),
		"quarantine_reason":     fmt.Sprintf("event quarantined after repeated panics across %d entities", affectedCount),
		"affected_entity_count": affectedCount,
		"sample_error":          strings.TrimSpace(panicText),
		"timestamp":             time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := am.bus.Publish(am.runtimeContext(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("platform.event_quarantined"),
		SourceAgent: "runtime",
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		if am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "publish_event_quarantined_failed",
				EventID:   strings.TrimSpace(evt.ID),
				EventType: strings.TrimSpace(string(evt.Type)),
				AgentID:   strings.TrimSpace(agentID),
				EntityID:  strings.TrimSpace(evt.EntityID()),
				Error:     strings.TrimSpace(err.Error()),
			})
		}
	}
}

func (am *AgentManager) recordPoisonQuarantine(eventName, entityID string) (int, bool) {
	eventName = strings.TrimSpace(eventName)
	entityID = strings.TrimSpace(entityID)
	if eventName == "" {
		return 0, false
	}
	if entityID == "" {
		entityID = "__unknown__"
	}
	am.poisonMu.Lock()
	defer am.poisonMu.Unlock()
	if am.poisonEventEntities[eventName] == nil {
		am.poisonEventEntities[eventName] = map[string]struct{}{}
	}
	am.poisonEventEntities[eventName][entityID] = struct{}{}
	affectedCount := len(am.poisonEventEntities[eventName])
	if affectedCount < poisonEventEntityThreshold {
		return affectedCount, false
	}
	if am.poisonEventEmitted[eventName] {
		return affectedCount, false
	}
	am.poisonEventEmitted[eventName] = true
	return affectedCount, true
}

func deterministicOutputEventID(inbound events.Event, agentID string, index int, out events.Event) string {
	return DeterministicOutputEventID(inbound, agentID, index, out)
}

func (am *AgentManager) defaultManagerAgentID(cfg runtimeactors.AgentConfig) string {
	if managerID := normalizedManagerFallback(cfg, managerFallbackFromConfig(cfg)); managerID != "" {
		return managerID
	}
	if source := runtimepipeline.DefaultWorkflowSemanticSourceOrNil(); source != nil {
		if _, entry, ok := semanticview.ResolveAgentRegistryEntry(source, cfg); ok {
			if managerID := normalizedManagerFallback(cfg, strings.TrimSpace(entry.ManagerFallback)); managerID != "" {
				return managerID
			}
		}
	}
	return ""
}

func managerFallbackFromConfig(cfg runtimeactors.AgentConfig) string {
	if len(cfg.Config) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(cfg.Config, &payload); err != nil {
		return ""
	}
	if value, ok := payload["manager_fallback"].(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}

func normalizedManagerFallback(cfg runtimeactors.AgentConfig, managerID string) string {
	managerID = strings.TrimSpace(managerID)
	if managerID == "" {
		return ""
	}
	return managerID
}

func flowPathFromAgentConfig(cfg runtimeactors.AgentConfig) string {
	if len(cfg.Config) == 0 {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal(cfg.Config, &payload); err != nil {
		return ""
	}
	if value, ok := payload["flow_path"].(string); ok {
		return strings.Trim(strings.TrimSpace(value), "/")
	}
	return ""
}

type eventExistenceReader interface {
	EventExists(ctx context.Context, eventID string) (bool, error)
}

func (am *AgentManager) shouldSkipAlreadyPublishedOutput(ctx context.Context, eventID string) bool {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" || am.store == nil {
		return false
	}
	reader, ok := am.store.(eventExistenceReader)
	if !ok {
		return false
	}
	exists, err := reader.EventExists(ctx, eventID)
	if err != nil {
		return false
	}
	return exists
}

func (am *AgentManager) safeProcessEvent(ctx context.Context, agent Agent, evt events.Event) (err error, panicked bool, panicText string, stackTrace string) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			panicText = fmt.Sprint(r)
			stackTrace = strings.TrimSpace(string(debug.Stack()))
		}
	}()
	err = am.processEvent(ctx, agent, evt)
	return
}

func (am *AgentManager) ChatWithAgent(ctx context.Context, agentID, directive string, killPrevious bool) (string, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return "", errors.New("agent id is required")
	}
	am.mu.RLock()
	agent, ok := am.agents[agentID]
	am.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("agent not found: %s", agentID)
	}
	chatAgent, ok := agent.(BoardInteractiveAgent)
	if !ok {
		return "", fmt.Errorf("agent does not support board chat: %s", agentID)
	}
	if killPrevious {
		if err := am.killPreviousRuns(ctx, agentID); err != nil {
			return "", err
		}
	}
	return chatAgent.BoardStep(ctx, directive)
}

func (am *AgentManager) killPreviousRuns(ctx context.Context, producerID string) error {
	producerID = strings.TrimSpace(producerID)
	if producerID == "" {
		return nil
	}
	canceller, ok := am.store.(traceRunCanceller)
	if !ok || canceller == nil {
		return nil
	}
	affectedAgents, err := canceller.CancelActiveTraceWorkByProducer(ctx, producerID)
	if err != nil {
		return fmt.Errorf("cancel previous traces for %s: %w", producerID, err)
	}
	if killer, ok := am.workspaces.(workspace.OrphanKiller); ok && killer != nil {
		if err := killer.KillOrphanProcesses(am.runtimeContext()); err != nil {
			log.Printf("kill previous workspace orphan processes failed: %v", err)
			if am.bus != nil {
				am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "kill_previous_orphan_kill_failed",
					AgentID:   producerID,
					Error:     strings.TrimSpace(err.Error()),
				})
			}
		}
	}
	seen := make(map[string]struct{}, len(affectedAgents))
	for _, raw := range affectedAgents {
		agentID := strings.TrimSpace(raw)
		if agentID == "" || agentID == producerID {
			continue
		}
		if _, ok := seen[agentID]; ok {
			continue
		}
		seen[agentID] = struct{}{}
		if err := am.RestartAgent(agentID); err != nil {
			log.Printf("restart agent after kill_previous failed agent=%s err=%v", agentID, err)
			if am.bus != nil {
				am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "kill_previous_restart_failed",
					AgentID:   agentID,
					Error:     strings.TrimSpace(err.Error()),
					Detail: map[string]any{
						"producer_id": producerID,
					},
				})
			}
		}
	}
	return nil
}

func (am *AgentManager) Run(ctx context.Context) {
	am.runMu.Lock()
	if am.running {
		am.runMu.Unlock()
		return
	}
	runRoot := runtimebus.WithRuntimeEpoch(ctx, runtimebus.CurrentRuntimeEpoch())
	am.runCtx, am.cancelRun = context.WithCancel(runRoot)
	am.running = true
	am.authBreakerTripped = false
	am.runMu.Unlock()

	am.mu.RLock()
	agents := make([]Agent, 0, len(am.agents))
	for _, a := range am.agents {
		agents = append(agents, a)
	}
	am.mu.RUnlock()

	for _, a := range agents {
		am.startAgentLoop(am.runCtx, a)
	}

	am.runWG.Add(1)
	go func() {
		defer am.runWG.Done()
		am.retryLoop(am.runCtx)
	}()

	go func() {
		<-am.runCtx.Done()
		am.runMu.Lock()
		am.running = false
		for id, cancel := range am.loopCancel {
			cancel()
			delete(am.loopCancel, id)
		}
		am.runMu.Unlock()
	}()
}

func (am *AgentManager) Recover(ctx context.Context) error {
	if am.store == nil {
		return nil
	}

	agents, err := am.store.LoadAgents(ctx)
	if err != nil {
		return fmt.Errorf("load agents: %w", err)
	}
	sort.SliceStable(agents, func(i, j int) bool {
		return agents[i].StartedAt.Before(agents[j].StartedAt)
	})
	for _, rec := range agents {
		if rec.Config.ID == "" {
			continue
		}
		if err := am.spawnAgentInternal(ctx, rec, false); err != nil && !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("hydrate agent %s: %w", rec.Config.ID, err)
		}
	}
	if err := am.restoreFlowInstanceRoutes(ctx); err != nil {
		return err
	}

	if err := runtimepipeline.NewRecoveryManagerWith(am.bus.Store(), am.bus).Recover(ctx); err != nil {
		return fmt.Errorf("recover pipeline receipts: %w", err)
	}

	if err := am.replayPendingEvents(ctx); err != nil {
		return err
	}
	return nil
}

func (am *AgentManager) restoreFlowInstanceRoutes(ctx context.Context) error {
	if am == nil || am.bus == nil {
		return nil
	}
	installer, ok := am.bus.(flowInstanceRouteInstaller)
	if !ok || installer == nil {
		return nil
	}
	routeStore, ok := am.bus.Store().(runtimebus.FlowInstanceRoutePersistence)
	if !ok || routeStore == nil {
		return nil
	}
	routes, err := routeStore.ListFlowInstanceRoutes(ctx)
	if err != nil {
		return fmt.Errorf("list persisted flow instance routes: %w", err)
	}
	for _, route := range routes {
		if err := installer.AddFlowInstance(runtimecontracts.SystemNodeContract{}, route.InstancePath); err != nil {
			return fmt.Errorf("restore flow instance route %s/%s: %w", route.TemplateID, route.InstanceID, err)
		}
	}
	return nil
}

func (am *AgentManager) retryLoop(ctx context.Context) {
	if am.store == nil {
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := am.replayPendingEvents(ctx); err != nil {
				log.Printf("retry replay failed: %v", err)
				if am.bus != nil {
					am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
						Level:     "error",
						Component: "agent-manager",
						Action:    "retry_replay_failed",
						Error:     strings.TrimSpace(err.Error()),
					})
				}
			}
		}
	}
}

func (am *AgentManager) replayPendingEvents(ctx context.Context) error {
	if am.store == nil {
		return nil
	}
	if am.isAuthBreakerTripped() {
		return nil
	}

	am.mu.RLock()
	ids := make([]string, 0, len(am.agents))
	for id := range am.agents {
		ids = append(ids, id)
	}
	am.mu.RUnlock()

	for _, id := range ids {
		if am.isAuthBreakerTripped() {
			return nil
		}
		if err := am.ReplayAgentBacklog(ctx, id); err != nil {
			log.Printf("pending replay failed for agent=%s err=%v", id, err)
			if am.bus != nil {
				am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "pending_replay_failed",
					AgentID:   id,
					Error:     strings.TrimSpace(err.Error()),
				})
			}
		}
	}
	return nil
}

func (am *AgentManager) ReplayAgentBacklog(ctx context.Context, agentID string) error {
	if am.store == nil {
		return fmt.Errorf("manager store unavailable")
	}
	if am.isAuthBreakerTripped() {
		return nil
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return errors.New("agent id is required")
	}
	am.mu.RLock()
	agent := am.agents[agentID]
	cfg, ok := am.agentCfg[agentID]
	since := time.Now().Add(-30 * 24 * time.Hour)
	if upAt, ok := am.agentUpAt[agentID]; ok && !upAt.IsZero() {
		since = upAt
	}
	am.mu.RUnlock()
	if !ok || agent == nil {
		return fmt.Errorf("agent not found: %s", agentID)
	}
	pending, err := am.pendingEventsForAgent(ctx, agentID, cfg, agent, since)
	if err != nil {
		return err
	}
	for _, evt := range pending {
		if am.isAuthBreakerTripped() {
			return nil
		}
		if err := am.processEvent(ctx, agent, evt); err != nil {
			log.Printf("pending replay failed for agent=%s event=%s err=%v", agentID, evt.ID, err)
			if am.bus != nil {
				evtCtx := runtimecorrelation.WithInboundEvent(ctx, evt)
				evtCtx = runtimecorrelation.WithRunID(evtCtx, strings.TrimSpace(evt.RunID))
				evtCtx = runtimecorrelation.WithTraceID(evtCtx, strings.TrimSpace(evt.TraceID))
				am.bus.LogRuntime(evtCtx, runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "pending_replay_event_failed",
					EventID:   strings.TrimSpace(evt.ID),
					EventType: strings.TrimSpace(string(evt.Type)),
					AgentID:   agentID,
					EntityID:  strings.TrimSpace(evt.EntityID()),
					Error:     strings.TrimSpace(err.Error()),
				})
			}
			if isClaudeAuthError(err) {
				return nil
			}
		}
	}
	return nil
}

func (am *AgentManager) pendingEventsForAgent(
	ctx context.Context,
	agentID string,
	cfg runtimeactors.AgentConfig,
	agent Agent,
	since time.Time,
) ([]events.Event, error) {
	pending := make([]events.Event, 0, 400)
	pendingByID := make(map[string]events.Event)

	direct, err := am.store.ListPendingEventsForAgent(ctx, agentID, since, 300)
	if err != nil {
		return nil, fmt.Errorf("load pending delivered events for %s: %w", agentID, err)
	}
	for _, evt := range direct {
		pendingByID[evt.ID] = evt
	}

	subscribed, err := am.store.ListPendingSubscribedEvents(ctx, agentID, agent.Subscriptions(), since, 300)
	if err != nil {
		return nil, fmt.Errorf("load pending subscribed events for %s: %w", agentID, err)
	}
	for _, evt := range subscribed {
		pendingByID[evt.ID] = evt
	}

	for _, evt := range pendingByID {
		pending = append(pending, evt)
	}
	sort.SliceStable(pending, func(i, j int) bool {
		if pending[i].CreatedAt.Equal(pending[j].CreatedAt) {
			return pending[i].ID < pending[j].ID
		}
		return pending[i].CreatedAt.Before(pending[j].CreatedAt)
	})
	return pending, nil
}

func (am *AgentManager) ResetRuntimeState() error {
	return am.resetRuntimeState("")
}

func (am *AgentManager) ResetRuntimeStateWithSource(source string) error {
	return am.resetRuntimeState(source)
}

func (am *AgentManager) resetRuntimeState(source string) error {
	if err := am.Shutdown(); err != nil {
		return err
	}
	if killer, ok := am.workspaces.(workspace.OrphanKiller); ok && killer != nil {
		if err := killer.KillOrphanProcesses(am.runtimeContext()); err != nil {
			return fmt.Errorf("kill workspace orphan processes: %w", err)
		}
	}
	runtimemcp.ResetTurnContexts()
	if resetter, ok := am.sessions.(sessions.Resetter); ok && resetter != nil {
		if err := resetter.ResetAll(am.runtimeMode); err != nil {
			log.Printf("session reset failed: %v", err)
			if am.bus != nil {
				am.bus.LogRuntime(am.runtimeContext(), runtimepipeline.RuntimeLogEntry{
					Level:     "error",
					Component: "agent-manager",
					Action:    "session_reset_failed",
					Error:     strings.TrimSpace(err.Error()),
				})
			}
		}
	}
	if am.bus != nil {
		if err := am.bus.ResetInMemoryState(); err != nil {
			return fmt.Errorf("reset event bus state: %w", err)
		}
	}

	entities := map[string]struct{}{}
	am.mu.Lock()
	for _, cfg := range am.agentCfg {
		if entityID := cfg.EffectiveEntityID(); entityID != "" {
			entities[entityID] = struct{}{}
		}
	}
	am.agents = make(map[string]Agent)
	am.agentCfg = make(map[string]runtimeactors.AgentConfig)
	am.agentUpAt = make(map[string]time.Time)
	am.inFlight = make(map[string]struct{})
	am.mu.Unlock()
	am.poisonMu.Lock()
	am.poisonPanicCounts = make(map[string]int)
	am.poisonMu.Unlock()

	for entityID := range entities {
		if am.workspaces != nil {
			_ = am.workspaces.StopEntityWorkspace(am.runtimeContext(), entityID)
		}
	}
	source = strings.TrimSpace(source)
	if source != "" && am.bus != nil {
		payload, err := json.Marshal(map[string]any{
			"source":    source,
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		})
		if err != nil {
			return fmt.Errorf("marshal platform.reset payload: %w", err)
		}
		if err := am.bus.Publish(am.runtimeContext(), events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("platform.reset"),
			SourceAgent: "runtime",
			Payload:     payload,
			CreatedAt:   time.Now(),
		}); err != nil {
			return fmt.Errorf("publish platform.reset: %w", err)
		}
	}
	return nil
}

func (am *AgentManager) startAgentLoop(parent context.Context, agent Agent) {
	loopCtx, cancel := context.WithCancel(parent)

	am.runMu.Lock()
	if old, ok := am.loopCancel[agent.ID()]; ok {
		old()
	}
	am.loopCancel[agent.ID()] = cancel
	am.runMu.Unlock()

	ch := am.bus.Subscribe(agent.ID(), agent.Subscriptions()...)
	am.runWG.Add(1)
	go func() {
		defer am.runWG.Done()
		consecutivePanics := 0
		for {
			panicked := false
			panicText := ""
			lastEventType := ""
			stackTrace := ""
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicked = true
						panicText = fmt.Sprint(r)
						stackTrace = strings.TrimSpace(string(debug.Stack()))
					}
				}()
				for {
					select {
					case <-loopCtx.Done():
						return
					case evt, ok := <-ch:
						if !ok {
							return
						}
						evtCtx := runtimecorrelation.WithInboundEvent(loopCtx, evt)
						evtCtx = runtimecorrelation.WithRunID(evtCtx, strings.TrimSpace(evt.RunID))
						evtCtx = runtimecorrelation.WithTraceID(evtCtx, strings.TrimSpace(evt.TraceID))
						err, evtPanicked, evtPanicText, evtStackTrace := am.safeProcessEvent(evtCtx, agent, evt)
						if evtPanicked {
							panicCount := am.incrementPoisonPanicCount(agent.ID(), evt.ID)
							am.writeReceipt(evtCtx, evt.ID, agent.ID(), ReceiptStatusError, "panic: "+strings.TrimSpace(evtPanicText))
							if am.bus != nil {
								am.bus.LogRuntime(evtCtx, runtimepipeline.RuntimeLogEntry{
									Level:     "error",
									Component: "agent-manager",
									Action:    "agent_event_panic",
									EventID:   strings.TrimSpace(evt.ID),
									EventType: strings.TrimSpace(string(evt.Type)),
									AgentID:   agent.ID(),
									EntityID:  strings.TrimSpace(evt.EntityID()),
									Error:     strings.TrimSpace(evtPanicText),
									Detail: map[string]any{
										"stack_trace": evtStackTrace,
									},
								})
							}
							if panicCount >= poisonPanicQuarantineAt {
								am.quarantinePoisonEvent(evtCtx, agent.ID(), evt, panicCount, evtPanicText)
								am.clearPoisonPanicCount(agent.ID(), evt.ID)
								consecutivePanics = 0
								continue
							}
							panicked = true
							panicText = evtPanicText
							stackTrace = evtStackTrace
							lastEventType = strings.TrimSpace(string(evt.Type))
							return
						}
						am.clearPoisonPanicCount(agent.ID(), evt.ID)
						consecutivePanics = 0
						if err != nil {
							log.Printf("agent %s failed processing event %s: %v", agent.ID(), evt.Type, err)
							if am.bus != nil {
								am.bus.LogRuntime(evtCtx, runtimepipeline.RuntimeLogEntry{
									Level:     "error",
									Component: "agent-manager",
									Action:    "agent_event_failed",
									EventID:   strings.TrimSpace(evt.ID),
									EventType: strings.TrimSpace(string(evt.Type)),
									AgentID:   agent.ID(),
									EntityID:  strings.TrimSpace(evt.EntityID()),
									Error:     strings.TrimSpace(err.Error()),
								})
							}
						}
					}
				}
			}()
			if !panicked {
				return
			}
			consecutivePanics++
			am.handleAgentLoopPanic(loopCtx, agent, consecutivePanics, lastEventType, panicText, stackTrace)
			if consecutivePanics >= 5 {
				return
			}
			wait := panicBackoff(consecutivePanics)
			select {
			case <-loopCtx.Done():
				return
			case <-time.After(wait):
			}
		}
	}()
}

func panicBackoff(consecutivePanics int) time.Duration {
	switch {
	case consecutivePanics <= 1:
		return 1 * time.Second
	case consecutivePanics == 2:
		return 5 * time.Second
	case consecutivePanics == 3:
		return 30 * time.Second
	case consecutivePanics == 4:
		return 2 * time.Minute
	default:
		return 10 * time.Minute
	}
}

func (am *AgentManager) handleAgentLoopPanic(ctx context.Context, agent Agent, consecutivePanics int, lastEventType, panicText, stackTrace string) {
	panicText = strings.TrimSpace(panicText)
	if panicText == "" {
		panicText = "unknown panic"
	}
	log.Printf("agent loop panic: agent=%s count=%d err=%s", agent.ID(), consecutivePanics, panicText)

	entityID := ""
	flowInstance := ""
	am.mu.RLock()
	cfg, ok := am.agentCfg[agent.ID()]
	am.mu.RUnlock()
	if ok {
		entityID = cfg.EffectiveEntityID()
		flowInstance = flowPathFromAgentConfig(cfg)
	}
	if am.bus != nil {
		am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
			Level:     "error",
			Component: "agent-manager",
			Action:    "agent_loop_panic",
			AgentID:   agent.ID(),
			EntityID:  entityID,
			Error:     panicText,
			Detail: map[string]any{
				"count":           consecutivePanics,
				"last_event_type": strings.TrimSpace(lastEventType),
				"stack_trace":     stackTrace,
			},
		})
	}

	if err := am.bus.Publish(am.runtimeContext(), (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("platform.agent_panic"),
		SourceAgent: "runtime",
		Payload: mustJSON(map[string]any{
			"agent_id":        agent.ID(),
			"flow_instance":   flowInstance,
			"entity_id":       entityID,
			"error":           panicText,
			"stack_trace":     stackTrace,
			"conversation_id": "",
			"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
		}),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID(entityID)); err != nil {
		if am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "publish_agent_panic_failed",
				AgentID:   agent.ID(),
				EntityID:  entityID,
				Error:     strings.TrimSpace(err.Error()),
			})
		}
	}

	if consecutivePanics < 5 {
		return
	}

	if ok && am.store != nil {
		_ = am.store.UpsertAgent(ctx, PersistedAgent{
			Config:          cfg,
			ParentAgentID:   cfg.ParentAgent,
			CoordinatorID:   am.resolveManagerAgentID(agent.ID()),
			Status:          "failed",
			HiredBy:         "runtime",
			TemplateVersion: "",
			StartedAt:       time.Now(),
		})
	}

	if err := am.bus.Publish(am.runtimeContext(), (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("platform.agent_failed"),
		SourceAgent: "runtime",
		Payload: mustJSON(map[string]any{
			"agent_id":        agent.ID(),
			"flow_instance":   flowInstance,
			"entity_id":       entityID,
			"error":           panicText,
			"retry_count":     consecutivePanics,
			"last_event_type": strings.TrimSpace(lastEventType),
			"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
		}),
		CreatedAt: time.Now().UTC(),
	}).WithEntityID(entityID)); err != nil {
		if am.bus != nil {
			am.bus.LogRuntime(ctx, runtimepipeline.RuntimeLogEntry{
				Level:     "error",
				Component: "agent-manager",
				Action:    "publish_agent_failed_failed",
				AgentID:   agent.ID(),
				EntityID:  entityID,
				Error:     strings.TrimSpace(err.Error()),
			})
		}
	}
}
