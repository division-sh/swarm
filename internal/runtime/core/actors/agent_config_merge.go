package actors

import "strings"

// MergeAgentConfig applies the canonical runtime reconfiguration patch semantics.
func MergeAgentConfig(base, patch AgentConfig) AgentConfig {
	out := base
	if patch.ID != "" {
		out.ID = patch.ID
	}
	if patch.Type != "" {
		out.Type = patch.Type
	}
	if patch.Role != "" {
		out.Role = patch.Role
	}
	if patch.FlowID != "" {
		out.FlowID = patch.FlowID
	}
	if patch.LLMBackend != "" {
		out.LLMBackend = patch.LLMBackend
	}
	if patch.Model != "" {
		out.Model = patch.Model
	}
	if patch.ExecutionMode.Valid() {
		out.ExecutionMode = patch.ExecutionMode
	}
	if patch.Mock.Configured() {
		out.Mock = patch.Mock
	}
	if strings.TrimSpace(string(patch.Memory.Source)) != "" {
		out.Memory = patch.Memory
	}
	if patch.MaxTurnsPerTask > 0 {
		out.MaxTurnsPerTask = patch.MaxTurnsPerTask
	}
	if patch.EntityID != "" {
		out.EntityID = patch.EntityID
	}
	if patch.ParentAgent != "" {
		out.ParentAgent = patch.ParentAgent
	}
	if patch.WorkspaceClass != "" {
		out.WorkspaceClass = patch.WorkspaceClass
	}
	if patch.ManagerFallback != "" {
		out.ManagerFallback = patch.ManagerFallback
	}
	if patch.FlowPath != "" {
		out.FlowPath = patch.FlowPath
	}
	if len(patch.Subscriptions) > 0 {
		out.Subscriptions = patch.Subscriptions
	}
	if len(patch.EmitEvents) > 0 {
		out.EmitEvents = patch.EmitEvents
	}
	if len(patch.Tools) > 0 {
		out.Tools = patch.Tools
	}
	if len(patch.Permissions) > 0 {
		out.Permissions = patch.Permissions
	}
	if patch.NativeTools.Any() {
		out.NativeTools = patch.NativeTools
	}
	if len(patch.Config) > 0 {
		out.Config = patch.Config
	}
	if patch.BudgetEnvelope != 0 {
		out.BudgetEnvelope = patch.BudgetEnvelope
	}
	out.NormalizeEntityID()
	out.NormalizeRuntimeDescriptor()
	return out
}
