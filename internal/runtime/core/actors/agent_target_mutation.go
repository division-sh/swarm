package actors

// AgentTargetMutationResult is the process-visible evidence from one exact
// target mutation. Callers consume committed configurations as data; the
// lifecycle owner never executes caller-supplied commit hooks.
type AgentTargetMutationResult struct {
	PreviousConfig AgentConfig
	CurrentConfig  AgentConfig
	Transitioned   bool
}
