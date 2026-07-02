package values

import "github.com/division-sh/swarm/internal/runtime/core/paths"

type Context struct {
	Entity      Bucket
	Event       Bucket
	Policy      Bucket
	Metadata    Bucket
	Gates       Bucket
	Payload     Bucket
	Accumulated Bucket
	FanOut      Bucket
	BatchAgent  Bucket
	Computed    Bucket
}

func NewContext() Context {
	return Context{
		Entity:      Wrap(map[string]any{}),
		Event:       Wrap(map[string]any{}),
		Policy:      Wrap(map[string]any{}),
		Metadata:    Wrap(map[string]any{}),
		Gates:       Wrap(map[string]any{}),
		Payload:     Wrap(map[string]any{}),
		Accumulated: Wrap(map[string]any{}),
		FanOut:      Wrap(map[string]any{}),
		BatchAgent:  Wrap(map[string]any{}),
		Computed:    Wrap(map[string]any{}),
	}
}

func (c Context) Clone() Context {
	return Context{
		Entity:      c.Entity.Clone(),
		Event:       c.Event.Clone(),
		Policy:      c.Policy.Clone(),
		Metadata:    c.Metadata.Clone(),
		Gates:       c.Gates.Clone(),
		Payload:     c.Payload.Clone(),
		Accumulated: c.Accumulated.Clone(),
		FanOut:      c.FanOut.Clone(),
		BatchAgent:  c.BatchAgent.Clone(),
		Computed:    c.Computed.Clone(),
	}
}

func (c Context) WithPayload(payload map[string]any) Context {
	c.Payload = Wrap(payload).Clone()
	return c
}

func (c Context) WithEvent(event map[string]any) Context {
	c.Event = Wrap(event).Clone()
	return c
}

func (c Context) WithAccumulated(accumulated map[string]any) Context {
	c.Accumulated = Wrap(accumulated).Clone()
	return c
}

func (c Context) WithFanOut(fanOut map[string]any) Context {
	c.FanOut = Wrap(fanOut).Clone()
	return c
}

func (c Context) WithBatchAgent(batchAgent map[string]any) Context {
	c.BatchAgent = Wrap(batchAgent).Clone()
	return c
}

func (c Context) Bucket(root paths.PathRoot) Bucket {
	switch root {
	case paths.RootEntity:
		return c.Entity
	case paths.RootEvent:
		return c.Event
	case paths.RootPolicy:
		return c.Policy
	case paths.RootMetadata:
		return c.Metadata
	case paths.RootGates:
		return c.Gates
	case paths.RootPayload:
		return c.Payload
	case paths.RootAccumulated:
		return c.Accumulated
	case paths.RootFanOut:
		return c.FanOut
	case paths.RootBatchAgent:
		return c.BatchAgent
	case paths.RootComputed:
		return c.Computed
	default:
		return Bucket{}
	}
}

func (c Context) Lookup(path paths.Path) (any, bool) {
	if path.IsZero() || !path.HasExplicitRoot() {
		return nil, false
	}
	return c.Bucket(path.Root).Lookup(path)
}
