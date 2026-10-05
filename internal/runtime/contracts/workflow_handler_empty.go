package contracts

// EmptyExecution excludes annotations, but includes compiled consumers and
// every executable declaration. Unchanged outputs are not an empty handler.
func (h SystemNodeEventHandler) EmptyExecution() bool {
	return h.Activity.Empty() && !h.CreateEntity && h.Emit.Empty() && h.OnSuccess.Empty() &&
		h.Guard == nil && h.AdvancesTo == "" && h.SetsGate == nil && len(h.ClearGates) == 0 &&
		!h.DataAccumulation.HasWrites() && h.DataAccumulation.SourceEvent == "" &&
		h.Condition == "" && h.Logic == "" && h.Loop == nil &&
		len(h.OnComplete) == 0 && len(h.Rules) == 0 && h.Accumulate == nil &&
		h.Join == nil && len(h.JoinUntilPlans) == 0 && h.Compute == nil && h.Query == nil &&
		h.FanOut == nil && h.GroupBy == nil && h.Filter == nil && h.Reduce == nil &&
		h.Count == nil && h.Clear == nil
}
