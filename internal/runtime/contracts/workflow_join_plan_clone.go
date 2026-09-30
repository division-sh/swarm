package contracts

// Clone detaches the compiler-owned collection, catalog, and outcome evidence.
func (p WorkflowJoinPlan) Clone() WorkflowJoinPlan {
	p.MembersCollectionProjection = p.MembersCollectionProjection.Clone()
	p.ResultType.Catalog = cloneTypeCatalogDocument(p.ResultType.Catalog)
	p.Spec.Members.FromPath.Segments = append([]string(nil), p.Spec.Members.FromPath.Segments...)
	p.Spec.Members.ByPath.Segments = append([]string(nil), p.Spec.Members.ByPath.Segments...)
	p.Spec.OutputPath.Segments = append([]string(nil), p.Spec.OutputPath.Segments...)
	if p.Spec.Members.Count != nil {
		count := *p.Spec.Members.Count
		p.Spec.Members.Count = &count
	}
	if p.Spec.Deadline != nil {
		deadline := *p.Spec.Deadline
		p.Spec.Deadline = &deadline
	}
	p.Spec.OnComplete = cloneJoinOutcome(p.Spec.OnComplete)
	p.Spec.OnDeadline = cloneJoinOutcome(p.Spec.OnDeadline)
	return p
}

func cloneJoinOutcome(rule HandlerRuleEntry) HandlerRuleEntry {
	rule.Emit = cloneEmitSpec(rule.Emit)
	for key, value := range rule.Emit.Fields {
		value.Literal = cloneEventSchemaValue(value.Literal)
		value.RefPath.Segments = append([]string(nil), value.RefPath.Segments...)
		rule.Emit.Fields[key] = value
	}
	rule.DataAccumulation.Writes = cloneFanOutWrites(rule.DataAccumulation.Writes)
	for i := range rule.DataAccumulation.Writes {
		write := &rule.DataAccumulation.Writes[i]
		write.Value.Literal = cloneEventSchemaValue(write.Value.Literal)
		write.Key.Literal = cloneEventSchemaValue(write.Key.Literal)
		write.Index.Literal = cloneEventSchemaValue(write.Index.Literal)
	}
	return rule
}
