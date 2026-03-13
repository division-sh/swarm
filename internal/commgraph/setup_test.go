package commgraph

func init() {
	SetDefaultPolicyFactory(func() Policy {
		return NewGenericTestPolicy()
	})
}
