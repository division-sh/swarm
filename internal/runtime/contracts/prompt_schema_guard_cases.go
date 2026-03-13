package contracts

type PromptSchemaGuardCase struct {
	PromptFile       string
	EmitTool         string
	RequiredTopLevel []string
	ForbiddenTokens  []string
}

func PromptSchemaGuards() []PromptSchemaGuardCase {
	return nil
}
