package contracts

import "github.com/division-sh/swarm/internal/sourceartifact"

func loadOptionalRulesDeclarationsFromSource(artifact *sourceartifact.AdmittedSourceArtifact, label string) (RulesDocument, error) {
	admission, present, err := admitOptionalDeclarationSource(artifact, label, optionalDeclarationRules)
	if err != nil || !present {
		return RulesDocument{}, err
	}
	declarations, err := projectRulesDeclarationsValue(admission.document.Root())
	if err != nil {
		return nil, wrapLoaderDiagnosticFile(err, label)
	}
	return declarations, admission.RequireLive(len(declarations))
}

func loadOptionalRulesDeclarations(path string) (RulesDocument, error) {
	admission, present, err := admitOptionalDeclarationFile(path, optionalDeclarationRules)
	if err != nil || !present {
		return RulesDocument{}, err
	}
	declarations, err := projectRulesDeclarationsValue(admission.document.Root())
	if err != nil {
		return nil, wrapLoaderDiagnosticFile(err, path)
	}
	return declarations, admission.RequireLive(len(declarations))
}
