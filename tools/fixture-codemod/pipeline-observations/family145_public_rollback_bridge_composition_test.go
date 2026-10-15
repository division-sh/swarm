package main

import (
	"strings"
	"testing"
)

func TestFamily145PublicRollbackBridgeCompositionRetainsEveryOtherByte(t *testing.T) {
	for _, endpoint := range [][2]string{
		{"CountTemplatePrefixRoutingRulesForTest", `func CountTemplatePrefixRoutingRulesForTest(ctx context.Context, selected any, template string)(int,error){return readFanOutSemanticStorage(ctx,selected,nil,func(ctx context.Context,tx *sql.Tx)(int,error){return pipelinepersistence.CountTemplatePrefixRoutingRulesTx(ctx,tx,template)})}`},
		{"CountTemplateEventReplayScopesForTest", `func CountTemplateEventReplayScopesForTest(ctx context.Context, selected any, event string)(int,error){return readFanOutSemanticStorage(ctx,selected,[]string{event},func(ctx context.Context,tx *sql.Tx)(int,error){return pipelinepersistence.CountTemplateEventReplayScopesTx(ctx,tx,event)})}`},
	} {
		prefix, suffix := "package proof\n// unrelated leading source\n", "// unrelated trailing source\nfunc retained() {}\n"
		current := []byte(prefix + endpoint[1] + "\n" + suffix)
		prior, err := removePublicRollbackDeclaration(current, endpoint[0], endpoint[1])
		if err != nil || string(prior) != prefix+suffix {
			t.Fatal("bridge composition changed unrelated bytes:", err)
		}
		for _, cut := range []string{"selected", "readFanOutSemanticStorage", "pipelinepersistence", "ctx", "[]string{event}", "nil", "template"} {
			if !strings.Contains(endpoint[1], cut) {
				continue
			}
			mutant := []byte(prefix + strings.Replace(endpoint[1], cut, "foreignRollbackCut", 1) + "\n" + suffix)
			if _, err := removePublicRollbackDeclaration(mutant, endpoint[0], endpoint[1]); err == nil {
				t.Fatal("changed bridge received inverse:", cut)
			}
		}
		if _, err := removePublicRollbackDeclaration([]byte(prefix+endpoint[1]+"\n"+endpoint[1]+"\n"+suffix), endpoint[0], endpoint[1]); err == nil {
			t.Fatal("duplicate bridge received inverse")
		}
		if _, err := removePublicRollbackDeclaration([]byte(prefix+suffix), endpoint[0], endpoint[1]); err == nil {
			t.Fatal("missing bridge received inverse")
		}
	}
}
