package main

import (
	"fmt"
	"strings"
)

func family145TemplateWaitAssertions(before, after string) error {
	original, err := runSetupAssertions(before)
	if err != nil {
		return err
	}
	current, err := runSetupAssertions(after)
	if err != nil {
		return err
	}
	for i, assertion := range original {
		for _, replacement := range [][2]string{{"for query %s", "for observation %s"}, {"from query %s", "from observation %s"}, {"strings.TrimSpace(query)", "observation"}, {"runtimeTestEventDiagnostics(ctx, db)", "runtimeTestEventDiagnostics(ctx, selected)"}} {
			assertion = strings.ReplaceAll(assertion, replacement[0], replacement[1])
		}
		original[i] = assertion
	}
	if strings.Join(original, "\n") != strings.Join(current, "\n") || strings.Count(after, "read()") != 1 || family145PublicationRawEscape([]byte("package proof\n"+after)) != nil {
		return fmt.Errorf("template wait/assertion changed an original refusal, cardinality or closed read")
	}
	if strings.Contains(before, "10 * time.Second") && (!strings.Contains(after, "10 * time.Second") || !strings.Contains(after, "25 * time.Millisecond")) {
		return fmt.Errorf("template polling changed original deadline or spacing")
	}
	return nil
}

func family145TemplateNativePublicationFault(source string) bool {
	create := "storetest.NewPublicationFailure(pg, storetest.PublicationAfterMaterialization, \"consumer\")"
	restore := "fault.RestoreForTest(context.Background())"
	install := "fault.InstallForTest(ctx)"
	cleanup := strings.Index(source, "t.Cleanup(func()")
	return strings.Count(source, create) == 1 && strings.Count(source, restore) == 1 &&
		strings.Count(source, install) == 1 && cleanup >= 0 &&
		strings.Index(source, create) < cleanup && cleanup < strings.Index(source, restore) &&
		strings.Index(source, restore) < strings.Index(source, install)
}
