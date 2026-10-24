package main

import (
	"strings"
)

func normalizedForegroundClaimJoin(source string) string {
	source = strings.Replace(source, "\t\t\tvar publisher sync.WaitGroup\n\t\t\tvar releaseOnce sync.Once\n\t\t\treleaseDispatch := func() { releaseOnce.Do(func() { close(release) }) }\n\t\t\tdefer func() {\n\t\t\t\treleaseDispatch()\n\t\t\t\tpublisher.Wait()\n\t\t\t\tif err := foreground.WaitForQuiescence(context.Background()); err != nil {\n\t\t\t\t\tt.Errorf(\"join foreground fixture dispatch: %v\", err)\n\t\t\t\t}\n\t\t\t}()\n\t\t\tpublisher.Add(1)\n", "", 1)
	source = strings.Replace(source, "\t\t\t\tdefer publisher.Done()\n", "", 1)
	return strings.Replace(source, "\t\t\treleaseDispatch()\n", "\t\t\tclose(release)\n", 1)
}
