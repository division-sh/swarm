package checkoutsource

import (
	"path/filepath"
	"strings"
)

// IsTestFixturePackage classifies source roles, not permission to construct
// storage or export authority. It accepts repository and module import paths.
func IsTestFixturePackage(path string) bool {
	const module = "github.com/division-sh/swarm/internal/"
	internal := strings.HasPrefix(path, module) || strings.HasPrefix(path, "internal/")
	path = strings.TrimPrefix(path, module)
	path = strings.TrimPrefix(path, "internal/")
	publicTestSupport := !strings.HasPrefix(path, "store/internal/") && strings.HasSuffix(filepath.Base(path), "test")
	return internal && (publicTestSupport ||
		strings.Contains("/"+path+"/", "/testfixtures/") ||
		path == "store/storetest" || path == "store/testsql" ||
		path == "store/eventfixture" || strings.HasPrefix(path, "store/testutil/") ||
		strings.HasPrefix(path, "testutil/"))
}
