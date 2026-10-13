package canonicalrouting

import "testing"

func CopyRunControlPublication(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	writeClosedVariantFile(t, root, "schema.yaml", `name: run-control-publication
pins:
  inputs: [custom.run_control, custom.run_control.acked, custom.run_control.intercepted, custom.run_control.deferred, custom.run_control.postcommit, custom.run_control.postcommit.deferred]
  outputs: [custom.run_control, custom.run_control.acked, custom.run_control.intercepted, custom.run_control.deferred, custom.run_control.postcommit, custom.run_control.postcommit.deferred]
`)
	writeClosedVariantFile(t, root, "events.yaml", `custom.run_control:
custom.run_control.acked:
custom.run_control.intercepted:
custom.run_control.deferred:
custom.run_control.postcommit:
custom.run_control.postcommit.deferred:
`)
	return root
}
