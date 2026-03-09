//go:build ignore

package pipeline

func defaultScanPolicy() ScanPolicy {
	return defaultWorkflowModule().ScanPolicy()
}
