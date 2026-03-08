package pipeline

func defaultScanPolicy() ScanPolicy {
	return defaultWorkflowModule().ScanPolicy()
}
