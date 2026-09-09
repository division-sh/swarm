package runforkadmission

// Integration tests import the selected store from an external test package to
// avoid a store -> readiness -> admission test-only cycle. No production export.
var (
	TestContractFrontierTemplateConnectSource = testContractFrontierTemplateConnectSource
	TestContractFrontierConnectSource         = testContractFrontierConnectSource
	TestContractFrontierRootConnectSource     = testContractFrontierRootConnectSource
)
