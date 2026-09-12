package runforkadmission

// The store-backed admission tests use an external package to avoid importing
// the selected store back into its own readiness/admission dependency graph.
var (
	ContractFrontierTemplateConnectSourceForTest = testContractFrontierTemplateConnectSource
	ContractFrontierConnectSourceForTest         = testContractFrontierConnectSource
	ContractFrontierRootConnectSourceForTest     = testContractFrontierRootConnectSource
)
