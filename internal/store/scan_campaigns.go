package store

import runtimepipeline "empireai/internal/runtime/pipeline"

// ScanCampaignStore preserves the scan campaign persistence surface after the
// Empire implementation moved under internal/empire/store.
type ScanCampaignStore interface {
	runtimepipeline.ScanCampaignPersistence
}
