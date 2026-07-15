package mockperformance

const (
	KindPython  = "python"
	EntryHandle = "handle"

	ValidationMemoryPages uint32 = 8192
	ValidationOutputBytes        = 256 * 1024
	ExecutionFuel         uint64 = 2_000_000_000
	ExecutionMemoryPages  uint32 = 8192
	ExecutionOutputBytes         = 256 * 1024
)

// Performance is the generation-owned deterministic completion artifact.
// Source is captured during contract compilation; runtime code never rereads
// Module from the ambient filesystem.
type Performance struct {
	Kind       string `yaml:"kind" json:"kind"`
	Module     string `yaml:"module" json:"module"`
	Source     []byte `yaml:"-" json:"source,omitempty"`
	Digest     string `yaml:"-" json:"digest,omitempty"`
	SourcePath string `yaml:"-" json:"source_path,omitempty"`
}

func (p Performance) Configured() bool {
	return p.Kind != "" || p.Module != "" || len(p.Source) > 0 || p.Digest != ""
}
