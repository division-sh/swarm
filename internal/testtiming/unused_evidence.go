package testtiming

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
)

type UnusedConfiguration struct {
	Tool, Toolchain, Checks, Matrix, Packages, TargetGo, GOFLAGS, GOWORK, GOENV, U1000Directives string
	Tests, ShowIgnored                                                                           bool
}

type UnusedNative struct {
	GOOS, GOARCH, CGO_ENABLED, GOHOSTOS, GOHOSTARCH, GOVERSION, GOEXPERIMENT string
}

type UnusedEvidence struct {
	Schema       int                 `json:"schema"`
	State        string              `json:"state"`
	SourceSHA    string              `json:"source_sha"`
	Config       UnusedConfiguration `json:"config"`
	Native       UnusedNative        `json:"native"`
	ResultSHA256 string              `json:"result_sha256"`
	ResultBytes  int64               `json:"result_bytes"`
}

// Identity is independent of the pinned analyzer configuration and U1000
// verdict. The qualification owner still checks both before granting credit.
func ValidateUnusedIdentity(e UnusedEvidence, head, platform string) error {
	if e.Schema != 1 || e.State != "complete" || !hexIdentity(head, 20) || e.SourceSHA != head {
		return fmt.Errorf("missing, stale or incomplete native unused identity")
	}
	if err := ValidateUnusedNative(e.Native, platform, e.Config.Toolchain); err != nil {
		return err
	}
	if !hexIdentity(e.ResultSHA256, sha256.Size) || e.ResultBytes <= 0 {
		return fmt.Errorf("empty result or invalid native unused result hash")
	}
	return nil
}

func ValidateUnusedNative(n UnusedNative, platform, toolchain string) error {
	if (platform != "linux" && platform != "darwin") || n.GOOS != platform || n.GOHOSTOS != platform || n.GOARCH == "" || n.GOARCH != n.GOHOSTARCH || n.CGO_ENABLED != "1" || toolchain == "" || n.GOVERSION != toolchain || n.GOEXPERIMENT != "" {
		return fmt.Errorf("invalid native %s environment: %+v (native CGO and pinned toolchain required)", platform, n)
	}
	return nil
}

func ReadUnusedIdentity(metadata, result io.Reader, head, platform string) (UnusedEvidence, error) {
	var e UnusedEvidence
	dec := json.NewDecoder(metadata)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&e); err != nil {
		return UnusedEvidence{}, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return UnusedEvidence{}, fmt.Errorf("trailing native unused metadata")
	}
	if err := ValidateUnusedIdentity(e, head, platform); err != nil {
		return UnusedEvidence{}, err
	}
	hash := sha256.New()
	size, err := io.Copy(hash, result)
	if err != nil {
		return UnusedEvidence{}, err
	}
	if size != e.ResultBytes || hex.EncodeToString(hash.Sum(nil)) != e.ResultSHA256 {
		return UnusedEvidence{}, fmt.Errorf("corrupt native unused result: size/hash mismatch")
	}
	return e, nil
}
