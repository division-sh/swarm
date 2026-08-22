//go:build !linux && !darwin && !windows

package packartifact

import "fmt"

func readRegularDevelopmentPackFile(_, name string) ([]byte, error) {
	return nil, fmt.Errorf("development pack artifact %q cannot be admitted safely on this platform", name)
}
