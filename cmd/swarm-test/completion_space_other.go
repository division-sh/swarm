//go:build !darwin && !linux

package main

import "fmt"

func checkCompletionScratchSpace(path string, capacity int) error {
	return fmt.Errorf("explicit local qualification scratch observation is unsupported on this host; no capacity/result credit is granted")
}
