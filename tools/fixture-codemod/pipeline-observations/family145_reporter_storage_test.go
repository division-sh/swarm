package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func family145ReporterRebaseProofError(before, after string) error {
	original, beforeErr := runSetupAssertions(before)
	current, afterErr := runSetupAssertions(after)
	if beforeErr != nil || afterErr != nil {
		return fmt.Errorf("parse rebased reporter assertions: %v/%v", beforeErr, afterErr)
	}
	// These are the complete assertion calls in master f8ffc42420a93b6dfb3575c4a3dddeee9d858693.
	encoded, err := json.Marshal(current)
	if err != nil || len(current) != 30 || fmt.Sprintf("%x", sha256.Sum256(encoded)) != "647e224519541aa6a026e67429c4d51c29c0b18d3ba28d5bc771fc3b845ad866" {
		return fmt.Errorf("reporter lost an original or master-added assertion")
	}
	next := 0
	for _, assertion := range current {
		if next < len(original) && original[next] == assertion {
			next++
		}
	}
	if next != len(original) || strings.Count(after, "storetest.StartPostgresRuntimeStore(t)") != 1 ||
		strings.Count(after, "storetest.StartSQLiteRuntimeStore(t)") != 1 || strings.Contains(after, "*sql.DB") ||
		family145PublicationRawEscape([]byte("package proof\n"+after)) != nil {
		return fmt.Errorf("reporter changed original assertion order or native ownership")
	}
	return nil
}
