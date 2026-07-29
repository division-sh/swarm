package entityruntime

import (
	"fmt"
	"strings"
)

// RunSummary is the entity owner's exact terminal-descriptor projection for
// one run. Unknown descriptors are malformed, not nonterminal guesses.
type RunSummary struct {
	RunID       string
	Total       int
	Terminal    int
	Nonterminal int
	Malformed   int
}

func (s RunSummary) Validate() error {
	if strings.TrimSpace(s.RunID) == "" {
		return fmt.Errorf("entity run summary requires run_id")
	}
	if s.Total < 0 || s.Terminal < 0 || s.Nonterminal < 0 || s.Malformed < 0 {
		return fmt.Errorf("entity run summary counts cannot be negative")
	}
	if s.Terminal+s.Nonterminal+s.Malformed != s.Total {
		return fmt.Errorf("entity run summary counts do not cover total")
	}
	return nil
}

func (s RunSummary) ReadyForCompletion() bool {
	return s.Total > 0 && s.Terminal == s.Total
}
