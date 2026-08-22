package packartifact

import (
	"fmt"
	"strings"
	"sync"
)

type PlatformPackBaseResolver interface {
	CurrentPlatformPackBase() (*PlatformPackInventory, error)
	ResolvePlatformPackBase(PackSelectionReceipt) (*PlatformPackInventory, error)
}

// PlatformPackBaseGenerationOwner retains exactly the immutable bases selected
// by the current process. It is not a historical archive across binary starts.
type PlatformPackBaseGenerationOwner struct {
	mu          sync.RWMutex
	current     string
	generations map[string]*PlatformPackInventory
}

type PreparedPlatformPackBaseSelection struct {
	once  sync.Once
	owner *PlatformPackBaseGenerationOwner
	base  *PlatformPackInventory
}

func NewPlatformPackBaseGenerationOwner(initial *PlatformPackInventory) (*PlatformPackBaseGenerationOwner, error) {
	if err := validatePlatformPackBase(initial); err != nil {
		return nil, err
	}
	owner := &PlatformPackBaseGenerationOwner{generations: map[string]*PlatformPackInventory{initial.Digest(): initial}}
	owner.current = initial.Digest()
	return owner, nil
}

func (o *PlatformPackBaseGenerationOwner) CurrentPlatformPackBase() (*PlatformPackInventory, error) {
	if o == nil {
		return nil, fmt.Errorf("platform pack base generation owner is required")
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	base := o.generations[o.current]
	if err := validatePlatformPackBase(base); err != nil {
		return nil, fmt.Errorf("current platform pack base generation: %w", err)
	}
	return base, nil
}

func (o *PlatformPackBaseGenerationOwner) ResolvePlatformPackBase(receipt PackSelectionReceipt) (*PlatformPackInventory, error) {
	if o == nil {
		return nil, fmt.Errorf("platform pack base generation owner is required")
	}
	if err := receipt.Validate(); err != nil {
		return nil, err
	}
	digest := strings.TrimSpace(receipt.BaseDigest)
	o.mu.RLock()
	base := o.generations[digest]
	o.mu.RUnlock()
	if base == nil || !receipt.MatchesBase(base) {
		return nil, fmt.Errorf("pack selection receipt requires %s base %s but that generation is not retained by this process", receipt.BaseMode, digest)
	}
	return base, nil
}

// Select retains base and makes it authoritative for newly authored bundles.
// Existing receipts continue resolving to every prior same-process generation.
func (o *PlatformPackBaseGenerationOwner) Select(base *PlatformPackInventory) error {
	if o == nil {
		return fmt.Errorf("platform pack base generation owner is required")
	}
	if err := validatePlatformPackBase(base); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if existing := o.generations[base.Digest()]; existing != nil && existing.SelectionMode() != base.SelectionMode() {
		return fmt.Errorf("platform pack base digest %s has competing selection modes %q and %q", base.Digest(), existing.SelectionMode(), base.SelectionMode())
	}
	o.generations[base.Digest()] = base
	o.current = base.Digest()
	return nil
}

func (o *PlatformPackBaseGenerationOwner) PrepareSelection(base *PlatformPackInventory) (*PreparedPlatformPackBaseSelection, error) {
	if o == nil {
		return nil, fmt.Errorf("platform pack base generation owner is required")
	}
	if err := validatePlatformPackBase(base); err != nil {
		return nil, err
	}
	o.mu.RLock()
	existing := o.generations[base.Digest()]
	o.mu.RUnlock()
	if existing != nil && existing.SelectionMode() != base.SelectionMode() {
		return nil, fmt.Errorf("platform pack base digest %s has competing selection modes %q and %q", base.Digest(), existing.SelectionMode(), base.SelectionMode())
	}
	return &PreparedPlatformPackBaseSelection{owner: o, base: base}, nil
}

func (s *PreparedPlatformPackBaseSelection) Commit() {
	if s == nil || s.owner == nil || s.base == nil {
		return
	}
	s.once.Do(func() {
		s.owner.mu.Lock()
		s.owner.generations[s.base.Digest()] = s.base
		s.owner.current = s.base.Digest()
		s.owner.mu.Unlock()
	})
}

func validatePlatformPackBase(base *PlatformPackInventory) error {
	if base == nil || strings.TrimSpace(base.Digest()) == "" || !base.SelectionMode().Valid() {
		return fmt.Errorf("selected platform pack base inventory is required")
	}
	return nil
}
