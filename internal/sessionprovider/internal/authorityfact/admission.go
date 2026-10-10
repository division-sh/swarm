package authorityfact

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// Account is durable provenance. Only Admission proves native possession.
type Account struct {
	Provider     string `json:"provider"`
	ConnectionID string `json:"connection_id"`
	AccountRef   string `json:"account_reference"`
	AdmissionID  string `json:"admission_id"`
	Revision     int64  `json:"admission_revision"`
}

func (s Account) Validate() error {
	if strings.TrimSpace(s.Provider) == "" || strings.TrimSpace(s.AccountRef) == "" ||
		uuid.Validate(s.ConnectionID) != nil || uuid.Validate(s.AdmissionID) != nil || s.Revision < 1 {
		return fmt.Errorf("session account admission requires provider, stable connection, exact account and admission revision")
	}
	return nil
}

type ownedAuthority struct {
	account        Account
	parentID       string
	parentRevision int64
	ctx            context.Context
	current        func() bool
	release        func()
	once           sync.Once
}

type Admission struct{ value *ownedAuthority }

func (a Admission) Empty() bool { return a.value == nil }

// SealOwnedAccount is restricted to the native subtree and its single issuer.
func SealOwnedAccount(account Account, parentID string, parentRevision int64, ctx context.Context, current func() bool, release func()) Admission {
	return Admission{value: &ownedAuthority{account: account, parentID: parentID, parentRevision: parentRevision, ctx: ctx, current: current, release: release}}
}

func (a Admission) Validate(ctx context.Context, expected Account) error {
	if a.value == nil || ctx == nil || ctx.Err() != nil || expected.Validate() != nil ||
		a.value.account != expected || a.value.parentID == "" || a.value.parentRevision < 1 || a.value.ctx == nil || a.value.ctx.Err() != nil ||
		a.value.release == nil || a.value.current == nil || !a.value.current() || ctx.Err() != nil || a.value.ctx.Err() != nil {
		return fmt.Errorf("exact current owned session authority is required")
	}
	return nil
}

func (a Admission) Close() {
	if a.value != nil {
		a.value.once.Do(a.value.release)
	}
}

func (a Admission) Parent() (string, int64) {
	if a.value == nil {
		return "", 0
	}
	return a.value.parentID, a.value.parentRevision
}
