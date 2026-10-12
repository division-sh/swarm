package runtimepersistence

import (
	"context"
	"testing"
)

func TestConversationForkCompletionFaultsRespectOriginalSQLiteWriterAdmission(t *testing.T) {
	for _, fault := range []struct {
		name  string
		apply func(context.Context, any) error
	}{
		{"install", InstallConversationForkCompletionInsertFailureForTest},
		{"remove", RemoveConversationForkCompletionInsertFailureForTest},
	} {
		t.Run(fault.name, func(t *testing.T) {
			assertGateFaultWriterAdmission(t, func(ctx context.Context, selected any, _ string) error {
				return fault.apply(ctx, selected)
			})
		})
	}
}
