package testpostgres

import (
	"context"
	"fmt"
	"os"
	"time"
)

const fileLockWaitInterval = 10 * time.Millisecond

type fileLock struct {
	file *os.File
}

func (l *fileLock) File() *os.File { return l.file }

// Drop closes only this process's descriptor. Unlike Close, it does not issue
// an explicit unlock that could release a lock shared with an inherited child.
func (l *fileLock) Drop() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	return file.Close()
}

func waitForFileLock(ctx context.Context, path string) (*fileLock, error) {
	if ctx == nil {
		return nil, fmt.Errorf("lock wait context is nil")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("wait for lock %s: %w", path, err)
		}
		lock, acquired, err := acquireFileLock(path, true)
		if err != nil {
			return nil, err
		}
		if acquired {
			return lock, nil
		}
		timer := time.NewTimer(fileLockWaitInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, fmt.Errorf("wait for lock %s: %w", path, ctx.Err())
		case <-timer.C:
		}
	}
}
