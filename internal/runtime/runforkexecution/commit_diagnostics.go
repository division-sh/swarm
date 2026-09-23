package runforkexecution

import "errors"

type selectedForkCommitDiagnostics struct {
	errors []error
}

func (d *selectedForkCommitDiagnostics) add(err error) {
	if d != nil && err != nil {
		d.errors = append(d.errors, err)
	}
}

func (d *selectedForkCommitDiagnostics) err() error {
	if d == nil {
		return nil
	}
	return errors.Join(d.errors...)
}
