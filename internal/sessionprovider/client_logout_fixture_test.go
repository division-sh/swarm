package sessionprovider

import "context"

// Component fixtures exercise the SDK/drain without claiming selected-store
// destruction admission. Production can unlink only through RuntimeConnection.
func (o *clientOccurrence) logout(ctx context.Context) error {
	workCtx, release, err := o.prepareLogout(ctx)
	if err != nil {
		return err
	}
	defer release()
	return o.unlink(workCtx)
}
