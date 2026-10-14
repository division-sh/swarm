package sessionprovider

import "errors"

var errSessionProtection = errors.New("WhatsApp session directory requires exact private ownership and v1 state")
