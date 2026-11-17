//go:build !linux

package cli

import "os"

// reopenPollable has no portable equivalent outside Linux; the caller falls
// back to closing the original reader.
func reopenPollable(*os.File) (*os.File, bool) { return nil, false }
