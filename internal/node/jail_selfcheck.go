package node

import (
	"context"

	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// toolSelfCheckFunc is the startup self-check of the installed qemu-img: it
// runs under the Store lock after SettleTools and VerifyImages, before
// RecoverSnapshots, on the default image's verified base.
type toolSelfCheckFunc func(ctx context.Context, store *storage.Store, img storage.Image) (storage.ToolSelfCheckReport, error)

// toolSelfCheckFor returns the self-check New runs: the test seam when set,
// else the Store's own for the production jailed imager (a Jailer and no
// injected Imager), else nil.
func toolSelfCheckFor(cfg Config) toolSelfCheckFunc {
	switch {
	case cfg.toolSelfCheck != nil:
		return cfg.toolSelfCheck
	case cfg.Jailer != nil && cfg.Imager == nil:
		return func(ctx context.Context, store *storage.Store, img storage.Image) (storage.ToolSelfCheckReport, error) {
			return store.ToolSelfCheck(ctx, img)
		}
	}
	return nil
}
