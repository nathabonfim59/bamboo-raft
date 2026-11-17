package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"

	"github.com/nathabonfim59/bamboo-raft/internal/node/jailer"
	"github.com/nathabonfim59/bamboo-raft/internal/node/storage"
)

// A10 D1 self-check sequencing in New: after the Store lock, SettleTools
// and VerifyImages; before RecoverSnapshots; on the default image.

// selfCheckOrder records the startup events the sequencing tests observe.
type selfCheckOrder struct {
	mu     sync.Mutex
	events []string
}

func (o *selfCheckOrder) add(e string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, e)
}

func (o *selfCheckOrder) list() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.events)
}

// selfCheckImager is jailImager recording the image-root (verification)
// calls.
type selfCheckImager struct {
	jailImager
	order *selfCheckOrder
}

func (i selfCheckImager) Info(ctx context.Context, ref storage.ImageRef) (storage.QemuInfo, error) {
	if ref.Root == storage.ImageRootImages {
		i.order.add("verify")
	}
	return i.jailImager.Info(ctx, ref)
}

func (i selfCheckImager) Check(ctx context.Context, ref storage.ImageRef) error {
	if ref.Root == storage.ImageRootImages {
		i.order.add("verify")
	}
	return i.jailImager.Check(ctx, ref)
}

// selfCheckStage plants an in-flight snapshot stage RecoverSnapshots
// removes, returning its path.
func selfCheckStage(t *testing.T, dataDir string) string {
	t.Helper()
	stage := filepath.Join(dataDir, "snapshots", "staging", storage.SnapshotKey("in flight"))
	if err := os.MkdirAll(stage, 0o700); err != nil {
		t.Fatal(err)
	}
	return stage
}

// TestNewRunsToolSelfCheckInOrder: the self-check runs once, after
// SettleTools and every image verification call and before snapshot
// recovery, on the default image's verified record.
func TestNewRunsToolSelfCheckInOrder(t *testing.T) {
	order := &selfCheckOrder{}
	fj := newFakeJail(t)
	fj.settleTools = func(context.Context) (jailer.ToolReport, error) {
		order.add("settle")
		return jailer.ToolReport{}, nil
	}
	var stage string
	var got storage.Image
	newJailedHarness(t, jailedOptions{jail: fj,
		prepare: func(t *testing.T, dataDir, _ string) { stage = selfCheckStage(t, dataDir) },
		configure: func(cfg *Config) {
			cfg.Imager = selfCheckImager{order: order}
			cfg.toolSelfCheck = func(_ context.Context, store *storage.Store, img storage.Image) (storage.ToolSelfCheckReport, error) {
				order.add("selfcheck")
				if store == nil {
					t.Error("self-check got no store")
				}
				if _, err := os.Lstat(stage); err != nil {
					t.Errorf("snapshot staging was recovered before the self-check: %v", err)
				}
				got = img
				return storage.ToolSelfCheckReport{Image: img.ID}, nil
			}
		}})
	events := order.list()
	i := slices.Index(events, "selfcheck")
	if i < 0 || slices.Index(events[i+1:], "selfcheck") >= 0 {
		t.Fatalf("startup events %q: want exactly one self-check", events)
	}
	if events[0] != "settle" || !slices.Contains(events[:i], "verify") || slices.Contains(events[i+1:], "verify") || slices.Contains(events[i+1:], "settle") {
		t.Fatalf("startup events %q: want settle, then every verify, then the self-check", events)
	}
	if got.ID != testImageID || got.RootVirtualSize == 0 {
		t.Fatalf("self-check image = %+v, want the verified %s", got, testImageID)
	}
	if _, err := os.Lstat(stage); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("snapshot recovery did not run after the self-check: %v", err)
	}
}

// TestNewToolSelfCheckDefaultsToFirstImage: with no configured default the
// self-check uses the first verified image.
func TestNewToolSelfCheckDefaultsToFirstImage(t *testing.T) {
	var got string
	newJailedHarness(t, jailedOptions{configure: func(cfg *Config) {
		cfg.DefaultImage = ""
		cfg.toolSelfCheck = func(_ context.Context, _ *storage.Store, img storage.Image) (storage.ToolSelfCheckReport, error) {
			got = img.ID
			return storage.ToolSelfCheckReport{}, nil
		}
	}})
	if got != testImageID {
		t.Fatalf("self-check image %q, want %s", got, testImageID)
	}
}

// TestNewToolSelfCheckRunsUnderTheStoreLock is the lock positive control.
func TestNewToolSelfCheckRunsUnderTheStoreLock(t *testing.T) {
	var dataDir string
	var locked error
	ran := false
	newJailedHarness(t, jailedOptions{
		prepare: func(_ *testing.T, d, _ string) { dataDir = d },
		configure: func(cfg *Config) {
			cfg.toolSelfCheck = func(context.Context, *storage.Store, storage.Image) (storage.ToolSelfCheckReport, error) {
				ran = true
				f, err := os.OpenFile(filepath.Join(dataDir, "node.lock"), os.O_RDWR, 0)
				if err != nil {
					locked = err
					return storage.ToolSelfCheckReport{}, nil
				}
				defer f.Close()
				locked = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
				if locked == nil {
					_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
				}
				return storage.ToolSelfCheckReport{}, nil
			}
		}})
	if !ran || !errors.Is(locked, syscall.EWOULDBLOCK) {
		t.Fatalf("flock during the self-check = %v (ran %v), want EWOULDBLOCK", locked, ran)
	}
}

// TestNewFailsWhenToolSelfCheckFails: a self-check failure fails startup
// before snapshot recovery, and the Store is closed (its lock released).
func TestNewFailsWhenToolSelfCheckFails(t *testing.T) {
	fj := newFakeJail(t)
	var dataDir, imageDir, stage string
	jailNewFails(t, fj, "", func(t *testing.T, d, i string) {
		dataDir, imageDir = d, i
		stage = selfCheckStage(t, d)
	}, "qemu-img self-check", func(cfg *Config) {
		cfg.toolSelfCheck = func(context.Context, *storage.Store, storage.Image) (storage.ToolSelfCheckReport, error) {
			return storage.ToolSelfCheckReport{}, errors.New("injected semantic mismatch")
		}
	})
	if _, err := os.Lstat(stage); err != nil {
		t.Fatalf("a failed self-check let snapshot recovery run: %v", err)
	}
	s, err := storage.Open(storage.Config{DataDir: dataDir, ImageDir: imageDir})
	if err != nil {
		t.Fatalf("the store stayed locked after a failed self-check: %v", err)
	}
	s.Close()
}

// TestNewUnknownDefaultImageSkipsToolSelfCheck: an uninstalled default image
// fails startup before the self-check runs.
func TestNewUnknownDefaultImageSkipsToolSelfCheck(t *testing.T) {
	ran := false
	jailNewFails(t, newFakeJail(t), "", nil, "is not installed", func(cfg *Config) {
		cfg.DefaultImage = "absent-v9"
		cfg.toolSelfCheck = func(context.Context, *storage.Store, storage.Image) (storage.ToolSelfCheckReport, error) {
			ran = true
			return storage.ToolSelfCheckReport{}, nil
		}
	})
	if ran {
		t.Fatal("the self-check ran for an uninstalled default image")
	}
}

// TestToolSelfCheckForSelectsProduction: the Store's self-check is selected
// exactly for the production jailed imager; the seam wins when set.
func TestToolSelfCheckForSelectsProduction(t *testing.T) {
	seamRan := false
	seam := toolSelfCheckFunc(func(context.Context, *storage.Store, storage.Image) (storage.ToolSelfCheckReport, error) {
		seamRan = true
		return storage.ToolSelfCheckReport{}, nil
	})
	for _, tc := range []struct {
		name string
		cfg  Config
		want bool
	}{
		{"jailer without an injected imager", Config{Jailer: &jailer.Manager{}}, true},
		{"jailer with an injected imager", Config{Jailer: &jailer.Manager{}, Imager: jailImager{}}, false},
		{"no jailer", Config{}, false},
		{"seam", Config{toolSelfCheck: seam}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := toolSelfCheckFor(tc.cfg); (got != nil) != tc.want {
				t.Fatalf("toolSelfCheckFor = %v, want selected %v", got != nil, tc.want)
			}
		})
	}
	f := toolSelfCheckFor(Config{Jailer: &jailer.Manager{}, toolSelfCheck: seam})
	if f == nil {
		t.Fatal("the seam was not selected")
	}
	if _, err := f(t.Context(), nil, storage.Image{}); err != nil || !seamRan {
		t.Fatal("the selected function is not the seam")
	}
}
