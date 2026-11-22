package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strconv"
	"syscall"

	"github.com/nathabonfim59/bamboo-raft/internal/ids"
)

// A live snapshot's fleece image (D4) is an empty standalone qcow2 of the
// VM disk's virtual size. Creating one is a jailed qemu-img run, so the node
// creates a template once per virtual size under phase2/ and gives every
// cut a plain copy of it: an empty qcow2 is a few hundred KiB of metadata.
const (
	// phase2Dir holds the fleece templates. It must equal the jailer's
	// StandaloneCreateDir, the only place a create without a backing may
	// target.
	phase2Dir = "phase2"
	// maxFleeceTemplate bounds a template's size: an empty qcow2 of the
	// largest disk is far smaller.
	maxFleeceTemplate = 64 << 20
)

// StandaloneCreator is implemented by an [Imager] that can create a qcow2
// with no backing (the fleece templates).
type StandaloneCreator interface {
	// CreateStandalone makes dst, a new empty pinned file under phase2/, a
	// qcow2 with no backing name and the given virtual size.
	CreateStandalone(ctx context.Context, dst ImageRef, virtualSize uint64) error
}

// fleeceTemplateName is the template of one virtual size, relative to the
// data root.
func fleeceTemplateName(virtualSize uint64) string {
	return path.Join(phase2Dir, "fleece-"+strconv.FormatUint(virtualSize, 10)+".qcow2")
}

// PrepareFleece creates vms/<id>/fleece.qcow2 as a fresh copy of the
// standalone template for virtualSize (made first if it is missing or fails
// its check) and returns it opened read-write. The copy is proven by its
// header: a qcow2 of exactly virtualSize with no backing name. A leftover
// fleece of an earlier cut is replaced. The caller removes the file
// (RemovePhase2Scratch) when the cut ends.
func (s *Store) PrepareFleece(ctx context.Context, id string, virtualSize uint64) (*os.File, error) {
	if !ids.ValidSandbox(id) {
		return nil, fmt.Errorf("storage: %q is not a sandbox id", id)
	}
	if virtualSize == 0 || virtualSize%512 != 0 {
		return nil, fmt.Errorf("storage: fleece virtual size %d is not a positive multiple of 512", virtualSize)
	}
	tmpl, err := s.fleeceTemplate(ctx, virtualSize)
	if err != nil {
		return nil, err
	}
	defer tmpl.Close()

	rel := path.Join(vmsDir, id, FleeceName)
	if err := s.removeRegular(rel); err != nil {
		return nil, err
	}
	f, err := s.root.OpenFile(rel, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("storage: vm %s: create fleece: %w", id, err)
	}
	fail := func(err error) (*os.File, error) {
		f.Close()
		if rmErr := s.root.Remove(rel); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("storage: vm %s: remove fleece: %w", id, rmErr))
		}
		return nil, err
	}
	if _, err := io.Copy(f, io.NewSectionReader(tmpl, 0, maxFleeceTemplate)); err != nil {
		return fail(fmt.Errorf("storage: vm %s: copy fleece template: %w", id, err))
	}
	if err := ctx.Err(); err != nil {
		return fail(fmt.Errorf("storage: vm %s: copy fleece template: %w", id, err))
	}
	if err := checkFleeceHeader(f, virtualSize); err != nil {
		return fail(fmt.Errorf("storage: vm %s: fleece copy: %w", id, err))
	}
	return f, nil
}

// fleeceTemplate returns the template for virtualSize opened read-only,
// creating it when it is missing or fails its check.
func (s *Store) fleeceTemplate(ctx context.Context, virtualSize uint64) (*os.File, error) {
	s.fleeceMu.Lock()
	defer s.fleeceMu.Unlock()
	name := fleeceTemplateName(virtualSize)
	f, err := s.openFleeceTemplate(name, virtualSize)
	if err == nil {
		return f, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		s.log.Warn("storage: fleece template failed its check; recreating it", "template", name, "error", err)
	}
	if err := s.createFleeceTemplate(ctx, name, virtualSize); err != nil {
		return nil, err
	}
	return s.openFleeceTemplate(name, virtualSize)
}

// openFleeceTemplate opens and checks an existing template. A missing file
// wraps fs.ErrNotExist.
func (s *Store) openFleeceTemplate(name string, virtualSize uint64) (*os.File, error) {
	f, err := s.root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("storage: open fleece template %s: %w", name, err)
	}
	info, err := f.Stat()
	switch {
	case err != nil:
		err = fmt.Errorf("storage: stat fleece template %s: %w", name, err)
	case !info.Mode().IsRegular():
		err = fmt.Errorf("storage: fleece template %s is not a regular file", name)
	case info.Size() > maxFleeceTemplate:
		err = fmt.Errorf("storage: fleece template %s is %d bytes, over %d", name, info.Size(), maxFleeceTemplate)
	default:
		if herr := checkFleeceHeader(f, virtualSize); herr != nil {
			err = fmt.Errorf("storage: fleece template %s: %w", name, herr)
		}
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// createFleeceTemplate builds the template for virtualSize with the
// imager's standalone create and publishes it atomically under name.
func (s *Store) createFleeceTemplate(ctx context.Context, name string, virtualSize uint64) error {
	creator, ok := s.imager.(StandaloneCreator)
	if !ok {
		return errors.New("storage: the image tool cannot create standalone fleece templates")
	}
	if err := s.root.Mkdir(phase2Dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("storage: create %s: %w", phase2Dir, err)
	}
	if info, err := s.root.Lstat(phase2Dir); err != nil || !info.IsDir() {
		return fmt.Errorf("storage: %s is not a directory (%v)", phase2Dir, err)
	}
	tmp := name + manifestTmpExt
	if err := s.removeRegular(tmp); err != nil {
		return err
	}
	f, err := s.root.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		return fmt.Errorf("storage: create fleece template %s: %w", tmp, err)
	}
	ref := ImageRef{File: f, Root: ImageRootData, Path: tmp, Format: "qcow2"}
	err = creator.CreateStandalone(ctx, ref, virtualSize)
	if err == nil {
		err = checkFleeceHeader(f, virtualSize)
	}
	if err == nil {
		err = s.cfg.Snapshots.SyncFile(f)
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = s.root.Rename(tmp, name)
	}
	if err != nil {
		if errors.Is(err, ErrToolRetained) {
			// A jailed tool may still hold the file: leave it for its
			// settlement; the next attempt replaces it.
			return fmt.Errorf("storage: create fleece template %s: %w", name, err)
		}
		if rmErr := s.root.Remove(tmp); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			err = errors.Join(err, rmErr)
		}
		return fmt.Errorf("storage: create fleece template %s: %w", name, err)
	}
	// A template lost to a crash is only recreated; the directory sync is
	// best effort.
	if err := syncDir(s.root, phase2Dir); err != nil {
		s.log.Warn("storage: sync fleece template dir", "error", err)
	}
	return nil
}

// checkFleeceHeader proves f is a qcow2 of exactly virtualSize with no
// backing name.
func checkFleeceHeader(f *os.File, virtualSize uint64) error {
	h, err := readQCOWHeader(f)
	switch {
	case err != nil:
		return err
	case h.backing != "":
		return fmt.Errorf("stores backing %q, want none", h.backing)
	case h.size != virtualSize:
		return fmt.Errorf("virtual size %d, want %d", h.size, virtualSize)
	}
	return nil
}

// removeRegular removes rel when it is a regular file; a missing name is
// fine, anything else is an error and left alone.
func (s *Store) removeRegular(rel string) error {
	info, err := s.root.Lstat(rel)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("storage: inspect %s: %w", rel, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("storage: %s has type %v; leaving it alone", rel, info.Mode().Type())
	}
	if err := s.root.Remove(rel); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("storage: remove %s: %w", rel, err)
	}
	return nil
}
