package jailer

import (
	"context"
	"debug/elf"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	maxRuntimeFiles = 256
	maxRuntimeBytes = 4 << 30
	maxInterpreter  = 4096
)

// runtimeFile is one trusted host file staged read-only in the jail.
type runtimeFile struct {
	source string // resolved host path whose content is copied
	target string // slash-separated path relative to the jail root
	size   uint64
}

// reservedJailNames are the jail root entries the package creates itself.
// No runtime or firmware file may be staged at or below them.
func reservedJailNames() []string {
	return []string{"proc", "control", "dev", "tmp", "serial.log"}
}

// resolveRuntime returns the QEMU binary (first, staged at its declared
// path), its ELF interpreter and DT_NEEDED closure, and the explicit
// firmware files. It parses ELF metadata directly and never runs ldd, the
// loader or any other probe. Every source is checked again when copied.
func resolveRuntime(ctx context.Context, binary string, libraryDirs, firmware []string) ([]runtimeFile, error) {
	var (
		files   []runtimeFile
		targets = make(map[string]string)
		parsed  = make(map[string]struct{})
		total   uint64
	)
	add := func(declared string) (runtimeFile, bool, error) {
		target, err := runtimeTarget(declared)
		if err != nil {
			return runtimeFile{}, false, err
		}
		resolved, err := filepath.EvalSymlinks(declared)
		if err != nil {
			return runtimeFile{}, false, fmt.Errorf("jailer: resolve runtime %s: %w", declared, err)
		}
		if previous, exists := targets[target]; exists {
			if previous != resolved {
				return runtimeFile{}, false, fmt.Errorf("jailer: runtime target /%s has two sources", target)
			}
			return runtimeFile{}, false, nil
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return runtimeFile{}, false, fmt.Errorf("jailer: stat runtime %s: %w", resolved, err)
		}
		if !info.Mode().IsRegular() {
			return runtimeFile{}, false, fmt.Errorf("jailer: runtime %s is not a regular file", resolved)
		}
		if len(files) >= maxRuntimeFiles {
			return runtimeFile{}, false, fmt.Errorf("jailer: runtime exceeds %d files", maxRuntimeFiles)
		}
		size := uint64(info.Size())
		if info.Size() < 0 || total+size > maxRuntimeBytes {
			return runtimeFile{}, false, fmt.Errorf("jailer: runtime exceeds %d bytes", maxRuntimeBytes)
		}
		total += size
		targets[target] = resolved
		file := runtimeFile{source: resolved, target: target, size: size}
		files = append(files, file)
		return file, true, nil
	}
	queue := []string{binary}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, context.Cause(ctx)
		}
		declared := queue[0]
		queue = queue[1:]
		file, added, err := add(declared)
		if err != nil {
			return nil, err
		}
		if !added {
			continue
		}
		if _, done := parsed[file.source]; done {
			continue
		}
		parsed[file.source] = struct{}{}
		interpreter, needed, err := elfDependencies(file.source)
		if err != nil {
			return nil, fmt.Errorf("jailer: parse ELF %s: %w", file.source, err)
		}
		if interpreter != "" {
			queue = append(queue, interpreter)
		}
		for _, name := range needed {
			if name == "" || strings.ContainsRune(name, 0) || filepath.Base(name) != name || name == "." || name == ".." {
				return nil, fmt.Errorf("jailer: unsafe DT_NEEDED name %q", name)
			}
			source := ""
			for _, dir := range libraryDirs {
				candidate := filepath.Join(dir, name)
				if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() {
					source = candidate
					break
				}
			}
			if source == "" {
				return nil, fmt.Errorf("jailer: DT_NEEDED %s of %s was not found in trusted library dirs", name, file.source)
			}
			queue = append(queue, source)
		}
	}
	// Firmware is data: it is never parsed as ELF or searched for.
	for _, name := range firmware {
		if _, _, err := add(name); err != nil {
			return nil, fmt.Errorf("jailer: firmware: %w", err)
		}
	}
	return files, nil
}

// runtimeTarget maps a clean absolute host path to the same path relative to
// the jail root, refusing the jail's own mount points.
func runtimeTarget(source string) (string, error) {
	if !filepath.IsAbs(source) || filepath.Clean(source) != source {
		return "", fmt.Errorf("jailer: runtime path %q is not absolute and clean", source)
	}
	target := strings.TrimPrefix(path.Clean(filepath.ToSlash(source)), "/")
	if target == "" || target == "." {
		return "", fmt.Errorf("jailer: unsafe runtime path %q", source)
	}
	first, _, _ := strings.Cut(target, "/")
	for _, reserved := range reservedJailNames() {
		if first == reserved {
			return "", fmt.Errorf("jailer: runtime path %q collides with the jail's /%s", source, reserved)
		}
	}
	return target, nil
}

func elfDependencies(name string) (string, []string, error) {
	f, err := elf.Open(name)
	if err != nil {
		return "", nil, err
	}
	defer f.Close()
	hasDynamic := false
	var interpreter string
	for _, prog := range f.Progs {
		switch prog.Type {
		case elf.PT_DYNAMIC:
			hasDynamic = true
		case elf.PT_INTERP:
			if prog.Filesz == 0 || prog.Filesz > maxInterpreter {
				return "", nil, fmt.Errorf("invalid PT_INTERP size %d", prog.Filesz)
			}
			raw, err := io.ReadAll(io.LimitReader(prog.Open(), maxInterpreter))
			if err != nil {
				return "", nil, err
			}
			interpreter = strings.TrimRight(string(raw), "\x00")
			if !filepath.IsAbs(interpreter) || filepath.Clean(interpreter) != interpreter {
				return "", nil, fmt.Errorf("unsafe PT_INTERP %q", interpreter)
			}
		}
	}
	if !hasDynamic {
		return interpreter, nil, nil
	}
	needed, err := f.DynString(elf.DT_NEEDED)
	if err != nil {
		return "", nil, err
	}
	return interpreter, needed, nil
}

// trustedParents requires every directory above name to be root-owned and
// not group/other writable, so no unprivileged user can swap the file.
func trustedParents(name string) error {
	for current := filepath.Dir(name); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("jailer: inspect trusted parent %s: %w", current, err)
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("jailer: trusted parent %s has no Linux ownership", current)
		}
		if !info.IsDir() || st.Uid != 0 || info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("jailer: runtime parent %s must be a root-owned directory that is not group/other writable", current)
		}
		if current == "/" {
			return nil
		}
	}
}

// openTrustedSource opens a resolved runtime source without following a
// final symlink and checks the opened inode itself: root-owned, regular,
// not group/other writable and the expected size.
func openTrustedSource(source string, size uint64) (*os.File, error) {
	if err := trustedParents(source); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(source, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("jailer: open runtime %s: %w", source, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("jailer: stat runtime %s: %w", source, err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || st.Uid != 0 || info.Mode().Perm()&0o022 != 0 {
		f.Close()
		return nil, fmt.Errorf("jailer: runtime source %s must be a root-owned regular file that is not group/other writable", source)
	}
	if uint64(info.Size()) != size {
		f.Close()
		return nil, fmt.Errorf("jailer: runtime source %s changed size", source)
	}
	return f, nil
}

// copyRuntime stages every file under the jail root through b, so each new
// directory and file is attempt-owned and recorded before it is written. It
// returns the staged QEMU binary's identity (files[0]).
func copyRuntime(ctx context.Context, b *jailBuilder, files []runtimeFile) (inodeIdentity, error) {
	var binary inodeIdentity
	for i, file := range files {
		if err := ctx.Err(); err != nil {
			return inodeIdentity{}, context.Cause(ctx)
		}
		in, err := openTrustedSource(file.source, file.size)
		if err != nil {
			return inodeIdentity{}, err
		}
		identity, err := b.copyFile(ctx, path.Join("root", file.target), in, file.size)
		closeErr := in.Close()
		if err == nil && closeErr != nil {
			err = fmt.Errorf("jailer: close runtime %s: %w", file.source, closeErr)
		}
		if err != nil {
			return inodeIdentity{}, err
		}
		if i == 0 {
			binary = identity
		}
	}
	if !binary.valid() {
		return inodeIdentity{}, errors.New("jailer: no QEMU binary was staged")
	}
	return binary, nil
}
