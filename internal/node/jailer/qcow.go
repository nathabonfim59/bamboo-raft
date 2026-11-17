package jailer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	qcowHeaderBytes = 104
	maxBackingName  = 4095

	qcowIncompatibleDirty    = 1 << 0
	qcowIncompatibleCorrupt  = 1 << 1
	qcowIncompatibleDataFile = 1 << 2
)

var qcowMagic = [4]byte{'Q', 'F', 'I', 0xfb}

type qcowHeader struct {
	backingOffset uint64
	backingSize   uint32
	incompatible  uint64
	encrypted     bool
}

// validateDiskChain proves that the explicit descriptors cover the complete
// qcow2 chain. It deliberately never interprets or opens the backing filename:
// that string is guest-controlled metadata. A10 renders an explicit blockdev
// graph whose every file node is one of these inherited descriptors.
//
// It returns, per disk, whether the image is qcow2 (true) or raw (false).
func validateDiskChain(spec LaunchSpec) ([]bool, error) {
	formats := make([]bool, 0, len(spec.Disks))
	seen := make(map[string]struct{}, len(spec.Disks))
	for i, name := range spec.Disks {
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("jailer: disk descriptor %q occurs twice in the chain", name)
		}
		seen[name] = struct{}{}
		f := spec.descriptor(name)
		if f == nil {
			return nil, fmt.Errorf("jailer: missing disk descriptor %q", name)
		}
		header, isQCOW, err := readQCOWHeader(f)
		if err != nil {
			return nil, fmt.Errorf("jailer: disk %s: %w", name, err)
		}
		if !isQCOW {
			if i != 0 || len(spec.Disks) != 1 {
				return nil, fmt.Errorf("jailer: disk %s is raw; only a standalone raw base is supported", name)
			}
			formats = append(formats, false)
			continue
		}
		if header.incompatible&(qcowIncompatibleDirty|qcowIncompatibleCorrupt|qcowIncompatibleDataFile) != 0 {
			return nil, fmt.Errorf("jailer: disk %s has unsupported dirty, corrupt or external-data-file features %#x", name, header.incompatible)
		}
		if header.encrypted {
			return nil, fmt.Errorf("jailer: disk %s is encrypted; no secret prompt is available", name)
		}
		if i == 0 && header.backingSize != 0 {
			return nil, fmt.Errorf("jailer: base disk %s still names a backing file; the explicit chain is incomplete", name)
		}
		if i > 0 && header.backingSize == 0 {
			return nil, fmt.Errorf("jailer: overlay disk %s has no backing file but follows another disk", name)
		}
		if header.backingSize > maxBackingName {
			return nil, fmt.Errorf("jailer: disk %s backing name is %d bytes", name, header.backingSize)
		}
		if header.backingSize != 0 {
			if header.backingOffset < qcowHeaderBytes {
				return nil, fmt.Errorf("jailer: disk %s backing name points inside its header", name)
			}
			nameBytes := make([]byte, int(header.backingSize))
			if _, err := f.ReadAt(nameBytes, int64(header.backingOffset)); err != nil {
				return nil, fmt.Errorf("jailer: disk %s backing name: %w", name, err)
			}
		}
		formats = append(formats, true)
	}
	return formats, nil
}

func readQCOWHeader(f *os.File) (qcowHeader, bool, error) {
	raw := make([]byte, qcowHeaderBytes)
	n, err := f.ReadAt(raw, 0)
	if errors.Is(err, io.EOF) && n >= 4 {
		err = nil
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return qcowHeader{}, false, err
	}
	if n < 4 || [4]byte(raw[:4]) != qcowMagic {
		return qcowHeader{}, false, nil
	}
	if n < 44 {
		return qcowHeader{}, false, errors.New("short qcow2 header")
	}
	version := binary.BigEndian.Uint32(raw[4:8])
	if version != 2 && version != 3 {
		return qcowHeader{}, false, fmt.Errorf("unsupported qcow version %d", version)
	}
	header := qcowHeader{
		backingOffset: binary.BigEndian.Uint64(raw[8:16]),
		backingSize:   binary.BigEndian.Uint32(raw[16:20]),
		encrypted:     binary.BigEndian.Uint32(raw[32:36]) != 0,
	}
	if version == 3 {
		if n < qcowHeaderBytes {
			return qcowHeader{}, false, errors.New("short qcow2 v3 header")
		}
		header.incompatible = binary.BigEndian.Uint64(raw[72:80])
		headerLength := binary.BigEndian.Uint32(raw[100:104])
		if headerLength < qcowHeaderBytes {
			return qcowHeader{}, false, fmt.Errorf("invalid qcow2 header length %d", headerLength)
		}
		if header.backingSize != 0 && header.backingOffset < uint64(headerLength) {
			return qcowHeader{}, false, errors.New("backing name overlaps qcow2 header")
		}
	}
	return header, true, nil
}
