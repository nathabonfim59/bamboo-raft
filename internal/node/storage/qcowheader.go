package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	qcowHeaderBytes      = 104
	qcowMaxBackingName   = 1023
	qcowIncompatDirty    = 1 << 0
	qcowIncompatCorrupt  = 1 << 1
	qcowIncompatDataFile = 1 << 2
)

// qcowHeader is what the node needs from a qcow2 header. The backing name is
// guest-influenced metadata: it is compared, never opened.
type qcowHeader struct {
	backing string
	// size is the virtual disk size in bytes.
	size uint64
}

// readQCOWHeader parses f's qcow2 header strictly: version 2 or 3, no
// dirty, corrupt or external-data-file feature, no encryption and a bounded
// backing name outside the header.
func readQCOWHeader(f *os.File) (qcowHeader, error) {
	raw := make([]byte, qcowHeaderBytes)
	n, err := f.ReadAt(raw, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return qcowHeader{}, err
	}
	if n < 4 || string(raw[:4]) != "QFI\xfb" {
		return qcowHeader{}, errors.New("not a qcow2 image")
	}
	if n < 72 {
		return qcowHeader{}, errors.New("short qcow2 header")
	}
	version := binary.BigEndian.Uint32(raw[4:8])
	if version != 2 && version != 3 {
		return qcowHeader{}, fmt.Errorf("unsupported qcow2 version %d", version)
	}
	backingOffset := binary.BigEndian.Uint64(raw[8:16])
	backingSize := binary.BigEndian.Uint32(raw[16:20])
	if binary.BigEndian.Uint32(raw[32:36]) != 0 {
		return qcowHeader{}, errors.New("unsupported feature: encryption")
	}
	size := binary.BigEndian.Uint64(raw[24:32])
	headerLength := uint64(72)
	if version == 3 {
		if n < qcowHeaderBytes {
			return qcowHeader{}, errors.New("short qcow2 v3 header")
		}
		incompatible := binary.BigEndian.Uint64(raw[72:80])
		if incompatible&(qcowIncompatDirty|qcowIncompatCorrupt|qcowIncompatDataFile) != 0 {
			return qcowHeader{}, fmt.Errorf("unsupported feature: incompatible bits %#x", incompatible)
		}
		headerLength = uint64(binary.BigEndian.Uint32(raw[100:104]))
		if headerLength < qcowHeaderBytes {
			return qcowHeader{}, fmt.Errorf("invalid qcow2 header length %d", headerLength)
		}
	}
	if backingSize == 0 {
		return qcowHeader{size: size}, nil
	}
	if backingSize > qcowMaxBackingName {
		return qcowHeader{}, fmt.Errorf("backing name is %d bytes", backingSize)
	}
	if backingOffset < headerLength {
		return qcowHeader{}, errors.New("backing name overlaps the qcow2 header")
	}
	name := make([]byte, backingSize)
	if _, err := f.ReadAt(name, int64(backingOffset)); err != nil {
		return qcowHeader{}, fmt.Errorf("read backing name: %w", err)
	}
	return qcowHeader{backing: string(name), size: size}, nil
}
