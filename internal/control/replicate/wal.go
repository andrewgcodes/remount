package replicate

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	walHeaderBytes = int64(32)
	walMagicLE     = uint32(0x377f0682)
	walMagicBE     = uint32(0x377f0683)
	walVersion     = uint32(3007000)
)

type walInspection struct {
	CommittedBytes int64
	Lineage        string
}

func inspectWAL(reader io.ReaderAt, size, max int64, requireExact bool) (walInspection, error) {
	if size < walHeaderBytes || size > max {
		return walInspection{}, fmt.Errorf("%w: WAL size %d outside [32,%d]", ErrCorruptRecoveryPoint, size, max)
	}
	header := make([]byte, walHeaderBytes)
	if _, err := reader.ReadAt(header, 0); err != nil {
		return walInspection{}, fmt.Errorf("read WAL header: %w", err)
	}
	magic := binary.BigEndian.Uint32(header[0:4])
	var checksumOrder binary.ByteOrder
	switch magic {
	case walMagicLE:
		checksumOrder = binary.LittleEndian
	case walMagicBE:
		checksumOrder = binary.BigEndian
	default:
		return walInspection{}, fmt.Errorf("%w: invalid WAL magic", ErrCorruptRecoveryPoint)
	}
	if binary.BigEndian.Uint32(header[4:8]) != walVersion {
		return walInspection{}, fmt.Errorf("%w: unsupported WAL version", ErrCorruptRecoveryPoint)
	}
	pageSize := binary.BigEndian.Uint32(header[8:12])
	if pageSize == 1 {
		pageSize = 65536
	}
	if pageSize < 512 || pageSize > 65536 || pageSize&(pageSize-1) != 0 {
		return walInspection{}, fmt.Errorf("%w: invalid WAL page size", ErrCorruptRecoveryPoint)
	}
	s0, s1 := walChecksum(checksumOrder, header[:24], 0, 0)
	if binary.BigEndian.Uint32(header[24:28]) != s0 || binary.BigEndian.Uint32(header[28:32]) != s1 {
		return walInspection{}, fmt.Errorf("%w: invalid WAL header checksum", ErrCorruptRecoveryPoint)
	}
	salt1, salt2 := binary.BigEndian.Uint32(header[16:20]), binary.BigEndian.Uint32(header[20:24])
	inspection := walInspection{Lineage: fmt.Sprintf("%08x%08x", salt1, salt2)}
	frameSize := int64(24) + int64(pageSize)
	frame := make([]byte, frameSize)
	for offset := walHeaderBytes; offset+frameSize <= size; offset += frameSize {
		if _, err := reader.ReadAt(frame, offset); err != nil {
			return walInspection{}, fmt.Errorf("read WAL frame: %w", err)
		}
		if binary.BigEndian.Uint32(frame[0:4]) == 0 || binary.BigEndian.Uint32(frame[8:12]) != salt1 || binary.BigEndian.Uint32(frame[12:16]) != salt2 {
			break
		}
		n0, n1 := walChecksum(checksumOrder, frame[:8], s0, s1)
		n0, n1 = walChecksum(checksumOrder, frame[24:], n0, n1)
		if binary.BigEndian.Uint32(frame[16:20]) != n0 || binary.BigEndian.Uint32(frame[20:24]) != n1 {
			break
		}
		s0, s1 = n0, n1
		if binary.BigEndian.Uint32(frame[4:8]) != 0 {
			inspection.CommittedBytes = offset + frameSize
		}
	}
	if inspection.CommittedBytes == 0 {
		return inspection, nil
	}
	if requireExact && inspection.CommittedBytes != size {
		return walInspection{}, fmt.Errorf("%w: WAL has bytes after its last valid commit", ErrCorruptRecoveryPoint)
	}
	return inspection, nil
}

func walChecksum(order binary.ByteOrder, data []byte, s0, s1 uint32) (uint32, uint32) {
	for len(data) >= 8 {
		s0 += order.Uint32(data[:4]) + s1
		s1 += order.Uint32(data[4:8]) + s0
		data = data[8:]
	}
	return s0, s1
}

func validateStoredWAL(reader io.ReaderAt, size, max int64, lineage string) error {
	inspection, err := inspectWAL(reader, size, max, true)
	if err != nil {
		return err
	}
	if inspection.CommittedBytes == 0 {
		return errors.New("control replication: stored WAL has no commit")
	}
	if lineage != "" && inspection.Lineage != lineage {
		return fmt.Errorf("%w: WAL lineage does not match manifest", ErrCorruptRecoveryPoint)
	}
	return nil
}
