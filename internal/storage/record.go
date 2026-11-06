package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	MaxKeySize   = 1024
	MaxValueSize = 1 << 20 // 1 MB
)

// Operation represents one database operation.
type Operation struct {
	Type  OperationType
	Key   string
	Value []byte
}

// Transaction groups operations into one atomic unit.
type Transaction struct {
	ID         uint64
	Operations []Operation
}

// EncodeTransaction converts a transaction into a binary frame.
func EncodeTransaction(tx Transaction) ([]byte, error) {
	if len(tx.Operations) == 0 {
		return nil, errors.New("transaction cannot be empty")
	}

	if len(tx.Operations) > MaxBatchOperations {
		return nil, errors.New("too many operations")
	}

	payload := make([]byte, TransactionHeaderSize)

	binary.LittleEndian.PutUint64(payload[0:8], tx.ID)
	binary.LittleEndian.PutUint32(
		payload[8:12],
		uint32(len(tx.Operations)),
	)

	for _, op := range tx.Operations {
		if len(op.Key) == 0 || len(op.Key) > MaxKeySize {
			return nil, errors.New("key must be between 1 and 1024 bytes")
		}

		if len(op.Value) > MaxValueSize {
			return nil, errors.New("value exceeds 1 MB")
		}

		switch op.Type {
		case OpPut:
		case OpDelete:
			if len(op.Value) != 0 {
				return nil, errors.New("delete cannot contain a value")
			}
		default:
			return nil, errors.New("invalid operation type")
		}

		payload = append(payload, byte(op.Type))

		field := make([]byte, 8)
		binary.LittleEndian.PutUint32(
			field[0:4],
			uint32(len(op.Key)),
		)
		binary.LittleEndian.PutUint32(
			field[4:8],
			uint32(len(op.Value)),
		)

		payload = append(payload, field...)
		payload = append(payload, []byte(op.Key)...)
		payload = append(payload, op.Value...)
	}

	frame := make([]byte, FrameHeaderSize)
	copy(frame[0:4], FrameMagic)

	binary.LittleEndian.PutUint32(
		frame[4:8],
		uint32(len(payload)),
	)

	checksum := crc32.ChecksumIEEE(payload)
	binary.LittleEndian.PutUint32(frame[8:12], checksum)

	frame = append(frame, payload...)
	return frame, nil
}

// DecodeTransaction validates and decodes a binary frame.
func DecodeTransaction(frame []byte) (Transaction, error) {
	var tx Transaction

	if len(frame) < FrameHeaderSize {
		return tx, errors.New("incomplete frame header")
	}

	if string(frame[0:4]) != FrameMagic {
		return tx, errors.New("invalid frame magic")
	}

	payloadLength := binary.LittleEndian.Uint32(frame[4:8])
	if uint64(payloadLength) != uint64(len(frame)-FrameHeaderSize) {
		return tx, errors.New("invalid payload length")
	}

	payload := frame[FrameHeaderSize:]

	expectedChecksum := binary.LittleEndian.Uint32(frame[8:12])
	actualChecksum := crc32.ChecksumIEEE(payload)
	if expectedChecksum != actualChecksum {
		return tx, errors.New("checksum mismatch")
	}

	if len(payload) < TransactionHeaderSize {
		return tx, errors.New("incomplete transaction header")
	}

	tx.ID = binary.LittleEndian.Uint64(payload[0:8])
	count := binary.LittleEndian.Uint32(payload[8:12])

	if count == 0 || count > MaxBatchOperations {
		return Transaction{}, errors.New("invalid operation count")
	}

	offset := TransactionHeaderSize
	tx.Operations = make([]Operation, 0, count)

	for i := uint32(0); i < count; i++ {
		if len(payload)-offset < 9 {
			return Transaction{}, errors.New("incomplete operation")
		}

		opType := OperationType(payload[offset])
		offset++

		keyLength := binary.LittleEndian.Uint32(
			payload[offset : offset+4],
		)
		valueLength := binary.LittleEndian.Uint32(
			payload[offset+4 : offset+8],
		)
		offset += 8

		if keyLength == 0 || keyLength > MaxKeySize {
			return Transaction{}, errors.New("invalid key length")
		}

		if valueLength > MaxValueSize {
			return Transaction{}, errors.New("invalid value length")
		}

		if opType != OpPut && opType != OpDelete {
			return Transaction{}, errors.New("invalid operation type")
		}

		if opType == OpDelete && valueLength != 0 {
			return Transaction{}, errors.New("delete contains a value")
		}

		required := uint64(keyLength) + uint64(valueLength)
		if required > uint64(len(payload)-offset) {
			return Transaction{}, errors.New("incomplete operation data")
		}

		key := string(payload[offset : offset+int(keyLength)])
		offset += int(keyLength)

		value := make([]byte, int(valueLength))
		copy(value, payload[offset:offset+int(valueLength)])
		offset += int(valueLength)

		tx.Operations = append(tx.Operations, Operation{
			Type:  opType,
			Key:   key,
			Value: value,
		})
	}

	if offset != len(payload) {
		return Transaction{}, errors.New("unexpected trailing payload data")
	}

	return tx, nil
}
