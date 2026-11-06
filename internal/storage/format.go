
package storage

// Database file identification.
const (
    // Magic number identifies an Akuryx database.
    FileMagic = "AKURYXDB"

    // Current database format version.
    FormatVersion uint32 = 1

    // Database header layout:
    // 8 bytes magic + 4 bytes version + 4 bytes checksum.
    FileHeaderSize = 16
)

// Record frame identification.
const (
    // Identifies the beginning of a transaction frame.
    FrameMagic = "AKTX"

    // Frame header:
    // 4 bytes magic + 4 bytes payload length + 4 bytes checksum.
    FrameHeaderSize = 12
)

// Operation types stored inside transaction records.
type OperationType byte

const (
    OpPut OperationType = 1
    OpDelete OperationType = 2
)

// Transaction payload layout:
// 8 bytes transaction ID
// 4 bytes operation count
// Followed by encoded operations.
const (
    TransactionHeaderSize = 12
)

// Maximum number of operations in one batch.
const MaxBatchOperations = 10000
