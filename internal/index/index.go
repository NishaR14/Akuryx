package index

import "sync"

// Entry identifies an operation inside a transaction in the database file.
type Entry struct {
	Offset       int64
	OperationPos int
}

// Index tracks the latest location of each key.
type Index struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

// New creates an empty index.
func New() *Index {
	return &Index{
		entries: make(map[string]Entry),
	}
}

// Set records the location of a key.
func (idx *Index) Set(key string, entry Entry) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.entries[key] = entry
}

// Get returns the location of a key.
func (idx *Index) Get(key string) (Entry, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entry, exists := idx.entries[key]
	return entry, exists
}

// Delete removes a key.
func (idx *Index) Delete(key string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	delete(idx.entries, key)
}

// Len returns the number of keys.
func (idx *Index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return len(idx.entries)
}

// Entries returns a copy of all key-location pairs.
func (idx *Index) Entries() map[string]Entry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make(map[string]Entry, len(idx.entries))

	for key, entry := range idx.entries {
		result[key] = entry
	}

	return result
}
