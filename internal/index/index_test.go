package index

import "testing"

func TestSetAndGet(t *testing.T) {
	idx := New()

	expected := Entry{Offset: 100, OperationPos: 0}
	idx.Set("apple", expected)

	entry, exists := idx.Get("apple")
	if !exists {
		t.Fatal("expected key to exist")
	}
	if entry != expected {
		t.Fatalf("expected %+v, got %+v", expected, entry)
	}
}

func TestDelete(t *testing.T) {
	idx := New()
	idx.Set("apple", Entry{Offset: 100})
	idx.Delete("apple")

	_, exists := idx.Get("apple")
	if exists {
		t.Fatal("expected key to be deleted")
	}
}

func TestLen(t *testing.T) {
	idx := New()
	idx.Set("apple", Entry{Offset: 100})
	idx.Set("banana", Entry{Offset: 200})

	if idx.Len() != 2 {
		t.Fatalf("expected length 2, got %d", idx.Len())
	}
}

func TestUpdateExistingKey(t *testing.T) {
	idx := New()
	idx.Set("apple", Entry{Offset: 100})

	expected := Entry{Offset: 250, OperationPos: 1}
	idx.Set("apple", expected)

	entry, exists := idx.Get("apple")
	if !exists || entry != expected {
		t.Fatalf("expected %+v, got %+v", expected, entry)
	}
}
