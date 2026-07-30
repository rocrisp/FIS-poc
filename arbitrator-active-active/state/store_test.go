package state

import (
	"encoding/json"
	"sync"
	"testing"
)

func TestNewStore(t *testing.T) {
	s := NewStore()
	data, version, _ := s.Read()
	if version != 0 {
		t.Fatalf("expected version 0, got %d", version)
	}
	if data != nil {
		t.Fatalf("expected nil data, got %s", data)
	}
}

func TestWriteAndRead(t *testing.T) {
	s := NewStore()
	payload := json.RawMessage(`{"decision":"keep-cluster1"}`)

	newVersion, err := s.Write(payload, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if newVersion != 1 {
		t.Fatalf("expected version 1, got %d", newVersion)
	}

	data, version, updated := s.Read()
	if version != 1 {
		t.Fatalf("expected version 1, got %d", version)
	}
	if string(data) != `{"decision":"keep-cluster1"}` {
		t.Fatalf("unexpected data: %s", data)
	}
	if updated.IsZero() {
		t.Fatal("expected non-zero updated time")
	}
}

func TestWriteVersionConflict(t *testing.T) {
	s := NewStore()
	payload := json.RawMessage(`{"decision":"keep-cluster1"}`)

	_, err := s.Write(payload, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Try to write with stale version 0
	_, err = s.Write(json.RawMessage(`{"decision":"keep-cluster2"}`), 0)
	if err != ErrVersionConflict {
		t.Fatalf("expected ErrVersionConflict, got %v", err)
	}

	// Data should still be the first write
	data, version, _ := s.Read()
	if version != 1 {
		t.Fatalf("expected version 1, got %d", version)
	}
	if string(data) != `{"decision":"keep-cluster1"}` {
		t.Fatalf("expected first write to persist, got %s", data)
	}
}

func TestWriteSequentialVersions(t *testing.T) {
	s := NewStore()

	v1, _ := s.Write(json.RawMessage(`{"step":1}`), 0)
	v2, _ := s.Write(json.RawMessage(`{"step":2}`), v1)
	v3, _ := s.Write(json.RawMessage(`{"step":3}`), v2)

	if v3 != 3 {
		t.Fatalf("expected version 3, got %d", v3)
	}

	data, _, _ := s.Read()
	if string(data) != `{"step":3}` {
		t.Fatalf("unexpected data: %s", data)
	}
}

func TestConcurrentWrites(t *testing.T) {
	s := NewStore()
	const writers = 10
	var wg sync.WaitGroup
	successes := make(chan int64, writers)
	conflicts := make(chan struct{}, writers)

	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(id int) {
			defer wg.Done()
			payload := json.RawMessage(`{"writer":` + string(rune('0'+id)) + `}`)
			newVersion, err := s.Write(payload, 0)
			if err == ErrVersionConflict {
				conflicts <- struct{}{}
			} else if err == nil {
				successes <- newVersion
			}
		}(i)
	}

	wg.Wait()
	close(successes)
	close(conflicts)

	successCount := 0
	for range successes {
		successCount++
	}
	conflictCount := 0
	for range conflicts {
		conflictCount++
	}

	if successCount != 1 {
		t.Fatalf("expected exactly 1 success, got %d", successCount)
	}
	if conflictCount != writers-1 {
		t.Fatalf("expected %d conflicts, got %d", writers-1, conflictCount)
	}
}
