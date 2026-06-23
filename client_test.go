package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- chunkKey ---

func TestChunkKey(t *testing.T) {
	k := chunkKey(chunkTask{Path: "a/b", Idx: 42})
	if k != "a/b\x0042" {
		t.Errorf("unexpected key: %q", k)
	}
}

// --- parseSize ---

func TestParseSize(t *testing.T) {
	tests := []struct {
		in  string
		out int64
		err bool
	}{
		{"0", 0, false},
		{"100", 100, false},
		{"1K", 1024, false},
		{"2k", 2048, false},
		{"3M", 3 * 1024 * 1024, false},
		{"1G", 1024 * 1024 * 1024, false},
		{" 4M ", 4 * 1024 * 1024, false},
	}
	for _, tt := range tests {
		n, err := parseSize(tt.in)
		if tt.err && err == nil {
			t.Errorf("parseSize(%q) expected error", tt.in)
		}
		if !tt.err && err != nil {
			t.Errorf("parseSize(%q) unexpected error: %v", tt.in, err)
		}
		if n != tt.out {
			t.Errorf("parseSize(%q) = %d, want %d", tt.in, n, tt.out)
		}
	}
}

// --- humanBytes ---

func TestHumanBytes(t *testing.T) {
	if s := humanBytes(100); s != "100 B" {
		t.Errorf("humanBytes(100) = %q", s)
	}
	if s := humanBytes(1024); s != "1.00 KiB" {
		t.Errorf("humanBytes(1024) = %q", s)
	}
}

// --- escapePath ---

func TestEscapePath(t *testing.T) {
	if s := escapePath("a b/c"); s != "a%20b/c" {
		t.Errorf("escapePath = %q", s)
	}
}

// --- Manifest markComplete dedup ---

func TestMarkCompleteDedup(t *testing.T) {
	m := &Manifest{
		Completed: map[string][]int{},
	}
	m.markComplete("f", 1)
	m.markComplete("f", 2)
	m.markComplete("f", 1) // duplicate
	m.markComplete("f", 2) // duplicate
	m.markComplete("f", 3)

	m.mu.Lock()
	ids := m.Completed["f"]
	m.mu.Unlock()

	if len(ids) != 3 {
		t.Fatalf("expected 3 unique entries, got %d: %v", len(ids), ids)
	}
	for i, expect := range []int{1, 2, 3} {
		if ids[i] != expect {
			t.Errorf("ids[%d] = %d, want %d", i, ids[i], expect)
		}
	}
}

func TestMarkCompleteEmpty(t *testing.T) {
	m := &Manifest{
		Completed: map[string][]int{},
	}
	// no-op on empty path
	m.markComplete("x", 0)
	if len(m.Completed["x"]) != 1 {
		t.Fatal("expected 1 entry")
	}
}

// --- Manifest buildIndex ---

func TestBuildIndex(t *testing.T) {
	m := &Manifest{
		Completed: map[string][]int{
			"a": {1, 2, 3},
			"b": {5},
		},
	}
	idx := m.buildIndex()
	if !idx["a"][1] || !idx["a"][2] || !idx["a"][3] {
		t.Error("missing entries for a")
	}
	if idx["a"][0] {
		t.Error("unexpected entry 0 for a")
	}
	if !idx["b"][5] {
		t.Error("missing entry 5 for b")
	}
}

// --- Manifest flush/load round-trip ---

func TestManifestFlushLoad(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "m.json")

	m := &Manifest{
		Version:   1,
		ChunkSize: 256 * 1024,
		BaseURL:   "http://example.com:8080",
		Remote:    "data/",
		Completed: map[string][]int{
			"f1": {0, 1, 2},
			"f2": {0},
		},
		path:  p,
		dirty: true,
	}
	if err := m.flush(); err != nil {
		t.Fatal(err)
	}
	// verify file exists
	if _, err := os.Stat(p); err != nil {
		t.Fatal(err)
	}

	m2, err := loadManifest(p)
	if err != nil {
		t.Fatal(err)
	}
	if m2.Version != 1 || m2.ChunkSize != 256*1024 || m2.BaseURL != "http://example.com:8080" {
		t.Error("metadata mismatch")
	}
	if len(m2.Completed["f1"]) != 3 {
		t.Error("completed f1 mismatch")
	}
}

// --- Manifest flush skip when not dirty ---

func TestManifestFlushSkipClean(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "m2.json")
	m := &Manifest{
		path:  p,
		dirty: false,
	}
	if err := m.flush(); err != nil {
		t.Fatal("flush on clean manifest should be no-op")
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Error("expected manifest file not to exist when flush skipped")
	}
}

// --- Concurrent markComplete (stress test for dedup + mutex) ---

func TestMarkCompleteConcurrent(t *testing.T) {
	m := &Manifest{
		Completed: map[string][]int{},
	}
	const n = 1000
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			m.markComplete("f", idx)
		}(i)
	}
	wg.Wait()

	m.mu.Lock()
	ids := m.Completed["f"]
	m.mu.Unlock()

	if len(ids) != n {
		t.Errorf("expected %d unique entries, got %d", n, len(ids))
	}
}

// --- chunkState done flag: success-before-failure race ---
// Simulates: success path sets done=true, then failure path tries to claim.
// The failure path should detect done=true and re-queue rather than declare failure.

func TestDoneFlagPreventsFalseFailure(t *testing.T) {
	states := &sync.Map{}
	task := chunkTask{Path: "f", Idx: 0, Offset: 0, Length: 100}
	key := chunkKey(task)
	cs := &chunkState{task: task}
	states.Store(key, cs)

	var failedChunks int64
	var doneCount int64
	_ = int64(1) // totalChunks placeholder

	// Simulate success path: set done=true BEFORE LoadAndDelete
	sRaw, _ := states.Load(key)
	s := sRaw.(*chunkState)
	s.mu.Lock()
	s.done = true
	s.mu.Unlock()

	// Simulate failure path: load, then LoadAndDelete, then check done
	sRaw2, _ := states.Load(key)
	if _, claimed := states.LoadAndDelete(key); claimed {
		if sRaw2 != nil {
			s2 := sRaw2.(*chunkState)
			s2.mu.Lock()
			wasDone := s2.done
			s2.mu.Unlock()
			if wasDone {
				// Should re-queue, not fail
				states.Store(key, &chunkState{task: task, done: true})
			} else {
				atomic.AddInt64(&failedChunks, 1)
				atomic.AddInt64(&doneCount, 1)
			}
		}
	}

	if failedChunks != 0 {
		t.Errorf("expected 0 failed chunks, got %d — done flag should have prevented false failure", failedChunks)
	}
}

// --- chunkState done flag: failure should win when no speculator ---

func TestDoneFlagFailureWinsWhenNoSpeculator(t *testing.T) {
	states := &sync.Map{}
	task := chunkTask{Path: "f", Idx: 0, Offset: 0, Length: 100}
	key := chunkKey(task)
	cs := &chunkState{task: task}
	states.Store(key, cs)

	var failedChunks int64
	var doneCount int64

	// Failure path: LoadAndDelete, then check done (not set)
	sRaw, _ := states.Load(key)
	if _, claimed := states.LoadAndDelete(key); claimed {
		if sRaw != nil {
			s := sRaw.(*chunkState)
			s.mu.Lock()
			wasDone := s.done
			s.mu.Unlock()
			if wasDone {
				t.Error("done should be false")
			} else {
				atomic.AddInt64(&failedChunks, 1)
				atomic.AddInt64(&doneCount, 1)
			}
		}
	}

	if failedChunks != 1 {
		t.Errorf("expected 1 failed chunk, got %d", failedChunks)
	}
	if doneCount != 1 {
		t.Errorf("expected doneCount=1, got %d", doneCount)
	}
}

// --- writeChunk with real file: verify fsync path and data integrity ---

func TestWriteChunkDataIntegrity(t *testing.T) {
	dir := t.TempDir()
	localPath := filepath.Join(dir, "data.bin")

	var fhMu sync.Mutex
	fileHandles := map[string]*os.File{}
	getHandle := func(relPath string) (*os.File, error) {
		fhMu.Lock()
		defer fhMu.Unlock()
		if f, ok := fileHandles[relPath]; ok {
			return f, nil
		}
		f, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		fileHandles[relPath] = f
		return f, nil
	}
	defer func() {
		for _, f := range fileHandles {
			f.Close()
		}
	}()

	// Truncate to 4096
	f, err := getHandle("data.bin")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(4096); err != nil {
		t.Fatal(err)
	}

	// Write chunks at different offsets
	task1 := chunkTask{Path: "data.bin", Idx: 0, Offset: 0, Length: 1024}
	buf1 := make([]byte, 1024)
	for i := range buf1 {
		buf1[i] = byte(i % 256)
	}
	if err := writeChunk(buf1, task1, getHandle); err != nil {
		t.Fatal(err)
	}

	task2 := chunkTask{Path: "data.bin", Idx: 1, Offset: 2048, Length: 1024}
	buf2 := make([]byte, 1024)
	for i := range buf2 {
		buf2[i] = byte((i + 128) % 256)
	}
	if err := writeChunk(buf2, task2, getHandle); err != nil {
		t.Fatal(err)
	}

	// Close to ensure everything is on disk
	for _, f := range fileHandles {
		f.Close()
	}
	fileHandles = map[string]*os.File{}

	// Read back and verify
	data, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 4096 {
		t.Fatalf("expected file size 4096, got %d", len(data))
	}

	for i := 0; i < 1024; i++ {
		if data[i] != byte(i%256) {
			t.Fatalf("mismatch at offset %d: got %d, want %d", i, data[i], byte(i%256))
		}
	}
	// middle section (1024-2047) should be zeros (Truncate holes)
	for i := 1024; i < 2048; i++ {
		if data[i] != 0 {
			t.Fatalf("expected zero at offset %d, got %d", i, data[i])
		}
	}
	for i := 2048; i < 3072; i++ {
		if data[i] != byte((i-2048+128)%256) {
			t.Fatalf("mismatch at offset %d: got %d, want %d", i, data[i], byte((i-2048+128)%256))
		}
	}
}

// --- Manifest BaseURL/Remote mismatch — should produce warning and fresh manifest ---
// (Test at the logic level — the real runPull is an integration test)

func TestLoadManifestJSON(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "manifest.json")

	data, _ := json.MarshalIndent(&Manifest{
		Version:   1,
		ChunkSize: 256 * 1024,
		BaseURL:   "http://old:8080",
		Remote:    "old/",
		Completed: map[string][]int{"f": {0, 1}},
	}, "", "  ")
	os.WriteFile(p, data, 0644)

	m, err := loadManifest(p)
	if err != nil {
		t.Fatal(err)
	}
	if m.BaseURL != "http://old:8080" {
		t.Errorf("BaseURL = %q", m.BaseURL)
	}
	if m.Remote != "old/" {
		t.Errorf("Remote = %q", m.Remote)
	}
	if len(m.Completed["f"]) != 2 {
		t.Error("completed entries lost")
	}
}

// --- chunkState concurrency stress test ---

func TestChunkStateConcurrentDoneFlag(t *testing.T) {
	states := &sync.Map{}
	task := chunkTask{Path: "f", Idx: 0, Offset: 0, Length: 100}
	key := chunkKey(task)
	cs := &chunkState{task: task, lastInjection: time.Now()}
	states.Store(key, cs)

	const workers = 50
	var setDone int32
	var wg sync.WaitGroup

	// Simulate multiple speculators all trying to set done
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if sRaw, ok := states.Load(key); ok {
				s := sRaw.(*chunkState)
				s.mu.Lock()
				s.done = true
				s.mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// All sets should succeed — just verify no panic
	if sRaw, ok := states.Load(key); ok {
		s := sRaw.(*chunkState)
		s.mu.Lock()
		if !s.done {
			t.Error("done should be true after all workers")
		}
		s.mu.Unlock()
	}
	_ = setDone
}

// --- Test that chunkState.lastInjection updates are thread-safe ---

func TestChunkStateLastInjectionConcurrent(t *testing.T) {
	cs := &chunkState{lastInjection: time.Now()}
	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cs.mu.Lock()
			cs.lastInjection = time.Now()
			cs.specCount++
			cs.mu.Unlock()
		}()
	}
	wg.Wait()
	if cs.specCount != n {
		t.Errorf("specCount = %d, want %d", cs.specCount, n)
	}
}
