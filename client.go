package main

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type chunkTask struct {
	Path    string
	Idx     int
	Offset  int64
	Length  int64
	Attempt int
}

type Manifest struct {
	Version   int              `json:"version"`
	ChunkSize int64            `json:"chunk_size"`
	BaseURL   string           `json:"base_url"`
	Remote    string           `json:"remote"`
	Completed map[string][]int `json:"completed"`

	mu    sync.Mutex `json:"-"`
	path  string     `json:"-"`
	dirty bool       `json:"-"`
}

func loadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	m := &Manifest{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, err
	}
	if m.Completed == nil {
		m.Completed = map[string][]int{}
	}
	m.path = path
	return m, nil
}

func (m *Manifest) markComplete(path string, idx int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Completed[path] = append(m.Completed[path], idx)
	m.dirty = true
}

func (m *Manifest) buildIndex() map[string]map[int]bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := make(map[string]map[int]bool, len(m.Completed))
	for p, ids := range m.Completed {
		s := make(map[int]bool, len(ids))
		for _, i := range ids {
			s[i] = true
		}
		idx[p] = s
	}
	return idx
}

func (m *Manifest) flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.dirty {
		return nil
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, m.path); err != nil {
		return err
	}
	m.dirty = false
	return nil
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "K"):
		mult = 1024
		s = strings.TrimSuffix(s, "K")
	case strings.HasSuffix(s, "M"):
		mult = 1024 * 1024
		s = strings.TrimSuffix(s, "M")
	case strings.HasSuffix(s, "G"):
		mult = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "G")
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return n * mult, nil
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// escapePath URL-escapes each path segment but keeps "/" separators intact.
func escapePath(p string) string {
	parts := strings.Split(p, "/")
	for i, x := range parts {
		parts[i] = url.PathEscape(x)
	}
	return strings.Join(parts, "/")
}

func runPull(args []string) error {
	fs_ := flag.NewFlagSet("pull", flag.ExitOnError)
	urlFlag := fs_.String("url", "", "server base URL (e.g. http://100.x.x.x:8080)")
	remote := fs_.String("remote", "", "remote path (file or dir, relative to server root); empty = whole root")
	localDir := fs_.String("local", "", "local destination directory")
	token := fs_.String("token", "", "bearer token")
	concurrency := fs_.Int("concurrency", 64, "number of parallel chunk workers")
	chunkSizeStr := fs_.String("chunk", "4M", "chunk size (e.g. 1M, 4M, 16M)")
	retries := fs_.Int("retries", 5, "max retries per chunk")
	timeout := fs_.Duration("chunk-timeout", 5*time.Minute, "per-chunk HTTP timeout")
	if err := fs_.Parse(args); err != nil {
		return err
	}
	if *urlFlag == "" || *localDir == "" || *token == "" {
		return errors.New("--url, --local, --token are required")
	}
	baseURL := strings.TrimSuffix(*urlFlag, "/")
	chunkSize, err := parseSize(*chunkSizeStr)
	if err != nil {
		return fmt.Errorf("invalid --chunk: %w", err)
	}
	if chunkSize <= 0 {
		return errors.New("--chunk must be positive")
	}
	if err := os.MkdirAll(*localDir, 0755); err != nil {
		return err
	}

	// Critical: force HTTP/1.1, raise per-host connection caps. Go's
	// defaults silently throttle real concurrency: ForceAttemptHTTP2=true
	// multiplexes everything onto one TCP connection (the opposite of what
	// we want here), and MaxIdleConnsPerHost defaults to 2.
	transport := &http.Transport{
		MaxIdleConns:        *concurrency + 16,
		MaxIdleConnsPerHost: *concurrency + 16,
		MaxConnsPerHost:     *concurrency + 16,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   false,
		TLSNextProto:        make(map[string]func(string, *tls.Conn) http.RoundTripper),
	}
	httpClient := &http.Client{Transport: transport, Timeout: *timeout}

	// 1. fetch file list
	listURL := fmt.Sprintf("%s/list?path=%s", baseURL, url.QueryEscape(*remote))
	req, _ := http.NewRequest("GET", listURL, nil)
	req.Header.Set("Authorization", "Bearer "+*token)
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("list request: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("list failed: %s — %s", resp.Status, body)
	}
	var entries []FileEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return err
	}
	if len(entries) == 0 {
		return errors.New("no files at remote path")
	}

	var totalBytes int64
	for _, e := range entries {
		totalBytes += e.Size
	}
	fmt.Printf("found %d files, %s total\n", len(entries), humanBytes(totalBytes))

	// 2. lazy file-handle map (one handle per file, opened on first write)
	fileHandles := map[string]*os.File{}
	var fhMu sync.Mutex
	getHandle := func(relPath string) (*os.File, error) {
		fhMu.Lock()
		defer fhMu.Unlock()
		if f, ok := fileHandles[relPath]; ok {
			return f, nil
		}
		localPath := filepath.Join(*localDir, filepath.FromSlash(relPath))
		if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		fileHandles[relPath] = f
		return f, nil
	}
	defer func() {
		fhMu.Lock()
		for _, f := range fileHandles {
			f.Close()
		}
		fhMu.Unlock()
	}()

	// Pre-allocate (Truncate) each local file to expected size so WriteAt
	// can land at any offset without growing the file repeatedly.
	for _, e := range entries {
		f, err := getHandle(e.Path)
		if err != nil {
			return fmt.Errorf("open %s: %w", e.Path, err)
		}
		if err := f.Truncate(e.Size); err != nil {
			return fmt.Errorf("truncate %s: %w", e.Path, err)
		}
	}

	// 3. manifest (resume)
	manifestPath := filepath.Join(*localDir, ".opensender-manifest.json")
	manifest, err := loadManifest(manifestPath)
	if err != nil {
		manifest = &Manifest{
			Version:   1,
			ChunkSize: chunkSize,
			BaseURL:   baseURL,
			Remote:    *remote,
			Completed: map[string][]int{},
			path:      manifestPath,
		}
	} else if manifest.ChunkSize != chunkSize {
		return fmt.Errorf("manifest chunk_size %d differs from --chunk %d; either delete %s or pass --chunk %d",
			manifest.ChunkSize, chunkSize, manifestPath, manifest.ChunkSize)
	}
	doneIdx := manifest.buildIndex()

	// 4. build chunk queue (global, all files mixed)
	var tasks []chunkTask
	var doneBytes int64
	for _, e := range entries {
		nChunks := int((e.Size + chunkSize - 1) / chunkSize)
		done := doneIdx[e.Path]
		for i := 0; i < nChunks; i++ {
			off := int64(i) * chunkSize
			length := chunkSize
			if off+length > e.Size {
				length = e.Size - off
			}
			if done[i] {
				doneBytes += length
				continue
			}
			tasks = append(tasks, chunkTask{Path: e.Path, Idx: i, Offset: off, Length: length})
		}
	}
	fmt.Printf("resumed: %s already done, %d chunks pending\n", humanBytes(doneBytes), len(tasks))
	if len(tasks) == 0 {
		fmt.Println("nothing to do — everything already present")
		return nil
	}

	// queue capacity == len(tasks): always sufficient because the number of
	// in-flight items (channel + workers) never exceeds the original count.
	queue := make(chan chunkTask, len(tasks))
	for _, t := range tasks {
		queue <- t
	}

	var transferred int64 = doneBytes
	var failedChunks int64
	var pending int64 = int64(len(tasks))
	startTime := time.Now()

	// progress reporter
	stopProgress := make(chan struct{})
	var progressDone sync.WaitGroup
	progressDone.Add(1)
	go func() {
		defer progressDone.Done()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		lastBytes := atomic.LoadInt64(&transferred)
		lastTime := time.Now()
		for {
			select {
			case <-stopProgress:
				return
			case <-ticker.C:
				now := time.Now()
				cur := atomic.LoadInt64(&transferred)
				dt := now.Sub(lastTime).Seconds()
				inst := 0.0
				if dt > 0 {
					inst = float64(cur-lastBytes) / dt
				}
				elapsed := now.Sub(startTime).Seconds()
				avg := 0.0
				if elapsed > 0 {
					avg = float64(cur-doneBytes) / elapsed
				}
				remaining := totalBytes - cur
				etaStr := "--:--"
				if avg > 0 {
					eta := time.Duration(float64(remaining)/avg) * time.Second
					etaStr = eta.Truncate(time.Second).String()
				}
				fmt.Printf("\r[%s/%s] %.1f%%  inst %s/s  avg %s/s  ETA %s   ",
					humanBytes(cur), humanBytes(totalBytes),
					100*float64(cur)/float64(totalBytes),
					humanBytes(int64(inst)), humanBytes(int64(avg)), etaStr)
				lastBytes = cur
				lastTime = now
			}
		}
	}()

	// manifest flusher
	stopFlush := make(chan struct{})
	var flushDone sync.WaitGroup
	flushDone.Add(1)
	go func() {
		defer flushDone.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopFlush:
				manifest.flush()
				return
			case <-ticker.C:
				manifest.flush()
			}
		}
	}()

	// 5. workers
	finalize := func() {
		// called when pending hits 0, exactly once
		close(queue)
	}
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range queue {
				buf, err := fetchChunk(httpClient, baseURL, *token, task)
				if err != nil {
					task.Attempt++
					if task.Attempt >= *retries {
						atomic.AddInt64(&failedChunks, 1)
						fmt.Fprintf(os.Stderr, "\nchunk %s#%d failed after %d retries: %v\n",
							task.Path, task.Idx, *retries, err)
						if atomic.AddInt64(&pending, -1) == 0 {
							finalize()
						}
						continue
					}
					time.Sleep(time.Duration(task.Attempt) * 500 * time.Millisecond)
					queue <- task
					continue
				}
				if err := writeChunk(buf, task, getHandle); err != nil {
					fmt.Fprintf(os.Stderr, "\nchunk %s#%d write failed: %v\n", task.Path, task.Idx, err)
					atomic.AddInt64(&failedChunks, 1)
					if atomic.AddInt64(&pending, -1) == 0 {
						finalize()
					}
					continue
				}
				manifest.markComplete(task.Path, task.Idx)
				atomic.AddInt64(&transferred, task.Length)
				if atomic.AddInt64(&pending, -1) == 0 {
					finalize()
				}
			}
		}()
	}
	wg.Wait()

	close(stopFlush)
	flushDone.Wait()
	close(stopProgress)
	progressDone.Wait()

	elapsed := time.Since(startTime)
	gotBytes := atomic.LoadInt64(&transferred) - doneBytes
	rate := float64(gotBytes) / elapsed.Seconds() / 1024 / 1024
	fmt.Printf("\ndone: %s downloaded in %s (%.2f MiB/s)\n",
		humanBytes(gotBytes), elapsed.Truncate(time.Second), rate)
	if failedChunks > 0 {
		return fmt.Errorf("%d chunks failed permanently — rerun to retry", failedChunks)
	}
	return nil
}

// fetchChunk performs the HTTP Range GET, validates SHA256, and returns the
// bytes. It does NOT write to disk — the caller decides whether to commit the
// bytes (used by hedging: a worker that loses the claim race discards the buf).
func fetchChunk(client *http.Client, baseURL, token string, task chunkTask) ([]byte, error) {
	u := fmt.Sprintf("%s/file/%s", baseURL, escapePath(task.Path))
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", task.Offset, task.Offset+task.Length-1))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("expected 206, got %s: %s", resp.Status, body)
	}
	expectedSHA := resp.Header.Get("X-Chunk-SHA256")
	if expectedSHA == "" {
		return nil, errors.New("server did not return X-Chunk-SHA256")
	}
	buf := make([]byte, task.Length)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	sum := sha256.Sum256(buf)
	got := hex.EncodeToString(sum[:])
	if got != expectedSHA {
		return nil, fmt.Errorf("sha256 mismatch (got %s, expected %s)", got, expectedSHA)
	}
	return buf, nil
}

// writeChunk commits the bytes to the local file at the chunk's offset.
func writeChunk(buf []byte, task chunkTask, getHandle func(string) (*os.File, error)) error {
	f, err := getHandle(task.Path)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(buf, task.Offset); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}
