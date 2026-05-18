package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func runServe(args []string) error {
	fs_ := flag.NewFlagSet("serve", flag.ExitOnError)
	root := fs_.String("root", "", "root directory to serve")
	token := fs_.String("token", "", "bearer token for auth")
	listen := fs_.String("listen", ":8080", "listen address")
	if err := fs_.Parse(args); err != nil {
		return err
	}
	if *root == "" || *token == "" {
		return errors.New("--root and --token are required")
	}
	rootAbs, err := filepath.Abs(*root)
	if err != nil {
		return err
	}
	info, err := os.Stat(rootAbs)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("--root must be a directory")
	}

	srv := &server{root: rootAbs, token: *token}
	mux := http.NewServeMux()
	mux.HandleFunc("/list", srv.auth(srv.handleList))
	mux.HandleFunc("/file/", srv.auth(srv.handleFile))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	s := &http.Server{Addr: *listen, Handler: mux}
	fmt.Printf("opensender serve: root=%s listen=%s\n", rootAbs, *listen)
	return s.ListenAndServe()
}

type server struct {
	root  string
	token string
}

func (s *server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+s.token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// safePath turns a URL-style relative path into an absolute filesystem path
// inside the root, rejecting any attempt to escape via ".." or absolute paths.
func (s *server) safePath(rel string) (string, error) {
	decoded, err := url.PathUnescape(rel)
	if err != nil {
		return "", err
	}
	// Clean and re-anchor under root. filepath.Clean strips ".." segments.
	clean := filepath.Clean("/" + filepath.ToSlash(decoded))
	abs := filepath.Join(s.root, filepath.FromSlash(strings.TrimPrefix(clean, "/")))
	relCheck, err := filepath.Rel(s.root, abs)
	if err != nil || strings.HasPrefix(relCheck, "..") {
		return "", errors.New("path escapes root")
	}
	return abs, nil
}

func (s *server) handleList(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Query().Get("path")
	abs, err := s.safePath(p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	info, err := os.Stat(abs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	var entries []FileEntry
	if info.IsDir() {
		err := filepath.WalkDir(abs, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			fi, err := d.Info()
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(s.root, path)
			if err != nil {
				return err
			}
			entries = append(entries, FileEntry{
				Path: filepath.ToSlash(rel),
				Size: fi.Size(),
			})
			return nil
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		rel, _ := filepath.Rel(s.root, abs)
		entries = append(entries, FileEntry{
			Path: filepath.ToSlash(rel),
			Size: info.Size(),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

func (s *server) handleFile(w http.ResponseWriter, r *http.Request) {
	rel := strings.TrimPrefix(r.URL.Path, "/file/")
	abs, err := s.safePath(rel)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f, err := os.Open(abs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fileSize := info.Size()

	rangeHeader := r.Header.Get("Range")
	var start, end int64
	if rangeHeader == "" {
		start, end = 0, fileSize-1
	} else {
		start, end, err = parseRange(rangeHeader, fileSize)
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
			return
		}
	}
	length := end - start + 1

	// Per-chunk validation: hash the bytes we're about to send and put it in
	// a response header so the client can verify after receiving the body.
	// Reading into memory is acceptable because the client picks the chunk
	// size (typically 1-16 MB).
	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, start); err != nil && err != io.EOF {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sum := sha256.Sum256(buf)

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
	w.Header().Set("X-Chunk-SHA256", hex.EncodeToString(sum[:]))
	w.Header().Set("Accept-Ranges", "bytes")
	if rangeHeader != "" {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Write(buf)
}

func parseRange(h string, size int64) (int64, int64, error) {
	if !strings.HasPrefix(h, "bytes=") {
		return 0, 0, errors.New("invalid range header")
	}
	spec := strings.TrimPrefix(h, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, 0, errors.New("invalid range header (need bytes=START-END)")
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if start < 0 || end >= size || start > end {
		return 0, 0, fmt.Errorf("range %d-%d out of bounds (size %d)", start, end, size)
	}
	return start, end, nil
}
