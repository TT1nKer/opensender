package main

import (
	"fmt"
	"os"
)

// FileEntry is the JSON shape returned by /list and consumed by the client.
type FileEntry struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "serve":
		if err := runServe(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "pull":
		if err := runPull(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "init":
		if err := runInit(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `opensender — high-concurrency file transfer over high-latency links

Usage:
  opensender init                                    # write ~/.opensender.json
  opensender pull --remote PATH                      # daily use, after init
  opensender pull --remote DIR --include '*.pdf'     # pull matching files
  opensender pull --url URL --remote PATH --local DIR --token TOKEN
                  [--concurrency N] [--chunk SIZE]   # full form (no config)
  opensender serve --root DIR --token TOKEN [--listen ADDR]

Notes:
  - Built-in defaults (concurrency=1024, chunk=256K) are tuned for the
    target link from real-world benchmarks. Override per-run with flags.
  - 'init' creates ~/.opensender.json so url/token/local don't need to be
    typed every time. CLI flags always override config.
`)
}
