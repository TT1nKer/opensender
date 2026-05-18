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
  opensender serve --root DIR --token TOKEN [--listen ADDR]
  opensender pull  --url URL --remote PATH --local DIR --token TOKEN
                   [--concurrency N] [--chunk SIZE] [--retries N]

Examples:
  # Server (Linux side)
  opensender serve --root /data/models --token s3cret --listen :8080

  # Client (Windows side)
  opensender.exe pull --url http://100.x.x.x:8080 \
      --remote checkpoints/ --local D:\models\ \
      --concurrency 128 --chunk 4M --token s3cret
`)
}
