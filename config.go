package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Config holds user-specific defaults for `opensender pull`. It is loaded
// from ~/.opensender.json (Windows: %USERPROFILE%\.opensender.json) before
// CLI flags are parsed, so flags override config values.
//
// Daily usage after running `opensender init`:
//
//	opensender pull --remote checkpoints/sd_xl.safetensors
//
// The url/token/local are taken from config; concurrency/chunk fall back to
// in-binary sweet-spot defaults if config doesn't set them.
type Config struct {
	URL         string `json:"url,omitempty"`
	Token       string `json:"token,omitempty"`
	Local       string `json:"local,omitempty"`
	Concurrency int    `json:"concurrency,omitempty"`
	Chunk       string `json:"chunk,omitempty"`
	HedgeAfter  string `json:"hedge_after,omitempty"`
}

// configPath returns ~/.opensender.json on Linux/macOS or
// %USERPROFILE%\.opensender.json on Windows.
func configPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".opensender.json"), nil
}

func loadConfig() Config {
	var cfg Config
	p, err := configPath()
	if err != nil {
		return cfg
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return cfg // not-exist is fine; return zero Config
	}
	_ = json.Unmarshal(data, &cfg) // ignore parse errors; user can fix file
	return cfg
}

func saveConfig(cfg Config) error {
	p, err := configPath()
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0600) // 0600 because token is sensitive
}

// runInit interactively prompts for the user-specific bits and writes the
// config file. Re-running it shows existing values as defaults; press Enter
// to keep them.
func runInit(_ []string) error {
	cfg := loadConfig()
	reader := bufio.NewReader(os.Stdin)

	prompt := func(label, def string) string {
		if def != "" {
			fmt.Printf("%s [%s]: ", label, def)
		} else {
			fmt.Printf("%s: ", label)
		}
		s, err := reader.ReadString('\n')
		if err != nil {
			return def
		}
		s = strings.TrimSpace(s)
		if s == "" {
			return def
		}
		return s
	}

	cfg.URL = prompt("Server URL (e.g. http://100.x.x.x:8080)", cfg.URL)
	cfg.Token = prompt("Bearer token", cfg.Token)
	cfg.Local = prompt("Default local directory", cfg.Local)

	// Perf-related fields aren't prompted; they have proven defaults baked
	// into the binary (concurrency 1024, chunk 256K, hedge-after 3s). If a
	// user wants to override them per-machine, they can edit the JSON
	// directly — but most people shouldn't need to.

	if err := saveConfig(cfg); err != nil {
		return err
	}
	p, _ := configPath()
	fmt.Printf("\nWrote config: %s\n", p)
	fmt.Println("Daily usage:")
	fmt.Println("  opensender pull --remote path/to/file.safetensors")
	fmt.Println("  opensender pull --remote dir/                       # recursive")
	return nil
}
