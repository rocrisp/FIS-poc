package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	cfg := Default()
	if cfg.Server.Port != 8080 {
		t.Fatalf("expected port 8080, got %d", cfg.Server.Port)
	}
	if len(cfg.Priority) != 2 || cfg.Priority[0] != "cluster1-fis" {
		t.Fatalf("expected default FIS priority, got %v", cfg.Priority)
	}
	if cfg.Submariner.PollInterval != 10*time.Second {
		t.Fatalf("expected 10s poll interval, got %v", cfg.Submariner.PollInterval)
	}
	if cfg.Submariner.StabilizationPeriod != 30*time.Second {
		t.Fatalf("expected 30s stabilization, got %v", cfg.Submariner.StabilizationPeriod)
	}
	if cfg.Persistence.Backend != "memory" {
		t.Fatalf("expected memory backend, got %s", cfg.Persistence.Backend)
	}
}

func TestLoadFromFile(t *testing.T) {
	content := `
server:
  port: 9090
priority:
  - cluster1-fis
  - cluster2-fis
submariner:
  pollInterval: 15s
  stabilizationPeriod: 45s
persistence:
  backend: configmap
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Server.Port != 9090 {
		t.Fatalf("expected port 9090, got %d", cfg.Server.Port)
	}
	if len(cfg.Priority) != 2 || cfg.Priority[0] != "cluster1-fis" {
		t.Fatalf("unexpected priority: %v", cfg.Priority)
	}
	if cfg.Submariner.PollInterval != 15*time.Second {
		t.Fatalf("expected 15s, got %v", cfg.Submariner.PollInterval)
	}
	if cfg.Submariner.StabilizationPeriod != 45*time.Second {
		t.Fatalf("expected 45s, got %v", cfg.Submariner.StabilizationPeriod)
	}
	if cfg.Persistence.Backend != "configmap" {
		t.Fatalf("expected configmap, got %s", cfg.Persistence.Backend)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load("/nonexistent/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}
