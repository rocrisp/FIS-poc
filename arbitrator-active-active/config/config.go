package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Priority    []string          `yaml:"priority"`
	Submariner  SubmarinerConfig  `yaml:"submariner"`
	Persistence PersistenceConfig `yaml:"persistence"`
}

type ServerConfig struct {
	Port int `yaml:"port"`
}

type SubmarinerConfig struct {
	PollInterval        time.Duration `yaml:"pollInterval"`
	StabilizationPeriod time.Duration `yaml:"stabilizationPeriod"`
}

type PersistenceConfig struct {
	Backend string `yaml:"backend"`
}

func Default() *Config {
	return &Config{
		Server:   ServerConfig{Port: 8080},
		Priority: []string{"cluster1-fis", "cluster2-fis"},
		Submariner: SubmarinerConfig{
			PollInterval:        10 * time.Second,
			StabilizationPeriod: 30 * time.Second,
		},
		Persistence: PersistenceConfig{Backend: "memory"},
	}
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
