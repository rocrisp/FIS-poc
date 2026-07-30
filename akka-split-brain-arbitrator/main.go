package main

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fis/akka-split-brain-arbitrator/api"
	"github.com/fis/akka-split-brain-arbitrator/config"
	"github.com/fis/akka-split-brain-arbitrator/datacenter"
	"github.com/fis/akka-split-brain-arbitrator/monitor"
	"github.com/fis/akka-split-brain-arbitrator/state"
)

//go:embed web/*
var uiRoot embed.FS

func main() {
	configPath := flag.String("config", "", "path to config.yaml")
	flag.Parse()

	if envPath := os.Getenv("CONFIG_PATH"); envPath != "" && *configPath == "" {
		*configPath = envPath
	}

	var cfg *config.Config
	var err error
	if *configPath != "" {
		cfg, err = config.Load(*configPath)
		if err != nil {
			log.Fatalf("failed to load config from %s: %v", *configPath, err)
		}
		log.Printf("loaded config from %s", *configPath)
	} else {
		cfg = config.Default()
		log.Printf("using default config")
	}

	if len(cfg.Priority) == 0 {
		cfg.Priority = monitor.ParseClusterNames(os.Getenv("CLUSTER_NAMES"))
	}
	if len(cfg.Priority) == 0 {
		cfg.Priority = []string{"cluster1-fis", "cluster2-fis"}
	}

	store := state.NewStore()
	resolver := datacenter.NewPriorityResolver(cfg.Priority)
	mon := monitor.NewSubmarinerMonitor(resolver, cfg.Submariner.PollInterval, cfg.Submariner.StabilizationPeriod)

	if hub, err := monitor.NewHubFetcherFromEnv(cfg.Priority); err != nil {
		log.Printf("hub fetcher unavailable (using stub): %v", err)
	} else {
		mon.SetGatewayFetcher(hub.Fetch)
		log.Printf("hub fetcher enabled for clusters: %v", cfg.Priority)
	}

	handler := api.NewHandler(store, resolver)
	if webFS, err := fs.Sub(uiRoot, "web"); err != nil {
		log.Printf("hub UI unavailable: %v", err)
	} else {
		handler.SetUI(webFS)
	}
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mon.Start(ctx)

	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	server := &http.Server{Addr: addr, Handler: mux}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Printf("shutting down")
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown error: %v", err)
		}
	}()

	log.Printf("listening on %s", addr)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}
