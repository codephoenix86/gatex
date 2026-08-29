// Command gateway starts the Gatex API gateway.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/codephoenix86/gatex/internal/config"
	"github.com/codephoenix86/gatex/internal/proxy"
)

func main() {
	configPath := flag.String("config", "configs/gateway.example.yaml", "path to the gateway YAML configuration")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	gateway, err := proxy.NewGateway(cfg)
	if err != nil {
		log.Fatalf("create gateway: %v", err)
	}
	healthCheckContext, stopHealthChecks := context.WithCancel(context.Background())
	defer stopHealthChecks()
	gateway.StartHealthChecks(healthCheckContext)

	server := &http.Server{
		Addr:              cfg.ListenAddress,
		Handler:           gateway,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       cfg.Timeouts.IdleConnection,
	}

	serverErrors := make(chan error, 1)
	go func() {
		log.Printf("gatex listening on %s", cfg.ListenAddress)
		serverErrors <- server.ListenAndServe()
	}()

	shutdownSignal := make(chan os.Signal, 1)
	signal.Notify(shutdownSignal, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(shutdownSignal)

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("gateway server: %v", err)
		}
	case signal := <-shutdownSignal:
		log.Printf("received %s; draining in-flight requests", signal)
		shutdownContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
			if closeErr := server.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
				log.Printf("force-close server: %v", closeErr)
			}
		}
		stopHealthChecks()
		gateway.WaitForHealthChecks()
		gateway.CloseIdleConnections()
	}
}
