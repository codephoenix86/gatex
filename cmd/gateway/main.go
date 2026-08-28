// Command gateway starts the Gatex API gateway.
package main

import (
	"flag"
	"log"

	"github.com/naresh-lohar/gatex/internal/config"
)

func main() {
	configPath := flag.String("config", "configs/gateway.example.yaml", "path to the gateway YAML configuration")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	// The HTTP server is introduced in Phase 1. Keep this executable small so
	// process wiring stays separate from the reusable internal packages.
	log.Printf("gatex startup scaffold; would listen on %s", cfg.ListenAddress)
}
