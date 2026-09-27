// Command mockbackend runs a small HTTP service for local Gatex load tests.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	listenAddress := flag.String("listen", "127.0.0.1:18081", "HTTP listen address")
	name := flag.String("name", "backend", "backend name returned in responses")
	delay := flag.Duration("delay", 2*time.Millisecond, "simulated request processing time")
	flag.Parse()

	handler := http.NewServeMux()
	handler.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	handler.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		if *delay > 0 {
			timer := time.NewTimer(*delay)
			defer timer.Stop()
			select {
			case <-request.Context().Done():
				return
			case <-timer.C:
			}
		}
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writer.Header().Set("X-Mock-Backend", *name)
		_, _ = fmt.Fprintln(writer, *name)
	})

	server := &http.Server{
		Addr:              *listenAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	log.Printf("mock backend %s listening on %s with %s delay", *name, *listenAddress, *delay)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
