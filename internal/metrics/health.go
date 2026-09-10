package metrics

import (
	"encoding/json"
	"net/http"
	"sort"
)

// ReadinessSource reports backend pools that cannot currently serve normal
// gateway traffic.
type ReadinessSource interface {
	UnavailablePools() []string
}

type healthResponse struct {
	Status           string   `json:"status"`
	UnavailablePools []string `json:"unavailable_pools,omitempty"`
}

// HealthHandler reports process liveness. Reaching this handler means the HTTP
// server and its handler stack are running.
func HealthHandler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !allowsProbeMethod(writer, request) {
			return
		}
		writeHealthResponse(writer, request, http.StatusOK, healthResponse{Status: "ok"})
	})
}

// ReadinessHandler reports whether every configured backend pool has a healthy
// backend and a closed circuit breaker. A missing source is not ready because
// the process cannot prove that its request path is initialized.
func ReadinessHandler(source ReadinessSource) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !allowsProbeMethod(writer, request) {
			return
		}
		if source == nil {
			writeHealthResponse(writer, request, http.StatusServiceUnavailable, healthResponse{Status: "not_ready"})
			return
		}

		unavailablePools := append([]string(nil), source.UnavailablePools()...)
		sort.Strings(unavailablePools)
		if len(unavailablePools) > 0 {
			writeHealthResponse(writer, request, http.StatusServiceUnavailable, healthResponse{
				Status:           "not_ready",
				UnavailablePools: unavailablePools,
			})
			return
		}
		writeHealthResponse(writer, request, http.StatusOK, healthResponse{Status: "ready"})
	})
}

func allowsProbeMethod(writer http.ResponseWriter, request *http.Request) bool {
	if request.Method == http.MethodGet || request.Method == http.MethodHead {
		return true
	}
	writer.Header().Set("Allow", "GET, HEAD")
	http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func writeHealthResponse(writer http.ResponseWriter, request *http.Request, status int, response healthResponse) {
	body, err := json.Marshal(response)
	if err != nil {
		http.Error(writer, "encode health response", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if request.Method == http.MethodHead {
		return
	}
	if _, err := writer.Write(append(body, '\n')); err != nil {
		return
	}
}
