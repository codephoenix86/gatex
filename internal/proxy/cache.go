package proxy

import (
	"net/http"
	"strings"

	responsecache "github.com/codephoenix86/gatex/internal/cache"
	"github.com/codephoenix86/gatex/internal/middleware"
)

// The fixed body limit bounds temporary memory use independently of the
// configured number of cache entries.
const maxCacheableResponseBytes = 1 << 20

func (r *route) cacheResponses() middleware.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if !isCacheableRequest(request) {
				next.ServeHTTP(w, request)
				return
			}

			requestID := incomingOrNewRequestID(request.Header.Get(RequestIDHeader))
			requestWithID := request.Clone(request.Context())
			requestWithID.Header.Set(RequestIDHeader, requestID)
			key := responseCacheKey(requestWithID)
			if cached, ok := r.responseCache.Get(key); ok {
				writeCachedResponse(w, cached, requestID)
				return
			}

			capture := &cacheResponseWriter{ResponseWriter: w}
			next.ServeHTTP(capture, requestWithID)
			if response, ok := capture.response(); ok {
				response.Header.Del(RequestIDHeader)
				r.responseCache.Set(key, response)
			}
		})
	}
}

type cacheResponseWriter struct {
	http.ResponseWriter
	statusCode  int
	header      http.Header
	body        []byte
	wroteHeader bool
	recording   bool
	writeFailed bool
}

func (w *cacheResponseWriter) WriteHeader(statusCode int) {
	if statusCode >= 100 && statusCode < 200 && statusCode != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(statusCode)
		return
	}
	if w.wroteHeader {
		return
	}
	w.statusCode = statusCode
	w.header = w.ResponseWriter.Header().Clone()
	w.wroteHeader = true
	w.recording = statusCode == http.StatusOK && !responseHeadersPreventCaching(w.header)
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *cacheResponseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}

	written, err := w.ResponseWriter.Write(body)
	if err != nil || written != len(body) {
		w.writeFailed = true
	}
	if w.recording {
		if len(w.body)+written > maxCacheableResponseBytes {
			w.recording = false
			w.body = nil
		} else {
			w.body = append(w.body, body[:written]...)
		}
	}
	return written, err
}

// Unwrap lets ResponseController retain streaming and connection-control
// capabilities supplied by the underlying writer.
func (w *cacheResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *cacheResponseWriter) response() (responsecache.Response, bool) {
	if !w.wroteHeader {
		w.statusCode = http.StatusOK
		w.header = w.ResponseWriter.Header().Clone()
		w.recording = !responseHeadersPreventCaching(w.header)
	}
	if !w.recording || w.writeFailed {
		return responsecache.Response{}, false
	}
	return responsecache.Response{
		StatusCode: w.statusCode,
		Header:     w.header,
		Body:       w.body,
	}, true
}

func isCacheableRequest(request *http.Request) bool {
	if request.Method != http.MethodGet || (request.Body != nil && request.Body != http.NoBody) {
		return false
	}
	for _, header := range []string{
		"Authorization",
		"Cookie",
		"If-Match",
		"If-Modified-Since",
		"If-None-Match",
		"If-Range",
		"If-Unmodified-Since",
		"Range",
		"Upgrade",
	} {
		if request.Header.Get(header) != "" {
			return false
		}
	}
	return !cacheControlPreventsCaching(request.Header) && !headerHasDirective(request.Header.Values("Pragma"), "no-cache")
}

func responseHeadersPreventCaching(header http.Header) bool {
	return cacheControlPreventsCaching(header) ||
		header.Get("Set-Cookie") != "" ||
		header.Get("Vary") != "" ||
		header.Get("Content-Range") != "" ||
		header.Get("Content-Encoding") != "" ||
		header.Get("Trailer") != ""
}

func cacheControlPreventsCaching(header http.Header) bool {
	for _, value := range header.Values("Cache-Control") {
		for _, directive := range strings.Split(value, ",") {
			name, argument, hasArgument := strings.Cut(strings.TrimSpace(directive), "=")
			switch strings.ToLower(name) {
			case "no-cache", "no-store", "private":
				return true
			case "max-age", "s-maxage":
				if hasArgument && strings.Trim(strings.TrimSpace(argument), `"`) == "0" {
					return true
				}
			}
		}
	}
	return false
}

func headerHasDirective(values []string, wanted string) bool {
	for _, value := range values {
		for _, directive := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(directive), wanted) {
				return true
			}
		}
	}
	return false
}

func responseCacheKey(request *http.Request) string {
	scheme := "http"
	if request.TLS != nil {
		scheme = "https"
	}
	return scheme + "\x00" + strings.ToLower(request.Host) + "\x00" + request.URL.RequestURI()
}

func writeCachedResponse(writer http.ResponseWriter, response responsecache.Response, requestID string) {
	for name, values := range response.Header {
		writer.Header()[name] = append([]string(nil), values...)
	}
	writer.Header().Set(RequestIDHeader, requestID)
	writer.WriteHeader(response.StatusCode)
	_, _ = writer.Write(response.Body)
}
