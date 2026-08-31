package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	allowCredentialsHeader = "Access-Control-Allow-Credentials"
	allowHeadersHeader     = "Access-Control-Allow-Headers"
	allowMethodsHeader     = "Access-Control-Allow-Methods"
	allowOriginHeader      = "Access-Control-Allow-Origin"
	exposeHeadersHeader    = "Access-Control-Expose-Headers"
	maxAgeHeader           = "Access-Control-Max-Age"
	requestHeadersHeader   = "Access-Control-Request-Headers"
	requestMethodHeader    = "Access-Control-Request-Method"
)

// CORSOptions defines the origins, methods, and headers accepted by CORS.
// Empty AllowedOrigins disables the middleware.
type CORSOptions struct {
	AllowedOrigins   []string
	AllowedMethods   []string
	AllowedHeaders   []string
	ExposedHeaders   []string
	AllowCredentials bool
	MaxAge           time.Duration
}

// CORS adds cross-origin response headers and handles browser preflight
// requests without sending them to the next handler.
func CORS(options CORSOptions) Middleware {
	allowedOrigins := makeStringSet(options.AllowedOrigins, false)
	allowedMethods := makeStringSet(options.AllowedMethods, true)
	allowedHeaders := makeStringSet(options.AllowedHeaders, true)
	allowEveryOrigin := contains(allowedOrigins, "*")
	allowEveryHeader := contains(allowedHeaders, "*")
	methodsHeader := strings.Join(uppercaseStrings(options.AllowedMethods), ", ")
	headersHeader := strings.Join(options.AllowedHeaders, ", ")
	exposedHeaders := strings.Join(options.ExposedHeaders, ", ")
	maxAgeSeconds := int64(options.MaxAge / time.Second)

	return func(next http.Handler) http.Handler {
		if len(allowedOrigins) == 0 {
			return next
		}

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" {
				next.ServeHTTP(w, r)
				return
			}

			useWildcardOriginHeader := allowEveryOrigin && !options.AllowCredentials
			if !useWildcardOriginHeader {
				addVary(w.Header(), "Origin")
			}
			preflight := r.Method == http.MethodOptions && r.Header.Get(requestMethodHeader) != ""
			if preflight {
				addVary(w.Header(), requestMethodHeader)
				addVary(w.Header(), requestHeadersHeader)
			}

			if !allowEveryOrigin && !contains(allowedOrigins, origin) {
				if preflight {
					http.Error(w, "CORS request denied", http.StatusForbidden)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			if preflight {
				requestedMethod := strings.ToUpper(strings.TrimSpace(r.Header.Get(requestMethodHeader)))
				if !contains(allowedMethods, requestedMethod) || !headersAllowed(r.Header.Values(requestHeadersHeader), allowedHeaders, allowEveryHeader) {
					http.Error(w, "CORS request denied", http.StatusForbidden)
					return
				}
			}

			if useWildcardOriginHeader {
				w.Header().Set(allowOriginHeader, "*")
			} else {
				w.Header().Set(allowOriginHeader, origin)
			}
			if options.AllowCredentials {
				w.Header().Set(allowCredentialsHeader, "true")
			}
			if exposedHeaders != "" {
				w.Header().Set(exposeHeadersHeader, exposedHeaders)
			}

			if !preflight {
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set(allowMethodsHeader, methodsHeader)
			if requestedHeaders := r.Header.Get(requestHeadersHeader); allowEveryHeader && requestedHeaders != "" {
				w.Header().Set(allowHeadersHeader, requestedHeaders)
			} else if headersHeader != "" {
				w.Header().Set(allowHeadersHeader, headersHeader)
			}
			if maxAgeSeconds > 0 {
				w.Header().Set(maxAgeHeader, strconv.FormatInt(maxAgeSeconds, 10))
			}
			w.WriteHeader(http.StatusNoContent)
		})
	}
}

func uppercaseStrings(values []string) []string {
	upper := make([]string, len(values))
	for index, value := range values {
		upper[index] = strings.ToUpper(value)
	}
	return upper
}

func makeStringSet(values []string, upper bool) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if upper {
			value = strings.ToUpper(value)
		}
		set[value] = struct{}{}
	}
	return set
}

func contains(set map[string]struct{}, value string) bool {
	_, ok := set[value]
	return ok
}

func headersAllowed(headerValues []string, allowedHeaders map[string]struct{}, allowEveryHeader bool) bool {
	if allowEveryHeader {
		return true
	}
	for _, headerValue := range headerValues {
		for _, header := range strings.Split(headerValue, ",") {
			header = strings.ToUpper(strings.TrimSpace(header))
			if header != "" && !contains(allowedHeaders, header) {
				return false
			}
		}
	}
	return true
}

func addVary(header http.Header, value string) {
	for _, existingValue := range header.Values("Vary") {
		for _, existing := range strings.Split(existingValue, ",") {
			if strings.EqualFold(strings.TrimSpace(existing), value) {
				return
			}
		}
	}
	header.Add("Vary", value)
}
