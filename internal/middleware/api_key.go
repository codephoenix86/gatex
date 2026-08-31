package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
)

const (
	// APIKeyHeader carries credentials for routes protected by APIKeyAuth.
	APIKeyHeader = "X-API-Key"

	apiKeyChallenge = "ApiKey"
)

// APIKeyAuth authenticates requests against one or more API keys. Comparisons
// use fixed-length digests and constant-time operations. Accepted credentials
// are removed before the request is delegated because authentication
// terminates at the gateway.
func APIKeyAuth(validKeys ...string) Middleware {
	validKeyDigests := make([][sha256.Size]byte, len(validKeys))
	for index, key := range validKeys {
		validKeyDigests[index] = sha256.Sum256([]byte(key))
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			providedKey := r.Header.Get(APIKeyHeader)
			if providedKey == "" || !matchesAPIKey(providedKey, validKeyDigests) {
				w.Header().Set("WWW-Authenticate", apiKeyChallenge)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			authenticatedRequest := r.Clone(r.Context())
			authenticatedRequest.Header.Del(APIKeyHeader)
			next.ServeHTTP(w, authenticatedRequest)
		})
	}
}

func matchesAPIKey(providedKey string, validKeyDigests [][sha256.Size]byte) bool {
	providedDigest := sha256.Sum256([]byte(providedKey))
	matched := 0
	for _, validDigest := range validKeyDigests {
		matched |= subtle.ConstantTimeCompare(providedDigest[:], validDigest[:])
	}
	return matched == 1
}
