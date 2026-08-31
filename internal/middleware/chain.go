package middleware

import "net/http"

// Middleware wraps an HTTP handler with cross-cutting request behavior.
type Middleware func(http.Handler) http.Handler

// Chain composes middleware into a single Middleware. Middleware is applied in
// the order it is passed, so the first item is the outermost handler and sees a
// request first (and its response last).
func Chain(middlewares ...Middleware) Middleware {
	chain := append([]Middleware(nil), middlewares...)

	return func(next http.Handler) http.Handler {
		for index := len(chain) - 1; index >= 0; index-- {
			next = chain[index](next)
		}
		return next
	}
}
