package metrics

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
)

func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		route := routePattern(r)
		code := fmtInt(int64(ww.Status()))
		ObserveHTTPRequest(r.Method, route, code, time.Since(start))
	})
}

func routePattern(r *http.Request) string {
	if r == nil {
		return ""
	}
	rc := chi.RouteContext(r.Context())
	if rc == nil {
		return r.URL.Path
	}
	if p := rc.RoutePattern(); p != "" {
		return p
	}
	return r.URL.Path
}
