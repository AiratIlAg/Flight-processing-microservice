package handlers

import "github.com/go-chi/chi/v5"

func RegisterFlightRoutes(r chi.Router, h *FlightHandler) {
	r.Route("/api/flights", func(r chi.Router) {
		r.Post("/", h.CreateFlight)
		r.Get("/", h.GetFlight)
		r.Get("/{flight_number}/meta", h.GetFlightMeta)
	})
}
