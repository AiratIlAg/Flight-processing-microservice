package cache

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// GET /api/flights
// flight:data:{flight_number}:{departure_date}
func FlightDataKey(flightNumber string, departure time.Time) string {
	fn := url.PathEscape(strings.TrimSpace(flightNumber))
	d := departure.UTC().Format(time.RFC3339)
	return fmt.Sprintf("flight:data:%s:%s", fn, d)
}

// GET /api/flights/{flight_number}/meta
// flight:meta:{flight_number}:status={status}:limit={limit}:offset={offset}
func FlightMetaKey(flightNumber, status string, limit, offset int) string {
	fn := url.PathEscape(strings.TrimSpace(flightNumber))

	s := strings.ToLower(strings.TrimSpace(status))
	if s == "" {
		s = "all"
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	return fmt.Sprintf("flight:meta:%s:status=%s:limit=%d:offset=%d", fn, s, limit, offset)
}

// Для хранения всех meta-ключей по рейсу (для быстрой инвалидации без SCAN)
func FlightMetaKeysSetKey(flightNumber string) string {
	fn := url.PathEscape(strings.TrimSpace(flightNumber))
	return fmt.Sprintf("flight:meta:%s:keys", fn)
}
