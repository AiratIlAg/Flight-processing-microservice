package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFlightDataKey_NormalizesUTC(t *testing.T) {
	tm := time.Date(2023, 10, 1, 12, 0, 0, 0, time.FixedZone("X", 3*3600))
	key := FlightDataKey("SU-123", tm)

	// должно быть в UTC и RFC3339
	require.Contains(t, key, "flight:data:SU-123:2023-10-01T09:00:00Z")
}

func TestFlightMetaKey_Defaults(t *testing.T) {
	key := FlightMetaKey("SU-123", "", 0, -1)
	require.Equal(t, "flight:meta:SU-123:status=all:limit=50:offset=0", key)
}
