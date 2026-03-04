package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// FlightMetaKey нормализует параметры (status=all, clamp limit, offset>=0, lower-case).
// гарантирует, что ключи всегда одинаковые для одинакового смысла запроса (важно для кеш-хитов)
func TestFlightMetaKey_Normalization(t *testing.T) {
	// status -> lower-case
	key1 := FlightMetaKey("SU-123", "PrOcEsSeD", 50, 10)
	require.Equal(t, "flight:meta:SU-123:status=processed:limit=50:offset=10", key1)

	// status empty -> all
	key2 := FlightMetaKey("SU-123", "", 50, 0)
	require.Equal(t, "flight:meta:SU-123:status=all:limit=50:offset=0", key2)

	// limit <=0 -> 50
	key3 := FlightMetaKey("SU-123", "pending", 0, 0)
	require.Equal(t, "flight:meta:SU-123:status=pending:limit=50:offset=0", key3)

	// limit >100 -> 100
	key4 := FlightMetaKey("SU-123", "pending", 999, 0)
	require.Equal(t, "flight:meta:SU-123:status=pending:limit=100:offset=0", key4)

	// offset <0 -> 0
	key5 := FlightMetaKey("SU-123", "pending", 50, -1)
	require.Equal(t, "flight:meta:SU-123:status=pending:limit=50:offset=0", key5)
}
