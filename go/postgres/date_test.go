package postgres

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	datepb "google.golang.org/genproto/googleapis/type/date"
)

func TestDateFromPb(t *testing.T) {
	t.Run("FullDate", func(t *testing.T) {
		d, err := DateFromPb(&datepb.Date{Year: 2025, Month: 9, Day: 1})
		require.NoError(t, err)
		require.Equal(t, pgtype.Date{Time: time.Date(2025, time.September, 1, 0, 0, 0, 0, time.UTC), Valid: true}, d)
	})

	t.Run("Unset", func(t *testing.T) {
		_, err := DateFromPb(nil)
		require.Error(t, err)
	})

	for name, partial := range map[string]*datepb.Date{
		"YearOnly":      {Year: 2025},
		"MonthDayOnly":  {Month: 9, Day: 1},
		"MissingDay":    {Year: 2025, Month: 9},
		"NonExistent":   {Year: 2025, Month: 2, Day: 30},
		"MonthOverflow": {Year: 2025, Month: 13, Day: 1},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DateFromPb(partial)
			require.Error(t, err)
		})
	}
}

func TestDateToPb(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		want := &datepb.Date{Year: 2025, Month: 9, Day: 1}
		d, err := DateFromPb(want)
		require.NoError(t, err)
		require.Equal(t, want, DateToPb(d))
	})

	t.Run("Null", func(t *testing.T) {
		require.Nil(t, DateToPb(pgtype.Date{}))
	})

	t.Run("Infinity", func(t *testing.T) {
		require.Nil(t, DateToPb(pgtype.Date{Valid: true, InfinityModifier: pgtype.Infinity}))
	})
}
