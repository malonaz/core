package cron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParse_Rejects(t *testing.T) {
	t.Parallel()
	for _, expression := range []string{"", "* * * *", "* * * * * *", "@hourly", "60 * * * *", "TZ=UTC * * * * *", "CRON_TZ=UTC * * * * *"} {
		_, err := Parse(expression, "")
		require.Error(t, err, expression)
	}
	_, err := Parse("* * * * *", "Mars/Olympus_Mons")
	require.Error(t, err)
}

func TestNext_StrictlyAfter(t *testing.T) {
	t.Parallel()
	expression, err := Parse("* * * * *", "")
	require.NoError(t, err)
	tick := time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC)
	require.Equal(t, tick.Add(time.Minute), expression.Next(tick))
	require.Equal(t, tick, expression.Next(tick.Add(-time.Nanosecond)))
	require.Equal(t, tick.Add(time.Minute), expression.Next(tick.Add(time.Nanosecond)))
}

func TestNext_InLocation(t *testing.T) {
	t.Parallel()
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	expression, err := Parse("0 9 * * *", "America/New_York")
	require.NoError(t, err)
	// The argument's zone is irrelevant: the tick lands at 9am New York time.
	next := expression.Next(time.Date(2026, time.June, 1, 12, 0, 0, 0, time.UTC))
	require.Equal(t, time.Date(2026, time.June, 1, 9, 0, 0, 0, newYork), next)
	require.Equal(t, newYork, next.Location())
}

func TestNext_AcrossDaylightSavingTransition(t *testing.T) {
	t.Parallel()
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	expression, err := Parse("0 12 * * *", "America/New_York")
	require.NoError(t, err)

	// Spring forward (2026-03-08): the next noon is 23 hours away.
	springForward := time.Date(2026, time.March, 7, 12, 0, 0, 0, newYork)
	next := expression.Next(springForward)
	require.Equal(t, time.Date(2026, time.March, 8, 12, 0, 0, 0, newYork), next)
	require.Equal(t, 23*time.Hour, next.Sub(springForward))

	// Fall back (2026-11-01): the next noon is 25 hours away.
	fallBack := time.Date(2026, time.October, 31, 12, 0, 0, 0, newYork)
	next = expression.Next(fallBack)
	require.Equal(t, time.Date(2026, time.November, 1, 12, 0, 0, 0, newYork), next)
	require.Equal(t, 25*time.Hour, next.Sub(fallBack))
}

func TestNext_MonthAndDayOfWeekFields(t *testing.T) {
	t.Parallel()
	firstOfMonth, err := Parse("0 9 1 * *", "")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, time.March, 1, 9, 0, 0, 0, time.UTC), firstOfMonth.Next(time.Date(2026, time.February, 1, 9, 0, 0, 0, time.UTC)))

	mondays, err := Parse("30 8 * * MON", "")
	require.NoError(t, err)
	// 2026-03-04 is a Wednesday; the next Monday is the 9th.
	require.Equal(t, time.Date(2026, time.March, 9, 8, 30, 0, 0, time.UTC), mondays.Next(time.Date(2026, time.March, 4, 0, 0, 0, 0, time.UTC)))

	quarterly, err := Parse("0 0 1 1,4,7,10 *", "")
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, time.April, 1, 0, 0, 0, 0, time.UTC), quarterly.Next(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)))
}
