package postgres

import (
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	datepb "google.golang.org/genproto/googleapis/type/date"
)

// DateFromPb converts a google.type.Date into a DATE column value. A DATE is
// a whole calendar day, so the partial forms google.type.Date allows (a year
// alone, a month and day without a year) are rejected, as is an unset date.
func DateFromPb(d *datepb.Date) (pgtype.Date, error) {
	if d == nil {
		return pgtype.Date{}, errors.New("date is not set")
	}
	year, month, day := int(d.GetYear()), time.Month(d.GetMonth()), int(d.GetDay())
	if year == 0 || month == 0 || day == 0 {
		return pgtype.Date{}, fmt.Errorf("date %04d-%02d-%02d is not a full calendar day", year, month, day)
	}
	t := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	// time.Date normalizes an out-of-range day (February 30) into the next month.
	if t.Year() != year || t.Month() != month || t.Day() != day {
		return pgtype.Date{}, fmt.Errorf("date %04d-%02d-%02d does not exist", year, month, day)
	}
	return pgtype.Date{Time: t, Valid: true}, nil
}

// DateToPb converts a DATE column value into a google.type.Date. NULL and the
// infinities have no proto rendering and yield nil.
func DateToPb(d pgtype.Date) *datepb.Date {
	if !d.Valid || d.InfinityModifier != pgtype.Finite {
		return nil
	}
	return &datepb.Date{
		Year:  int32(d.Time.Year()),
		Month: int32(d.Time.Month()),
		Day:   int32(d.Time.Day()),
	}
}
