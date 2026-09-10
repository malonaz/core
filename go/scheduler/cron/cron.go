// Package cron evaluates the five-field cron expressions a Schedule recurs on,
// so that the service and its tests compute ticks the same way.
package cron

import (
	"fmt"
	"strings"
	"time"
	// The next tick of a zoned schedule must not depend on the host having zoneinfo.
	_ "time/tzdata"

	robfig "github.com/robfig/cron/v3"
)

// parser accepts exactly the standard five fields: no seconds, no descriptors.
var parser = robfig.NewParser(robfig.Minute | robfig.Hour | robfig.Dom | robfig.Month | robfig.Dow)

// Expression is a parsed cron expression bound to the location it is evaluated in.
type Expression struct {
	schedule robfig.Schedule
	location *time.Location
}

// Parse parses a five-field cron expression (`minute hour day-of-month month
// day-of-week`) evaluated in the named IANA time zone; "" is UTC.
func Parse(expression, timeZone string) (*Expression, error) {
	location := time.UTC
	if timeZone != "" {
		var err error
		if location, err = time.LoadLocation(timeZone); err != nil {
			return nil, fmt.Errorf("loading time zone %q: %w", timeZone, err)
		}
	}
	// The library would honour an inline zone over time_zone; one source of truth.
	if strings.HasPrefix(expression, "TZ=") || strings.HasPrefix(expression, "CRON_TZ=") {
		return nil, fmt.Errorf("parsing cron expression %q: the time zone belongs in time_zone", expression)
	}
	schedule, err := parser.Parse(expression)
	if err != nil {
		return nil, fmt.Errorf("parsing cron expression %q: %w", expression, err)
	}
	return &Expression{schedule: schedule, location: location}, nil
}

// Next returns the first occurrence strictly after the given time, in the
// expression's location.
func (e *Expression) Next(after time.Time) time.Time {
	return e.schedule.Next(after.In(e.location))
}
