// Package sentry reports ERROR log records to Sentry.
package sentry

import (
	"context"
	"log/slog"
	"regexp"
	"slices"
	"time"

	"github.com/getsentry/sentry-go"
)

var (
	emailRegexp = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}`)
	phoneRegexp = regexp.MustCompile(`\+\d{10,15}|\(?\b\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}\b`)
)

// Opts configures Sentry.
type Opts struct {
	DSN         string `long:"dsn" env:"DSN" description:"Sentry DSN ERROR logs are reported to. Sentry is off without it."`
	Environment string `long:"environment" env:"ENVIRONMENT" description:"Environment of the events."`
	Release     string `long:"release" env:"RELEASE" description:"Release of the events."`

	SkipCallServers     []string `long:"skip-call-server" env:"SKIP_CALL_SERVERS" env-delim:"," description:"gRPC servers whose failed calls are not reported, e.g. a gateway's, reported by the server it reaches."`
	CallerAttributes    []string `long:"caller-attribute" env:"CALLER_ATTRIBUTES" env-delim:"," description:"Log keys naming the caller, in order: the first in an organization is the event's user."`
	ScopeAttributes     []string `long:"scope-attribute" env:"SCOPE_ATTRIBUTES" env-delim:"," description:"Log keys listing the caller's scopes as [a b]: the first one's organization is the event's."`
	OrganizationPattern string   `long:"organization-pattern" env:"ORGANIZATION_PATTERN" description:"Resource name pattern of an organization, e.g. organizations/{organization}, tagged on events."`
	UserPattern         string   `long:"user-pattern" env:"USER_PATTERN" description:"Resource name pattern of a user: a logged resource's user is the event's when no caller is."`
}

// Init makes the default logger also report ERROR records to Sentry. module is the import path
// prefix of in-app frames, e.g. `github.com/org/repo/`. A no-op without a DSN; a bad one only warns.
func Init(opts *Opts, module string) {
	if opts == nil || opts.DSN == "" {
		return
	}
	if err := sentry.Init(clientOptions(opts.DSN, opts.Environment, opts.Release)); err != nil {
		slog.Warn("sentry disabled: initializing client", "error", err)
		return
	}
	slog.SetDefault(slog.New(NewHandler(slog.Default().Handler(), sentry.CurrentHub(), opts, module)))
}

func clientOptions(dsn, environment, release string) sentry.ClientOptions {
	return sentry.ClientOptions{
		Dsn:            dsn,
		Environment:    environment,
		Release:        release,
		SendDefaultPII: false,
	}
}

// scrub masks emails and phone numbers.
func scrub(s string) string {
	s = emailRegexp.ReplaceAllString(s, "[email]")
	return phoneRegexp.ReplaceAllString(s, "[phone]")
}

// Handler tees records to next and sends ERROR records to Sentry.
type Handler struct {
	next   slog.Handler
	hub    *sentry.Hub
	opts   *Opts
	module string
	attrs  []slog.Attr
	groups []string
}

// NewHandler wraps next.
func NewHandler(next slog.Handler, hub *sentry.Hub, opts *Opts, module string) *Handler {
	return &Handler{next: next, hub: hub, opts: opts, module: module}
}

func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= slog.LevelError || h.next.Enabled(ctx, level)
}

func (h *Handler) Handle(ctx context.Context, record slog.Record) error {
	var err error
	if h.next.Enabled(ctx, record.Level) {
		err = h.next.Handle(ctx, record)
	}
	if record.Level >= slog.LevelError {
		h.capture(ctx, record)
	}
	return err
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clone := *h
	clone.next = h.next.WithAttrs(attrs)
	clone.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &clone
}

func (h *Handler) WithGroup(name string) slog.Handler {
	clone := *h
	clone.next = h.next.WithGroup(name)
	clone.groups = append(append([]string{}, h.groups...), name)
	return &clone
}

// capture never panics: a bug here only drops the event.
func (h *Handler) capture(ctx context.Context, record slog.Record) {
	defer func() {
		if v := recover(); v != nil && h.next.Enabled(ctx, slog.LevelWarn) {
			// Straight to next: logging through the default handler would recurse.
			warning := slog.NewRecord(time.Now(), slog.LevelWarn, "dropping sentry event", 0)
			warning.AddAttrs(slog.Any("panic", v), slog.String("message", record.Message))
			_ = h.next.Handle(ctx, warning)
		}
	}()
	if event := h.newEvent(ctx, record); event != nil {
		// Fresh trace per event: the hub's propagation context lives for the whole process.
		hub := h.hub.Clone()
		hub.Scope().SetPropagationContext(sentry.NewPropagationContext())
		hub.CaptureEvent(event)
	}
}

func (h *Handler) skipCalls(server string) bool {
	return slices.Contains(h.opts.SkipCallServers, server)
}
