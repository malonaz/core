package sentry

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"path"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"github.com/getsentry/sentry-go"
	"github.com/malonaz/core/go/grpc/middleware"
	"github.com/malonaz/core/go/scheduler"
	"go.einride.tech/aip/resourcename"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	// Emitted by core's gRPC logging interceptor.
	messageFinishedCall = "finished call"
	// Emitted by core's panic recovery.
	messagePanic = "panic"

	grpcServerGroup = "grpc_server"

	resourcePrefix = "grpc.request."
	// A service account's principal; it has no readable attribute.
	serviceAccountPrefix = "serviceAccounts/"
)

var (
	// PII, or already on the exception.
	droppedKeys = []string{"ip_address", "user_agent", "stack", "grpc.error.DebugInfo"}
	tagKeys     = []string{"grpc.code", "caller", "issuer", "client_platform", "client_version"}
)

type fields map[string]slog.Value

func (f fields) str(key string) string {
	value, ok := f[key]
	if !ok {
		return ""
	}
	return value.String()
}

func flatten(dst fields, prefix string, attrs ...slog.Attr) {
	for _, attr := range attrs {
		value := attr.Value.Resolve()
		if value.Kind() == slog.KindGroup {
			groupPrefix := prefix
			if attr.Key != "" {
				groupPrefix += attr.Key + "."
			}
			flatten(dst, groupPrefix, value.Group()...)
			continue
		}
		dst[prefix+attr.Key] = value
	}
}

// newEvent returns nil for records that must not be reported.
func (h *Handler) newEvent(ctx context.Context, record slog.Record) *sentry.Event {
	groups, attrs := h.groups, h.attrs
	f := fields{}
	// Session and resource fields a handler's own logs lack; the record's attrs win.
	if ctxFields, ok := middleware.GetFields(ctx); ok {
		for i := 0; i+1 < len(ctxFields); i += 2 {
			if key, ok := ctxFields[i].(string); ok {
				f[key] = slog.AnyValue(ctxFields[i+1]).Resolve()
			}
		}
	}
	flatten(f, "", attrs...)
	record.Attrs(func(attr slog.Attr) bool {
		flatten(f, "", attr)
		return true
	})

	var server string
	if slices.Contains(groups, grpcServerGroup) {
		server = f.str("name")
		delete(f, "name")
	}
	if record.Message == messageFinishedCall && h.skipCalls(server) {
		return nil
	}

	transaction := transactionName(ctx, groups, f)
	code := f.str("grpc.code")
	panicked := record.Message == messagePanic

	exception := sentry.Exception{
		Type:       record.Message,
		Value:      record.Message,
		Stacktrace: stacktrace(f, h.module),
	}
	if code != "" {
		exception.Type = code
	}
	if value := f.str("grpc.error"); value != "" {
		exception.Value = value
	} else if value := f.str("error"); value != "" {
		exception.Value = value
	}
	if panicked {
		exception.Type = messagePanic
		exception.Value = f.str("panic")
	}
	exception.Value = scrub(exception.Value)

	// Handler logs carry the code on the error.
	if st, ok := errorStatus(f); ok && code == "" {
		code = st.Code().String()
	}

	fingerprint := []string{transaction, record.Message}
	switch {
	case panicked:
		fingerprint = []string{messagePanic, transaction}
	case record.Message == messageFinishedCall:
		fingerprint = []string{transaction, code}
	case code != "":
		fingerprint = append(fingerprint, code)
	}

	level := sentry.LevelError
	if panicked {
		level = sentry.LevelFatal
	}

	tags := map[string]string{}
	for _, key := range tagKeys {
		if value := f.str(key); value != "" {
			tags[key] = value
		}
	}
	if server != "" {
		tags["server"] = server
	}
	if code != "" {
		tags["grpc.code"] = code
	}

	user := sentry.User{ID: h.firstUserName(f), Data: map[string]string{}}
	// An operator signed in as the principal ("login as").
	if actor := f.str("actor"); actor != "" {
		user.Data["actor"] = actor
	}

	contexts := map[string]sentry.Context{}
	job, _ := scheduler.JobFromIncomingContext(ctx)
	if job != "" {
		contexts["job"] = sentry.Context{"name": job}
	}
	if organization := h.firstOrganizationName(f, job); organization != "" {
		tags["organization"] = organization
		user.Data["organization"] = organization
	}

	log := sentry.Context{}
	for key, value := range f {
		switch {
		case slices.Contains(droppedKeys, key), strings.HasPrefix(key, "attribute."),
			slices.Contains(tagKeys, key), key == "actor", key == "grpc.error", key == "error", key == "panic":
		default:
			log[key] = scrub(value.String())
		}
	}
	if len(log) > 0 {
		contexts["log"] = log
	}

	event := sentry.NewEvent()
	event.Level = level
	event.Message = record.Message
	event.Transaction = transaction
	event.Fingerprint = fingerprint
	event.Exception = []sentry.Exception{exception}
	event.Tags = tags
	event.User = user
	event.Contexts = contexts
	event.Timestamp = record.Time
	return event
}

func transactionName(ctx context.Context, groups []string, f fields) string {
	if method, ok := grpc.Method(ctx); ok {
		return method
	}
	if routine := f.str("routine"); routine != "" {
		return routine
	}
	return strings.Join(groups, ".")
}

// callers returns the caller attributes' values, in order.
func (h *Handler) callers(f fields) []string {
	var callers []string
	for _, key := range h.opts.CallerAttributes {
		if value := f.str(key); value != "" {
			callers = append(callers, value)
		}
	}
	return callers
}

// firstOrganizationName checks the callers, their scopes, the job, then logged resources.
func (h *Handler) firstOrganizationName(f fields, job string) string {
	if h.opts.OrganizationPattern == "" {
		return ""
	}
	candidates := h.callers(f)
	for _, key := range h.opts.ScopeAttributes {
		scope, _, _ := strings.Cut(strings.Trim(f.str(key), "[]"), " ")
		candidates = append(candidates, scope)
	}
	candidates = append(append(candidates, job), resourceNames(f)...)
	for _, candidate := range candidates {
		if organization := ancestor(candidate, h.opts.OrganizationPattern); organization != "" {
			return organization
		}
	}
	return ""
}

// firstUserName checks the callers, the principal, then logged resources.
func (h *Handler) firstUserName(f fields) string {
	for _, caller := range h.callers(f) {
		if h.opts.OrganizationPattern == "" || ancestor(caller, h.opts.OrganizationPattern) != "" {
			return caller
		}
	}
	if principal := f.str("principal"); strings.HasPrefix(principal, serviceAccountPrefix) {
		return principal
	}
	if h.opts.UserPattern == "" {
		return ""
	}
	for _, name := range resourceNames(f) {
		if user := ancestor(name, h.opts.UserPattern); user != "" {
			return user
		}
	}
	return ""
}

// resourceNames returns attr values, grpc.request.* first, each sorted by key.
func resourceNames(f fields) []string {
	var request, other []string
	for _, key := range slices.Sorted(maps.Keys(f)) {
		switch {
		case strings.HasPrefix(key, "attribute."), slices.Contains(droppedKeys, key):
		case strings.HasPrefix(key, resourcePrefix):
			request = append(request, f.str(key))
		default:
			other = append(other, f.str(key))
		}
	}
	return append(request, other...)
}

// ancestor returns the concrete ancestor of name matching pattern, or "".
func ancestor(name, pattern string) string {
	match, ok := resourcename.Ancestor(name, pattern)
	// Validate rejects free text, e.g. "organizations/o1 failed".
	if !ok || resourcename.Validate(match) != nil || resourcename.ContainsWildcard(match) {
		return ""
	}
	return match
}

// stacktrace prefers a panic stack, then core status DebugInfo. Frames under module are in-app.
func stacktrace(f fields, module string) *sentry.Stacktrace {
	var frames []runtime.Frame
	if stack := f.str("stack"); stack != "" {
		frames = parseGoStack(stack)
	} else if debugInfo := findDebugInfo(f); debugInfo != nil {
		frames = parseDebugInfo(debugInfo.GetStackEntries())
	}
	if len(frames) == 0 {
		return nil
	}
	// Sentry orders frames oldest first.
	stacktrace := &sentry.Stacktrace{}
	for i := len(frames) - 1; i >= 0; i-- {
		frame := sentry.NewFrame(frames[i])
		frame.InApp = module != "" && strings.HasPrefix(frames[i].Function, module)
		if frame.InApp {
			frame.Filename = sourcePath(frames[i].Function, frames[i].File, module)
		}
		stacktrace.Frames = append(stacktrace.Frames, frame)
	}
	return stacktrace
}

// findDebugInfo reads the gRPC detail, else the one on the "error" attr.
func findDebugInfo(f fields) *errdetails.DebugInfo {
	if debugInfo, ok := f["grpc.error.DebugInfo"].Any().(*errdetails.DebugInfo); ok {
		return debugInfo
	}
	st, ok := errorStatus(f)
	if !ok {
		return nil
	}
	for _, detail := range st.Details() {
		if debugInfo, ok := detail.(*errdetails.DebugInfo); ok {
			return debugInfo
		}
	}
	return nil
}

// errorStatus returns the status the "error" attr wraps, mapping context errors.
func errorStatus(f fields) (*grpcstatus.Status, bool) {
	err, ok := f["error"].Any().(error)
	if !ok {
		return nil, false
	}
	if st, ok := grpcstatus.FromError(err); ok {
		return st, true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return grpcstatus.FromContextError(err), true
	}
	return nil, false
}

// sourcePath returns the repo-relative path, e.g. user/user_service/get_contact.go.
func sourcePath(function, file, module string) string {
	pkg := function
	if slash := strings.LastIndex(function, "/"); slash >= 0 {
		if dot := strings.Index(function[slash:], "."); dot >= 0 {
			pkg = function[:slash+dot]
		}
	}
	return strings.TrimPrefix(pkg, module) + "/" + path.Base(file)
}

// parseGoStack parses debug.Stack output, keeping frames below the panic.
func parseGoStack(stack string) []runtime.Frame {
	lines := strings.Split(strings.TrimSpace(stack), "\n")
	var frames []runtime.Frame
	// lines[0] is the goroutine header.
	for i := 1; i+1 < len(lines); i += 2 {
		function := strings.TrimPrefix(lines[i], "created by ")
		if j := strings.Index(function, " in goroutine "); j >= 0 {
			function = function[:j]
		} else if j := strings.LastIndex(function, "("); j > 0 && strings.HasSuffix(function, ")") {
			function = function[:j]
		}
		location := strings.TrimSpace(lines[i+1])
		if j := strings.LastIndex(location, " +0x"); j >= 0 {
			location = location[:j]
		}
		file, line := splitLocation(location)
		if function == "panic" {
			frames = frames[:0]
			continue
		}
		frames = append(frames, runtime.Frame{Function: function, File: file, Line: line})
	}
	return frames
}

// parseDebugInfo parses core status "function file.go:line" entries.
func parseDebugInfo(entries []string) []runtime.Frame {
	frames := make([]runtime.Frame, 0, len(entries))
	for _, entry := range entries {
		function, location, ok := strings.Cut(entry, " ")
		if !ok {
			continue
		}
		file, line := splitLocation(location)
		frames = append(frames, runtime.Frame{Function: function, File: file, Line: line})
	}
	return frames
}

func splitLocation(location string) (string, int) {
	j := strings.LastIndex(location, ":")
	if j < 0 {
		return location, 0
	}
	line, _ := strconv.Atoi(location[j+1:])
	return location[:j], line
}
