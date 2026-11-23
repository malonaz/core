package sentry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/malonaz/core/go/grpc/middleware"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
)

type captureTransport struct {
	mu     sync.Mutex
	events []*sentry.Event
}

func (t *captureTransport) Configure(sentry.ClientOptions)        {}
func (t *captureTransport) Flush(time.Duration) bool              { return true }
func (t *captureTransport) FlushWithContext(context.Context) bool { return true }
func (t *captureTransport) Close()                                {}
func (t *captureTransport) SendEvent(event *sentry.Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, event)
}

const (
	userAttribute       = "attribute.example.com/User"
	readScopesAttribute = "attribute.example.com/ReadScopes"
	module              = "example.com/repo/"
)

var testOpts = &Opts{
	SkipCallServers:     []string{"gateway"},
	CallerAttributes:    []string{userAttribute, "attribute.sub"},
	ScopeAttributes:     []string{readScopesAttribute},
	OrganizationPattern: "organizations/{organization}",
	UserPattern:         "organizations/{organization}/users/{user}",
}

func newTestHandlerWith(t *testing.T, next slog.Handler) (*Handler, *captureTransport) {
	transport := &captureTransport{}
	options := clientOptions("https://key@o0.ingest.sentry.io/0", "test", "repo@abc")
	options.Transport = transport
	client, err := sentry.NewClient(options)
	require.NoError(t, err)
	return NewHandler(next, sentry.NewHub(client, sentry.NewScope()), testOpts, module), transport
}

func newTestHandler(t *testing.T) (slog.Handler, *captureTransport) {
	return newTestHandlerWith(t, slog.NewTextHandler(io.Discard, nil))
}

// grpcServer mimics core's server logger: grpc.Server.WithLogger and the logging interceptor.
func grpcServer(handler slog.Handler, name string, fields ...any) slog.Handler {
	logger := slog.New(handler).WithGroup("grpc_server").With("name", name)
	return logger.With(fields...).Handler()
}

// methodStream sets the method grpc.Method reads from ctx.
type methodStream struct {
	grpc.ServerTransportStream
	method string
}

func (s methodStream) Method() string { return s.method }

func withMethod(ctx context.Context, method string) context.Context {
	return grpc.NewContextWithServerTransportStream(ctx, methodStream{method: method})
}

var at = time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)

func record(message string, attrs ...any) slog.Record {
	r := slog.NewRecord(at, slog.LevelError, message, 0)
	r.Add(attrs...)
	return r
}

func TestHandler(t *testing.T) {
	finishedCallFields := []any{
		"protocol", "grpc", "grpc.component", "server", "grpc.service", "user.user_service.v1.UserService",
		"grpc.method", "GetContact", "grpc.method_type", "unary", "peer.address", "@",
		"grpc.start_time", "2026-09-29T12:00:00Z", "grpc.code", "Internal",
		"grpc.error", "rpc error: code = Internal desc = reading contact for jane@example.com",
		"grpc.time_ms", "12", "grpc.error.DebugInfo", &errdetails.DebugInfo{StackEntries: []string{
			"example.com/repo/user/user_service.(*Service).GetContact get_contact.go:42",
			"example.com/repo/user/user_service.(*Service).getContact get_contact.go:10",
		}},
		"session_id", "s1", "principal", "uid-1", "issuer", "kenji", "role", "kenji", "ip_address", "1.2.3.4", "user_agent", "ua",
		"client_platform", "ios", "client_version", "1.2.3",
		userAttribute, "organizations/o1/users/u1", "attribute.example.com/Admin", "false",
		"grpc.request.name", "organizations/o1/contacts/c1",
	}
	var apiKeyCtx context.Context
	_, err := middleware.UnaryServerLogContextTagInitializer()(context.Background(), nil, &grpc.UnaryServerInfo{},
		func(ctx context.Context, _ any) (any, error) {
			middleware.InjectLogFields(ctx, "principal", "serviceAccounts/api-key-1",
				"grpc.request.parent", "organizations/o3/contacts/c1")
			apiKeyCtx = ctx
			return nil, nil
		})
	require.NoError(t, err)
	jobCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-scheduler-job", "organizations/o2/jobs/j1"))
	statusStack, err := grpcstatus.New(codes.Internal, "syncing contact").WithDetails(&errdetails.DebugInfo{StackEntries: []string{
		"github.com/malonaz/core/go/store.(*Store).Get store.go:7",
		"example.com/repo/user/user_processor.(*Service).handleContactMessage contact_message.go:55",
	}})
	require.NoError(t, err)
	statusStackErr := statusStack.Err()
	goStack := "goroutine 7 [running]:\n" +
		"runtime/debug.Stack()\n\t/go/src/runtime/debug/stack.go:26 +0x5e\n" +
		"github.com/malonaz/core/go/grpc.recoveryHandler(...)\n\t/core/go/grpc/server.go:110 +0x1\n" +
		"panic({0x1, 0x2})\n\t/go/src/runtime/panic.go:787 +0x132\n" +
		"example.com/repo/email/email_processor.(*Handler).HandleEvent(0xc0, {0x1})\n\t/src/email/email_processor/analyze.go:31 +0x9\n" +
		"created by github.com/malonaz/core/go/nats.(*Processor[...]).processFn in goroutine 5\n\t/core/go/nats/processor.go:231 +0x2\n"

	tests := []struct {
		name    string
		ctx     context.Context
		handler func(slog.Handler) slog.Handler
		records []slog.Record
		want    []*sentry.Event
	}{
		{
			name: "internal server finished call",
			ctx:  withMethod(context.Background(), "/user.user_service.v1.UserService/GetContact"),
			handler: func(h slog.Handler) slog.Handler {
				return grpcServer(h, "user-service", finishedCallFields...)
			},
			records: []slog.Record{record("finished call")},
			want: []*sentry.Event{{
				Level:       sentry.LevelError,
				Message:     "finished call",
				Transaction: "/user.user_service.v1.UserService/GetContact",
				Fingerprint: []string{"/user.user_service.v1.UserService/GetContact", "Internal"},
				Exception: []sentry.Exception{{
					Type:  "Internal",
					Value: "rpc error: code = Internal desc = reading contact for [email]",
					Stacktrace: &sentry.Stacktrace{Frames: []sentry.Frame{
						{Function: "(*Service).getContact", Module: "example.com/repo/user/user_service", Filename: "user/user_service/get_contact.go", Lineno: 10, InApp: true},
						{Function: "(*Service).GetContact", Module: "example.com/repo/user/user_service", Filename: "user/user_service/get_contact.go", Lineno: 42, InApp: true},
					}},
				}},
				Tags: map[string]string{
					"server": "user-service", "grpc.code": "Internal", "issuer": "kenji",
					"client_platform": "ios", "client_version": "1.2.3", "organization": "organizations/o1",
				},
				User: sentry.User{ID: "organizations/o1/users/u1", Data: map[string]string{"organization": "organizations/o1"}},
				Contexts: map[string]sentry.Context{
					"log": {
						"grpc.service": "user.user_service.v1.UserService", "grpc.method": "GetContact", "grpc.method_type": "unary",
						"grpc.component": "server", "grpc.start_time": "2026-09-29T12:00:00Z", "grpc.time_ms": "12",
						"peer.address": "@", "protocol": "grpc", "session_id": "s1", "principal": "uid-1", "role": "kenji",
						"grpc.request.name": "organizations/o1/contacts/c1",
					},
				},
			}},
		},
		{
			name: "external server finished call is skipped",
			ctx:  context.Background(),
			handler: func(h slog.Handler) slog.Handler {
				return grpcServer(h, "gateway", finishedCallFields...)
			},
			records: []slog.Record{record("finished call")},
		},
		{
			name: "scheduled job",
			ctx:  withMethod(jobCtx, "/financing.v1.FinancingService/SyncAccount"),
			handler: func(h slog.Handler) slog.Handler {
				return grpcServer(h, "financing-service",
					"grpc.service", "financing.v1.FinancingService", "grpc.method", "SyncAccount", "grpc.code", "Unknown",
					"grpc.error", "boom", "principal", "serviceAccounts/scheduler-dispatcher",
					"caller", "serviceAccounts/scheduler-dispatcher", "role", "account:scheduler-dispatcher")
			},
			records: []slog.Record{record("finished call")},
			want: []*sentry.Event{{
				Level:       sentry.LevelError,
				Message:     "finished call",
				Transaction: "/financing.v1.FinancingService/SyncAccount",
				Fingerprint: []string{"/financing.v1.FinancingService/SyncAccount", "Unknown"},
				Exception: []sentry.Exception{{
					Type: "Unknown", Value: "boom",
				}},
				Tags: map[string]string{
					"server": "financing-service", "grpc.code": "Unknown", "caller": "serviceAccounts/scheduler-dispatcher",
					"organization": "organizations/o2",
				},
				User: sentry.User{ID: "serviceAccounts/scheduler-dispatcher", Data: map[string]string{"organization": "organizations/o2"}},
				Contexts: map[string]sentry.Context{
					"job": {"name": "organizations/o2/jobs/j1"},
					"log": {
						"grpc.service": "financing.v1.FinancingService", "grpc.method": "SyncAccount",
						"principal": "serviceAccounts/scheduler-dispatcher", "role": "account:scheduler-dispatcher",
					},
				},
			}},
		},
		{
			name: "routine error",
			ctx:  context.Background(),
			handler: func(h slog.Handler) slog.Handler {
				return slog.New(h).With("routine", "nats-processor-email-processor-analyze-message").Handler()
			},
			records: []slog.Record{record("executing fn", "error", fmt.Errorf("calling +14155550100: %w", context.DeadlineExceeded))},
			want: []*sentry.Event{{
				Level:       sentry.LevelError,
				Message:     "executing fn",
				Transaction: "nats-processor-email-processor-analyze-message",
				Fingerprint: []string{"nats-processor-email-processor-analyze-message", "executing fn", "DeadlineExceeded"},
				Exception: []sentry.Exception{{
					Type: "executing fn", Value: "calling [phone]: context deadline exceeded",
				}},
				Tags:     map[string]string{"grpc.code": "DeadlineExceeded"},
				Contexts: map[string]sentry.Context{"log": {"routine": "nats-processor-email-processor-analyze-message"}},
			}},
		},
		{
			name: "routine error carrying a status stack",
			ctx:  context.Background(),
			handler: func(h slog.Handler) slog.Handler {
				return slog.New(h).With("routine", "nats-processor-user-processor-contact-message").Handler()
			},
			records: []slog.Record{record("executing fn", "error", fmt.Errorf("processing messages: %w", errors.Join(statusStackErr)),
				"contact", "organizations/o6/contacts/c1")},
			want: []*sentry.Event{{
				Level:       sentry.LevelError,
				Message:     "executing fn",
				Transaction: "nats-processor-user-processor-contact-message",
				Fingerprint: []string{"nats-processor-user-processor-contact-message", "executing fn", "Internal"},
				Exception: []sentry.Exception{{
					Type:  "executing fn",
					Value: "processing messages: rpc error: code = Internal desc = syncing contact",
					Stacktrace: &sentry.Stacktrace{Frames: []sentry.Frame{
						{Function: "(*Service).handleContactMessage", Module: "example.com/repo/user/user_processor", Filename: "user/user_processor/contact_message.go", Lineno: 55, InApp: true},
						{Function: "(*Store).Get", Module: "github.com/malonaz/core/go/store", Filename: "store.go", Lineno: 7},
					}},
				}},
				Tags: map[string]string{"grpc.code": "Internal", "organization": "organizations/o6"},
				User: sentry.User{Data: map[string]string{"organization": "organizations/o6"}},
				Contexts: map[string]sentry.Context{"log": {
					"routine": "nats-processor-user-processor-contact-message", "contact": "organizations/o6/contacts/c1",
				}},
			}},
		},
		{
			name: "kacho session",
			ctx:  withMethod(context.Background(), "/user.user_service.v1.UserService/GetContact"),
			handler: func(h slog.Handler) slog.Handler {
				return grpcServer(h, "user-service",
					"grpc.service", "user.user_service.v1.UserService", "grpc.method", "GetContact", "grpc.code", "Internal",
					"grpc.error", "boom", "principal", "organizations/o4/contacts/c9/points/p1", "issuer", "kacho", "role", "kacho",
					"attribute.sub", "organizations/o4/contacts/c9/points/p1")
			},
			records: []slog.Record{record("finished call")},
			want: []*sentry.Event{{
				Level:       sentry.LevelError,
				Message:     "finished call",
				Transaction: "/user.user_service.v1.UserService/GetContact",
				Fingerprint: []string{"/user.user_service.v1.UserService/GetContact", "Internal"},
				Exception: []sentry.Exception{{
					Type: "Internal", Value: "boom",
				}},
				Tags: map[string]string{
					"server": "user-service", "grpc.code": "Internal", "issuer": "kacho", "organization": "organizations/o4",
				},
				User: sentry.User{ID: "organizations/o4/contacts/c9/points/p1", Data: map[string]string{"organization": "organizations/o4"}},
				Contexts: map[string]sentry.Context{
					"log": {
						"grpc.service": "user.user_service.v1.UserService", "grpc.method": "GetContact",
						"principal": "organizations/o4/contacts/c9/points/p1", "role": "kacho",
					},
				},
			}},
		},
		{
			name: "handler log takes session and organization from ctx",
			ctx:  apiKeyCtx,
			handler: func(h slog.Handler) slog.Handler {
				return slog.New(h).WithGroup("user_service").Handler()
			},
			records: []slog.Record{record("syncing contact", "error", errors.New("timeout"),
				"call", "organizations/o3/users/u3/phoneNumbers/p1/calls/c1")},
			want: []*sentry.Event{{
				Level:       sentry.LevelError,
				Message:     "syncing contact",
				Transaction: "user_service",
				Fingerprint: []string{"user_service", "syncing contact"},
				Exception: []sentry.Exception{{
					Type: "syncing contact", Value: "timeout",
				}},
				Tags: map[string]string{"organization": "organizations/o3"},
				User: sentry.User{ID: "serviceAccounts/api-key-1", Data: map[string]string{"organization": "organizations/o3"}},
				Contexts: map[string]sentry.Context{
					"log": {
						"principal": "serviceAccounts/api-key-1", "grpc.request.parent": "organizations/o3/contacts/c1",
						"call": "organizations/o3/users/u3/phoneNumbers/p1/calls/c1",
					},
				},
			}},
		},
		{
			name: "carried call",
			ctx:  withMethod(context.Background(), "/user.user_service.v1.UserService/GetContact"),
			handler: func(h slog.Handler) slog.Handler {
				return grpcServer(h, "user-service",
					"grpc.service", "user.user_service.v1.UserService", "grpc.method", "GetContact", "grpc.code", "Internal",
					"grpc.error", "boom", "principal", "uid-5", "caller", "serviceAccounts/kenji-service", "role", "carrier",
					userAttribute, "organizations/o5/users/u5")
			},
			records: []slog.Record{record("finished call")},
			want: []*sentry.Event{{
				Level:       sentry.LevelError,
				Message:     "finished call",
				Transaction: "/user.user_service.v1.UserService/GetContact",
				Fingerprint: []string{"/user.user_service.v1.UserService/GetContact", "Internal"},
				Exception: []sentry.Exception{{
					Type: "Internal", Value: "boom",
				}},
				Tags: map[string]string{
					"server": "user-service", "grpc.code": "Internal", "caller": "serviceAccounts/kenji-service", "organization": "organizations/o5",
				},
				User: sentry.User{ID: "organizations/o5/users/u5", Data: map[string]string{"organization": "organizations/o5"}},
				Contexts: map[string]sentry.Context{
					"log": {
						"grpc.service": "user.user_service.v1.UserService", "grpc.method": "GetContact",
						"principal": "uid-5", "role": "carrier",
					},
				},
			}},
		},
		{
			name: "operator signed in as a contractor",
			ctx:  withMethod(context.Background(), "/user.user_service.v1.UserService/GetContact"),
			handler: func(h slog.Handler) slog.Handler {
				return grpcServer(h, "user-service",
					"grpc.service", "user.user_service.v1.UserService", "grpc.method", "GetContact", "grpc.code", "Internal",
					"grpc.error", "boom", "principal", "uid-6", "issuer", "kenji", "actor", "uid-operator", "role", "kenji",
					userAttribute, "organizations/o6/users/u6")
			},
			records: []slog.Record{record("finished call")},
			want: []*sentry.Event{{
				Level:       sentry.LevelError,
				Message:     "finished call",
				Transaction: "/user.user_service.v1.UserService/GetContact",
				Fingerprint: []string{"/user.user_service.v1.UserService/GetContact", "Internal"},
				Exception: []sentry.Exception{{
					Type: "Internal", Value: "boom",
				}},
				Tags: map[string]string{
					"server": "user-service", "grpc.code": "Internal", "issuer": "kenji", "organization": "organizations/o6",
				},
				User: sentry.User{ID: "organizations/o6/users/u6", Data: map[string]string{
					"actor": "uid-operator", "organization": "organizations/o6",
				}},
				Contexts: map[string]sentry.Context{
					"log": {
						"grpc.service": "user.user_service.v1.UserService", "grpc.method": "GetContact",
						"principal": "uid-6", "role": "kenji",
					},
				},
			}},
		},
		{
			name: "panic",
			ctx:  context.Background(),
			handler: func(h slog.Handler) slog.Handler {
				return slog.New(h).WithGroup("email_processor").Handler()
			},
			records: []slog.Record{record("panic",
				"routine", "nats-processor-email-processor-analyze-message", "panic", "nil map", "stack", goStack,
				"subject", "email.message.created")},
			want: []*sentry.Event{{
				Level:       sentry.LevelFatal,
				Message:     "panic",
				Transaction: "nats-processor-email-processor-analyze-message",
				Fingerprint: []string{"panic", "nats-processor-email-processor-analyze-message"},
				Exception: []sentry.Exception{{
					Type: "panic", Value: "nil map",
					Stacktrace: &sentry.Stacktrace{Frames: []sentry.Frame{
						{Function: "(*Processor[...]).processFn", Module: "github.com/malonaz/core/go/nats", AbsPath: "/core/go/nats/processor.go", Lineno: 231},
						{Function: "(*Handler).HandleEvent", Module: "example.com/repo/email/email_processor", Filename: "email/email_processor/analyze.go", AbsPath: "/src/email/email_processor/analyze.go", Lineno: 31, InApp: true},
					}},
				}},
				Contexts: map[string]sentry.Context{"log": {
					"routine": "nats-processor-email-processor-analyze-message", "subject": "email.message.created",
				}},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, transport := newTestHandler(t)
			handler = tt.handler(handler)
			for _, r := range tt.records {
				require.NoError(t, handler.Handle(tt.ctx, r))
			}
			for _, event := range tt.want {
				event.Timestamp = at
				event.Environment = "test"
				event.Release = "repo@abc"
			}
			require.Empty(t, cmp.Diff(tt.want, transport.events,
				cmpopts.EquateEmpty(),
				cmpopts.IgnoreUnexported(sentry.Event{}),
				cmpopts.IgnoreFields(sentry.Event{}, "EventID", "Sdk", "Platform", "ServerName", "Modules"),
				cmpopts.IgnoreMapEntries(func(key string, _ sentry.Context) bool {
					return key == "device" || key == "os" || key == "runtime" || key == "trace"
				}),
			))
		})
	}
}

// Guards the record shape against core's real logging interceptor.
func TestHandlerCoreLoggingInterceptor(t *testing.T) {
	handler, transport := newTestHandler(t)
	logger := slog.New(handler).WithGroup("grpc_server").With("name", "user-service")
	interceptor := middleware.UnaryServerLogging(logger)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-scheduler-job", "organizations/o1/jobs/j1"))
	ctx = withMethod(ctx, "/user.user_service.v1.UserService/GetContact")

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/user.user_service.v1.UserService/GetContact"},
		func(context.Context, any) (any, error) {
			return nil, status.Errorf(codes.Internal, "reading contact").Err()
		})
	require.Error(t, err)

	require.Len(t, transport.events, 1)
	require.NotNil(t, transport.events[0].Exception[0].Stacktrace)
	want := []*sentry.Event{{
		Level:       sentry.LevelError,
		Message:     "finished call",
		Transaction: "/user.user_service.v1.UserService/GetContact",
		Fingerprint: []string{"/user.user_service.v1.UserService/GetContact", "Internal"},
		Exception: []sentry.Exception{{
			Type:  "Internal",
			Value: "rpc error: code = Internal desc = reading contact",
		}},
		Tags:        map[string]string{"server": "user-service", "grpc.code": "Internal", "organization": "organizations/o1"},
		User:        sentry.User{Data: map[string]string{"organization": "organizations/o1"}},
		Environment: "test",
		Release:     "repo@abc",
		Contexts: map[string]sentry.Context{
			"job": {"name": "organizations/o1/jobs/j1"},
			"log": {
				"grpc.service": "user.user_service.v1.UserService", "grpc.method": "GetContact", "grpc.method_type": "unary",
				"grpc.component": "server", "protocol": "grpc",
			},
		},
	}}
	require.Empty(t, cmp.Diff(want, transport.events,
		cmpopts.EquateEmpty(),
		cmpopts.IgnoreUnexported(sentry.Event{}),
		cmpopts.IgnoreFields(sentry.Event{}, "EventID", "Sdk", "Platform", "ServerName", "Modules", "Timestamp"),
		cmpopts.IgnoreFields(sentry.Exception{}, "Stacktrace"),
		cmpopts.IgnoreMapEntries(func(key string, _ any) bool { return key == "grpc.time_ms" || key == "grpc.start_time" }),
		cmpopts.IgnoreMapEntries(func(key string, _ sentry.Context) bool {
			return key == "device" || key == "os" || key == "runtime" || key == "trace"
		}),
	))
}

func TestFirstOrganizationName(t *testing.T) {
	tests := []struct {
		name   string
		fields fields
		job    string
		want   string
	}{
		{name: "read scopes", fields: fields{readScopesAttribute: slog.StringValue("[organizations/o1 organizations/o2]")}, want: "organizations/o1"},
		{name: "wildcard skipped", fields: fields{"grpc.request.name": slog.StringValue("organizations/-/contacts/c1"), "grpc.request.parent": slog.StringValue("organizations/o3")}, want: "organizations/o3"},
		{name: "job before request", fields: fields{"grpc.request.name": slog.StringValue("organizations/o3")}, job: "organizations/o2/jobs/j1", want: "organizations/o2"},
		{name: "request before logged resource", fields: fields{"contact": slog.StringValue("organizations/o4/contacts/c1"), "grpc.request.name": slog.StringValue("organizations/o3")}, want: "organizations/o3"},
		{name: "free text skipped", fields: fields{"error": slog.StringValue("organizations/o5 not found"), "note": slog.StringValue("organizations/o6")}, want: "organizations/o6"},
		{name: "numeric id", fields: fields{"contact": slog.StringValue("organizations/49170/contacts/agqq3wjowf2mjapual2rsbow7y")}, want: "organizations/49170"},
		{name: "none", fields: fields{"grpc.request.name": slog.StringValue("jobs/j1")}},
	}
	handler, _ := newTestHandler(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, handler.(*Handler).firstOrganizationName(tt.fields, tt.job))
		})
	}
}

// Without a span, the SDK stamps the hub's process-wide trace ID on every event.
func TestHandlerTracePerEvent(t *testing.T) {
	handler, transport := newTestHandler(t)
	require.NoError(t, handler.Handle(context.Background(), record("first")))
	require.NoError(t, handler.Handle(context.Background(), record("second")))

	require.Len(t, transport.events, 2)
	first := transport.events[0].Contexts["trace"]["trace_id"]
	second := transport.events[1].Contexts["trace"]["trace_id"]
	require.NotEmpty(t, first)
	require.NotEqual(t, first, second)
}

type recordingHandler struct{ records []slog.Record }

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler            { return h }
func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	h.records = append(h.records, record)
	return nil
}

// panicStatusError panics inside grpcstatus.FromError while the event is built.
type panicStatusError struct{}

func (panicStatusError) Error() string                  { return "boom" }
func (panicStatusError) GRPCStatus() *grpcstatus.Status { panic("broken error") }

func TestHandlerPanicDropsEvent(t *testing.T) {
	next := &recordingHandler{}
	handler, transport := newTestHandlerWith(t, next)

	require.NoError(t, handler.Handle(context.Background(), record("executing fn", "error", panicStatusError{})))

	type logged struct {
		Level   slog.Level
		Message string
		Attrs   map[string]string
	}
	var got []logged
	for _, r := range next.records {
		attrs := map[string]string{}
		r.Attrs(func(attr slog.Attr) bool {
			attrs[attr.Key] = attr.Value.String()
			return true
		})
		got = append(got, logged{Level: r.Level, Message: r.Message, Attrs: attrs})
	}
	require.Equal(t, []logged{
		{Level: slog.LevelError, Message: "executing fn", Attrs: map[string]string{"error": "boom"}},
		{Level: slog.LevelWarn, Message: "dropping sentry event", Attrs: map[string]string{"panic": "broken error", "message": "executing fn"}},
	}, got)
	require.Empty(t, transport.events)
}
