package logging

import (
	"context"
	"log/slog"
)

// ExtractFromContextFn defines a function that extracts key-value pairs from context.
// It should return pairs of keys and values as []any, e.g., []any{"key1", "value1", "key2", "value2"}
type ExtractFromContextFn func(context.Context) []any

// ContextHandler wraps an slog.Handler and extracts values from context before logging.
type ContextHandler struct {
	handler slog.Handler
	extract ExtractFromContextFn
}

// NewContextHandler creates a new handler that extracts context values before delegating to the wrapped handler.
func NewContextHandler(handler slog.Handler, extract ExtractFromContextFn) *ContextHandler {
	return &ContextHandler{
		handler: handler,
		extract: extract,
	}
}

func (h *ContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.extract != nil {
		kvPairs := h.extract(ctx)
		if len(kvPairs) > 0 {
			// Convert key-value pairs to attributes
			for i := 0; i < len(kvPairs)-1; i += 2 {
				key, ok := kvPairs[i].(string)
				if !ok {
					continue
				}
				r.AddAttrs(slog.Any(key, kvPairs[i+1]))
			}
		}
	}
	return h.handler.Handle(ctx, r)
}

func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return NewContextHandler(h.handler.WithAttrs(attrs), h.extract)
}

func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return NewContextHandler(h.handler.WithGroup(name), h.extract)
}
