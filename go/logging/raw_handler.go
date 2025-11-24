package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// RawHandler is a simple handler that outputs messages with key-value pairs in a simple format.
type RawHandler struct {
	writer io.Writer
	level  slog.Level
	attrs  []slog.Attr
	groups []string
}

// NewRawHandler creates a new RawHandler.
func NewRawHandler(w io.Writer, opts *slog.HandlerOptions) *RawHandler {
	level := slog.LevelInfo
	if opts != nil && opts.Level != nil {
		level = opts.Level.Level()
	}
	return &RawHandler{
		writer: w,
		level:  level,
		attrs:  []slog.Attr{},
		groups: []string{},
	}
}

// Enabled reports whether the handler handles records at the given level.
func (h *RawHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

// Handle outputs the log message with key-value pairs in a simple format.
func (h *RawHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder

	// Write the message
	b.WriteString(r.Message)

	// Write handler attributes
	for _, attr := range h.attrs {
		h.appendAttr(&b, attr, h.groups)
	}

	// Write record attributes
	r.Attrs(func(attr slog.Attr) bool {
		h.appendAttr(&b, attr, h.groups)
		return true
	})

	_, err := fmt.Fprintln(h.writer, b.String())
	return err
}

// appendAttr appends an attribute to the string builder.
func (h *RawHandler) appendAttr(b *strings.Builder, attr slog.Attr, groups []string) {
	// Skip empty attributes
	if attr.Equal(slog.Attr{}) {
		return
	}

	b.WriteString(" ")

	// Add group prefix if any
	if len(groups) > 0 {
		b.WriteString(strings.Join(groups, "."))
		b.WriteString(".")
	}

	b.WriteString(attr.Key)
	b.WriteString("=")
	b.WriteString(fmt.Sprintf("%v", attr.Value))
}

// WithAttrs returns a new handler with additional attributes.
func (h *RawHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	copy(newAttrs[len(h.attrs):], attrs)

	return &RawHandler{
		writer: h.writer,
		level:  h.level,
		attrs:  newAttrs,
		groups: h.groups,
	}
}

// WithGroup returns a new handler with a group name.
func (h *RawHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	newGroups := make([]string, len(h.groups)+1)
	copy(newGroups, h.groups)
	newGroups[len(h.groups)] = name

	return &RawHandler{
		writer: h.writer,
		level:  h.level,
		attrs:  h.attrs,
		groups: newGroups,
	}
}
