package contexttag

import (
	"context"
)

type ctxMarkerLogTagsKey struct{}
type ctxMarkerTrailerTagsKey struct{}

func SetOntoContext(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, ctxMarkerLogTagsKey{}, &logTags{values: map[string]any{}})
	ctx = context.WithValue(ctx, ctxMarkerTrailerTagsKey{}, &trailerTags{values: map[string][]string{}})
	return ctx
}

func SetOntoContextNoop(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, ctxMarkerLogTagsKey{}, &noopLogTags{})
	ctx = context.WithValue(ctx, ctxMarkerTrailerTagsKey{}, &noopTrailerTags{})
	return ctx
}

// ///////////// LOG TAGS ///////////////
type LogTags interface {
	Append(key string, value any) LogTags
	Get(key string) (any, bool)
	Values() map[string]any
}

type logTags struct {
	values map[string]any
}

func (t *logTags) Append(key string, value any) LogTags {
	t.values[key] = value
	return t
}

func (t *logTags) Get(key string) (any, bool) {
	value, ok := t.values[key]
	return value, ok
}

var noopLogTagsValues = map[string]any{}

func (t *logTags) Values() map[string]any {
	return t.values
}

type noopLogTags struct{}

func (t *noopLogTags) Append(key string, value any) LogTags {
	return t
}

func (t *noopLogTags) Get(key string) (any, bool) {
	return nil, false
}

func (t *noopLogTags) Values() map[string]any {
	return noopLogTagsValues
}

func GetLogTags(ctx context.Context) (LogTags, bool) {
	tags, ok := ctx.Value(ctxMarkerLogTagsKey{}).(LogTags)
	return tags, ok
}

// ///////////// TRAILER TAGS ///////////////
type TrailerTags interface {
	Append(key string, values ...string) TrailerTags
	Set(key string, values ...string) TrailerTags
	Get(key string) ([]string, bool)
	Values() map[string][]string
}

type trailerTags struct {
	values map[string][]string
}

func (t *trailerTags) Append(key string, values ...string) TrailerTags {
	t.values[key] = append(t.values[key], values...)
	return t
}

func (t *trailerTags) Set(key string, values ...string) TrailerTags {
	t.values[key] = values
	return t
}

func (t *trailerTags) Get(key string) ([]string, bool) {
	value, ok := t.values[key]
	return value, ok
}

func (t *trailerTags) Values() map[string][]string {
	return t.values
}

type noopTrailerTags struct{}

func (t *noopTrailerTags) Append(key string, values ...string) TrailerTags {
	return t
}

func (t *noopTrailerTags) Set(key string, values ...string) TrailerTags {
	return t
}

func (t *noopTrailerTags) Get(key string) ([]string, bool) {
	return nil, false
}

var noopTrailerTagsValue = map[string][]string{}

func (t *noopTrailerTags) Values() map[string][]string {
	return noopTrailerTagsValue
}

func GetTrailersTags(ctx context.Context) (TrailerTags, bool) {
	tags, ok := ctx.Value(ctxMarkerTrailerTagsKey{}).(TrailerTags)
	return tags, ok
}
