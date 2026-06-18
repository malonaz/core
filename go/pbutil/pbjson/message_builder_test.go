package pbjson

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	librarypb "github.com/malonaz/core/genproto/test/library/v1"
)

func TestBuildMessage(t *testing.T) {
	dummyDescriptor := findMessageDescriptor(t, (&librarypb.Dummy{}).ProtoReflect().Descriptor().FullName())

	t.Run("string field", func(t *testing.T) {
		args := map[string]any{
			"title": "hello world",
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		require.Equal(t, "hello world", message.Get(dummyDescriptor.Fields().ByName("title")).String())
	})

	t.Run("int32 field", func(t *testing.T) {
		args := map[string]any{
			"quantity": float64(42),
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		require.Equal(t, int64(42), message.Get(dummyDescriptor.Fields().ByName("quantity")).Int())
	})

	t.Run("double field", func(t *testing.T) {
		args := map[string]any{
			"rate": 3.14,
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		require.InDelta(t, 3.14, message.Get(dummyDescriptor.Fields().ByName("rate")).Float(), 0.001)
	})

	t.Run("bool field", func(t *testing.T) {
		args := map[string]any{
			"active": true,
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		require.True(t, message.Get(dummyDescriptor.Fields().ByName("active")).Bool())
	})

	t.Run("bool field false", func(t *testing.T) {
		args := map[string]any{
			"active": false,
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		require.False(t, message.Get(dummyDescriptor.Fields().ByName("active")).Bool())
	})

	t.Run("timestamp field", func(t *testing.T) {
		args := map[string]any{
			"expire_time": "2025-06-15T10:30:00Z",
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		expireTime := message.Get(dummyDescriptor.Fields().ByName("expire_time")).Message()
		require.Equal(t, int64(1749983400), expireTime.Get(expireTime.Descriptor().Fields().ByName("seconds")).Int())
		require.Equal(t, int64(0), expireTime.Get(expireTime.Descriptor().Fields().ByName("nanos")).Int())
	})

	t.Run("timestamp field with nanos", func(t *testing.T) {
		args := map[string]any{
			"expire_time": "2025-06-15T10:30:00.500Z",
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		expireTime := message.Get(dummyDescriptor.Fields().ByName("expire_time")).Message()
		require.Equal(t, int64(1749983400), expireTime.Get(expireTime.Descriptor().Fields().ByName("seconds")).Int())
		require.Equal(t, int64(500000000), expireTime.Get(expireTime.Descriptor().Fields().ByName("nanos")).Int())
	})

	t.Run("duration field", func(t *testing.T) {
		args := map[string]any{
			"duration": "3600s",
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		duration := message.Get(dummyDescriptor.Fields().ByName("duration")).Message()
		require.Equal(t, int64(3600), duration.Get(duration.Descriptor().Fields().ByName("seconds")).Int())
	})

	t.Run("duration field with nanos", func(t *testing.T) {
		args := map[string]any{
			"duration": "1.5s",
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		duration := message.Get(dummyDescriptor.Fields().ByName("duration")).Message()
		require.Equal(t, int64(1), duration.Get(duration.Descriptor().Fields().ByName("seconds")).Int())
		require.Equal(t, int64(500000000), duration.Get(duration.Descriptor().Fields().ByName("nanos")).Int())
	})

	t.Run("repeated string field", func(t *testing.T) {
		args := map[string]any{
			"tags": []any{"go", "proto", "test"},
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		tags := message.Get(dummyDescriptor.Fields().ByName("tags")).List()
		require.Equal(t, 3, tags.Len())
		require.Equal(t, "go", tags.Get(0).String())
		require.Equal(t, "proto", tags.Get(1).String())
		require.Equal(t, "test", tags.Get(2).String())
	})

	t.Run("repeated string field empty", func(t *testing.T) {
		args := map[string]any{
			"tags": []any{},
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		tags := message.Get(dummyDescriptor.Fields().ByName("tags")).List()
		require.Equal(t, 0, tags.Len())
	})

	t.Run("map field", func(t *testing.T) {
		args := map[string]any{
			"labels": map[string]any{
				"env":     "prod",
				"version": "v2",
			},
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		labels := message.Get(dummyDescriptor.Fields().ByName("labels")).Map()
		require.Equal(t, 2, labels.Len())
		require.Equal(t, "prod", labels.Get(protoreflect.MapKey(protoreflect.ValueOfString("env"))).String())
		require.Equal(t, "v2", labels.Get(protoreflect.MapKey(protoreflect.ValueOfString("version"))).String())
	})

	t.Run("map field empty", func(t *testing.T) {
		args := map[string]any{
			"labels": map[string]any{},
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		labels := message.Get(dummyDescriptor.Fields().ByName("labels")).Map()
		require.Equal(t, 0, labels.Len())
	})

	t.Run("money field positive with cents", func(t *testing.T) {
		args := map[string]any{
			"subtotal": "USD 25.50",
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		assertMoney(t, message, "subtotal", "USD", 25, 500000000)
	})

	t.Run("money field negative", func(t *testing.T) {
		args := map[string]any{
			"subtotal": "EUR -1.75",
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		assertMoney(t, message, "subtotal", "EUR", -1, -750000000)
	})

	t.Run("money field whole amount", func(t *testing.T) {
		args := map[string]any{
			"tax": "JPY 1000",
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		assertMoney(t, message, "tax", "JPY", 1000, 0)
	})

	t.Run("money field zero", func(t *testing.T) {
		args := map[string]any{
			"subtotal": "USD 0.00",
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		assertMoney(t, message, "subtotal", "USD", 0, 0)
	})

	t.Run("money field high precision nanos", func(t *testing.T) {
		args := map[string]any{
			"subtotal": "GBP 1.123456789",
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		assertMoney(t, message, "subtotal", "GBP", 1, 123456789)
	})

	t.Run("nested message field", func(t *testing.T) {
		args := map[string]any{
			"metadata": map[string]any{
				"summary":  "A great book",
				"language": "en",
			},
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		metadata := message.Get(dummyDescriptor.Fields().ByName("metadata")).Message()
		metadataFields := metadata.Descriptor().Fields()
		require.Equal(t, "A great book", metadata.Get(metadataFields.ByName("summary")).String())
		require.Equal(t, "en", metadata.Get(metadataFields.ByName("language")).String())
	})

	t.Run("nested message with duration", func(t *testing.T) {
		args := map[string]any{
			"metadata": map[string]any{
				"summary":  "Audio book",
				"duration": "7200s",
			},
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		metadata := message.Get(dummyDescriptor.Fields().ByName("metadata")).Message()
		duration := metadata.Get(metadata.Descriptor().Fields().ByName("duration")).Message()
		require.Equal(t, int64(7200), duration.Get(duration.Descriptor().Fields().ByName("seconds")).Int())
	})

	t.Run("oneof field with money amount", func(t *testing.T) {
		adjustmentDescriptor := findMessageDescriptor(t, (&librarypb.DummyAdjustment{}).ProtoReflect().Descriptor().FullName())
		args := map[string]any{
			"amount": "USD 50.00",
		}
		message, err := BuildMessage(adjustmentDescriptor, args)
		require.NoError(t, err)
		amountField := adjustmentDescriptor.Fields().ByName("amount")
		moneyMessage := message.Get(amountField).Message()
		moneyFields := moneyMessage.Descriptor().Fields()
		require.Equal(t, "USD", moneyMessage.Get(moneyFields.ByName("currency_code")).String())
		require.Equal(t, int64(50), moneyMessage.Get(moneyFields.ByName("units")).Int())
		require.Equal(t, int32(0), int32(moneyMessage.Get(moneyFields.ByName("nanos")).Int()))
	})

	t.Run("oneof field with percentage", func(t *testing.T) {
		adjustmentDescriptor := findMessageDescriptor(t, (&librarypb.DummyAdjustment{}).ProtoReflect().Descriptor().FullName())
		args := map[string]any{
			"percentage": 15.5,
		}
		message, err := BuildMessage(adjustmentDescriptor, args)
		require.NoError(t, err)
		require.InDelta(t, 15.5, message.Get(adjustmentDescriptor.Fields().ByName("percentage")).Float(), 0.001)
	})

	t.Run("multiple money fields", func(t *testing.T) {
		args := map[string]any{
			"subtotal": "USD 100.00",
			"tax":      "USD 8.25",
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		assertMoney(t, message, "subtotal", "USD", 100, 0)
		assertMoney(t, message, "tax", "USD", 8, 250000000)
	})

	t.Run("all scalar fields together", func(t *testing.T) {
		args := map[string]any{
			"title":    "full test",
			"quantity": float64(10),
			"rate":     2.5,
			"active":   true,
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		require.Equal(t, "full test", message.Get(dummyDescriptor.Fields().ByName("title")).String())
		require.Equal(t, int64(10), message.Get(dummyDescriptor.Fields().ByName("quantity")).Int())
		require.InDelta(t, 2.5, message.Get(dummyDescriptor.Fields().ByName("rate")).Float(), 0.001)
		require.True(t, message.Get(dummyDescriptor.Fields().ByName("active")).Bool())
	})

	t.Run("all fields populated", func(t *testing.T) {
		args := map[string]any{
			"subtotal":    "USD 100.00",
			"tax":         "USD 8.25",
			"title":       "comprehensive",
			"quantity":    float64(5),
			"rate":        1.99,
			"active":      true,
			"expire_time": "2025-12-31T23:59:59Z",
			"duration":    "300s",
			"tags":        []any{"a", "b"},
			"labels":      map[string]any{"k": "v"},
			"metadata": map[string]any{
				"summary":  "test",
				"language": "fr",
			},
			"adjustment": map[string]any{
				"percentage": 10.0,
			},
		}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		assertMoney(t, message, "subtotal", "USD", 100, 0)
		assertMoney(t, message, "tax", "USD", 8, 250000000)
		require.Equal(t, "comprehensive", message.Get(dummyDescriptor.Fields().ByName("title")).String())
		require.Equal(t, int64(5), message.Get(dummyDescriptor.Fields().ByName("quantity")).Int())
		require.InDelta(t, 1.99, message.Get(dummyDescriptor.Fields().ByName("rate")).Float(), 0.001)
		require.True(t, message.Get(dummyDescriptor.Fields().ByName("active")).Bool())
		require.Equal(t, 2, message.Get(dummyDescriptor.Fields().ByName("tags")).List().Len())
		require.Equal(t, 1, message.Get(dummyDescriptor.Fields().ByName("labels")).Map().Len())
	})

	t.Run("empty args", func(t *testing.T) {
		args := map[string]any{}
		message, err := BuildMessage(dummyDescriptor, args)
		require.NoError(t, err)
		require.NotNil(t, message)
	})

	t.Run("nil args", func(t *testing.T) {
		message, err := BuildMessage(dummyDescriptor, nil)
		require.NoError(t, err)
		require.NotNil(t, message)
	})
}

func assertMoney(t *testing.T, message protoreflect.Message, fieldName string, wantCurrency string, wantUnits int64, wantNanos int32) {
	t.Helper()
	field := message.Descriptor().Fields().ByName(protoreflect.Name(fieldName))
	require.NotNil(t, field)
	moneyMessage := message.Get(field).Message()
	moneyFields := moneyMessage.Descriptor().Fields()
	require.Equal(t, wantCurrency, moneyMessage.Get(moneyFields.ByName("currency_code")).String())
	require.Equal(t, wantUnits, moneyMessage.Get(moneyFields.ByName("units")).Int())
	require.Equal(t, wantNanos, int32(moneyMessage.Get(moneyFields.ByName("nanos")).Int()))
}

func findMessageDescriptor(t *testing.T, fullName protoreflect.FullName) protoreflect.MessageDescriptor {
	t.Helper()
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(fullName)
	require.NoError(t, err, "message %s not found in global registry", fullName)
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	require.True(t, ok)
	return messageDescriptor
}
