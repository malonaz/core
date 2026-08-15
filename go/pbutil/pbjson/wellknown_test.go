package pbjson

import (
	"testing"

	"github.com/stretchr/testify/require"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/genproto/googleapis/type/postaladdress"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/malonaz/core/go/pbutil"
)

func TestRPCStatusSchema(t *testing.T) {
	t.Run("drops the opaque details field", func(t *testing.T) {
		schema := rpcStatusSchema()
		var fieldNames []string
		for fieldName := range schema.Properties {
			fieldNames = append(fieldNames, fieldName)
		}
		require.ElementsMatch(t, []string{"code", "message"}, fieldNames)
		require.NotContains(t, schema.Properties, "details")
	})

	t.Run("is small", func(t *testing.T) {
		bytes, err := pbutil.JSONMarshal(rpcStatusSchema())
		require.NoError(t, err)
		t.Logf("rpc.Status schema size: %d bytes", len(bytes))
		require.Less(t, len(bytes), 600)
	})

	// The pruned schema must still parse back into the real proto message.
	t.Run("round trips into the proto message", func(t *testing.T) {
		descriptor := (&rpcstatus.Status{}).ProtoReflect().Descriptor()
		message, err := BuildMessage(descriptor, map[string]any{
			"code":    float64(5),
			"message": "chat not found",
		})
		require.NoError(t, err)
		fields := descriptor.Fields()
		require.Equal(t, int64(5), message.Get(fields.ByName("code")).Int())
		require.Equal(t, "chat not found", message.Get(fields.ByName("message")).String())
	})
}

func TestPostalAddressSchema(t *testing.T) {
	t.Run("exposes only the rendered subset", func(t *testing.T) {
		schema := postalAddressSchema()
		var fieldNames []string
		for fieldName := range schema.Properties {
			fieldNames = append(fieldNames, fieldName)
		}
		require.ElementsMatch(t, []string{
			"region_code", "postal_code", "administrative_area", "locality", "address_lines",
		}, fieldNames)
		require.Equal(t, []string{"region_code"}, schema.Required)
	})

	t.Run("is far smaller than the generated message", func(t *testing.T) {
		bytes, err := pbutil.JSONMarshal(postalAddressSchema())
		require.NoError(t, err)
		t.Logf("pruned schema size: %d bytes", len(bytes))
		require.Less(t, len(bytes), 1200)
	})

	// The pruned schema must still parse back into the real proto message.
	t.Run("round trips into the proto message", func(t *testing.T) {
		descriptor := (&postaladdress.PostalAddress{}).ProtoReflect().Descriptor()
		message, err := BuildMessage(descriptor, map[string]any{
			"region_code":         "US",
			"locality":            "San Francisco",
			"administrative_area": "CA",
			"postal_code":         "94103",
			"address_lines":       []any{"1 Market St", "Suite 300"},
		})
		require.NoError(t, err)
		fields := descriptor.Fields()
		require.Equal(t, "US", message.Get(fields.ByName("region_code")).String())
		require.Equal(t, "San Francisco", message.Get(fields.ByName("locality")).String())
		require.Equal(t, "CA", message.Get(fields.ByName("administrative_area")).String())
		require.Equal(t, "94103", message.Get(fields.ByName("postal_code")).String())
		addressLines := message.Get(fields.ByName("address_lines")).List()
		require.Equal(t, 2, addressLines.Len())
		require.Equal(t, "1 Market St", addressLines.Get(0).String())
		var _ protoreflect.Message = message
	})
}
