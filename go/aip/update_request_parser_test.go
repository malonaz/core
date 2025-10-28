package aip

import (
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	pb "github.com/malonaz/core/genproto/test/aip"
)

func TestUpdateRequestParser_ParseWithAuthorizedPaths(t *testing.T) {
	parser := NewUpdateRequestParser(&pb.UpdateResourceRequest{})

	tests := []struct {
		name             string
		fieldMaskPaths   []string
		wantUpdateClause string
		wantErr          bool
	}{
		// Tests that should succeed because the paths are authorized
		{
			name:             "single authorized field path",
			fieldMaskPaths:   []string{"field1"},
			wantUpdateClause: "field_one = EXCLUDED.field_one",
			wantErr:          false,
		},
		{
			name:             "single authorized nested field path",
			fieldMaskPaths:   []string{"nested.field2"},
			wantUpdateClause: "nested_field_two = EXCLUDED.nested_field_two",
			wantErr:          false,
		},
		{
			name:             "multiple authorized field paths",
			fieldMaskPaths:   []string{"field1", "nested.field2"},
			wantUpdateClause: "field_one = EXCLUDED.field_one, nested_field_two = EXCLUDED.nested_field_two",
			wantErr:          false,
		},
		{
			name:             "single nested field using regex",
			fieldMaskPaths:   []string{"nested2"},
			wantUpdateClause: "nested2 = EXCLUDED.nested2",
			wantErr:          false,
		},
		{
			name:             "authorized field path without explicit mapping",
			fieldMaskPaths:   []string{"nested"},
			wantUpdateClause: "nested = EXCLUDED.nested",
			wantErr:          false,
		},
		{
			name:             "map to multiple values",
			fieldMaskPaths:   []string{"nested3"},
			wantUpdateClause: "field1 = EXCLUDED.field1, field2 = EXCLUDED.field2",
			wantErr:          false,
		},

		// Tests that should fail because the paths are not authorized
		{
			name:             "unauthorized field path",
			fieldMaskPaths:   []string{"nested.field3"},
			wantUpdateClause: "",
			wantErr:          true,
		},
		{
			name:             "authorized and unauthorized field paths",
			fieldMaskPaths:   []string{"field1", "nested.field3"},
			wantUpdateClause: "",
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fieldMask := &fieldmaskpb.FieldMask{Paths: tt.fieldMaskPaths}
			resource := &pb.Resource{}
			parsedRequest, err := parser.Parse(fieldMask, resource, false)

			// Check for errors if expected.
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Verify the parsed request SQL Update Clause.
				require.Equal(t, tt.wantUpdateClause, parsedRequest.GetSQLUpsertClause())
			}
		})
	}
}

func TestUpdateRequestParser_ParseWithWildcardMapping(t *testing.T) {
	parser := NewUpdateRequestParser(&pb.UpdateResourceRequest{})
	tests := []struct {
		name             string
		fieldMaskPaths   []string
		wantUpdateClause string
		wantErr          bool
	}{
		{
			name:             "matching wildcard path",
			fieldMaskPaths:   []string{"nested4.field1"},
			wantUpdateClause: "nested4_jsonb = EXCLUDED.nested4_jsonb",
			wantErr:          false,
		},
		{
			name:             "match wildcard path + unauthorized field",
			fieldMaskPaths:   []string{"nested4.field1", "nested4.field2"},
			wantUpdateClause: "",
			wantErr:          true,
		},
		{
			name:             "two matching wildcard path",
			fieldMaskPaths:   []string{"nested4.field1", "nested4.field3"},
			wantUpdateClause: "nested4_jsonb = EXCLUDED.nested4_jsonb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fieldMask := &fieldmaskpb.FieldMask{Paths: tt.fieldMaskPaths}
			resource := &pb.Resource{}
			parsedRequest, err := parser.Parse(fieldMask, resource, false)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantUpdateClause, parsedRequest.GetSQLUpsertClause())
			}
		})
	}
}

func TestUpdateRequestParser_ParseWithDefaultPaths(t *testing.T) {
	parser := NewUpdateRequestParser(&pb.UpdateResource2Request{})
	tests := []struct {
		name             string
		fieldMaskPaths   []string
		wantUpdateClause string
		wantErr          bool
	}{
		{
			name:             "explicit field path without default path",
			fieldMaskPaths:   []string{"nested"},
			wantUpdateClause: "nested = EXCLUDED.nested, field1 = EXCLUDED.field1",
			wantErr:          false,
		},
		{
			name:             "explicit field path with default path",
			wantUpdateClause: "field1 = EXCLUDED.field1",
			wantErr:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fieldMask := &fieldmaskpb.FieldMask{Paths: tt.fieldMaskPaths}
			resource := &pb.Resource{}
			parsedRequest, err := parser.Parse(fieldMask, resource, false)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantUpdateClause, parsedRequest.GetSQLUpsertClause())
			}
		})
	}
}

func TestUpdateRequestParser_ParseWithAdminPrivileges(t *testing.T) {
	parser := NewUpdateRequestParser(&pb.UpdateResource2Request{})
	tests := []struct {
		name             string
		fieldMaskPaths   []string
		wantUpdateClause string
		requiresAdmin    bool
	}{
		{
			name:             "regular fields",
			fieldMaskPaths:   []string{"nested"},
			wantUpdateClause: "nested = EXCLUDED.nested, field1 = EXCLUDED.field1",
			requiresAdmin:    false,
		},
		{
			name:             "touches admin field",
			fieldMaskPaths:   []string{"nested2"},
			wantUpdateClause: "nested2 = EXCLUDED.nested2, field1 = EXCLUDED.field1",
			requiresAdmin:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fieldMask := &fieldmaskpb.FieldMask{Paths: tt.fieldMaskPaths}
			resource := &pb.Resource{}
			parsedRequest, err := parser.Parse(fieldMask, resource, false)
			if tt.requiresAdmin {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantUpdateClause, parsedRequest.GetSQLUpsertClause())
			}

			t.Run("admin=true", func(t *testing.T) {
				parsedRequest, err := parser.Parse(fieldMask, resource, true)
				require.NoError(t, err)
				require.Equal(t, tt.wantUpdateClause, parsedRequest.GetSQLUpsertClause())
			})
		})
	}
}

func TestParsedUpdateRequest_ApplyFieldMask(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		panic("instantiating proto validator")
	}

	existingResource := &pb.Resource{
		Field1: "initialValue1",
		Nested: &pb.NestedResource{
			Field2: 123,
			Field3: "initialValue2",
		},
		Nested2: &pb.NestedResource{
			Field2: 456,
			Field3: "initialValue3",
		},
	}
	newResource := &pb.Resource{
		Field1: "updatedValue1",
		Nested: &pb.NestedResource{
			Field2: 789,
			Field3: "updatedValue2",
		},
		Nested2: &pb.NestedResource{
			Field2: 101112,
			Field3: "updatedValue3",
		},
	}

	// Test updating specific nested fields
	fieldMaskSpecificNestedFields := &fieldmaskpb.FieldMask{Paths: []string{"field1", "nested.field3"}}
	parsedRequestSpecificNestedFields := &ParsedUpdateRequest{
		validator: validator,
		fieldMask: fieldMaskSpecificNestedFields,
	}
	parsedRequestSpecificNestedFields.ApplyFieldMask(existingResource, newResource)

	// Verify that only the specified fields have been updated
	require.Equal(t, "updatedValue1", existingResource.Field1)         // field1 should be updated
	require.Equal(t, int64(123), existingResource.Nested.Field2)       // nested.field2 should NOT be updated
	require.Equal(t, "updatedValue2", existingResource.Nested.Field3)  // nested.field3 should be updated
	require.Equal(t, int64(456), existingResource.Nested2.Field2)      // nested2.field2 should NOT be updated
	require.Equal(t, "initialValue3", existingResource.Nested2.Field3) // nested2.field3 should NOT be updated

	// Set up a fresh instance of the existing resource for the next test
	existingResource = &pb.Resource{
		Field1: "initialValue1",
		Nested: &pb.NestedResource{
			Field2: 123,
			Field3: "initialValue2",
		},
		Nested2: &pb.NestedResource{
			Field2: 456,
			Field3: "initialValue3",
		},
	}

	// Test replacing the entire nested field
	fieldMaskEntireNestedField := &fieldmaskpb.FieldMask{Paths: []string{"nested2"}}
	parsedRequestEntireNestedField := &ParsedUpdateRequest{
		validator: validator,
		fieldMask: fieldMaskEntireNestedField,
	}
	parsedRequestEntireNestedField.ApplyFieldMask(existingResource, newResource)

	// Verify that the entire nested2 field has been replaced
	require.Equal(t, "initialValue1", existingResource.Field1)         // field1 should NOT be updated
	require.Equal(t, int64(123), existingResource.Nested.Field2)       // nested.field2 should NOT be updated
	require.Equal(t, "initialValue2", existingResource.Nested.Field3)  // nested.field3 should NOT be updated
	require.Equal(t, int64(101112), existingResource.Nested2.Field2)   // nested2.field2 should be updated
	require.Equal(t, "updatedValue3", existingResource.Nested2.Field3) // nested2.field3 should be updated
}
