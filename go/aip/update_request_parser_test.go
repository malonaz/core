package aip

import (
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	pb "github.com/malonaz/core/genproto/test/aip"
)

func TestUpdateRequestParser_ParseWithAuthorizedPaths(t *testing.T) {
	parser, err := NewUpdateRequestParser[*pb.UpdateResourceRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name             string
		fieldMaskPaths   []string
		wantUpdateClause string
		wantErr          bool
	}{
		// Tests that should succeed because the paths are authorized
		{
			name:             "single authorized field path with mapping",
			fieldMaskPaths:   []string{"field1"},
			wantUpdateClause: "field1 = EXCLUDED.field1",
			wantErr:          false,
		},
		{
			name:             "single authorized nested field path",
			fieldMaskPaths:   []string{"nested.field2"},
			wantUpdateClause: "nested = EXCLUDED.nested",
			wantErr:          false,
		},
		{
			name:             "multiple authorized field paths",
			fieldMaskPaths:   []string{"field1", "nested.field2"},
			wantUpdateClause: "field1 = EXCLUDED.field1, nested = EXCLUDED.nested",
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
			wantUpdateClause: "deleted = EXCLUDED.deleted, my_enum = EXCLUDED.my_enum",
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
			resource := &pb.Resource{}
			updateResourceRequest := &pb.UpdateResourceRequest{
				Resource:   resource,
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.fieldMaskPaths},
			}
			parsedRequest, err := parser.Parse(updateResourceRequest)

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
	parser, err := NewUpdateRequestParser[*pb.UpdateResourceRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name             string
		fieldMaskPaths   []string
		wantUpdateClause string
		wantErr          bool
	}{
		{
			name:             "matching wildcard path",
			fieldMaskPaths:   []string{"nested4.field1"},
			wantUpdateClause: "nested4 = EXCLUDED.nested4, nested3 = EXCLUDED.nested3",
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
			wantUpdateClause: "nested4 = EXCLUDED.nested4, nested3 = EXCLUDED.nested3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resource := &pb.Resource{}
			updateResourceRequest := &pb.UpdateResourceRequest{
				Resource:   resource,
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.fieldMaskPaths},
			}
			parsedRequest, err := parser.Parse(updateResourceRequest)

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
	parser, err := NewUpdateRequestParser[*pb.UpdateResource2Request, *pb.Resource]()
	require.NoError(t, err)

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
			resource := &pb.Resource{}
			updateResourceRequest := &pb.UpdateResource2Request{
				Resource:   resource,
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.fieldMaskPaths},
			}
			parsedRequest, err := parser.Parse(updateResourceRequest)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantUpdateClause, parsedRequest.GetSQLUpsertClause())
			}
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

func TestUpdateRequestParser_ParseWithColumnNameChange(t *testing.T) {
	parser, err := NewUpdateRequestParser[*pb.UpdateResourceWithColumnNameRequest, *pb.Resource]()
	require.NoError(t, err)

	tests := []struct {
		name             string
		fieldMaskPaths   []string
		wantUpsertClause string
		wantUpdateClause string
		wantColumns      []string
		wantErr          bool
	}{
		{
			name:             "field with column name change",
			fieldMaskPaths:   []string{"column_name_changed"},
			wantUpsertClause: "new_name = EXCLUDED.new_name",
			wantUpdateClause: "new_name = $1",
			wantColumns:      []string{"new_name"},
			wantErr:          false,
		},
		{
			name:             "field with column name change and regular field",
			fieldMaskPaths:   []string{"column_name_changed", "field1"},
			wantUpsertClause: "new_name = EXCLUDED.new_name, field1 = EXCLUDED.field1",
			wantUpdateClause: "new_name = $1, field1 = $2",
			wantColumns:      []string{"new_name", "field1"},
			wantErr:          false,
		},
		{
			name:             "regular field only",
			fieldMaskPaths:   []string{"field1"},
			wantUpsertClause: "field1 = EXCLUDED.field1",
			wantUpdateClause: "field1 = $1",
			wantColumns:      []string{"field1"},
			wantErr:          false,
		},
		{
			name:           "unauthorized field",
			fieldMaskPaths: []string{"nested"},
			wantErr:        true,
		},
		// New test cases for nested_changed with column name change
		{
			name:             "nested field with column name change via path mapping",
			fieldMaskPaths:   []string{"nested_changed.field2"},
			wantUpsertClause: "nested_new_name = EXCLUDED.nested_new_name",
			wantUpdateClause: "nested_new_name = $1",
			wantColumns:      []string{"nested_new_name"},
			wantErr:          false,
		},
		{
			name:             "nested field with column name change - full object update",
			fieldMaskPaths:   []string{"nested_changed"},
			wantUpsertClause: "nested_new_name = EXCLUDED.nested_new_name",
			wantUpdateClause: "nested_new_name = $1",
			wantColumns:      []string{"nested_new_name"},
			wantErr:          false,
		},
		{
			name:             "nested field with column name change combined with regular fields",
			fieldMaskPaths:   []string{"nested_changed.field2", "field1", "column_name_changed"},
			wantUpsertClause: "nested_new_name = EXCLUDED.nested_new_name, field1 = EXCLUDED.field1, new_name = EXCLUDED.new_name",
			wantUpdateClause: "nested_new_name = $1, field1 = $2, new_name = $3",
			wantColumns:      []string{"nested_new_name", "field1", "new_name"},
			wantErr:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resource := &pb.Resource{}
			updateResourceRequest := &pb.UpdateResourceWithColumnNameRequest{
				Resource:   resource,
				UpdateMask: &fieldmaskpb.FieldMask{Paths: tt.fieldMaskPaths},
			}
			parsedRequest, err := parser.Parse(updateResourceRequest)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantUpsertClause, parsedRequest.GetSQLUpsertClause())
				require.Equal(t, tt.wantUpdateClause, parsedRequest.GetSQLUpdateClause())
				require.Equal(t, tt.wantColumns, parsedRequest.GetSQLColumns())
			}
		})
	}
}
