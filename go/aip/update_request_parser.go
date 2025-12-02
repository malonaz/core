package aip

import (
	"fmt"
	"strings"

	"buf.build/go/protovalidate"
	"go.einride.tech/aip/fieldmask"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/pbutil"
)

// UpdateRequest defines the interface of an AIP update request.
type updateRequest interface {
	proto.Message
	GetUpdateMask() *fieldmaskpb.FieldMask
}

// ParsedUpdateRequest is a request that is parsed.
type ParsedUpdateRequest struct {
	validator      protovalidate.Validator
	fieldMask      *fieldmaskpb.FieldMask
	updateFieldSet map[string]struct{}
	updateFields   []string
}

// ApplyFieldMask applies the field mask and validates the newly formed resource using protovalidate.
// Existing resource should be used to update.
func (p *ParsedUpdateRequest) ApplyFieldMask(existingResource, newResource proto.Message) error {
	fieldmask.Update(p.fieldMask, existingResource, newResource)
	if err := p.validator.Validate(existingResource); err != nil {
		return fmt.Errorf("validating updated resource: %w", err)
	}
	return nil
}

func (p *ParsedUpdateRequest) GetSQLUpsertClause() string {
	var sb strings.Builder
	for i, updateField := range p.updateFields {
		sb.WriteString(fmt.Sprintf("%s = EXCLUDED.%s", updateField, updateField))
		if i < len(p.updateFields)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

func (p *ParsedUpdateRequest) GetSQLUpdateClause() string {
	var sb strings.Builder
	for i, updateField := range p.updateFields {
		sb.WriteString(fmt.Sprintf("%s = $%d", updateField, i+1))
		if i < len(p.updateFields)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

func (p *ParsedUpdateRequest) GetSQLColumns() []string {
	return p.updateFields
}

// UpdateRequestParser implements update request parsing.
type UpdateRequestParser[T updateRequest, R proto.Message] struct {
	validator         protovalidate.Validator
	paths             []string
	protoPathToColumn map[string]string
	mappings          map[string]string
}

// MustNewUpdateRequestParser instantiates and returns a new update request parser, panicking on error.
func MustNewUpdateRequestParser[T updateRequest, R proto.Message]() *UpdateRequestParser[T, R] {
	parser, err := NewUpdateRequestParser[T, R]()
	if err != nil {
		panic(err)
	}
	return parser
}

// NewUpdateRequestParser instantiates and returns a new update request parser.
func NewUpdateRequestParser[T updateRequest, R proto.Message]() (*UpdateRequestParser[T, R], error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("instantiating proto validator: %w", err)
	}

	// Parse options.
	var zero T
	options, err := pbutil.GetMessageOption[*aippb.UpdateOptions](zero, aippb.E_Update)
	if err != nil {
		return nil, fmt.Errorf("getting update options: %w", err)
	}
	if options == nil {
		return nil, fmt.Errorf("%T must define UpdateOptions", zero)
	}

	// Validate the paths.
	var zeroResource R
	sanitizedPaths := make([]string, 0, len(options.GetPaths()))
	for _, path := range options.GetPaths() {
		sanitizedPaths = append(sanitizedPaths, strings.TrimSuffix(path, ".*"))
	}
	if len(sanitizedPaths) > 0 {
		if err := pbutil.ValidateMask(zeroResource, strings.Join(sanitizedPaths, ",")); err != nil {
			return nil, fmt.Errorf("validating filtering paths: %w", err)
		}
	}

	// Build tree to get column name mappings (without nested path transformation).
	tree, err := BuildResourceTree[R](10, []string{"*"})
	if err != nil {
		return nil, fmt.Errorf("building resource tree: %w", err)
	}

	paths := options.Paths
	protoPathToColumn := make(map[string]string)
	mappings := map[string]string{}
	for _, node := range tree.Nodes {
		// We always add 'update_timestamp' as an updatable field.
		if node.Path == "update_time" {
			paths = append(paths, node.Path)
		}
		columnName := node.Path
		if node.ReplacementPath != "" {
			columnName = node.ReplacementPath
		}
		protoPathToColumn[node.Path] = columnName

		// Auto-generate mappings for fields stored as JSON or Proto bytes.
		if node.AsJsonBytes || node.AsProtoBytes {
			mappings[node.Path+".*"] = columnName
		}
	}

	return &UpdateRequestParser[T, R]{
		validator:         validator,
		paths:             paths,
		protoPathToColumn: protoPathToColumn,
		mappings:          mappings,
	}, nil
}

func (p *UpdateRequestParser[T, R]) Parse(request T) (*ParsedUpdateRequest, error) {
	var resource R
	fieldMask := request.GetUpdateMask()
	if len(fieldMask.GetPaths()) == 0 {
		return nil, fmt.Errorf("no mask paths specified")
	}

	// Validate the paths are valid.
	if err := fieldmask.Validate(fieldMask, resource); err != nil {
		return nil, fmt.Errorf("invalid field mask paths: %v", err)
	}

	parsedUpdateRequest := &ParsedUpdateRequest{
		validator:      p.validator,
		fieldMask:      fieldMask,
		updateFieldSet: map[string]struct{}{},
	}

	// Iterate over each path in the field mask.
	for _, path := range fieldMask.Paths {
		// We only verify non default paths for authorization.
		if !p.isAuthorizedPath(path) {
			return nil, fmt.Errorf("unauthorized field mask path: %s", path)
		}

		// Check against configured fields to see if there is a match.
		mappingFound := false
		for mappingFrom, mappingTo := range p.mappings {
			if p.match(mappingFrom, path) {
				// Add the mapped update mapping to the set and list.
				// Translate mapped path to column name if applicable.
				columnName := p.translateToColumnName(mappingTo)
				if _, ok := parsedUpdateRequest.updateFieldSet[columnName]; !ok {
					parsedUpdateRequest.updateFieldSet[columnName] = struct{}{}
					parsedUpdateRequest.updateFields = append(parsedUpdateRequest.updateFields, columnName)
				}
				mappingFound = true
				break
			}
		}

		// If no mapping is found, we simply add the field (translated to column name).
		if !mappingFound {
			columnName := p.translateToColumnName(path)
			if _, ok := parsedUpdateRequest.updateFieldSet[columnName]; !ok {
				parsedUpdateRequest.updateFieldSet[columnName] = struct{}{}
				parsedUpdateRequest.updateFields = append(parsedUpdateRequest.updateFields, columnName)
			}
		}
	}
	return parsedUpdateRequest, nil
}

// translateToColumnName translates a proto path to its database column name.
func (p *UpdateRequestParser[T, R]) translateToColumnName(path string) string {
	if columnName, ok := p.protoPathToColumn[path]; ok {
		return columnName
	}
	return path
}

// Helper method to check if a fieldPath matches a from considering wildcard.
func (p *UpdateRequestParser[T, R]) match(mappingFrom string, fieldPath string) bool {
	if strings.HasSuffix(mappingFrom, ".*") {
		// If from is a wildcard pattern, strip the wildcard and compare prefixes.
		prefix := strings.TrimSuffix(mappingFrom, "*")
		return strings.HasPrefix(fieldPath, prefix)
	}
	// If from is not a wildcard pattern, compare them directly.
	return mappingFrom == fieldPath
}

// Helper method to check if a fieldPath is authorized considering wildcard.
func (p *UpdateRequestParser[T, R]) isAuthorizedPath(fieldPath string) bool {
	for _, path := range p.paths {
		if strings.HasSuffix(path, ".*") {
			// If authorizedPath is a wildcard pattern, strip the wildcard and compare prefixes.
			prefix := strings.TrimSuffix(path, "*")
			if strings.HasPrefix(fieldPath, prefix) {
				return true
			}
		} else if path == fieldPath {
			return true // Exact match.
		}
	}
	return false
}
