package aip

import (
	"fmt"
	"strings"

	"buf.build/go/protovalidate"
	"go.einride.tech/aip/fieldmask"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	aippb "github.com/malonaz/core/genproto/codegen/aip"
	"github.com/malonaz/core/go/pbutil"
)

// UpdateRequest defines the interface of an AIP update request.
type UpdateRequest interface {
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
type UpdateRequestParser struct {
	validator       protovalidate.Validator
	defaultPaths    []string
	mappings        []*aippb.UpdatePathMapping
	authorizedPaths []*aippb.AuthorizedUpdatePath
}

// NewUpdateRequestParser instantiates and returns a new update request parser.
func NewUpdateRequestParser(request UpdateRequest) *UpdateRequestParser {
	validator, err := protovalidate.New()
	if err != nil {
		panic("instantiating proto validator")
	}

	options := pbutil.MustGetMessageOption(request, aippb.E_Update).(*aippb.UpdateOptions)
	if options == nil {
		panic(fmt.Sprintf("%T must define UpdateOptions", request))
	}
	return &UpdateRequestParser{
		validator:       validator,
		defaultPaths:    options.DefaultPaths,
		mappings:        options.PathMappings,
		authorizedPaths: options.AuthorizedPaths,
	}
}

func (p *UpdateRequestParser) Parse(
	fieldMask *fieldmaskpb.FieldMask, resource proto.Message,
) (*ParsedUpdateRequest, error) {
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
	for i, path := range append(fieldMask.Paths, p.defaultPaths...) {
		// We only verify non default paths for authorization.
		if i < len(fieldMask.Paths) && !p.isAuthorizedPath(path) {
			return nil, fmt.Errorf("unauthorized field mask path: %s", path)
		}

		// Check against configured fields to see if there is a match.
		mappingFound := false
		for _, mapping := range p.mappings {
			if p.match(mapping, path) {
				// Add the mapped update mapping to the set and list.
				for _, v := range mapping.To {
					if _, ok := parsedUpdateRequest.updateFieldSet[v]; !ok {
						parsedUpdateRequest.updateFieldSet[v] = struct{}{}
						parsedUpdateRequest.updateFields = append(parsedUpdateRequest.updateFields, v)
					}
				}
				mappingFound = true
				break
			}
		}

		// If no mapping is found, we simply add the field as is.
		if !mappingFound {
			if _, ok := parsedUpdateRequest.updateFieldSet[path]; !ok {
				parsedUpdateRequest.updateFieldSet[path] = struct{}{}
				parsedUpdateRequest.updateFields = append(parsedUpdateRequest.updateFields, path)
			}
		}
	}
	return parsedUpdateRequest, nil
}

// Helper method to check if a fieldPath matches a from considering wildcard.
func (p *UpdateRequestParser) match(mapping *aippb.UpdatePathMapping, fieldPath string) bool {
	if strings.HasSuffix(mapping.From, ".*") {
		// If from is a wildcard pattern, strip the wildcard and compare prefixes.
		prefix := strings.TrimSuffix(mapping.From, "*")
		return strings.HasPrefix(fieldPath, prefix)
	}
	// If from is not a wildcard pattern, compare them directly.
	return mapping.From == fieldPath
}

// Helper method to check if a fieldPath is authorized considering wildcard.
func (p *UpdateRequestParser) isAuthorizedPath(fieldPath string) bool {
	for _, authorizedPath := range p.authorizedPaths {
		if strings.HasSuffix(authorizedPath.Path, ".*") {
			// If authorizedPath is a wildcard pattern, strip the wildcard and compare prefixes.
			prefix := strings.TrimSuffix(authorizedPath.Path, "*")
			if strings.HasPrefix(fieldPath, prefix) {
				return true
			}
		} else if authorizedPath.Path == fieldPath {
			return true // Exact match.
		}
	}
	return false
}
