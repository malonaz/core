package aip

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"

	annotationspb "google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	aippb "github.com/malonaz/core/genproto/codegen/aip/v1"
	"github.com/malonaz/core/go/pbutil"
	"github.com/malonaz/core/go/pbutil/pbfieldmask"
)

var (
	updateRequestParserInvalidPathSet = map[string]struct{}{
		"create_time": {},
		"update_time": {},
		"delete_time": {},
		"etag":        {},
	}
)

// A resource that has an etag field.
type ETagResource interface {
	proto.Message
	GetEtag() string
	GetUpdateTime() *timestamppb.Timestamp
}

// ComputeETag computes a deterministic ETag for a resource by hashing its serialized form,
// excluding update_time and etag fields to ensure the tag reflects only meaningful state changes.
func ComputeETag(m ETagResource) (string, error) {
	clone := proto.CloneOf(m)
	ref := clone.ProtoReflect()
	fields := ref.Descriptor().Fields()
	for _, name := range []string{"update_time", "etag"} {
		f := fields.ByName(protoreflect.Name(name))
		if f == nil {
			return "", fmt.Errorf("message %s missing required field %q for ETag computation", ref.Descriptor().FullName(), name)
		}
		ref.Clear(f)
	}
	bytes, err := pbutil.MarshalDeterministic(clone)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(bytes)
	return `"` + base64.StdEncoding.EncodeToString(hash[:]) + `"`, nil
}

// UpdateRequest defines the interface of an AIP update request.
type updateRequest interface {
	proto.Message
	GetUpdateMask() *fieldmaskpb.FieldMask
}

// ParsedUpdateRequest is a request that is parsed.
type ParsedUpdateRequest struct {
	fieldMask         *pbfieldmask.FieldMask
	updateColumnNames []string
}

// ApplyFieldMask merges any fields covered by the field mask from newResource into existingResource.
func (p *ParsedUpdateRequest) ApplyFieldMask(existingResource, newResource proto.Message) {
	p.fieldMask.Update(existingResource, newResource)
}

func (p *ParsedUpdateRequest) GetSQLUpsertClause() string {
	var sb strings.Builder
	for i, updateColumnName := range p.updateColumnNames {
		sb.WriteString(fmt.Sprintf("%s = EXCLUDED.%s", updateColumnName, updateColumnName))
		if i < len(p.updateColumnNames)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

func (p *ParsedUpdateRequest) GetSQLUpdateClause() string {
	var sb strings.Builder
	for i, updateColumnName := range p.updateColumnNames {
		sb.WriteString(fmt.Sprintf("%s = $%d", updateColumnName, i+1))
		if i < len(p.updateColumnNames)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

func (p *ParsedUpdateRequest) GetSQLColumns() []string {
	return p.updateColumnNames
}

// UpdateRequestParser implements update request parsing.
type UpdateRequestParser[T updateRequest, R proto.Message] struct {
	paths              []string
	protoPathToColumn  map[string]string
	mappings           map[string]string
	defaultColumnNames []string
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
	// Parse options.
	var zero T
	options, err := pbutil.GetMessageOption[*aippb.UpdateOptions](zero, aippb.E_Update)
	if err != nil {
		return nil, fmt.Errorf("getting update options: %v", err)
	}
	if options == nil {
		return nil, fmt.Errorf("%T must define UpdateOptions", zero)
	}

	// Validate the paths.
	var zeroResource R
	if len(options.GetPaths()) > 0 {
		for _, path := range options.GetPaths() {
			if _, ok := updateRequestParserInvalidPathSet[path]; ok {
				return nil, fmt.Errorf("cannot use protected path: %s", path)
			}
		}
		if err := pbfieldmask.FromPaths(options.GetPaths()...).Validate(zeroResource); err != nil {
			return nil, fmt.Errorf("validating paths: %w", err)
		}
	}

	// Build tree to get column name mappings.
	tree, err := BuildResourceTree[R](WithAllowedPaths([]string{"*"}))
	if err != nil {
		return nil, fmt.Errorf("building resource tree: %w", err)
	}

	paths := options.Paths
	protoPathToColumn := make(map[string]string)
	mappings := map[string]string{}
	defaultColumnNames := []string{}
	for _, node := range tree.Nodes {
		columnName := node.Path
		if node.ReplacementPath != "" {
			columnName = node.ReplacementPath
		}

		// We will auto-inject these fields.
		if node.Path == "update_time" || node.Path == "etag" {
			defaultColumnNames = append(defaultColumnNames, columnName)
			continue
		}

		if node.HasFieldBehavior(annotationspb.FieldBehavior_IDENTIFIER) ||
			node.HasFieldBehavior(annotationspb.FieldBehavior_IMMUTABLE) ||
			node.HasFieldBehavior(annotationspb.FieldBehavior_OUTPUT_ONLY) {
			continue
		}

		protoPathToColumn[node.Path] = columnName

		// Auto-generate mappings for fields stored as JSON or Proto bytes.
		// A path like "metadata" maps all descendants to the "metadata" column.
		if node.AsJsonBytes || node.AsProtoBytes {
			mappings[node.Path] = columnName
		}
	}

	return &UpdateRequestParser[T, R]{
		paths:              paths,
		protoPathToColumn:  protoPathToColumn,
		mappings:           mappings,
		defaultColumnNames: defaultColumnNames,
	}, nil
}

func (p *UpdateRequestParser[T, R]) Parse(request T) (*ParsedUpdateRequest, error) {
	var resource R
	fieldMask := pbfieldmask.New(request.GetUpdateMask())
	if len(fieldMask.GetPaths()) == 0 {
		return nil, fmt.Errorf("no mask paths specified")
	}

	// Validate the paths are valid.
	if err := fieldMask.Validate(resource); err != nil {
		return nil, fmt.Errorf("invalid field mask paths: %v", err)
	}

	updateColumnNameSet := map[string]struct{}{}
	var updateColumnNames []string
	for _, columnName := range p.defaultColumnNames {
		updateColumnNameSet[columnName] = struct{}{}
		updateColumnNames = append(updateColumnNames, columnName)
	}

	// Iterate over each path in the field mask.
	for _, path := range fieldMask.GetPaths() {
		if !p.isAuthorizedPath(path) {
			return nil, fmt.Errorf("unauthorized update mask path: %s", path)
		}

		// Check against configured mappings to see if there is a match.
		mappingFound := false
		for mappingFrom, mappingTo := range p.mappings {
			if p.match(mappingFrom, path) {
				columnName := p.translateToColumnName(mappingTo)
				if _, ok := updateColumnNameSet[columnName]; !ok {
					updateColumnNameSet[columnName] = struct{}{}
					updateColumnNames = append(updateColumnNames, columnName)
				}
				mappingFound = true
				break
			}
		}

		// If no mapping is found, we simply add the field (translated to column name).
		if !mappingFound {
			columnName := p.translateToColumnName(path)
			if _, ok := updateColumnNameSet[columnName]; !ok {
				updateColumnNameSet[columnName] = struct{}{}
				updateColumnNames = append(updateColumnNames, columnName)
			}
		}
	}

	return &ParsedUpdateRequest{
		fieldMask:         fieldMask,
		updateColumnNames: updateColumnNames,
	}, nil
}

// translateToColumnName translates a proto path to its database column name.
func (p *UpdateRequestParser[T, R]) translateToColumnName(path string) string {
	if columnName, ok := p.protoPathToColumn[path]; ok {
		return columnName
	}
	return path
}

// match checks if fieldPath is equal to or a descendant of mappingFrom.
func (p *UpdateRequestParser[T, R]) match(mappingFrom string, fieldPath string) bool {
	if mappingFrom == fieldPath {
		return true
	}
	return strings.HasPrefix(fieldPath, mappingFrom+".")
}

var updateRequestForbiddenPathSet = map[string]struct{}{
	"name":        {},
	"create_time": {},
	"delete_time": {},
}

// isAuthorizedPath checks if fieldPath is authorized. A configured path "foo"
// authorizes "foo" itself and any descendant like "foo.bar" or "foo.`my key`".
func (p *UpdateRequestParser[T, R]) isAuthorizedPath(fieldPath string) bool {
	if _, ok := updateRequestForbiddenPathSet[fieldPath]; ok {
		return false
	}
	// Strip backtick quoting to extract the base path for matching.
	basePath := stripBackticks(fieldPath)
	for _, path := range p.paths {
		if path == basePath || strings.HasPrefix(basePath, path+".") {
			return true
		}
	}
	return false
}

// stripBackticks removes backtick quoting from a field path, replacing
// backtick-quoted segments with their unquoted content. This allows
// "labels.`my key`" to be matched against the authorized path "labels".
func stripBackticks(fieldPath string) string {
	if !strings.Contains(fieldPath, "`") {
		return fieldPath
	}
	var sb strings.Builder
	for i := 0; i < len(fieldPath); i++ {
		if fieldPath[i] == '`' {
			end := strings.IndexByte(fieldPath[i+1:], '`')
			if end == -1 {
				sb.WriteString(fieldPath[i+1:])
				break
			}
			sb.WriteString(fieldPath[i+1 : i+1+end])
			i += end + 1
		} else {
			sb.WriteByte(fieldPath[i])
		}
	}
	return sb.String()
}
