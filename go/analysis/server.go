package analysis

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"go.einride.tech/aip/filtering"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/grpc/codes"

	analysisservicepb "github.com/malonaz/core/genproto/analysis/analysis_service/v1"
	analysispb "github.com/malonaz/core/genproto/analysis/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
)

const (
	maxResultResources = 100
	defaultPageSize    = 100
)

var resultFilterDeclarations = func() *filtering.Declarations {
	declarations, err := filtering.NewDeclarations(
		filtering.DeclareStandardFunctions(),
		filtering.DeclareIdent("resource", filtering.TypeString),
	)
	if err != nil {
		panic(err)
	}
	return declarations
}()

// Server implements AnalysisService over a registry.
type Server struct {
	analysisservicepb.UnimplementedAnalysisServiceServer
	registry *Registry
}

// NewServer implements AnalysisService over the registry.
func NewServer(registry *Registry) *Server {
	return &Server{registry: registry}
}

func (s *Server) GetAnalysis(ctx context.Context, request *analysisservicepb.GetAnalysisRequest) (*analysispb.Analysis, error) {
	_, analyses, err := s.registry.lookup([]string{request.GetName()})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err).Err()
	}
	return analyses[0].proto, nil
}

func (s *Server) ListAnalyses(ctx context.Context, request *analysisservicepb.ListAnalysesRequest) (*analysisservicepb.ListAnalysesResponse, error) {
	parent, err := analysispb.ParseResourceTypeRn(request.GetParent())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	var analyses []*analysispb.Analysis
	for _, rt := range s.registry.types() {
		service, kind, _ := strings.Cut(rt.name, "/")
		if (parent.Service != aip.Wildcard && parent.Service != service) || (parent.ResourceType != aip.Wildcard && parent.ResourceType != kind) {
			continue
		}
		for _, id := range rt.ids {
			analyses = append(analyses, rt.idToAnalysis[id].proto)
		}
	}
	offset := 0
	if request.GetPageToken() != "" {
		if offset, err = strconv.Atoi(request.GetPageToken()); err != nil || offset < 0 || offset > len(analyses) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page token").Err()
		}
	}
	pageSize := int(request.GetPageSize())
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	end := min(offset+pageSize, len(analyses))
	response := &analysisservicepb.ListAnalysesResponse{Analyses: analyses[offset:end]}
	if end < len(analyses) {
		response.NextPageToken = strconv.Itoa(end)
	}
	return response, nil
}

func (s *Server) GenerateResults(ctx context.Context, request *analysisservicepb.GenerateResultsRequest) (*analysisservicepb.GenerateResultsResponse, error) {
	rt, analyses, wildcard, err := s.registry.resolve(request.GetParent(), request.GetAnalyses())
	if err != nil {
		return nil, err
	}
	results, _, err := s.registry.run(ctx, rt, request.GetResource(), analyses, wildcard)
	if err != nil {
		if len(results) == 0 {
			return nil, status.FromError(err, "generating results").Err()
		}
		log.WarnContext(ctx, "generating some results", "resource", request.GetResource(), "error", err)
	}
	return &analysisservicepb.GenerateResultsResponse{Results: results}, nil
}

func (s *Server) ListResults(ctx context.Context, request *analysisservicepb.ListResultsRequest) (*analysisservicepb.ListResultsResponse, error) {
	rt, analyses, _, err := s.registry.resolve(request.GetParent(), nil)
	if err != nil {
		return nil, err
	}
	resources, err := parseResultFilter(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "filter: %v", err).Err()
	}
	response := &analysisservicepb.ListResultsResponse{}
	for _, resourceName := range resources {
		resource, err := rt.get(ctx, resourceName)
		if err != nil {
			return nil, status.FromError(err, "getting %s", resourceName).Err()
		}
		for _, analysis := range analyses {
			result, ok, err := analysispb.Annotations.Analysis(idOf(analysis.proto)).Get(resource.(aip.Annotatable))
			if err != nil {
				return nil, status.Errorf(codes.Internal, "%v", err).Err()
			}
			if ok {
				response.Results = append(response.Results, result)
			}
		}
	}
	return response, nil
}

// resolve returns the analyses a parent names: one, or with `-` all of the type, or those in ids.
func (r *Registry) resolve(parent string, ids []string) (*resourceType, []*registered, bool, error) {
	rn, err := analysispb.ParseAnalysisRn(parent)
	if err != nil {
		return nil, nil, false, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	rt, ok := r.typeToResource[rn.Service+"/"+rn.ResourceType]
	if !ok {
		return nil, nil, false, status.Errorf(codes.NotFound, "resource type %s/%s is not registered", rn.Service, rn.ResourceType).Err()
	}
	if rn.Analysis != aip.Wildcard {
		analysis, ok := rt.idToAnalysis[rn.Analysis]
		if !ok {
			return nil, nil, false, status.Errorf(codes.NotFound, "%s is not registered", parent).Err()
		}
		return rt, []*registered{analysis}, false, nil
	}
	if len(ids) == 0 {
		ids = rt.ids
	}
	analyses := make([]*registered, 0, len(ids))
	for _, id := range ids {
		analysis, ok := rt.idToAnalysis[id]
		if !ok {
			return nil, nil, false, status.Errorf(codes.NotFound, "analysis %q is not registered", id).Err()
		}
		analyses = append(analyses, analysis)
	}
	return rt, analyses, true, nil
}

func (r *Registry) types() []*resourceType {
	names := make([]string, 0, len(r.typeToResource))
	for name := range r.typeToResource {
		names = append(names, name)
	}
	slices.Sort(names)
	types := make([]*resourceType, len(names))
	for i, name := range names {
		types[i] = r.typeToResource[name]
	}
	return types
}

// parseResultFilter accepts only `resource = "x"` terms joined by OR.
func parseResultFilter(request *analysisservicepb.ListResultsRequest) ([]string, error) {
	filter, err := filtering.ParseFilter(request, resultFilterDeclarations)
	if err != nil {
		return nil, err
	}
	var resources []string
	var walk func(e *expr.Expr) error
	walk = func(e *expr.Expr) error {
		call := e.GetCallExpr()
		switch call.GetFunction() {
		case filtering.FunctionOr:
			for _, arg := range call.GetArgs() {
				if err := walk(arg); err != nil {
					return err
				}
			}
			return nil
		case filtering.FunctionEquals:
			if call.GetArgs()[0].GetIdentExpr().GetName() == "resource" && call.GetArgs()[1].GetConstExpr() != nil {
				resources = append(resources, call.GetArgs()[1].GetConstExpr().GetStringValue())
				return nil
			}
		}
		return fmt.Errorf("must restrict resource with `=`, alone or OR'd")
	}
	if filter.CheckedExpr == nil {
		return nil, fmt.Errorf("must restrict resource")
	}
	if err := walk(filter.CheckedExpr.GetExpr()); err != nil {
		return nil, err
	}
	if len(resources) > maxResultResources {
		return nil, fmt.Errorf("at most %d resources", maxResultResources)
	}
	return resources, nil
}
