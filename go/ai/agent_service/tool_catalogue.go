package agent_service

import (
	"cmp"
	"context"
	"maps"
	"slices"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	agentservicepb "github.com/malonaz/core/genproto/ai/agent/agent_service/v1"
	agentpb "github.com/malonaz/core/genproto/ai/agent/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc/status"
	"github.com/malonaz/core/go/pbutil/pbreflection"
)

var (
	listToolSetsRequestParser = aip.MustNewPaginationRequestParser[*agentservicepb.ListToolSetsRequest]()
	listToolsRequestParser    = aip.MustNewPaginationRequestParser[*agentservicepb.ListToolsRequest]()
)

// GetToolSet returns a configured tool set, read now: one that cannot be read carries its error.
func (s *Service) GetToolSet(ctx context.Context, request *agentservicepb.GetToolSetRequest) (*agentpb.ToolSet, error) {
	toolSetRn, err := agentpb.ParseToolSetRn(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing tool set name: %v", err).Err()
	}
	if !slices.Contains(s.toolSetIDs(), toolSetRn.ToolSet) {
		return nil, status.Errorf(codes.NotFound, "tool set %s is not configured", request.Name).Err()
	}
	toolSet, _ := s.readToolSet(ctx, toolSetRn.ToolSet)
	return toolSet, nil
}

// ListToolSets lists the configured tool sets by name, each read now: one that cannot be read carries its error,
// never failing the list.
func (s *Service) ListToolSets(ctx context.Context, request *agentservicepb.ListToolSetsRequest) (*agentservicepb.ListToolSetsResponse, error) {
	parsedRequest, err := listToolSetsRequestParser.Parse(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err).Err()
	}
	ids, nextPageToken := page(s.toolSetIDs(), int(parsedRequest.GetOffset()), int(request.GetPageSize()), parsedRequest.GetNextPageToken)
	toolSets := make([]*agentpb.ToolSet, len(ids))
	var group errgroup.Group
	for i, id := range ids {
		group.Go(func() error {
			toolSets[i], _ = s.readToolSet(ctx, id)
			return nil
		})
	}
	group.Wait()
	return &agentservicepb.ListToolSetsResponse{ToolSets: toolSets, NextPageToken: nextPageToken}, nil
}

// GetTool returns a tool of a configured tool set, read now.
func (s *Service) GetTool(ctx context.Context, request *agentservicepb.GetToolRequest) (*agentpb.Tool, error) {
	toolRn, err := agentpb.ParseToolRn(request.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing tool name: %v", err).Err()
	}
	tools, err := s.readTools(ctx, toolRn.ToolSet)
	if err != nil {
		return nil, err
	}
	index := slices.IndexFunc(tools, func(tool *agentpb.Tool) bool { return tool.Name == request.Name })
	if index < 0 {
		return nil, status.Errorf(codes.NotFound, "tool %s does not exist", request.Name).Err()
	}
	return tools[index], nil
}

// ListTools lists the tools of a configured tool set, or of every set with the parent toolSets/-, by name. A set that
// cannot be read fails the list of its own tools and is left out of the list across sets.
func (s *Service) ListTools(ctx context.Context, request *agentservicepb.ListToolsRequest) (*agentservicepb.ListToolsResponse, error) {
	parsedRequest, err := listToolsRequestParser.Parse(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err).Err()
	}
	toolSetRn, err := agentpb.ParseToolSetRn(request.Parent)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parsing parent: %v", err).Err()
	}
	var tools []*agentpb.Tool
	if toolSetRn.ContainsWildcard() {
		ids := s.toolSetIDs()
		idToTools := make([][]*agentpb.Tool, len(ids))
		var group errgroup.Group
		for i, id := range ids {
			group.Go(func() error {
				idToTools[i], _ = s.readTools(ctx, id)
				return nil
			})
		}
		group.Wait()
		tools = slices.Concat(idToTools...)
	} else if tools, err = s.readTools(ctx, toolSetRn.ToolSet); err != nil {
		return nil, err
	}
	slices.SortFunc(tools, func(tool, other *agentpb.Tool) int { return cmp.Compare(tool.Name, other.Name) })
	tools, nextPageToken := page(tools, int(parsedRequest.GetOffset()), int(request.GetPageSize()), parsedRequest.GetNextPageToken)
	return &agentservicepb.ListToolsResponse{Tools: tools, NextPageToken: nextPageToken}, nil
}

// page returns a page of items from an offset, and the token of the next page.
func page[T any](items []T, offset, pageSize int, nextPageToken func(int) string) ([]T, string) {
	offset = min(offset, len(items))
	// One more than the page, which tells whether another follows.
	items = items[offset:min(offset+pageSize+1, len(items))]
	token := nextPageToken(len(items))
	return items[:min(len(items), pageSize)], token
}

// toolSetIDs returns the ids of the configured tool sets, sorted: the routed services and the genui components.
func (s *Service) toolSetIDs() []string {
	ids := append(slices.Collect(maps.Keys(s.toolRouter.serviceToEndpoint)), genuiToolSet)
	slices.Sort(ids)
	return ids
}

// readTools returns the tools of a configured tool set, read now, or why they cannot be.
func (s *Service) readTools(ctx context.Context, id string) ([]*agentpb.Tool, error) {
	if !slices.Contains(s.toolSetIDs(), id) {
		return nil, status.Errorf(codes.NotFound, "tool set %s is not configured", (&agentpb.ToolSetRn{ToolSet: id}).String()).Err()
	}
	toolSet, tools := s.readToolSet(ctx, id)
	if toolSet.Error != nil {
		return nil, grpcstatus.FromProto(toolSet.Error).Err()
	}
	return tools, nil
}

// readToolSet reads a tool set and its tools: a service from the reflection its target serves, as the agent service,
// the same for every caller. A set that cannot be read carries its error and has no tools.
func (s *Service) readToolSet(ctx context.Context, id string) (*agentpb.ToolSet, []*agentpb.Tool) {
	toolSetRn := &agentpb.ToolSetRn{ToolSet: id}
	toolSet := &agentpb.ToolSet{Name: toolSetRn.String()}
	var tools []*agentpb.Tool
	if id == genuiToolSet {
		for _, genuiComponent := range s.genuiComponents.components {
			tools = append(tools, &agentpb.Tool{Name: genuiComponent.resourceName, ReadOnly: true, ResolvedByClient: genuiComponent.input})
		}
		return toolSet, tools
	}
	service, schema, err := s.toolRouter.service(s.asService(ctx), id)
	if err != nil {
		toolSet.Error = grpcstatus.Convert(err).Proto()
		return toolSet, nil
	}
	toolSet.Description = schema.GetComment(service.FullName(), pbreflection.CommentStyleMultiline)
	for i := 0; i < service.Methods().Len(); i++ {
		method, ok := unaryMethod(service, string(service.Methods().Get(i).Name()))
		if !ok {
			continue
		}
		tools = append(tools, &agentpb.Tool{
			Name:        toolSetRn.ToolRn(string(method.Name())).String(),
			Description: schema.GetComment(method.FullName(), pbreflection.CommentStyleMultiline),
			ReadOnly:    readOnly(method),
		})
	}
	return toolSet, tools
}
