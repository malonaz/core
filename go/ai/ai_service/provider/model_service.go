package provider

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"slices"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/aip"
	"github.com/malonaz/core/go/grpc"
)

const (
	Cartesia   = "cartesia"
	Elevenlabs = "elevenlabs"
	Anthropic  = "anthropic"
	Openai     = "openai"
	Groq       = "groq"
	Cerebras   = "cerebras"
	Google     = "google"
	Xai        = "xai"
)

var (
	//go:embed configs/*.json
	configsFS embed.FS
)

// Implements the model service.
type ModelService struct {
	// Validator.
	validator protovalidate.Validator
	// Tracks registered providers.
	providerIdToProvider map[string]Provider
	// Tracks registered models.
	providerIdToModelIdToModel map[string]map[string]*aipb.Model
}

func NewModelService() (*ModelService, error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, err
	}
	return &ModelService{
		validator:                  validator,
		providerIdToProvider:       map[string]Provider{},
		providerIdToModelIdToModel: map[string]map[string]*aipb.Model{},
	}, nil
}

func (s *ModelService) RegisterProvider(ctx context.Context, provider Provider) error {
	if _, ok := s.providerIdToProvider[provider.ProviderId()]; ok {
		return fmt.Errorf("duplicate provider %s", provider.ProviderId())
	}
	s.providerIdToProvider[provider.ProviderId()] = provider

	configPath := fmt.Sprintf("configs/%s.json", provider.ProviderId())
	configBytes, err := configsFS.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("parsing config %s: %w", configPath, err)
	}
	config, err := parseModels(configBytes)
	if err != nil {
		return fmt.Errorf("parsing config for %s: %v", provider.ProviderId(), err)
	}
	for _, model := range config.Models {
		modelRn := &aipb.ModelResourceName{}
		if err := modelRn.UnmarshalString(model.Name); err != nil {
			return fmt.Errorf("unmarshaling model name: %v", err)
		}
		model.Name = ""
		createModelRequest := &aiservicepb.CreateModelRequest{
			Parent:  modelRn.ProviderResourceName().String(),
			ModelId: modelRn.Model,
			Model:   model,
		}
		if _, err := s.CreateModel(ctx, createModelRequest); err != nil {
			return fmt.Errorf("creating model %s: %v", model.Name, err)
		}
	}
	slog.InfoContext(ctx, "completed registration", "provider", provider.ProviderId())
	return nil
}

func (s *ModelService) CreateModel(ctx context.Context, request *aiservicepb.CreateModelRequest) (*aipb.Model, error) {
	// Parse provider rn and instantiate model rn.
	providerRn := &aipb.ProviderResourceName{}
	if err := providerRn.UnmarshalString(request.Parent); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "unmarshaling provider name: %v", err).Err()
	}
	modelRn := providerRn.ModelResourceName(request.ModelId)
	request.Model.Name = modelRn.String()
	if err := s.validator.Validate(request); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "validating: %v", err).Err()
	}

	// Check provider is registered.
	if _, ok := s.providerIdToProvider[providerRn.Provider]; !ok {
		return nil, grpc.Errorf(codes.FailedPrecondition, "provider %s is not registered", providerRn.Provider).Err()
	}

	// Store model.
	modelIdToModel, ok := s.providerIdToModelIdToModel[modelRn.Provider]
	if !ok {
		modelIdToModel = map[string]*aipb.Model{}
		s.providerIdToModelIdToModel[modelRn.Provider] = modelIdToModel
	}
	modelIdToModel[modelRn.Model] = request.Model

	// Return it.
	return request.Model, nil
}

func (s *ModelService) GetModel(ctx context.Context, request *aiservicepb.GetModelRequest) (*aipb.Model, error) {
	modelRn := &aipb.ModelResourceName{}
	if err := modelRn.UnmarshalString(request.Name); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "unmarshaling model name: %v", err).Err()
	}
	modelIdToModel, ok := s.providerIdToModelIdToModel[modelRn.Provider]
	if !ok {
		return nil, grpc.Errorf(codes.NotFound, "unknown provider %s", modelRn.Provider).Err()
	}
	model, ok := modelIdToModel[modelRn.Model]
	if !ok {
		return nil, grpc.Errorf(codes.NotFound, "unknown model %s for provider %s", modelRn.Model, modelRn.Provider).Err()
	}
	return proto.Clone(model).(*aipb.Model), nil
}

var listModelsRequestParser = aip.MustNewPaginationRequestParser[*aiservicepb.ListModelsRequest]()

func (s *ModelService) ListModels(ctx context.Context, request *aiservicepb.ListModelsRequest) (*aiservicepb.ListModelsResponse, error) {
	// Step 1: Parse the pagination request.
	parsed, err := listModelsRequestParser.Parse(request)
	if err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, err.Error()).Err()
	}
	offset := int(parsed.GetOffset())
	pageSize := int(request.GetPageSize())

	// Step 2: Parse the provider name.
	providerRn := &aipb.ProviderResourceName{}
	if err := providerRn.UnmarshalString(request.Parent); err != nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "unmarshaling provider name: %v", err).Err()
	}

	// Step 3: Determine which models to paginate over.
	var modelsToPage []*aipb.Model
	for providerId, modelIdToModel := range s.providerIdToModelIdToModel {
		if providerRn.ContainsWildcard() || providerRn.Provider == providerId {
			for _, model := range modelIdToModel {
				modelsToPage = append(modelsToPage, proto.Clone(model).(*aipb.Model))
			}
		}
	}

	// Step 4: Sort models by name.
	slices.SortFunc(modelsToPage, func(a, b *aipb.Model) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})

	// Step 5: Apply pagination.
	response := &aiservicepb.ListModelsResponse{}

	// Handle offset beyond available items.
	if offset >= len(modelsToPage) {
		return response, nil
	}

	// Calculate end index - fetch one extra item to determine if there's a next page.
	end := offset + pageSize + 1
	if end > len(modelsToPage) {
		end = len(modelsToPage)
	}

	// Slice the models (including potential extra item).
	fetchedModels := modelsToPage[offset:end]
	numFetched := len(fetchedModels)

	// Return only pageSize items (not the extra one).
	if numFetched > pageSize {
		response.Models = fetchedModels[:pageSize]
	} else {
		response.Models = fetchedModels
	}

	// Pass the number of items actually fetched (including extra) to determine next page token.
	response.NextPageToken = parsed.GetNextPageToken(numFetched)

	return response, nil
}

func (s *ModelService) GetTextToTextProvider(ctx context.Context, modelName string) (TextToTextClient, *aipb.Model, error) {
	// Get the model.
	getModelRequest := &aiservicepb.GetModelRequest{Name: modelName}
	model, err := s.GetModel(ctx, getModelRequest)
	if err != nil {
		return nil, nil, err
	}
	if model.Ttt == nil {
		return nil, nil, grpc.Errorf(codes.InvalidArgument, "model %s is not of type TTT", modelName).Err()
	}

	// Parse the model name.
	modelRn := &aipb.ModelResourceName{}
	if err := modelRn.UnmarshalString(modelName); err != nil {
		return nil, nil, grpc.Errorf(codes.InvalidArgument, "unmarshaling model name: %v", err).Err()
	}

	// Get the provider.
	provider, ok := s.providerIdToProvider[modelRn.Provider]
	if !ok {
		return nil, nil, grpc.Errorf(codes.FailedPrecondition, "provider %s is not registered", modelRn.Provider).Err()
	}

	// Verify provider implements TTT interface.
	textToTextClient, ok := provider.(TextToTextClient)
	if !ok {
		return nil, nil, grpc.Errorf(codes.InvalidArgument, "provider %s does not support text to text", provider.ProviderId()).Err()
	}
	return textToTextClient, model, nil
}

func (s *ModelService) GetSpeechToTextProvider(ctx context.Context, modelName string) (SpeechToTextClient, *aipb.Model, error) {
	// Get the model.
	getModelRequest := &aiservicepb.GetModelRequest{Name: modelName}
	model, err := s.GetModel(ctx, getModelRequest)
	if err != nil {
		return nil, nil, err
	}
	if model.Stt == nil {
		return nil, nil, grpc.Errorf(codes.InvalidArgument, "model %s is not of type STT", modelName).Err()
	}

	// Parse the model name.
	modelRn := &aipb.ModelResourceName{}
	if err := modelRn.UnmarshalString(modelName); err != nil {
		return nil, nil, grpc.Errorf(codes.InvalidArgument, "unmarshaling model name: %v", err).Err()
	}

	// Get the provider.
	provider, ok := s.providerIdToProvider[modelRn.Provider]
	if !ok {
		return nil, nil, grpc.Errorf(codes.FailedPrecondition, "provider %s is not registered", modelRn.Provider).Err()
	}

	// Verify provider implements STT interface.
	speechToTextClient, ok := provider.(SpeechToTextClient)
	if !ok {
		return nil, nil, grpc.Errorf(codes.InvalidArgument, "provider %s does not support speech to text", provider.ProviderId()).Err()
	}
	return speechToTextClient, model, nil
}

func (s *ModelService) GetTextToSpeechProvider(ctx context.Context, modelName string) (TextToSpeechClient, *aipb.Model, error) {
	// Get the model.
	getModelRequest := &aiservicepb.GetModelRequest{Name: modelName}
	model, err := s.GetModel(ctx, getModelRequest)
	if err != nil {
		return nil, nil, err
	}
	if model.Tts == nil {
		return nil, nil, grpc.Errorf(codes.InvalidArgument, "model %s is not of type TTS", modelName).Err()
	}

	// Parse the model name.
	modelRn := &aipb.ModelResourceName{}
	if err := modelRn.UnmarshalString(modelName); err != nil {
		return nil, nil, grpc.Errorf(codes.InvalidArgument, "unmarshaling model name: %v", err).Err()
	}

	// Get the provider.
	provider, ok := s.providerIdToProvider[modelRn.Provider]
	if !ok {
		return nil, nil, grpc.Errorf(codes.FailedPrecondition, "provider %s is not registered", modelRn.Provider).Err()
	}

	// Verify provider implements TTS interface.
	textToSpeechClient, ok := provider.(TextToSpeechClient)
	if !ok {
		return nil, nil, grpc.Errorf(codes.InvalidArgument, "provider %s does not support text to speech", provider.ProviderId()).Err()
	}
	return textToSpeechClient, model, nil
}
