package ai_service

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/ai/ai_service/provider"
	"github.com/malonaz/core/go/ai/ai_service/provider/anthropic"
	"github.com/malonaz/core/go/ai/ai_service/provider/cartesia"
	"github.com/malonaz/core/go/ai/ai_service/provider/elevenlabs"
	"github.com/malonaz/core/go/ai/ai_service/provider/openai"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/grpc/grpcinproc"
	"github.com/malonaz/core/go/pbutil"
)

type Opts struct {
	VoicesFile       string `long:"voices-file"     env:"VOICES_FILE" description:"Path to JSON file containing voices to preload"`
	OpenAIApiKey     string `long:"openai-api-key"     env:"OPENAI_API_KEY" description:"Open AI api key"`
	GroqApiKey       string `long:"groq-api-key"     env:"GROQ_API_KEY" description:"Groq api key"`
	ElevenlabsApiKey string `long:"elevenlabs-api-key"     env:"ELEVENLABS_API_KEY" description:"Elevenlabs api key"`
	AnthropicApiKey  string `long:"anthropic-api-key"     env:"ANTHROPIC_API_KEY" description:"Anthropic api key"`
	CartesiaApiKey   string `long:"cartesia-api-key"     env:"CARTESIA_API_KEY" description:"Cartesia api key"`
	CerebrasApiKey   string `long:"cerebras-api-key"     env:"CEREBRAS_API_KEY" description:"Cerebras api key"`
	GoogleApiKey     string `long:"google-api-key"     env:"GOOGLE_API_KEY" description:"Google api key"`
	XaiApiKey        string `long:"xai-api-key"     env:"XAI_API_KEY" description:"Xai api key"`
}

type runtime struct {
	*provider.VoiceService
	*provider.ModelService
	cartesiaClient *cartesia.Client
	providers      []provider.Provider
}

func newRuntime(opts *Opts) (*runtime, error) {
	voiceService, err := provider.NewVoiceService()
	if err != nil {
		return nil, fmt.Errorf("creating new voice service: %v", err)
	}
	modelService, err := provider.NewModelService()
	if err != nil {
		return nil, fmt.Errorf("creating new model service: %v", err)
	}

	var providers []provider.Provider
	if opts.OpenAIApiKey != "" {
		providers = append(providers, openai.NewClient(opts.OpenAIApiKey, modelService))
	}
	if opts.GoogleApiKey != "" {
		providers = append(providers, openai.NewGoogleClient(opts.GoogleApiKey, modelService))
	}
	if opts.XaiApiKey != "" {
		providers = append(providers, openai.NewXaiClient(opts.XaiApiKey, modelService))
	}
	if opts.GroqApiKey != "" {
		providers = append(providers, openai.NewGroqClient(opts.GroqApiKey, modelService))
	}
	if opts.CerebrasApiKey != "" {
		providers = append(providers, openai.NewCerebrasClient(opts.CerebrasApiKey, modelService))
	}
	if opts.ElevenlabsApiKey != "" {
		providers = append(providers, elevenlabs.NewClient(opts.ElevenlabsApiKey, modelService))
	}
	if opts.AnthropicApiKey != "" {
		providers = append(providers, anthropic.NewClient(opts.AnthropicApiKey, modelService))
	}
	if opts.CartesiaApiKey != "" {
		providers = append(providers, cartesia.NewClient(opts.CartesiaApiKey, modelService))
	}

	return &runtime{
		VoiceService: voiceService,
		ModelService: modelService,
		providers:    providers,
	}, nil
}

func (s *Service) start(ctx context.Context) (func(), error) {
	for _, provider := range s.providers {
		if err := provider.Start(ctx); err != nil {
			return nil, fmt.Errorf("starting provider %s: %v", provider.ProviderId(), err)
		}
		if err := s.RegisterProvider(ctx, provider); err != nil {
			return nil, fmt.Errorf("registering provider %s: %v", provider.ProviderId(), err)
		}
	}

	// Load voices from file if specified
	if s.opts.VoicesFile != "" {
		if err := s.loadVoicesFromFile(ctx, s.opts.VoicesFile); err != nil {
			return nil, fmt.Errorf("loading voices from file: %v", err)
		}
	}

	return func() {
		for _, provider := range s.providers {
			provider.Stop()
		}
	}, nil
}

func (s *Service) loadVoicesFromFile(ctx context.Context, filepath string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("opening voices file: %v", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("reading voices file: %v", err)
	}

	// Unmarshal slice of CreateVoiceRequest messages
	requests, err := pbutil.JSONUnmarshalSlice[pb.CreateVoiceRequest](pbutil.ProtoJsonUnmarshalStrictOptions, data)
	if err != nil {
		return fmt.Errorf("parsing voices file: %v", err)
	}

	// Create each voice
	for i, request := range requests {
		if _, err := s.CreateVoice(ctx, request); err != nil {
			return fmt.Errorf("creating voice at index %d: %v", i, err)
		}
	}

	return nil
}

// TextToTextStream implements the gRPC streaming method - direct pass-through
func (s *Service) TextToTextStream(request *pb.TextToTextStreamRequest, srv pb.Ai_TextToTextStreamServer) error {
	ctx := srv.Context()
	provider, model, err := s.GetTextToTextProvider(ctx, request.Model)
	if err != nil {
		return err
	}
	if err := checkModelDeprecation(model); err != nil {
		return grpc.Errorf(codes.FailedPrecondition, err.Error()).Err()
	}
	if request.Configuration == nil {
		request.Configuration = &pb.TextToTextConfiguration{}
	}
	if request.Configuration.MaxTokens == 0 {
		request.Configuration.MaxTokens = model.Ttt.OutputTokenLimit
	}

	// Some verification.
	if request.Configuration.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED && !model.GetTtt().GetReasoning() {
		return grpc.Errorf(codes.InvalidArgument, "%s does not support reasoning", request.Model).Err()
	}
	if len(request.Tools) > 0 && !model.GetTtt().GetToolCall() {
		return grpc.Errorf(codes.InvalidArgument, "%s does not support tool calling", request.Model).Err()
	}

	// Direct pass-through - provider implements exact gRPC interface
	return provider.TextToTextStream(request, srv)
}

// TextToText collects all streamed text chunks into a single response
func (s *Service) TextToText(ctx context.Context, request *pb.TextToTextRequest) (*pb.TextToTextResponse, error) {
	// Convert to streaming request
	streamRequest := &pb.TextToTextStreamRequest{
		Model:         request.Model,
		Messages:      request.Messages,
		Tools:         request.Tools,
		Configuration: request.Configuration,
	}

	// Create a local streaming client using grpcinproc
	serverStreamClient := grpcinproc.NewServerStreamAsClient[
		pb.TextToTextStreamRequest,
		pb.TextToTextStreamResponse,
		pb.Ai_TextToTextStreamServer,
	](s.TextToTextStream)

	stream, err := serverStreamClient(ctx, streamRequest)
	if err != nil {
		return nil, err
	}

	// Collect all chunks into a single response
	response := &pb.TextToTextResponse{
		Message: &aipb.Message{
			Role: aipb.Role_ROLE_ASSISTANT,
		},
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		switch content := event.GetContent().(type) {
		case *pb.TextToTextStreamResponse_ContentChunk:
			response.Message.Content += content.ContentChunk

		case *pb.TextToTextStreamResponse_ReasoningChunk:
			response.Message.Reasoning += content.ReasoningChunk

		case *pb.TextToTextStreamResponse_StopReason:
			response.StopReason = content.StopReason

		case *pb.TextToTextStreamResponse_ToolCall:
			response.Message.ToolCalls = append(response.Message.ToolCalls, content.ToolCall)

		case *pb.TextToTextStreamResponse_ModelUsage:
			if response.ModelUsage == nil {
				response.ModelUsage = &aipb.ModelUsage{}
			}
			proto.Merge(response.ModelUsage, content.ModelUsage)

		case *pb.TextToTextStreamResponse_GenerationMetrics:
			if response.GenerationMetrics == nil {
				response.GenerationMetrics = &aipb.GenerationMetrics{}
			}
			proto.Merge(response.GenerationMetrics, content.GenerationMetrics)
			response.GenerationMetrics.Ttfb = nil // We do not set TTFB on unary calls.
		}
	}

	// Apply JSON extraction if requested
	if request.GetConfiguration().GetExtractJsonObject() {
		jsonContent, err := ai.ExtractJSONObject(response.Message.Content)
		if err != nil {
			return nil, grpc.Errorf(codes.Internal, "extracting json object: %v", err).WithErrorInfo(
				"JSON_EXTRACTION_FAILED",
				"ai_service",
				map[string]string{"original_content": response.Message.Content},
			).Err()
		}
		response.Message.Content = jsonContent
	}

	return response, nil
}

// SpeechToText forwards the request to the appropriate registered client.
func (s *Service) SpeechToText(ctx context.Context, request *pb.SpeechToTextRequest) (*pb.SpeechToTextResponse, error) {
	provider, model, err := s.GetSpeechToTextProvider(ctx, request.Model)
	if err != nil {
		return nil, err
	}
	if err := checkModelDeprecation(model); err != nil {
		return nil, grpc.Errorf(codes.FailedPrecondition, err.Error()).Err()
	}

	return provider.SpeechToText(ctx, request)
}

// TextToSpeechStream implements the gRPC streaming method - direct pass-through
func (s *Service) TextToSpeechStream(request *pb.TextToSpeechStreamRequest, srv pb.Ai_TextToSpeechStreamServer) error {
	ctx := srv.Context()
	provider, model, err := s.GetTextToSpeechProvider(ctx, request.Model)
	if err != nil {
		return err
	}
	if err := checkModelDeprecation(model); err != nil {
		return grpc.Errorf(codes.FailedPrecondition, err.Error()).Err()
	}

	if request.Voice != "" {
		getVoiceRequest := &pb.GetVoiceRequest{Name: request.Voice}
		voice, err := s.GetVoice(ctx, getVoiceRequest)
		if err != nil {
			return err
		}
		var providerVoiceId string
		for _, modelConfig := range voice.ModelConfigs {
			if request.Model == modelConfig.Model {
				providerVoiceId = modelConfig.ProviderVoiceId
				break
			}
		}
		if providerVoiceId == "" {
			return grpc.Errorf(codes.FailedPrecondition, "%s has no configuration for %s", request.Model, request.Voice).Err()
		}
		request.ProviderVoiceId = providerVoiceId
	}

	return provider.TextToSpeechStream(request, srv)
}

// TextToSpeech collects all streamed audio chunks into a single response
func (s *Service) TextToSpeech(ctx context.Context, request *pb.TextToSpeechRequest) (*pb.TextToSpeechResponse, error) {
	// Convert to streaming request
	streamRequest := &pb.TextToSpeechStreamRequest{
		Model: request.Model,
		Text:  request.Text,
		Voice: request.Voice,
	}

	// Create a local streaming client using grpcinproc
	serverStreamClient := grpcinproc.NewServerStreamAsClient[
		pb.TextToSpeechStreamRequest,
		pb.TextToSpeechStreamResponse,
		pb.Ai_TextToSpeechStreamServer,
	](s.TextToSpeechStream)

	stream, err := serverStreamClient(ctx, streamRequest)
	if err != nil {
		return nil, err
	}

	// Collect all chunks into a single response
	var totalDuration time.Duration
	response := &pb.TextToSpeechResponse{
		AudioChunk: &audiopb.Chunk{
			Index:       1,
			CaptureTime: timestamppb.Now(),
		},
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		switch content := event.GetContent().(type) {
		case *pb.TextToSpeechStreamResponse_AudioFormat:
			response.AudioFormat = content.AudioFormat

		case *pb.TextToSpeechStreamResponse_AudioChunk:
			if response.AudioFormat == nil {
				return nil, fmt.Errorf("received audio chunk before receiving audio format")
			}
			response.AudioChunk.Data = append(response.AudioChunk.Data, content.AudioChunk.Data...)
			totalDuration += content.AudioChunk.Duration.AsDuration()

		case *pb.TextToSpeechStreamResponse_ModelUsage:
			response.ModelUsage = content.ModelUsage

		case *pb.TextToSpeechStreamResponse_GenerationMetrics:
			content.GenerationMetrics.Ttfb = nil
			response.GenerationMetrics = content.GenerationMetrics
		}
	}

	response.AudioChunk.Duration = durationpb.New(totalDuration)
	return response, nil
}
