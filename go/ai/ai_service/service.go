package ai_service

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"

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
)

type Opts struct {
	OpenAIApiKey     string `long:"openai-api-key"     env:"OPENAI_API_KEY" description:"Open AI api key"`
	GroqApiKey       string `long:"gro-api-key"     env:"GROQ_API_KEY" description:"Groq api key"`
	ElevenlabsApiKey string `long:"elevenlabs-api-key"     env:"ELEVENLABS_API_KEY" description:"Elevenlabs api key"`
	AnthropicApiKey  string `long:"anthropic-api-key"     env:"ANTHROPIC_API_KEY" description:"Anthropic api key"`
	CartesiaApiKey   string `long:"cartesia-api-key"     env:"CARTESIA_API_KEY" description:"Cartesia api key"`
}

type runtime struct {
	*provider.ModelService
	opts           *Opts
	cartesiaClient *cartesia.Client
	providers      []provider.Provider
}

func newRuntime(opts *Opts) (*runtime, error) {
	modelService, err := provider.NewModelService()
	if err != nil {
		return nil, fmt.Errorf("creating new model service: %v", err)
	}

	var providers []provider.Provider
	if opts.OpenAIApiKey != "" {
		providers = append(providers, openai.NewClient(opts.OpenAIApiKey, modelService))
	}
	if opts.GroqApiKey != "" {
		providers = append(providers, openai.NewGroqClient(opts.GroqApiKey, modelService))
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
		ModelService: modelService,
		opts:         opts,
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
	return func() {
		for _, provider := range s.providers {
			provider.Stop()
		}
	}, nil
}

// TextToTextStream implements the gRPC streaming method - direct pass-through
func (s *Service) TextToTextStream(request *pb.TextToTextStreamRequest, srv pb.Ai_TextToTextStreamServer) error {
	ctx := srv.Context()
	provider, _, err := s.GetTextToTextProvider(ctx, request.Model)
	if err != nil {
		return err
	}
	// Direct pass-through - provider implements exact gRPC interface
	return provider.TextToTextStream(request, srv)
}

// textToText forwards the request to the appropriate registered client.
func (s *Service) TextToText(ctx context.Context, request *pb.TextToTextRequest) (*pb.TextToTextResponse, error) {
	provider, model, err := s.GetTextToTextProvider(ctx, request.Model)
	if err != nil {
		return nil, err
	}

	// Some verification.
	if request.Configuration.GetReasoningEffort() != aipb.ReasoningEffort_REASONING_EFFORT_UNSPECIFIED && !model.GetTtt().GetReasoning() {
		return nil, grpc.Errorf(codes.InvalidArgument, "%s does not support reasoning", request.Model).Err()
	}
	if len(request.Tools) > 0 && !model.GetTtt().GetToolCall() {
		return nil, grpc.Errorf(codes.InvalidArgument, "%s does not support tool calling", request.Model).Err()
	}

	response, err := provider.TextToText(ctx, request)
	if err != nil {
		return nil, err
	}
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
	provider, _, err := s.GetSpeechToTextProvider(ctx, request.Model)
	if err != nil {
		return nil, err
	}
	return provider.SpeechToText(ctx, request)
}

// TextToSpeechStream implements the gRPC streaming method - direct pass-through
func (s *Service) TextToSpeechStream(request *pb.TextToSpeechStreamRequest, srv pb.Ai_TextToSpeechStreamServer) error {
	ctx := srv.Context()
	provider, _, err := s.GetTextToSpeechProvider(ctx, request.Model)
	if err != nil {
		return err
	}
	return provider.TextToSpeechStream(request, srv)
}

// TextToSpeech collects all streamed audio chunks into a single response
func (s *Service) TextToSpeech(ctx context.Context, request *pb.TextToSpeechRequest) (*pb.TextToSpeechResponse, error) {
	provider, _, err := s.GetTextToSpeechProvider(ctx, request.Model)
	if err != nil {
		return nil, err
	}

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
	](provider.TextToSpeechStream)

	stream, err := serverStreamClient(ctx, streamRequest)
	if err != nil {
		return nil, err
	}

	// Collect all chunks into a single response
	response := &pb.TextToSpeechResponse{
		AudioChunk: &audiopb.Chunk{},
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

		case *pb.TextToSpeechStreamResponse_ModelUsage:
			response.ModelUsage = content.ModelUsage

		case *pb.TextToSpeechStreamResponse_GenerationMetrics:
			content.GenerationMetrics.Ttfb = nil
			response.GenerationMetrics = content.GenerationMetrics
		}
	}

	return response, nil
}
