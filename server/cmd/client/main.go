package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc"
)

var (
	socket          = flag.String("socket", "/tmp/core.socket", "Unix socket path")
	provider        = flag.String("provider", "anthropic", "Provider name")
	model           = flag.String("model", "claude-sonnet-4.5", "Model ID")
	systemMessage   = flag.String("system", "You are a helpful assistant.", "System message")
	userMessage     = flag.String("message", "Hello, how are you?", "User message")
	maxTokens       = flag.Int("max-tokens", 10000, "Max tokens to generate")
	temperature     = flag.Float64("temperature", 1.0, "Temperature 0.0-2.0")
	reasoningEffort = flag.String("reasoning", "", "Reasoning effort: LOW, MEDIUM, HIGH")
	useTool         = flag.Bool("use-tool", false, "Enable tool calling with a sample weather tool")
	stream          = flag.Bool("stream", false, "Use streaming API")
)

const (
	colorYellow = "\033[1;33m"
	colorCyan   = "\033[1;36m"
	colorReset  = "\033[0m"
)

func main() {
	flag.Parse()

	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	opts := &grpc.Opts{
		Host:       "localhost",
		SocketPath: *socket,
		DisableTLS: true,
	}

	conn, err := grpc.NewConnection(opts, nil, nil)
	if err != nil {
		return fmt.Errorf("creating connection: %w", err)
	}

	if err := conn.Connect(ctx); err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer conn.Close()

	client := aiservicepb.NewAiClient(conn.Get())

	printRequestInfo()

	if *stream {
		return runStream(ctx, client)
	}
	return runUnary(ctx, client)
}

func printRequestInfo() {
	fmt.Println("┌─────────────────────────────────────────────────────────")
	fmt.Printf("│ Provider: %s\n", *provider)
	fmt.Printf("│ Model: %s\n", *model)
	fmt.Printf("│ Message: %s\n", *userMessage)
	if *reasoningEffort != "" {
		fmt.Printf("│ Reasoning: REASONING_EFFORT_%s\n", *reasoningEffort)
	}
	fmt.Printf("│ Stream: %v\n", *stream)
	fmt.Println("└─────────────────────────────────────────────────────────")
	fmt.Println()
}

func buildConfig() (*aiservicepb.TextToTextConfiguration, error) {
	config := &aiservicepb.TextToTextConfiguration{
		MaxTokens:              int32(*maxTokens),
		Temperature:            float64(*temperature),
		StreamPartialToolCalls: true,
	}

	if *reasoningEffort != "" {
		var effort aipb.ReasoningEffort
		switch *reasoningEffort {
		case "LOW":
			effort = aipb.ReasoningEffort_REASONING_EFFORT_LOW
		case "MEDIUM":
			effort = aipb.ReasoningEffort_REASONING_EFFORT_MEDIUM
		case "HIGH":
			effort = aipb.ReasoningEffort_REASONING_EFFORT_HIGH
		default:
			return nil, fmt.Errorf("invalid reasoning effort: %s (must be LOW, MEDIUM, or HIGH)", *reasoningEffort)
		}
		config.ReasoningEffort = effort
	}

	return config, nil
}

func buildMessages() []*aipb.Message {
	return []*aipb.Message{
		ai.NewSystemMessage(&aipb.SystemMessage{Content: *systemMessage}),
		ai.NewUserMessage(&aipb.UserMessage{Content: *userMessage}),
	}
}

func buildTools() []*aipb.Tool {
	if !*useTool {
		return nil
	}
	return []*aipb.Tool{buildWeatherTool()}
}

func runStream(ctx context.Context, client aiservicepb.AiClient) error {
	modelResourceName := fmt.Sprintf("providers/%s/models/%s", *provider, *model)

	config, err := buildConfig()
	if err != nil {
		return err
	}

	request := &aiservicepb.TextToTextStreamRequest{
		Model:         modelResourceName,
		Messages:      buildMessages(),
		Tools:         buildTools(),
		Configuration: config,
	}

	if *useTool {
		request.ToolChoice = "auto"
	}

	stream, err := client.TextToTextStream(ctx, request)
	if err != nil {
		return fmt.Errorf("calling TextToTextStream: %w", err)
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("receiving stream: %w", err)
		}

		handleStreamResponse(response)
	}

	fmt.Println()
	return nil
}

func runUnary(ctx context.Context, client aiservicepb.AiClient) error {
	modelResourceName := fmt.Sprintf("providers/%s/models/%s", *provider, *model)

	config, err := buildConfig()
	if err != nil {
		return err
	}

	request := &aiservicepb.TextToTextRequest{
		Model:         modelResourceName,
		Messages:      buildMessages(),
		Tools:         buildTools(),
		Configuration: config,
	}

	if *useTool {
		request.Configuration.ToolChoice = &aipb.ToolChoice{
			Choice: &aipb.ToolChoice_Mode{
				Mode: aipb.ToolChoiceMode_TOOL_CHOICE_MODE_AUTO,
			},
		}
	}

	response, err := client.TextToText(ctx, request)
	if err != nil {
		return fmt.Errorf("calling TextToText: %w", err)
	}

	handleUnaryResponse(response)

	fmt.Println()
	return nil
}

func handleStreamResponse(response *aiservicepb.TextToTextStreamResponse) {
	switch content := response.Content.(type) {
	case *aiservicepb.TextToTextStreamResponse_ContentChunk:
		fmt.Printf("%s%s%s", colorCyan, content.ContentChunk, colorReset)

	case *aiservicepb.TextToTextStreamResponse_ReasoningChunk:
		fmt.Printf("%s%s%s", colorYellow, content.ReasoningChunk, colorReset)

	case *aiservicepb.TextToTextStreamResponse_ToolCall:
		bytes, err := json.MarshalIndent(content.ToolCall.Arguments.AsMap(), "  ", "")
		if err != nil {
			panic(err)
		}
		fmt.Printf("\n[Tool Call: %s(%s)]\n", content.ToolCall.Name, string(bytes))

	case *aiservicepb.TextToTextStreamResponse_PartialToolCall:
		bytes, err := json.MarshalIndent(content.PartialToolCall.Arguments.AsMap(), "  ", "")
		if err != nil {
			panic(err)
		}
		fmt.Printf("\n[PartialTool Call: %s(%s)]\n", content.PartialToolCall.Name, string(bytes))

	case *aiservicepb.TextToTextStreamResponse_StopReason:
		fmt.Printf("\n[Stop Reason: %s]\n", content.StopReason)

	case *aiservicepb.TextToTextStreamResponse_ModelUsage:
		printModelUsage(content.ModelUsage)

	case *aiservicepb.TextToTextStreamResponse_GenerationMetrics:
		printGenerationMetrics(content.GenerationMetrics)
	}
}

func handleUnaryResponse(response *aiservicepb.TextToTextResponse) {
	if response.Message != nil {
		switch msg := response.Message.Message.(type) {
		case *aipb.Message_Assistant:
			if msg.Assistant.Reasoning != "" {
				fmt.Printf("%s%s%s\n", colorYellow, msg.Assistant.Reasoning, colorReset)
			}
			if msg.Assistant.Content != "" {
				fmt.Printf("%s%s%s\n", colorCyan, msg.Assistant.Content, colorReset)
			}
			if len(msg.Assistant.ToolCalls) > 0 {
				for _, toolCall := range msg.Assistant.ToolCalls {
					fmt.Printf("\n[Tool Call: %s(%s)]\n", toolCall.Name, toolCall.Arguments)
				}
			}
		default:
			fmt.Printf("Unexpected message type: %T\n", msg)
		}
	}

	fmt.Printf("\n[Stop Reason: %s]\n", response.StopReason)

	if response.ModelUsage != nil {
		printModelUsage(response.ModelUsage)
	}

	if response.GenerationMetrics != nil {
		printGenerationMetrics(response.GenerationMetrics)
	}
}

func printModelUsage(usage *aipb.ModelUsage) {
	fmt.Println("\n")
	fmt.Println("┌─────────────────────────────────────────────────────────")
	fmt.Println("│ MODEL USAGE")
	fmt.Println("├─────────────────────────────────────────────────────────")
	fmt.Printf("│ Model: %s\n", usage.Model)

	if usage.InputToken != nil {
		fmt.Printf("│ Input tokens: %d\n", usage.InputToken.Quantity)
	}
	if usage.OutputToken != nil {
		fmt.Printf("│ Output tokens: %d\n", usage.OutputToken.Quantity)
	}
	if usage.OutputReasoningToken != nil {
		fmt.Printf("│ Reasoning tokens: %d\n", usage.OutputReasoningToken.Quantity)
	}
	if usage.InputCacheReadToken != nil {
		fmt.Printf("│ Cache read tokens: %d\n", usage.InputCacheReadToken.Quantity)
	}
	if usage.InputCacheWriteToken != nil {
		fmt.Printf("│ Cache write tokens: %d\n", usage.InputCacheWriteToken.Quantity)
	}
	fmt.Println("└─────────────────────────────────────────────────────────")
}

func printGenerationMetrics(metrics *aipb.GenerationMetrics) {
	fmt.Println("┌─────────────────────────────────────────────────────────")
	fmt.Println("│ GENERATION METRICS")
	fmt.Println("├─────────────────────────────────────────────────────────")
	if metrics.Ttfb != nil {
		fmt.Printf("│ Time to first byte: %s\n", metrics.Ttfb.AsDuration())
	}
	if metrics.Ttlb != nil {
		fmt.Printf("│ Time to last byte: %s\n", metrics.Ttlb.AsDuration())
	}
	fmt.Println("└─────────────────────────────────────────────────────────")
}

func buildWeatherTool() *aipb.Tool {
	return &aipb.Tool{
		Name:        "get_weather",
		Description: "Get the current weather for a location",
		JsonSchema: &aipb.JsonSchema{
			Type: "object",
			Properties: map[string]*aipb.JsonSchema{
				"location": {
					Type:        "string",
					Description: "The city and state, e.g. San Francisco, CA",
				},
				"unit": {
					Type:        "string",
					Description: "Temperature unit",
					Enum:        []string{"celsius", "fahrenheit"},
				},
			},
			Required: []string{"location"},
		},
	}
}
