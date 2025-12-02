package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	"github.com/malonaz/core/go/grpc"
)

var (
	socket          = flag.String("socket", "/tmp/core.socket", "Unix socket path")
	provider        = flag.String("provider", "openai", "Provider name")
	model           = flag.String("model", "gpt-4o", "Model ID")
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

	// Create connection options
	opts := &grpc.Opts{
		Host:       "localhost",
		SocketPath: *socket,
		DisableTLS: true,
	}

	// Create gRPC connection
	conn, err := grpc.NewConnection(opts, nil, nil)
	if err != nil {
		return fmt.Errorf("creating connection: %w", err)
	}

	if err := conn.Connect(ctx); err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer conn.Close()

	// Create AI service client
	client := aiservicepb.NewAiClient(conn.Get())

	// Print request info
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
		MaxTokens:   int32(*maxTokens),
		Temperature: float64(*temperature),
	}

	// Add reasoning effort if specified
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
		{
			Role:    aipb.Role_ROLE_SYSTEM,
			Content: *systemMessage,
		},
		{
			Role:    aipb.Role_ROLE_USER,
			Content: *userMessage,
		},
	}
}

func buildTools() []*aipb.Tool {
	if !*useTool {
		return nil
	}
	return []*aipb.Tool{buildWeatherTool()}
}

func runStream(ctx context.Context, client aiservicepb.AiClient) error {
	// Build the model resource name
	modelResourceName := fmt.Sprintf("providers/%s/models/%s", *provider, *model)

	// Build the configuration
	config, err := buildConfig()
	if err != nil {
		return err
	}

	// Build the request
	request := &aiservicepb.TextToTextStreamRequest{
		Model:         modelResourceName,
		Messages:      buildMessages(),
		Tools:         buildTools(),
		Configuration: config,
	}

	// Add tool choice if tools are enabled
	if *useTool {
		request.ToolChoice = "auto"
	}

	// Make the streaming API call
	stream, err := client.TextToTextStream(ctx, request)
	if err != nil {
		return fmt.Errorf("calling TextToTextStream: %w", err)
	}

	// Process the stream
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

	fmt.Println() // Final newline
	return nil
}

func runUnary(ctx context.Context, client aiservicepb.AiClient) error {
	// Build the model resource name
	modelResourceName := fmt.Sprintf("providers/%s/models/%s", *provider, *model)

	// Build the configuration
	config, err := buildConfig()
	if err != nil {
		return err
	}

	// Build the request
	request := &aiservicepb.TextToTextRequest{
		Model:         modelResourceName,
		Messages:      buildMessages(),
		Tools:         buildTools(),
		Configuration: config,
	}

	// Add tool choice if tools are enabled
	if *useTool {
		request.ToolChoice = "auto"
	}

	// Make the unary API call
	response, err := client.TextToText(ctx, request)
	if err != nil {
		return fmt.Errorf("calling TextToText: %w", err)
	}

	// Display the response
	handleUnaryResponse(response)

	fmt.Println() // Final newline
	return nil
}

func handleStreamResponse(response *aiservicepb.TextToTextStreamResponse) {
	switch content := response.Content.(type) {
	case *aiservicepb.TextToTextStreamResponse_ContentChunk:
		fmt.Printf("%s%s%s", colorCyan, content.ContentChunk, colorReset)

	case *aiservicepb.TextToTextStreamResponse_ReasoningChunk:
		fmt.Printf("%s%s%s", colorYellow, content.ReasoningChunk, colorReset)

	case *aiservicepb.TextToTextStreamResponse_ToolCall:
		fmt.Printf("\n[Tool Call: %s(%s)]\n", content.ToolCall.Name, content.ToolCall.Arguments)

	case *aiservicepb.TextToTextStreamResponse_StopReason:
		fmt.Printf("\n[Stop Reason: %s]\n", content.StopReason)

	case *aiservicepb.TextToTextStreamResponse_ModelUsage:
		printModelUsage(content.ModelUsage)

	case *aiservicepb.TextToTextStreamResponse_GenerationMetrics:
		printGenerationMetrics(content.GenerationMetrics)
	}
}

func handleUnaryResponse(response *aiservicepb.TextToTextResponse) {
	// Print the message content
	if response.Message != nil {
		if response.Message.Reasoning != "" {
			fmt.Printf("%s%s%s\n", colorYellow, response.Message.Reasoning, colorReset)
		}
		if response.Message.Content != "" {
			fmt.Printf("%s%s%s\n", colorCyan, response.Message.Content, colorReset)
		}
		if len(response.Message.ToolCalls) > 0 {
			for _, toolCall := range response.Message.ToolCalls {
				fmt.Printf("\n[Tool Call: %s(%s)]\n", toolCall.Name, toolCall.Arguments)
			}
		}
	}

	// Print stop reason
	fmt.Printf("\n[Stop Reason: %s]\n", response.StopReason)

	// Print model usage
	if response.ModelUsage != nil {
		printModelUsage(response.ModelUsage)
	}

	// Print generation metrics
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
