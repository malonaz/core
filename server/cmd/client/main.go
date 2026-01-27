package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	aipb "github.com/malonaz/core/genproto/ai/v1"
	jsonpb "github.com/malonaz/core/genproto/json/v1"
	"github.com/malonaz/core/go/ai"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/pbutil"
)

var (
	socket          = flag.String("socket", "/tmp/core.socket", "Unix socket path")
	model           = flag.String("model", "providers/anthropic/models/claude-sonnet-4-20250514", "Model resource name")
	systemMessage   = flag.String("system", "You are a helpful assistant.", "System message")
	userMessage     = flag.String("message", "", "User message (empty for interactive mode)")
	maxTokens       = flag.Int("max-tokens", 10000, "Max tokens to generate")
	temperature     = flag.Float64("temperature", 1.0, "Temperature 0.0-2.0")
	reasoningEffort = flag.String("reasoning", "", "Reasoning effort: LOW, MEDIUM, HIGH")
	useTool         = flag.Bool("use-tool", false, "Enable tool calling with a sample weather tool")
	stream          = flag.Bool("stream", true, "Use streaming API")
	imagePath       = flag.String("image", "", "Path to an image file to include in the message")
	imageURL        = flag.String("image-url", "", "URL of an image to include in the message")
	generateImage   = flag.Bool("generate-image", false, "Enable image generation (requires compatible model)")
	imageAspect     = flag.String("image-aspect", "1:1", "Aspect ratio for generated images")
	imageSize       = flag.String("image-size", "", "Image size: 1K, 2K, 4K (Gemini 3 Pro Image only)")
	imageOutput     = flag.String("image-output", "generated.png", "Output path for generated images")
)

const (
	colorYellow = "\033[1;33m"
	colorCyan   = "\033[1;36m"
	colorGreen  = "\033[1;32m"
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

	client := aiservicepb.NewAiServiceClient(conn.Get())

	printRequestInfo()

	if *userMessage != "" {
		if *stream {
			return runStream(ctx, client)
		}
		return runUnary(ctx, client)
	}

	return runInteractive(ctx, client)
}

func runInteractive(ctx context.Context, client aiservicepb.AiServiceClient) error {
	messages := []*aipb.Message{
		ai.NewSystemMessage(ai.NewTextBlock(*systemMessage)),
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("\n%sEntering multi-turn mode. Type 'exit' to quit.%s\n\n", colorGreen, colorReset)

	imageIndex := 0
	for {
		fmt.Printf("%sYou: %s", colorGreen, colorReset)
		os.Stdout.Sync()

		input, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("reading input: %w", err)
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}
		if input == "exit" || input == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		messages = append(messages, ai.NewUserMessage(ai.NewTextBlock(input)))

		fmt.Printf("%sAssistant: %s", colorCyan, colorReset)
		os.Stdout.Sync()

		assistantMsg, err := sendMessage(ctx, client, messages, &imageIndex)
		if err != nil {
			fmt.Printf("\n%sError: %v%s\n", "\033[1;31m", err, colorReset)
			messages = messages[:len(messages)-1]
			continue
		}

		messages = append(messages, assistantMsg)
		fmt.Println()
	}

	return nil
}

func sendMessage(ctx context.Context, client aiservicepb.AiServiceClient, messages []*aipb.Message, imageIndex *int) (*aipb.Message, error) {
	config, err := buildConfig()
	if err != nil {
		return nil, err
	}

	request := &aiservicepb.TextToTextStreamRequest{
		Model:         *model,
		Messages:      messages,
		Tools:         buildTools(),
		Configuration: config,
	}

	stream, err := client.TextToTextStream(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("calling TextToTextStream: %w", err)
	}

	var blocks []*aipb.Block
	blockContents := make(map[int64]*blockAccumulator)

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("receiving stream: %w", err)
		}

		switch c := response.Content.(type) {
		case *aiservicepb.TextToTextStreamResponse_Block:
			acc := getOrCreateAccumulator(blockContents, c.Block.Index)
			acc.merge(c.Block)

			if c.Block.GetText() != "" {
				fmt.Printf("%s", c.Block.GetText())
				os.Stdout.Sync()
			}
			if c.Block.GetThought() != "" {
				fmt.Printf("%s%s%s", colorYellow, c.Block.GetThought(), colorReset)
				os.Stdout.Sync()
			}
			if c.Block.GetToolCall() != nil {
				fmt.Printf("\n[Tool Call: %s]\n", c.Block.GetToolCall().Name)
			}
			if c.Block.GetImage() != nil {
				saveGeneratedImage(c.Block.GetImage(), imageIndex)
			}

		case *aiservicepb.TextToTextStreamResponse_StopReason:
			fmt.Printf("\n[Stop: %s]", c.StopReason)

		case *aiservicepb.TextToTextStreamResponse_ModelUsage:
			printModelUsageCompact(c.ModelUsage)

		case *aiservicepb.TextToTextStreamResponse_GenerationMetrics:
			printGenerationMetricsCompact(c.GenerationMetrics)
		}
	}

	for i := int64(0); ; i++ {
		acc, ok := blockContents[i]
		if !ok {
			break
		}
		blocks = append(blocks, acc.toBlock())
	}

	return ai.NewAssistantMessage(blocks...), nil
}

type blockAccumulator struct {
	index     int64
	signature string
	text      strings.Builder
	thought   strings.Builder
	toolCall  *aipb.ToolCall
	image     *aipb.Image
}

func getOrCreateAccumulator(m map[int64]*blockAccumulator, index int64) *blockAccumulator {
	if acc, ok := m[index]; ok {
		return acc
	}
	acc := &blockAccumulator{index: index}
	m[index] = acc
	return acc
}

func (a *blockAccumulator) merge(block *aipb.Block) {
	if block.Signature != "" {
		a.signature = block.Signature
	}
	if block.GetText() != "" {
		a.text.WriteString(block.GetText())
	}
	if block.GetThought() != "" {
		a.thought.WriteString(block.GetThought())
	}
	if block.GetToolCall() != nil {
		a.toolCall = block.GetToolCall()
	}
	if block.GetImage() != nil {
		a.image = block.GetImage()
	}
}

func (a *blockAccumulator) toBlock() *aipb.Block {
	block := &aipb.Block{
		Index:     a.index,
		Signature: a.signature,
	}
	if a.text.Len() > 0 {
		block.Content = &aipb.Block_Text{Text: a.text.String()}
	} else if a.thought.Len() > 0 {
		block.Content = &aipb.Block_Thought{Thought: a.thought.String()}
	} else if a.toolCall != nil {
		block.Content = &aipb.Block_ToolCall{ToolCall: a.toolCall}
	} else if a.image != nil {
		block.Content = &aipb.Block_Image{Image: a.image}
	}
	return block
}

func printModelUsageCompact(usage *aipb.ModelUsage) {
	var parts []string
	if usage.InputToken != nil {
		parts = append(parts, fmt.Sprintf("in:%d", usage.InputToken.Quantity))
	}
	if usage.OutputToken != nil {
		parts = append(parts, fmt.Sprintf("out:%d", usage.OutputToken.Quantity))
	}
	if usage.OutputReasoningToken != nil {
		parts = append(parts, fmt.Sprintf("reasoning:%d", usage.OutputReasoningToken.Quantity))
	}
	if len(parts) > 0 {
		fmt.Printf(" [tokens: %s]", strings.Join(parts, ", "))
	}
}

func printGenerationMetricsCompact(metrics *aipb.GenerationMetrics) {
	var parts []string
	if metrics.Ttfb != nil {
		parts = append(parts, fmt.Sprintf("ttfb:%s", metrics.Ttfb.AsDuration()))
	}
	if metrics.Ttlb != nil {
		parts = append(parts, fmt.Sprintf("ttlb:%s", metrics.Ttlb.AsDuration()))
	}
	if len(parts) > 0 {
		fmt.Printf(" [%s]", strings.Join(parts, ", "))
	}
}

func printRequestInfo() {
	fmt.Println("┌─────────────────────────────────────────────────────────")
	fmt.Printf("│ Model: %s\n", *model)
	if *userMessage != "" {
		fmt.Printf("│ Message: %s\n", *userMessage)
	} else {
		fmt.Printf("│ Mode: Interactive (multi-turn)\n")
	}
	if *reasoningEffort != "" {
		fmt.Printf("│ Reasoning: REASONING_EFFORT_%s\n", *reasoningEffort)
	}
	if *imagePath != "" {
		fmt.Printf("│ Image: %s\n", *imagePath)
	}
	if *imageURL != "" {
		fmt.Printf("│ Image URL: %s\n", *imageURL)
	}
	if *generateImage {
		fmt.Printf("│ Generate Image: true (aspect=%s, size=%s, output=%s)\n", *imageAspect, *imageSize, *imageOutput)
	}
	fmt.Printf("│ Stream: %v\n", *stream)
	fmt.Println("└─────────────────────────────────────────────────────────")
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

	if *generateImage {
		config.ImageConfig = &aiservicepb.ImageGenerationConfig{
			AspectRatio: *imageAspect,
			ImageSize:   *imageSize,
		}
	}

	if *useTool {
		config.ToolChoice = &aipb.ToolChoice{
			Choice: &aipb.ToolChoice_Mode{
				Mode: aipb.ToolChoiceMode_TOOL_CHOICE_MODE_AUTO,
			},
		}
	}

	return config, nil
}

func buildMessages() ([]*aipb.Message, error) {
	var blocks []*aipb.Block
	blocks = append(blocks, ai.NewTextBlock(*userMessage))

	if *imagePath != "" {
		imageBlock, err := buildImageBlockFromFile(*imagePath)
		if err != nil {
			return nil, fmt.Errorf("building image block from file: %w", err)
		}
		blocks = append(blocks, imageBlock)
	}

	if *imageURL != "" {
		blocks = append(blocks, ai.NewImageBlock(ai.NewImageFromURL(*imageURL)))
	}

	return []*aipb.Message{
		ai.NewSystemMessage(ai.NewTextBlock(*systemMessage)),
		ai.NewUserMessage(blocks...),
	}, nil
}

func buildImageBlockFromFile(path string) (*aipb.Block, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading image file: %w", err)
	}

	mediaType := detectMediaType(path)

	return ai.NewImageBlock(ai.NewImageFromData(data, mediaType)), nil
}

func detectMediaType(path string) string {
	switch {
	case strings.HasSuffix(path, ".png"):
		return "image/png"
	case strings.HasSuffix(path, ".gif"):
		return "image/gif"
	case strings.HasSuffix(path, ".webp"):
		return "image/webp"
	default:
		return "image/jpeg"
	}
}

func buildTools() []*aipb.Tool {
	if !*useTool {
		return nil
	}
	return []*aipb.Tool{buildWeatherTool()}
}

func runStream(ctx context.Context, client aiservicepb.AiServiceClient) error {
	config, err := buildConfig()
	if err != nil {
		return err
	}

	messages, err := buildMessages()
	if err != nil {
		return err
	}

	request := &aiservicepb.TextToTextStreamRequest{
		Model:         *model,
		Messages:      messages,
		Tools:         buildTools(),
		Configuration: config,
	}

	stream, err := client.TextToTextStream(ctx, request)
	if err != nil {
		return fmt.Errorf("calling TextToTextStream: %w", err)
	}

	imageIndex := 0
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("receiving stream: %w", err)
		}

		handleStreamResponse(response, &imageIndex)
	}

	fmt.Println()
	return nil
}

func runUnary(ctx context.Context, client aiservicepb.AiServiceClient) error {
	config, err := buildConfig()
	if err != nil {
		return err
	}

	messages, err := buildMessages()
	if err != nil {
		return err
	}

	request := &aiservicepb.TextToTextRequest{
		Model:         *model,
		Messages:      messages,
		Tools:         buildTools(),
		Configuration: config,
	}

	response, err := client.TextToText(ctx, request)
	if err != nil {
		return fmt.Errorf("calling TextToText: %w", err)
	}

	handleUnaryResponse(response)

	fmt.Println()
	return nil
}

func handleStreamResponse(response *aiservicepb.TextToTextStreamResponse, imageIndex *int) {
	switch content := response.Content.(type) {
	case *aiservicepb.TextToTextStreamResponse_Block:
		block := content.Block
		if block.GetText() != "" {
			fmt.Printf("%s%s%s", colorCyan, block.GetText(), colorReset)
		}
		if block.GetThought() != "" {
			fmt.Printf("%s%s%s", colorYellow, block.GetThought(), colorReset)
		}
		if block.GetToolCall() != nil {
			fmt.Println("Tool Call:")
			pbutil.MustPrintPretty(block.GetToolCall())
		}
		if block.GetPartialToolCall() != nil {
			fmt.Println("Partial Tool Call:")
			pbutil.MustPrintPretty(block.GetPartialToolCall())
		}
		if block.GetImage() != nil {
			saveGeneratedImage(block.GetImage(), imageIndex)
		}

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
		for _, block := range response.Message.Blocks {
			if block.GetThought() != "" {
				fmt.Printf("%s%s%s\n", colorYellow, block.GetThought(), colorReset)
			}
			if block.GetText() != "" {
				fmt.Printf("%s%s%s\n", colorCyan, block.GetText(), colorReset)
			}
			if block.GetToolCall() != nil {
				fmt.Printf("\n[Tool Call: %s(%v)]\n", block.GetToolCall().Name, block.GetToolCall().Arguments)
			}
			if block.GetImage() != nil {
				imageIndex := 0
				saveGeneratedImage(block.GetImage(), &imageIndex)
			}
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

func saveGeneratedImage(img *aipb.Image, index *int) {
	if img == nil {
		return
	}

	var data []byte
	switch src := img.Source.(type) {
	case *aipb.Image_Data:
		data = src.Data
	default:
		fmt.Printf("[Image received but not raw data, skipping save]\n")
		return
	}

	outputPath := *imageOutput
	if *index > 0 {
		ext := ".png"
		base := outputPath
		if strings.HasSuffix(outputPath, ".png") {
			base = outputPath[:len(outputPath)-4]
		} else if strings.HasSuffix(outputPath, ".jpg") {
			ext = ".jpg"
			base = outputPath[:len(outputPath)-4]
		}
		outputPath = fmt.Sprintf("%s_%d%s", base, *index, ext)
	}

	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		fmt.Printf("[Error saving image: %v]\n", err)
		return
	}

	fmt.Printf("\n[Image saved: %s (%d bytes, %s)]\n", outputPath, len(data), img.MediaType)
	*index++
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
		JsonSchema: &jsonpb.Schema{
			Type: "object",
			Properties: map[string]*jsonpb.Schema{
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
