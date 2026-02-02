package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gordonklaus/portaudio"
	"google.golang.org/protobuf/types/known/durationpb"

	aiservicepb "github.com/malonaz/core/genproto/ai/ai_service/v1"
	audiopb "github.com/malonaz/core/genproto/audio/v1"
	"github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/pbutil"
)

var (
	socket         = flag.String("socket", "/tmp/core.socket", "Unix socket path")
	provider       = flag.String("provider", "xai", "Provider name")
	model          = flag.String("model", "stt-streaming", "Model ID")
	sampleRate     = flag.Int("sample-rate", 16000, "Audio sample rate in Hz")
	channels       = flag.Int("channels", 1, "Number of audio channels")
	chunkSize      = flag.Int("chunk-size", 1024, "Audio chunk size in frames")
	commitStrategy = flag.String("commit-stategy", "end_of_turn", "([end_of_turn, vad])")

	commitStrategyVAD = &aiservicepb.SpeechToTextStreamConfiguration_Vad{
		Vad: &aiservicepb.SpeechToTextStreamCommitStrategyVad{
			SilenceThreshold:   durationpb.New(500 * time.Millisecond),
			VadThreshold:       0.5,
			MinSpeechDuration:  durationpb.New(100 * time.Millisecond),
			MinSilenceDuration: durationpb.New(500 * time.Millisecond),
		},
	}

	commitStrategyEndOfTurn = &aiservicepb.SpeechToTextStreamConfiguration_EndOfTurn{
		EndOfTurn: &aiservicepb.SpeechToTextStreamCommitStrategyEndOfTurn{
			ConfidenceThreshold:      0.8,
			EagerConfidenceThreshold: 0.5,
			Timeout:                  durationpb.New(3 * time.Second),
		},
	}
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nInterrupted")
		cancel()
	}()

	conn, err := grpc.NewConnection(&grpc.Opts{Host: "localhost", SocketPath: *socket, DisableTLS: true}, nil, nil)
	if err != nil {
		return fmt.Errorf("creating connection: %w", err)
	}
	if err := conn.Connect(ctx); err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer conn.Close()

	client := aiservicepb.NewAiServiceClient(conn.Get())

	if err := portaudio.Initialize(); err != nil {
		return fmt.Errorf("initializing portaudio: %w", err)
	}
	defer portaudio.Terminate()

	modelName := fmt.Sprintf("providers/%s/models/%s", *provider, *model)
	stream, err := client.SpeechToTextStream(ctx)
	if err != nil {
		return fmt.Errorf("creating stream: %w", err)
	}

	config := &aiservicepb.SpeechToTextStreamRequest{
		Content: &aiservicepb.SpeechToTextStreamRequest_Configuration{
			Configuration: &aiservicepb.SpeechToTextStreamConfiguration{
				Model: modelName,
				AudioFormat: &audiopb.Format{
					SampleRate:    int32(*sampleRate),
					Channels:      int32(*channels),
					BitsPerSample: 16,
				},
			},
		},
	}
	switch *commitStrategy {
	case "end_of_turn":
		config.GetConfiguration().CommitStrategy = commitStrategyEndOfTurn
	case "vad":
		config.GetConfiguration().CommitStrategy = commitStrategyVAD
	default:
		return fmt.Errorf("unknown commit strategy: %s", *commitStrategy)
	}

	if err := stream.Send(config); err != nil {
		return fmt.Errorf("sending config: %w", err)
	}

	fmt.Printf("Model: %s | Sample rate: %d Hz | Chunk size: %d\n", modelName, *sampleRate, *chunkSize)
	fmt.Println("Speak now... (Ctrl+C to stop)")

	errCh := make(chan error, 2)

	go func() {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				errCh <- nil
				return
			}
			if err != nil {
				errCh <- fmt.Errorf("receiving: %w", err)
				return
			}
			pbutil.MustPrintPretty(resp)
		}
	}()

	buffer := make([]int16, *chunkSize)
	audioStream, err := portaudio.OpenDefaultStream(*channels, 0, float64(*sampleRate), len(buffer), buffer)
	if err != nil {
		return fmt.Errorf("opening audio stream: %w", err)
	}
	defer audioStream.Close()

	if err := audioStream.Start(); err != nil {
		return fmt.Errorf("starting audio stream: %w", err)
	}
	defer audioStream.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				stream.CloseSend()
				errCh <- nil
				return
			default:
				if err := audioStream.Read(); err != nil {
					errCh <- fmt.Errorf("reading audio: %w", err)
					return
				}
				data := make([]byte, len(buffer)*2)
				for i, sample := range buffer {
					data[i*2] = byte(sample)
					data[i*2+1] = byte(sample >> 8)
				}
				req := &aiservicepb.SpeechToTextStreamRequest{
					Content: &aiservicepb.SpeechToTextStreamRequest_AudioChunk{
						AudioChunk: &audiopb.Chunk{
							Index:    2,
							Duration: durationpb.New(time.Second),
							Data:     data,
						},
					},
				}
				if err := stream.Send(req); err != nil {
					errCh <- fmt.Errorf("sending audio: %w", err)
					return
				}
			}
		}
	}()

	return <-errCh
}
