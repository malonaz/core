package mockserver

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/malonaz/core/go/certs"
	commongrpc "github.com/malonaz/core/go/grpc"
	"github.com/malonaz/core/go/logging"
	"github.com/malonaz/core/go/prometheus"
)

var logger = logging.NewLogger()

// Server mocks a gRPC server.
type Server struct {
	methodToHandler map[string]Handler
}

func (s *Server) getHandler(service, method string) (Handler, bool) {
	handler, ok := s.methodToHandler[service+"/"+method]
	return handler, ok
}

// NewServer returns a new server.
func NewServer(port string, opts *commongrpc.Opts, certsOpts *certs.Opts) *Server {
	server := &Server{
		methodToHandler: map[string]Handler{},
	}
	register := func(*commongrpc.Server) {}
	grpcServer, err := commongrpc.NewServer(opts, certsOpts, &prometheus.Opts{}, register)
	if err != nil {
		panic(err)
	}
	grpcServer.WithOptions(
		grpc.CustomCodec(Codec{}), grpc.UnknownServiceHandler(server.handleRPC),
	)
	go grpcServer.Serve(context.Background())
	return server
}

func (s *Server) handleRPC(_ any, stream grpc.ServerStream) error {
	fullMethod, _ := grpc.MethodFromServerStream(stream)
	f := &frame{}
	_ = stream.RecvMsg(f)
	splitMethod := strings.Split(fullMethod, "/")
	if len(splitMethod) != 3 {
		logger.Fatalf("Method format did not match grpc standard: %s", fullMethod)
	}
	service := splitMethod[1]
	method := splitMethod[2]

	handler, ok := s.getHandler(service, method)
	if !ok {
		logger.Fatalf("Method does not have a handler defined: %s", fullMethod)
	}

	response, err := handler(stream.Context(), f.payload)
	if err != nil {
		return err
	}

	f.payload = response
	if err := stream.SendMsg(f); err != nil {
		logger.Fatalf("Error whilst sending mock response %s", err)
	}
	return status.Error(codes.OK, "OK")
}
