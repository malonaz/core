// Package grpcinproc provides utilities for working with gRPC streaming RPCs.
//
// This package enables direct invocation of gRPC streaming handlers without
// requiring a full gRPC client-server setup, allowing server-side streaming
// implementations to be called in-process as if they were client streams.
//
// Key features:
//   - Convert server streaming handlers into in-process callable functions
//   - Bridge between gRPC streaming interfaces and Go iterators
//   - Eliminate network overhead for local service-to-service calls
//   - Enable flexible service composition within the same process
//
// This implementation is inspired by:
// https://www.bwplotka.dev/2025/go-grpc-inprocess-iter/
//
// Example usage:
//
//	// Given a gRPC server streaming handler:
//	func (s *MyService) StreamData(req *pb.Request, stream pb.MyService_StreamDataServer) error {
//		for i := 0; i < 10; i++ {
//			if err := stream.Send(&pb.Response{Data: i}); err != nil {
//				return err
//			}
//		}
//		return nil
//	}
//
//	// Create a client wrapper for direct invocation:
//	serverStreamClient := grpcinproc.NewServerStreamAsClient[pb.Request, pb.Response](s.StreamData)
//
//	// Call it directly without gRPC infrastructure:
//	ctx := context.Background()
//	stream, err := serverStreamClient(ctx, &pb.Request{})
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Receive messages from the stream:
//	for {
//		resp, err := stream.Recv()
//		if err == io.EOF {
//			break
//		}
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("Received: %v\n", resp.Data)
//	}
package grpcinproc
