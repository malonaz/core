package mockserver

import (
	"context"
	"fmt"
	"reflect"

	"github.com/malonaz/core/go/pbutil"
	"google.golang.org/protobuf/proto"
)

// Handler is a mock handler for an endpoint.
type Handler func(ctx context.Context, req []byte) (resp []byte, err error)

func getHandler(handler any) Handler {
	return func(ctx context.Context, req []byte) (resp []byte, err error) {
		// Get the function type
		handlerType := reflect.TypeOf(handler)
		// The second function argument is the pointer to the Proto request Message, *pb.handlerReqType
		handlerReqType := handlerType.In(1)
		// We want to create a new pointer to the base type. We use `.Elem()` to go from *pb.ReqType to pb.ReqType.
		// reflect.New will create a new pb.ReqType and return a pointer to it.
		handlerReqValue := reflect.New(handlerReqType.Elem())
		// Then we cast it to a proto.Message so that we can unmarshal bytes into it
		protoReqValue := handlerReqValue.Interface().(proto.Message)

		if err := pbutil.Unmarshal(req, protoReqValue); err != nil {
			logger.Warningf("Error unmarshaling bytes in grpc handler into proto. Err: <%v>", err)
			return nil, err
		}

		// then we call the Handler with the unmarshaled request value
		handlerReturns := reflect.ValueOf(handler).Call([]reflect.Value{reflect.ValueOf(ctx), handlerReqValue})

		protoResp, handlerErr := handlerReturns[0], handlerReturns[1]

		if !handlerErr.IsNil() {
			err := handlerErr.Interface().(error)
			logger.Warningf("Error returned by function handler. Err <%v>", err)
			return nil, err
		}

		// Finally we marshal the return value and return it
		respBytes, err := proto.Marshal(protoResp.Interface().(proto.Message))
		if err != nil {
			logger.Warningf("Error marshalling proto into bytes in grpc handler. Err: <%v>", err)
			return nil, err
		}

		return respBytes, nil
	}
}

func validateHandler(handler any) error {
	handlerType := reflect.TypeOf(handler)
	handlerKind := handlerType.Kind()

	if handlerKind != reflect.Func {
		return fmt.Errorf("expected mock grpc handler to be a function, instead got kind <%v>, type <%v>, value <%v>", handlerKind, handlerType, handler)
	}

	hasTwoInArgs := handlerType.NumIn() == 2
	hasTwoRetVals := handlerType.NumOut() == 2

	if !(hasTwoInArgs && hasTwoRetVals) {
		var inputTypes []reflect.Type

		for i := 0; i < handlerType.NumIn(); i++ {
			inputTypes = append(inputTypes, handlerType.In(i))
		}

		var outputTypes []reflect.Type

		for i := 0; i < handlerType.NumOut(); i++ {
			outputTypes = append(outputTypes, handlerType.Out(i))
		}

		return fmt.Errorf(
			"expected handler function to have 2 arguments (context.Context, proto.Message), "+
				"and return 2 values (proto.Message, error). "+
				"Got <%v> parameter types: <%v> "+
				"and <%v> return types <%v>",
			handlerType.NumIn(), inputTypes,
			handlerType.NumOut(), outputTypes,
		)
	}

	firstArgType := handlerType.In(0)
	contextReflectType := reflect.TypeOf(new(context.Context)).Elem()
	if !firstArgType.Implements(contextReflectType) {
		return fmt.Errorf("expected first argument type to implement context.Context, got type <%v>", firstArgType)
	}

	secondArgType := handlerType.In(1)
	protoReflectType := reflect.TypeOf(new(proto.Message)).Elem()
	if !secondArgType.Implements(protoReflectType) {
		return fmt.Errorf("expected second argument type to implement proto.Message, got type <%v>", firstArgType)
	}

	firstRetType := handlerType.Out(0)
	if !firstRetType.Implements(protoReflectType) {
		return fmt.Errorf("expected first return value type to implement proto.Message, got type <%v>", firstRetType)
	}

	secondRetType := handlerType.Out(1)
	errorReflectType := reflect.TypeOf(new(error)).Elem()
	if !secondRetType.Implements(errorReflectType) {
		return fmt.Errorf("expected second return value type to implement error, got type <%v>", secondRetType)
	}

	return nil
}

// SetHandler sets a handler to be called when a `method' is called on `service'
// The handler must be a function with the standard GRPC proto handler signature:
// (context.Context, req *serviceProto.MethodRequestType) -> (*serviceProto.MethodResponseType, error)
func (m *Server) SetHandler(service, method string, handler any) {
	m.methodToHandler[service+"/"+method] = getHandler(handler)
}
