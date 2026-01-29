package grpc

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/grpc-ecosystem/grpc-gateway/v2/utilities"
	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type urlEncodedMarshaler struct {
	runtime.Marshaler
}

func (u urlEncodedMarshaler) ContentType(_ any) string {
	return "application/json"
}

func (u urlEncodedMarshaler) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (u urlEncodedMarshaler) NewDecoder(r io.Reader) runtime.Decoder {
	return runtime.DecoderFunc(func(p any) error {
		msg, ok := p.(proto.Message)
		if !ok {
			return fmt.Errorf("not proto message")
		}

		// Read the request body.
		bytes, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}

		// Parse the form query.
		formData := string(bytes)
		values, err := url.ParseQuery(formData)
		if err != nil {
			return err
		}

		// Inject it into the raw form field, if it exists.
		if fd := msg.ProtoReflect().Descriptor().Fields().ByName("raw_form_urlencoded"); fd != nil && fd.Kind() == protoreflect.StringKind {
			msg.ProtoReflect().Set(fd, protoreflect.ValueOfString(formData))
		}

		// We normalize to match to proto text fields.
		normalizedValues := url.Values{}
		for key, vals := range values {
			normalizedValues[xstrings.ToSnakeCase(key)] = vals
		}

		// Populate the message.
		filter := &utilities.DoubleArray{}
		if err := runtime.PopulateQueryParameters(msg, normalizedValues, filter); err != nil {
			return err
		}
		return nil
	})
}
