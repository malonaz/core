package grpc

import (
	"fmt"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	grpcpb "github.com/malonaz/core/genproto/grpc"
	"github.com/malonaz/core/go/pbutil"
)

// /////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////// COOKIE CONVERSION METHODS ///////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////
func cookieToProto(httpCookie *http.Cookie) *grpcpb.HttpCookie {
	return &grpcpb.HttpCookie{
		Name:  httpCookie.Name,
		Value: httpCookie.Value,

		Path:    httpCookie.Path,
		Domain:  httpCookie.Domain,
		Expires: uint64(httpCookie.Expires.UnixMicro()),
		MaxAge:  int64(httpCookie.MaxAge),

		HttpOnly: httpCookie.HttpOnly,
		Secure:   httpCookie.Secure,
	}
}

func cookieFromProto(httpCookie *grpcpb.HttpCookie) *http.Cookie {
	return &http.Cookie{
		Name:  httpCookie.Name,
		Value: httpCookie.Value,

		Path:    httpCookie.Path,
		Domain:  httpCookie.Domain,
		Expires: time.UnixMicro(int64(httpCookie.Expires)),
		MaxAge:  int(httpCookie.MaxAge),

		HttpOnly: httpCookie.HttpOnly,
		Secure:   httpCookie.Secure,
	}
}

// /////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////// GRPC GATEWAY METHODS BELOW //////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////
const grpcGatewayCookieMetadataKey = "set-cookie-bin" // `-bin` suffix is necessary to send binary data.

// GatewayCookie namespaces methods for grpc-gateway cookies.
type GatewayCookie struct{}

// forwardOutOption sets cookies on an http response before forwarding the response back to a caller. a gRPC server can pass cookies to the
// grpc gateway by adding a proto.Cookie in the context metadata with the key `grpcGatewayCookieMetadataKey`.
func (GatewayCookie) forwardOutOption(ctx context.Context, w http.ResponseWriter, _ proto.Message) error {
	md, ok := runtime.ServerMetadataFromContext(ctx)
	if !ok {
		return nil
	}

	// Set the 'Set-Cookie' trailer for the HTTP response based on the gRPC response metadata
	for _, value := range md.HeaderMD.Get(grpcGatewayCookieMetadataKey) {
		httpCookie := &grpcpb.HttpCookie{}
		if err := pbutil.Unmarshal([]byte(value), httpCookie); err != nil {
			return fmt.Errorf("unmarshaling cookie from metadata: %w", err)
		}
		http.SetCookie(w, cookieFromProto(httpCookie))
		// Now remove this from the metadata (to prevent downstream options from touching this). Remove also from http response headers.
		delete(md.HeaderMD, grpcGatewayCookieMetadataKey)
		delete(w.Header(), grpcGatewayCookieMetadataKey)
	}
	return nil
}

func (GatewayCookie) forwardInOption(ctx context.Context, request *http.Request) metadata.MD {
	cookies := request.Cookies()
	if len(cookies) == 0 {
		return nil
	}

	md := metadata.MD{}
	for _, cookie := range cookies {
		httpCookie := cookieToProto(cookie)
		bytes, err := proto.Marshal(httpCookie)
		if err != nil {
			// We have no way to set an error... this is annoying...
			log.Errorf("Marshaling http cookie: %v", err)
			continue
		}
		md.Append(grpcGatewayCookieMetadataKey, string(bytes))
	}
	return md
}

// SetHTTPCookies is used by a server to send cookies through grpc headers, to the grpc gateway during an RPC call.
func (GatewayCookie) SetHTTPCookies(ctx context.Context, httpCookies ...*grpcpb.HttpCookie) error {
	md := metadata.MD{}
	for _, httpCookie := range httpCookies {
		bytes, err := proto.Marshal(httpCookie)
		if err != nil {
			return fmt.Errorf("marshaling http cookie: %w", err)
		}
		md.Append(grpcGatewayCookieMetadataKey, string(bytes))
	}
	if err := grpc.SetHeader(ctx, md); err != nil {
		return fmt.Errorf("sending header: %w", err)
	}
	return nil
}

// GetHTTPCookies retrieves any http cookies from a context.
func (GatewayCookie) GetHTTPCookies(ctx context.Context) ([]*grpcpb.HttpCookie, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, nil
	}
	httpCookies := []*grpcpb.HttpCookie{}
	values := md.Get(grpcGatewayCookieMetadataKey)
	for _, value := range values {
		httpCookie := &grpcpb.HttpCookie{}
		if err := pbutil.Unmarshal([]byte(value), httpCookie); err != nil {
			return nil, fmt.Errorf("unmarshaling cookie from metadata: %w", err)
		}
		httpCookies = append(httpCookies, httpCookie)
	}
	return httpCookies, nil
}

// GetHTTPCookie retrieves the http cookie with the given name from the context.
func (gc GatewayCookie) GetHTTPCookie(ctx context.Context, name string) (*grpcpb.HttpCookie, error) {
	httpCookies, err := gc.GetHTTPCookies(ctx)
	if err != nil {
		return nil, err
	}
	for _, httpCookie := range httpCookies {
		if httpCookie.Name == name {
			return httpCookie, nil
		}
	}
	return nil, nil
}

// /////////////////////////////////////////////////////////////////////////////////////////
// ///////////////////////////////// GRPC WEB METHODS BELOW ////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////
const (
	grpcWebSetCookieMetadataKey = "set-cookie"
	grpcWebGetCookieMetadataKey = "cookie"
)

// WebCookie namespaces methods for grpc-web cookies.
type WebCookie struct{}

// SetHTTPCookies adds cookies to a grpc header with the `set-cookie`key. This is used to send cookies back to grpc-web clients.
func (WebCookie) SetHTTPCookies(ctx context.Context, httpCookies ...*grpcpb.HttpCookie) error {
	md := metadata.MD{}
	for _, httpCookie := range httpCookies {
		cookie := cookieFromProto(httpCookie)
		md.Append(grpcWebSetCookieMetadataKey, cookie.String())
	}
	if err := grpc.SetHeader(ctx, md); err != nil {
		return fmt.Errorf("sending header: %w", err)
	}
	return nil
}

// GetHTTPCookies retrieves any http cookies from a context.
func (WebCookie) GetHTTPCookies(ctx context.Context) []*grpcpb.HttpCookie {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	httpCookies := []*grpcpb.HttpCookie{}
	values := md.Get(grpcWebGetCookieMetadataKey)
	for _, value := range values {
		// Use http request to parse cookies.
		header := http.Header{}
		header.Add("Cookie", value)
		httpRequest := http.Request{Header: header}
		for _, cookie := range httpRequest.Cookies() {
			httpCookies = append(httpCookies, cookieToProto(cookie))
		}
	}
	return httpCookies
}

// GetHTTPCookie retrieves the http cookie with the given name from the context.
func (WebCookie) GetHTTPCookie(ctx context.Context, name string) *grpcpb.HttpCookie {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	values := md.Get(grpcWebGetCookieMetadataKey)
	for _, value := range values {
		// Use http request to parse cookies.
		header := http.Header{}
		header.Add("Cookie", value)
		httpRequest := http.Request{Header: header}
		for _, cookie := range httpRequest.Cookies() {
			if cookie.Name == name {
				return cookieToProto(cookie)
			}
		}
	}
	return nil
}
