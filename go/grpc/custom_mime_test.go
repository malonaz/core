package grpc

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/api/annotations"
)

func TestHTTPRuleRoutes(t *testing.T) {
	rule := &annotations.HttpRule{
		Pattern: &annotations.HttpRule_Post{Post: "/v1/{name=lenders/*}/reports:receive"},
		AdditionalBindings: []*annotations.HttpRule{
			{Pattern: &annotations.HttpRule_Get{Get: "/v1/things/{id}"}},
		},
	}
	require.Equal(t, []string{
		"POST /v1/{name=lenders/*}/reports:receive",
		"GET /v1/things/{id=*}",
	}, httpRuleRoutes(rule))
}

// The route key a rule produces must be the one the mux reports for a matched request.
func TestCustomMimeMiddleware(t *testing.T) {
	routeToCustomMime := map[string]string{}
	for _, route := range httpRuleRoutes(&annotations.HttpRule{
		Pattern: &annotations.HttpRule_Post{Post: "/v1/{name=lenders/*}/reports:receive"},
	}) {
		routeToCustomMime[route] = "application/raw-webhook"
	}

	type seen struct{ contentType, original string }
	var got seen
	mux := runtime.NewServeMux(runtime.WithMiddlewares(customMimeMiddleware(routeToCustomMime)))
	handler := func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		got = seen{contentType: r.Header.Get("Content-Type"), original: r.Header.Get(HeaderXOriginalContentType)}
	}
	require.NoError(t, mux.HandlePath("POST", "/v1/{name=lenders/*}/reports:receive", handler))
	require.NoError(t, mux.HandlePath("POST", "/v1/{name=lenders/*}/other", handler))

	request := httptest.NewRequest("POST", "/v1/lenders/aven/reports:receive", strings.NewReader("<xml/>"))
	request.Header.Set("Content-Type", "text/xml")
	mux.ServeHTTP(httptest.NewRecorder(), request)
	require.Equal(t, seen{contentType: "application/raw-webhook", original: "text/xml"}, got)

	request = httptest.NewRequest("POST", "/v1/lenders/aven/other", strings.NewReader("{}"))
	request.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(httptest.NewRecorder(), request)
	require.Equal(t, seen{contentType: "application/json"}, got)
}
