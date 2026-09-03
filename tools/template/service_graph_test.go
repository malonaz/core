package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// writeManifest writes a service manifest into dir and returns the please target that names it,
// in the form the processed main manifest carries: a label with its output files in parentheses.
func writeManifest(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return "//" + dir + ":" + name + "(" + path + ")"
}

// writeProto writes a proto file declaring the given grpc services, and returns its target.
func writeProto(t *testing.T, dir, name string, services ...string) string {
	t.Helper()
	body := "syntax = \"proto3\";\n"
	for _, service := range services {
		body += "service " + service + " {\n}\n"
	}
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return "//" + dir + ":" + name + "(" + path + ")"
}

// parseServers unmarshals a main manifest's servers the way the template tool hands them to
// serviceGraph.
func parseServers(t *testing.T, body string) []any {
	t.Helper()
	data := map[string]any{}
	require.NoError(t, yaml.Unmarshal([]byte(body), &data))
	servers, ok := data["servers"].([]any)
	require.True(t, ok, "servers is not a list")
	return servers
}

// levelNames flattens levels into the service names they hold, in order.
func levelNames(levels [][]map[string]any) [][]string {
	result := make([][]string, 0, len(levels))
	for _, level := range levels {
		names := make([]string, 0, len(level))
		for _, manifest := range level {
			name, _ := manifest["name"].(string)
			names = append(names, name)
		}
		result = append(result, names)
	}
	return result
}

// setup gives each test a fresh working directory and clears the caches the template functions
// keep between calls, which are global because a template run is one process.
func setup(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	t.Chdir(dir)
	filepathToContent = map[string][]byte{}
	targetToGRPC = map[string]*GRPC{}
	return "svc"
}

func TestServiceGraph(t *testing.T) {
	t.Run("orders a caller after the service it calls", func(t *testing.T) {
		dir := setup(t)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		proto := writeProto(t, dir, "api.proto", "PlatformService")
		platform := writeManifest(t, dir, "platform", "name: platform-service\nimplementation: //svc/platform\n")
		lender := writeManifest(t, dir, "lender", `
name: lender-service
implementation: //svc/lender
dependencies:
  - type: grpc_client
    service: platform-service
    proto: `+proto+`
`)
		levels, err := serviceGraph(parseServers(t, `
servers:
  - type: grpc
    name: internal
    services:
      - service: platform-service
        proto: `+proto+`
        manifest: `+platform+`
      - service: lender-service
        proto: `+proto+`
        manifest: `+lender+`
`))
		require.NoError(t, err)
		assert.Equal(t, [][]string{{"platform-service"}, {"lender-service"}}, levelNames(levels))
	})

	t.Run("ignores a call to a service this binary does not serve", func(t *testing.T) {
		dir := setup(t)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		proto := writeProto(t, dir, "api.proto", "OwnService", "RemoteService")
		own := writeManifest(t, dir, "own", `
name: own-service
implementation: //svc/own
dependencies:
  - type: grpc_client
    service: remote-service
    proto: `+proto+`
`)
		levels, err := serviceGraph(parseServers(t, `
servers:
  - type: grpc
    name: internal
    services:
      - service: own-service
        proto: `+proto+`
        manifest: `+own+`
`))
		require.NoError(t, err)
		assert.Equal(t, [][]string{{"own-service"}}, levelNames(levels))
	})

	t.Run("drops the call edge that closes a cycle rather than failing", func(t *testing.T) {
		dir := setup(t)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		proto := writeProto(t, dir, "api.proto", "AService", "BService")
		a := writeManifest(t, dir, "a", `
name: a-service
implementation: //svc/a
dependencies:
  - type: grpc_client
    service: b-service
    proto: `+proto+`
`)
		b := writeManifest(t, dir, "b", `
name: b-service
implementation: //svc/b
dependencies:
  - type: grpc_client
    service: a-service
    proto: `+proto+`
`)
		levels, err := serviceGraph(parseServers(t, `
servers:
  - type: grpc
    name: internal
    services:
      - service: a-service
        proto: `+proto+`
        manifest: `+a+`
      - service: b-service
        proto: `+proto+`
        manifest: `+b+`
`))
		require.NoError(t, err)
		// a-service is walked first, so the edge back from b-service to it is the one dropped.
		assert.Equal(t, [][]string{{"b-service"}, {"a-service"}}, levelNames(levels))
	})

	t.Run("names a construction dependency and orders it first", func(t *testing.T) {
		dir := setup(t)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		proto := writeProto(t, dir, "api.proto", "AppService")
		base := writeManifest(t, dir, "base", "name: base-service\nimplementation: //svc/base\n")
		app := writeManifest(t, dir, "app", `
name: app-service
implementation: //svc/app
dependencies:
  - type: nats
  - type: service
    manifest: `+base+`
`)
		levels, err := serviceGraph(parseServers(t, `
servers:
  - type: grpc
    name: internal
    services:
      - service: app-service
        proto: `+proto+`
        manifest: `+app+`
`))
		require.NoError(t, err)
		// base-service is not hosted by any server; it is pulled in as a dependency.
		assert.Equal(t, [][]string{{"base-service"}, {"app-service"}}, levelNames(levels))

		// The template names the dependency from the key the graph resolved onto it, and every
		// other dependency is passed through untouched.
		dependencies, ok := levels[1][0]["dependencies"].([]any)
		require.True(t, ok)
		require.Len(t, dependencies, 2)
		assert.Equal(t, "nats", dependencies[0].(map[string]any)["type"])
		assert.Equal(t, "base-service", dependencies[1].(map[string]any)["name"])
	})

	t.Run("attributes a shared dependency to both services that reach it", func(t *testing.T) {
		dir := setup(t)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		proto := writeProto(t, dir, "api.proto", "OneService", "TwoService")
		base := writeManifest(t, dir, "base", "name: base-service\nimplementation: //svc/base\n")
		dependent := func(name string) string {
			return writeManifest(t, dir, name, `
name: `+name+`-service
implementation: //svc/`+name+`
dependencies:
  - type: service
    manifest: `+base+`
`)
		}
		one, two := dependent("one"), dependent("two")
		levels, err := serviceGraph(parseServers(t, `
servers:
  - type: grpc
    name: internal
    services:
      - service: one-service
        proto: `+proto+`
        manifest: `+one+`
      - service: two-service
        proto: `+proto+`
        manifest: `+two+`
`))
		require.NoError(t, err)
		assert.Equal(t, [][]string{{"base-service"}, {"one-service", "two-service"}}, levelNames(levels))
		assert.Equal(t, []string{"one-service", "two-service"}, levels[0][0]["roots"])

		// Reached twice, named once: the dependency list is not duplicated by the second walk.
		for _, rendered := range levels[1] {
			dependencies, ok := rendered["dependencies"].([]any)
			require.True(t, ok)
			require.Len(t, dependencies, 1)
			assert.Equal(t, "base-service", dependencies[0].(map[string]any)["name"])
		}
	})

	t.Run("rejects a cycle in the construction dependencies", func(t *testing.T) {
		dir := setup(t)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		proto := writeProto(t, dir, "api.proto", "AService")
		aTarget := "//" + dir + ":a(" + filepath.Join(dir, "a") + ")"
		bTarget := "//" + dir + ":b(" + filepath.Join(dir, "b") + ")"
		writeManifest(t, dir, "a", `
name: a-service
implementation: //svc/a
dependencies:
  - type: service
    manifest: `+bTarget+`
`)
		writeManifest(t, dir, "b", `
name: b-service
implementation: //svc/b
dependencies:
  - type: service
    manifest: `+aTarget+`
`)
		_, err := serviceGraph(parseServers(t, `
servers:
  - type: grpc
    name: internal
    services:
      - service: a-service
        proto: `+proto+`
        manifest: `+aTarget+`
`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "service dependency cycle")
	})
}
