# Core

A collection of reusable build rules, Go libraries, protocol buffer definitions, and tools for monorepo development with [Please Build](https://please.build).

## Structure

### Build Rules (`build_defs/`)

Reusable Please build definitions:

- **`docker.build_defs`** — Docker image building and management
- **`grafana.build_defs`** — Grafana dashboard generation
- **`k8s.build_defs`** — Kubernetes configuration templating and deployment
- **`remote.build_defs`** — Remote file fetching (GitHub, etc.)
- **`template.build_defs`** — Text templating and validation
- **`utils.build_defs`** — General-purpose build utilities
- **`proto/`** — Protobuf compilation rules for Go, JS, Python, and Ruby

#### Code Generation (`build_defs/codegen/`)

- **`go_service/`** — Full Go service scaffolding (main, runtime, K8s manifests)
- **`postgres/`** — Database migration and access layer generation
- **`protoc_gen_core/`** — Custom protobuf code generation plugin rules

### Protocol Definitions (`malonaz/`)

Core protobuf schemas providing standardized functionality:

- **`ai/`** — AI service definitions (chat, TTS, STT, models, GenUI components)
- **`aip/`** — AIP (API Improvement Proposals) extensions and labels
- **`audio/`** — Audio format definitions (PCM, μ-law)
- **`authentication/`** — Session management, RBAC, rate limiting
- **`canonicalize/`** — Data canonicalization annotations
- **`codegen/`** — Code generation annotations (model, AIP, gateway, NATS)
- **`grpc/`** — gRPC extensions (HTTP gateway, cookies, metadata)
- **`json/`** — JSON schema definitions
- **`nats/`** — NATS streaming definitions

### Go Libraries (`go/`)

Production-ready Go packages:

| Package | Description |
|---|---|
| `ai` | AI/LLM client abstractions |
| `aip` | AIP-compliant list/update helpers |
| `audio` | PCM ↔ μ-law audio conversion |
| `authentication` | JWT/Firebase auth with RBAC |
| `binary` | Subprocess lifecycle management |
| `canonicalize` | Data canonicalization utilities |
| `certs` | TLS certificate management |
| `flags` | Command-line flag parsing |
| `grafana` | Grafana API client |
| `grpc` | gRPC server/client with middleware |
| `health` | Health check endpoints |
| `http` | HTTP server utilities |
| `jsonnet` | Jsonnet template processing |
| `logging` | Structured logging |
| `mock` | gRPC and HTTP mock servers |
| `nats` | NATS client abstractions |
| `pbutil` | Protobuf utilities |
| `pgq` | PostgreSQL query builder |
| `postgres` | PostgreSQL client with connection pooling |
| `prometheus` | Metrics collection and exposition |
| `proxy` | HTTP/gRPC proxy utilities |
| `routine` | Goroutine lifecycle management |
| `sat` | System Acceptance Test framework |
| `test` | Test helpers |
| `uuid` | UUID generation utilities |
| `websocket` | WebSocket server utilities |

### Tools (`tools/`)

- **`protoc-gen-core`** — Custom protobuf code generation engine
- **`generate-logo`** — SVG/PNG logo and favicon generator
- **`grafana-upload-dashboard`** — Grafana dashboard uploader
- **`grafana-upload-alert-rules`** — Grafana alert rules uploader
- **`template`** — Generic template processing
- **`tidy`** — Code formatting and dependency management
- **`validate-schema`** — JSON/YAML schema validation
- **`proto/`** — Protobuf toolchain (protoc, protoc-gen-go, grpc-gateway, etc.)
- **`go/`** — Go toolchain

## License

[MIT](LICENSE)
