# Core

A collection of common build rules, tools, and Go libraries for monorepo development using [Please Build](https://please.build).

## Overview

This repository provides:

1. **Build Rules**: Reusable build definitions for Docker, Kubernetes, Protocol Buffers, templates, and services
2. **Code Generation**: Advanced protobuf code generation with custom templates for Go services
3. **Go Libraries**: Common utilities and abstractions for building Go services
4. **Tooling**: Development and build tools including linting, formatting, and schema validation

## Structure

### Build Rules (`build_defs/`)

- **Docker** (`docker.build_defs`): Rules for building and managing Docker images
- **Kubernetes** (`k8s.build_defs`): Configuration templating and deployment rules
- **Protocol Buffers** (`proto/`): Go protobuf compilation with gRPC support
- **Templates** (`template.build_defs`): Text templating and validation utilities
- **Services** (`codegen/go_service/`): High-level service generation from manifests

### Code Generation (`build_defs/codegen/`)

- **Proto Templates** (`go_proto_templates/`): Custom protobuf code generation
  - Model layer generation with database mapping
  - Service layer with AIP (API Improvement Proposals) compliance
  - Database access layer generation
- **Service Generation** (`go_service/`): Complete service scaffolding
  - Main function generation
  - Service runtime and configuration
  - Kubernetes deployment manifests

### Protocol Definitions (`proto/`)

Core protocol buffer schemas providing standardized functionality:

- **Authentication** (`authentication.proto`): Authentication and authorization primitives
  - **`Session`**: User session management with roles and permissions
  - **`Role`**: Role-based access control with inheritance and scoping
  - **`Requirements`**: Method-level authorization requirements (via `google.protobuf.MethodOptions`)
  - **`RateLimit`**: Request rate limiting configuration (via `google.protobuf.MethodOptions`)
  - **`SessionMetadata`**: Client tracking and session context

- **gRPC Extensions** (`grpc.proto`): HTTP gateway and cookie support
  - **`HttpCookie`**: Complete HTTP cookie specification with security flags
  - **Gateway Options**: Custom MIME types and HTTP-specific configurations
  - **Metadata Extensions**: Custom header, trailer, and metadata key mappings

- **Code Generation Annotations** (`codegen/`): Fine-grained control over generated code
  - **Model Generation** (`model.proto`): Database model configuration
    - Table naming, soft deletion, JSON/protobuf serialization
    - Field-level options for vectors, nullability, and embedding
  - **AIP Compliance** (`aip.proto`): API Improvement Proposal implementations
    - List operations with filtering, pagination, and ordering
    - Update operations with field masks and authorization paths
  - **RPC Generation** (`rpc.proto`): Service method generation control
    - CRUD operations with pre/post hooks
  - **Gateway Generation** (`gateway.proto`): HTTP gateway customization
    - Custom handlers and service-level options

### Go Libraries (`go/`)

Core Go utilities for building production services:

- **`authentication`**: JWT/Firebase auth with role-based access control
- **`binary`**: Common binary utilities and main function helpers
- **`certs`**: TLS certificate management
- **`contexttag`**: Context-based request tagging and tracing
- **`flags`**: Command-line flag parsing and configuration
- **`grpc`**: gRPC server/client abstractions with middleware
- **`health`**: Health check endpoints and monitoring
- **`jsonnet`**: Jsonnet template processing
- **`logging`**: Structured logging with multiple backends
- **`pbutil`**: Protocol buffer utilities and helpers
- **`postgres`**: PostgreSQL client with connection pooling
- **`prometheus`**: Metrics collection and exposition
- **`proxy`**: HTTP/gRPC proxy utilities
- **`routine`**: Goroutine management and lifecycle

### Tools (`tools/`)

Development and build tooling:

- **Protocol Buffers** (`proto/`): Complete protobuf toolchain setup
  - **`protoc`**: Protocol buffer compiler (version 32.1)
  - **`protoc-gen-go`**: Official Go code generator for protobuf
  - **`protoc-gen-go-grpc`**: Go gRPC service generator
  - **`protoc-gen-grpc-gateway`**: HTTP/JSON to gRPC gateway generator
  - **`protoc-gen-cobra`**: CLI command generator for protobuf services
  - **`protoc-gen-validate-go-dep`**: Protocol buffer validation support
- **`protoc-templates`**: Custom protobuf code generation engine
- **`template`**: Generic template processing tool
- **`tidy`**: Code formatting and dependency management
- **`validate-schema`**: JSON/YAML schema validation

## License

This project is available under standard open source licenses. See individual files for specific licensing information.
