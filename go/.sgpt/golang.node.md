@summary(Go style guide and preferred core libraries: gRPC errors, pbutil marshaling, field masks, AIP pagination, errgroup, resource names.)

# Style Guide

### Naming Conventions
- **No Abbreviations**: Use full, descriptive names matching the type.
    - `listChatsRequest` (NOT `req`)
    - `listChatsResponse` (NOT `resp`)
    - `message` (NOT `msg`)
    - `configuration` (NOT `cfg`)
    - **Exceptions**: `ctx` (Context), `err` (Error), `ok` (Boolean check).
- **Maps**: Must be named `<keyName>To<valueName>` (e.g., `userIDToConfig`, `chatNameToAnalysis`).
- **Sets**: Must be named `<keyName>Set` (e.g., `contactNameSet`, `organizationIDSet`).

### Code Structure
- **Early Returns**: Prefer guard clauses and early returns over deep nesting.
- **Architecture**: Follow the existing proto-based, service-oriented patterns.
- **Explicit Request Definitions**: Never inline request objects in function calls. Always define them as a variable first.

```go
// Bad
resp, err := s.userServiceClient.ListUsers(ctx, &pb.ListUsersRequest{
    PageSize: 10,
})
// Good
listUsersRequest := &pb.ListUsersRequest{
    PageSize: 10,
}
listUsersResponse, err := s.userServiceClient.ListUsers(ctx, listUsersRequest)
```

### Error patterns
```go
// Always prefer nesting:
if err := someFunc(); err != nil {}
// Bad
err := someFunc()
if err != nil {
}
// Unless the call returns a value:
listChatsResponse, err := s.chatServiceClient.ListChats(ctx, listChatsRequest)
if err != nil {}
```

---

# Preferred Libraries & Patterns

Strictly prefer these `github.com/malonaz/core` libraries over the standard library equivalents.

### 1. gRPC Errors (`go/grpc/status`)
**Import**: `"github.com/malonaz/core/go/grpc/status"`
**Key files**: `go/grpc/status/errors.go` (builder), `go/grpc/status/utils.go` (`HasCode`, `ErrorDetails`).
Always use this library for returning errors in RPC handlers and checking error codes. `Errorf` auto-attaches a DebugInfo stack trace.

```go
// Returning errors — must call .Err() at the end.
return nil, status.Errorf(codes.Internal, "db error: %v", err).Err()

// Wrapping an existing error while preserving its code (maps context errors too).
return nil, status.FromError(err, "listing chats").Err()

// Builder details before .Err():
return nil, status.Errorf(codes.InvalidArgument, "bad phone: %v", err).
    WithLocalizedMessage("Invalid phone number").
    Err()

// Checking error codes (one or many).
if status.HasCode(err, codes.NotFound, codes.PermissionDenied) {}

// Iterating typed error details.
for errorInfo := range status.ErrorDetails[*errdetails.ErrorInfo](err) {}
```

### 2. Protobuf Marshaling (`go/pbutil`)
**Import**: `"github.com/malonaz/core/go/pbutil"`
**Key files**: `go/pbutil/marshaller.go`, `go/pbutil/pretty.go`.
Never use `protojson` directly.

- **`pbutil.Marshal(m)` / `pbutil.MarshalDeterministic(m)` / `pbutil.Unmarshal(b, m)`**: binary wire format.
- **`pbutil.JSONMarshal(m)`**: JSON, snake_case field names.
- **`pbutil.JSONCamelCaseMarshal(m)`**: JSON, lowerCamelCase field names.
- **`pbutil.JSONMarshalPretty(m)`**: indented JSON.
- **`pbutil.JSONUnmarshal(b, m)`**: lenient — discards unknown fields.
- **`pbutil.JSONUnmarshalStrict(b, m)`**: errors on unknown fields.
- **`pbutil.MarshalToStruct(m)` / `pbutil.UnmarshalFromStruct(m, s)`**: `*structpb.Struct` conversion (common with AI/LLM outputs).
- **`pbutil.MustPrintPretty(m)`**: colored pretty print for debugging/CLI output.

### 3. Field Masks (`go/pbutil/pbfieldmask`)
**Import**: `"github.com/malonaz/core/go/pbutil/pbfieldmask"`
**Key file**: `go/pbutil/pbfieldmask/pbfieldmask.go`.
Used for partial updates and filtering. Always validate the mask against the target message type.

```go
// Static mask.
fieldMask := pbfieldmask.FromPaths("metadata.analysis", "status").
    MustValidate(&pb.Chat{}).
    Proto()

// Derive a mask from a populated message (Update requests).
fieldMask := pbfieldmask.FromMessage(updateChatRequest.Chat, pbfieldmask.WithOnlySet()).Proto()

// Applying: mask.Update(dest, src), mask.Apply(m), mask.ApplyInverse(m).
```

### 4. AIP Pagination (`go/aip`)
**Import**: `"github.com/malonaz/core/go/aip"`
**Key files**: `go/aip/paginate.go`; also see `go/aip/list_request_parser.go` and `go/aip/resource.go` for server-side List/resource helpers.
Do not write manual `for` loops over gRPC `List` endpoints.

```go
listChatsRequest := &chatpb.ListChatsRequest{
    Parent:  parent,
    OrderBy: "create_time asc", // stable ordering prevents shifting while paginating.
}

// Collect all items at once.
chats, err := aip.Paginate[*chatpb.Chat](ctx, listChatsRequest, s.chatServiceClient.ListChats)

// Or iterate one-by-one.
for chat, err := range aip.Iterator[*chatpb.Chat](ctx, listChatsRequest, s.chatServiceClient.ListChats) {
    if err != nil {
        return nil, err
    }
}

// Or page-by-page.
for chats, err := range aip.PageIterator[*chatpb.Chat](ctx, listChatsRequest, s.chatServiceClient.ListChats) {}
```

### 5. Concurrency (`errgroup`)
**Import**: `"golang.org/x/sync/errgroup"`
Always use `errgroup` for concurrent operations. Strictly name variables `eg` and `ctxEg`.

```go
eg, ctxEg := errgroup.WithContext(ctx)
eg.Go(func() error {
    user, err := s.userClient.GetUser(ctxEg, getUserRequest)
    return err
})
if err := eg.Wait(); err != nil {
    return nil, err
}
```

### 6. Resource Names
Never construct resource name strings manually via concatenation. Always use the generated protobuf resource name structs.

```go
// Bad
configName := "organizations/" + orgID + "/users/" + userID + "/config"

// Good
configName := contactRn.UserResourceName().ConfigResourceName().String()

// Parsing
contactRn := &userpb.ContactResourceName{}
if err := contactRn.UnmarshalString(request.GetContact()); err != nil {
    return nil, status.Errorf(codes.InvalidArgument, "unmarshaling contact resource name: %v", err).Err()
}
```

---

# Key files for further reading
- `go/grpc/server.go`, `go/grpc/client.go` — gRPC server/client construction and options.
- `go/grpc/status/errors.go` — full error-builder API.
- `go/aip/paginate.go`, `go/aip/list_request_parser.go` — pagination and List request parsing.
- `go/pbutil/marshaller.go`, `go/pbutil/pbfieldmask/pbfieldmask.go` — marshaling and field masks.
- `go/postgres/`, `go/pgq/` — database access patterns.
