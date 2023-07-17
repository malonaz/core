package(default_visibility = ["PUBLIC"])

subinclude("//build_defs:remote")

# Revision of googleapis/googelapis repo.
GOOGLEAPI_REVISION = "5e5d500bc4f8c4d5591cd1fda817556b3fe663a7"

def google_api(
        name,
        repo = "googleapis/googleapis",
        files = [],
        dir,
        deps = [],
        grpc = False,
        grpc_gateway = False):
    """Defines a rule to fetch the googleapis dependencies.
    Args:
      name (str): Name of the rule.
      repo (str): Repository to fetch files from. Defaults to grpc-gateway's.
      dir (str): Subdirectory in the repo to fetch files from.
      deps (list): Any other dependencies.
      visibility (list): Visibility spec of the rule.
    """
    srcs = []
    sub_directory = "/".join(dir.split("/")[1:])

    # Package name will be folder name. So we don't want to remove it.
    sub_directory = "/" + sub_directory if sub_directory != name else ""
    for f in files:
        sanitized_filename = f.replace("/", "_")
        src = github_file(
            name = f"_{name}#{sanitized_filename}",
            out = f,
            file = join_path(dir, f),
            repo = repo,
            revision = GOOGLEAPI_REVISION,
        )
        new_src = genrule(
            name = f"_{name}#{sanitized_filename}_correct_packages",
            srcs = [src],
            out = join_path(dir, f),
            cmd = "cat $SRCS > $OUT",
        )
        srcs += [new_src]
    proto_def = grpc_library if grpc else proto_library
    proto_def = grpc_gateway_library if grpc_gateway else proto_def
    proto_def(
        name = name,
        srcs = srcs,
        import_path = f"google.golang.org/genproto/googleapis/" + dir.removeprefix("google/") + (("/" + name) if dir.split("/")[-1] != name else ""),
        protoc_flags = ["-I" + package_name()],
        deps = deps,
    )

google_api(
    name = "annotations",
    dir = "google/api",
    files = [
        "annotations.proto",
        "client.proto",
        "field_behavior.proto",
        "http.proto",
        "resource.proto",
    ],
)

google_api(
    name = "v1alpha1",
    dir = "google/api/expr/v1alpha1",
    files = [
        "checked.proto",
        "eval.proto",
        "explain.proto",
        "syntax.proto",
        "value.proto",
    ],
    deps = [":status"],
)

google_api(
    name = "httpbody",
    dir = "google/api",
    files = ["httpbody.proto"],
)

google_api(
    name = "type",
    dir = "google/logging/type",
    files = [
        "http_request.proto",
        "log_severity.proto",
    ],
)

google_api(
    name = "status",
    dir = "google/rpc",
    files = ["status.proto"],
)

google_api(
    name = "code",
    dir = "google/rpc",
    files = ["code.proto"],
)

google_api(
    name = "longrunning",
    dir = "google/longrunning",
    files = ["operations.proto"],
    grpc_gateway = True,
    deps = [
        ":annotations",
        ":status",
    ],
)

google_api(
    name = "errdetails",
    dir = "google/rpc",
    files = ["error_details.proto"],
)

google_api(
    name = "visibility",
    dir = "google/api",
    files = ["visibility.proto"],
)
