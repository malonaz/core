package(default_visibility = ["PUBLIC"])

subinclude("//build_defs:remote")

subinclude("//build_defs:proto_extensions")

# Revision of googleapis/googelapis repo.
PROTOVALIDATE_REVISION = "9b627e484bd0df68054c5ae2d2ef288649534fee"

def protovalidate_api(
        name,
        repo = "bufbuild/protovalidate",
        files = [],
        dir,
        deps = []):
    """Defines a rule to fetch the googleapis dependencies.
    Args:
      name (str): Name of the rule.
      repo (str): Repository to fetch files from.
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
            revision = PROTOVALIDATE_REVISION,
        )
        new_src = genrule(
            name = f"_{name}#{sanitized_filename}_correct_packages",
            srcs = [src],
            out = join_path(dir, f),
            cmd = "cat $SRCS > $OUT",
        )
        srcs += [new_src]

    return proto_library(
        name = name,
        srcs = srcs,
        protoc_flags = ["-I" + package_name() + "/proto/protovalidate"],
        deps = deps,
    )

protovalidate_api(
    name = "priv",
    dir = "proto/protovalidate/buf/validate/priv",
    files = ["private.proto"],
)

protovalidate_api(
    name = "validate",
    dir = "proto/protovalidate/buf/validate",
    files = [
        "expression.proto",
        "validate.proto",
    ],
    deps = [":priv"],
)
