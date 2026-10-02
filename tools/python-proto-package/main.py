"""Rewrites generated Python proto modules under a package prefix.

protoc derives Python module paths from .proto import paths, and protobuf has no
python_package option, so first-party protos can shadow stdlib modules (e.g. `platform`).
"""

import argparse
import os
import re

# Proto module references emitted by protoc's python and grpc plugins.
_MODULE_REFERENCE = re.compile(
    r"^from ([\w.]+) import (\w+)_pb2\b"
    r"|^import ([\w.]+)\.(\w+)_pb2$"
    r"|BuildTopDescriptorsAndMessages\(DESCRIPTOR, '([\w.]+)\.(\w+)_pb2'",
    re.MULTILINE,
)


def first_party_packages(source: str) -> set[str]:
    """Returns the referenced proto packages whose .proto files are resolved from the repo root."""
    packages = set()
    for match in _MODULE_REFERENCE.finditer(source):
        package, name = [group for group in match.groups() if group]
        # Third-party protos are compiled from a root_dir, so only first-party ones exist at their import path.
        if os.path.exists(os.path.join(*package.split("."), name + ".proto")):
            packages.add(package)
    return packages


def rewrite(source: str, prefix: str) -> str:
    packages = first_party_packages(source)
    if not packages:
        return source
    alternation = "|".join(re.escape(package) for package in sorted(packages, key=len, reverse=True))
    # Only rewrite references to `_pb2` modules so strings like `platform.onikisu.com` are untouched.
    pattern = re.compile(rf"(?<![\w.])({alternation})(?=\.\w+_pb2\b| import \w+_pb2\b)")
    return pattern.sub(rf"{prefix}.\1", source)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--out", required=True, help="Directory the rewritten sources are written under, at {prefix}/{src}.")
    parser.add_argument("srcs", nargs="+")
    args = parser.parse_args()

    for src in args.srcs:
        with open(src) as f:
            source = f.read()
        if src.endswith(".py"):
            source = rewrite(source, args.prefix)
        out = os.path.join(args.out, *args.prefix.split("."), src)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w") as f:
            f.write(source)


if __name__ == "__main__":
    main()
