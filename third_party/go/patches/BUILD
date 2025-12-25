package(default_visibility = ["PUBLIC"])

_patches = [
    "go-openai.patch",
    "protoc-gen-doc.patch",
    "spanner-aip.patch",
]

for patch in _patches:
    export_file(
        name = patch,
        src = patch,
    )
