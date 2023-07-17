genrule(
    name = "node_modules",
    srcs = ["package.json"],
    outs = [
        "node_modules",
    ],
    building_description = "fetching node modules",
    cmd = "yarn --cwd $PKG_DIR install && mv $PKG_DIR/node_modules .",
    output_is_complete = True,
    visibility = ["PUBLIC"],
)
