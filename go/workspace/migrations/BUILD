subinclude("//build_defs/codegen/postgres:build_defs")

postgres_migrations(
    name = "migrations",
    dirs = [
        "filesystem",
        "snapshot",
        "workspace",
    ],
    visibility = ["PUBLIC"],
)
