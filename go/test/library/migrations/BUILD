subinclude("//build_defs/codegen/postgres:build_defs")

postgres_migrations(
    name = "migrations",
    dirs = [
        "library",
    ],
    includes = ["//go/outbox/migrations"],
    visibility = ["PUBLIC"],
)
