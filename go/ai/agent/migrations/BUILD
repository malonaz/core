subinclude("//build_defs/codegen/postgres:build_defs")

postgres_migrations(
    name = "migrations",
    dirs = [
        "agent",
        "definition",
        "event",
        "executor",
        "executor_refusal",
        "input",
    ],
    visibility = ["PUBLIC"],
)
