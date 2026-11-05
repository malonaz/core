subinclude("//build_defs/codegen/postgres:build_defs")

postgres_migrations(
    name = "migrations",
    dirs = [
        "agent",
        "agent_lease",
        "agent_mailbox",
        "definition",
    ],
    visibility = ["PUBLIC"],
)
