subinclude("//build_defs/codegen/postgres:build_defs")

postgres_migrations(
    name = "migrations",
    dirs = [
        "job",
        "queue",
        "queue_hour_stats",
        "schedule",
    ],
    visibility = ["PUBLIC"],
)
