# The outbox journal's migrations: a directory a database's own migrations
# include (`postgres_migrations(includes = ...)`) and order in their manifest.
filegroup(
    name = "migrations",
    srcs = glob(["outbox/*.sql"]),
    visibility = ["PUBLIC"],
)
