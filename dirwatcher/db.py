from mautrix.util.async_db import Connection, Scheme, UpgradeTable

upgrade_table = UpgradeTable()


async def _column_exists(
    conn: Connection, scheme: Scheme, table: str, column: str
) -> bool:
    """Scheme-agnostic check for whether `table.column` exists. Used in place
    of Postgres-only `ADD/DROP COLUMN IF [NOT] EXISTS`, which SQLite rejects."""
    if scheme == Scheme.POSTGRES:
        return bool(
            await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                "WHERE table_name=$1 AND column_name=$2)",
                table,
                column,
            )
        )
    rows = await conn.fetch(f"PRAGMA table_info({table})")
    # PRAGMA rows expose the column name under key "name" (index 1).
    return any((r["name"] if "name" in r.keys() else r[1]) == column for r in rows)


@upgrade_table.register(description="Initial schema")
async def upgrade_v1(conn: Connection) -> None:
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS directory_snapshot (
            server     TEXT NOT NULL,
            room_id    TEXT NOT NULL,
            alias      TEXT,
            name       TEXT,
            topic      TEXT,
            members    INTEGER NOT NULL DEFAULT 0,
            first_seen BIGINT NOT NULL,
            last_seen  BIGINT NOT NULL,
            removed    BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (server, room_id)
        )"""
    )
    await conn.execute(
        """CREATE INDEX IF NOT EXISTS idx_snapshot_server_removed
           ON directory_snapshot (server, removed)"""
    )
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS poll_state (
            server      TEXT PRIMARY KEY,
            last_polled BIGINT NOT NULL DEFAULT 0
        )"""
    )


@upgrade_table.register(
    description="Rename poll_state.server to poll_key, add room_watched_servers"
)
async def upgrade_v2(conn: Connection) -> None:
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS poll_state_new (
            poll_key    TEXT PRIMARY KEY,
            last_polled BIGINT NOT NULL DEFAULT 0
        )"""
    )
    await conn.execute(
        """INSERT INTO poll_state_new (poll_key, last_polled)
           SELECT server, last_polled FROM poll_state"""
    )
    await conn.execute("DROP TABLE poll_state")
    await conn.execute("ALTER TABLE poll_state_new RENAME TO poll_state")

    await conn.execute(
        """CREATE TABLE IF NOT EXISTS room_watched_servers (
            matrix_room_id   TEXT NOT NULL,
            server           TEXT NOT NULL,
            interval_minutes INTEGER NOT NULL DEFAULT 60,
            fetch_limit      INTEGER NOT NULL DEFAULT 500,
            include_topic    BOOLEAN NOT NULL DEFAULT TRUE,
            include_members  BOOLEAN NOT NULL DEFAULT TRUE,
            max_per_message  INTEGER NOT NULL DEFAULT 50,
            PRIMARY KEY (matrix_room_id, server)
        )"""
    )


@upgrade_table.register(
    description="Add pending_notifications table for multi-room delivery"
)
async def upgrade_v3(conn: Connection, scheme: Scheme) -> None:
    # Autoincrementing surrogate key. Postgres uses BIGSERIAL; SQLite uses a
    # plain INTEGER PRIMARY KEY, which aliases the implicit rowid and
    # autoincrements. Both yield monotonically increasing integer ids, which
    # is all the delivery/cleanup code relies on.
    id_column = (
        "id BIGSERIAL PRIMARY KEY"
        if scheme == Scheme.POSTGRES
        else "id INTEGER PRIMARY KEY"
    )
    await conn.execute(
        f"""CREATE TABLE IF NOT EXISTS pending_notifications (
            {id_column},
            server          TEXT NOT NULL,
            matrix_room_id  TEXT NOT NULL,
            change_type     TEXT NOT NULL,
            room_id         TEXT NOT NULL,
            alias           TEXT,
            name            TEXT,
            topic           TEXT,
            members         INTEGER NOT NULL DEFAULT 0,
            created_at      BIGINT NOT NULL
        )"""
    )
    await conn.execute(
        """CREATE INDEX IF NOT EXISTS idx_pending_room_server
           ON pending_notifications (matrix_room_id, server)"""
    )


@upgrade_table.register(
    description="Add topic_collapse_length option to room_watched_servers"
)
async def upgrade_v4(conn: Connection, scheme: Scheme) -> None:
    # Collapse threshold in characters:
    #   > 0  collapse topics longer than N chars behind a <details> disclosure
    #   0    never collapse (show topics inline, in full)
    #  -1    always collapse, regardless of length
    if await _column_exists(conn, scheme, "room_watched_servers", "collapse_topics"):
        await conn.execute(
            "ALTER TABLE room_watched_servers DROP COLUMN collapse_topics"
        )
    if not await _column_exists(
        conn, scheme, "room_watched_servers", "topic_collapse_length"
    ):
        await conn.execute(
            "ALTER TABLE room_watched_servers "
            "ADD COLUMN topic_collapse_length INTEGER NOT NULL DEFAULT 120"
        )


@upgrade_table.register(
    description="Add notify_removals option to room_watched_servers"
)
async def upgrade_v5(conn: Connection, scheme: Scheme) -> None:
    # When FALSE, the bot still tracks directory removals (snapshot/stats are
    # server-global and unaffected) but skips enqueuing "removed" notifications
    # for this (room, server). "added" notifications are unaffected.
    if not await _column_exists(
        conn, scheme, "room_watched_servers", "notify_removals"
    ):
        await conn.execute(
            "ALTER TABLE room_watched_servers "
            "ADD COLUMN notify_removals BOOLEAN NOT NULL DEFAULT TRUE"
        )
