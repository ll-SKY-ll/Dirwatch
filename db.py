from mautrix.util.async_db import Connection, UpgradeTable

upgrade_table = UpgradeTable()


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
async def upgrade_v3(conn: Connection) -> None:
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS pending_notifications (
            id              BIGSERIAL PRIMARY KEY,
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
async def upgrade_v4(conn: Connection) -> None:
    # Collapse threshold in characters:
    #   > 0  collapse topics longer than N chars behind a <details> disclosure
    #   0    never collapse (show topics inline, in full)
    #  -1    always collapse, regardless of length
    await conn.execute(
        "ALTER TABLE room_watched_servers "
        "DROP COLUMN IF EXISTS collapse_topics"
    )
    await conn.execute(
        "ALTER TABLE room_watched_servers "
        "ADD COLUMN IF NOT EXISTS topic_collapse_length INTEGER NOT NULL DEFAULT 120"
    )
