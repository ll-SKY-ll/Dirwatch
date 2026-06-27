from __future__ import annotations

import asyncio
import fnmatch
import html
import time
from typing import Any

from maubot import MessageEvent, Plugin
from maubot.handlers import command, event
from mautrix.types import EventType, Membership, RoomID, StateEvent
from mautrix.util.async_db import UpgradeTable
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper

from .db import upgrade_table


def _chunked(seq: list, size: int):
    """Yield successive `size`-length chunks of `seq`."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


DEFAULT_INTERVAL_MINUTES = 60
DEFAULT_FETCH_LIMIT = 500
DEFAULT_MAX_PER_MESSAGE = 50
DEFAULT_INCLUDE_TOPIC = True
DEFAULT_INCLUDE_MEMBERS = True
DEFAULT_NOTIFY_REMOVALS = True
# topic_collapse_length (per-(room, server) integer option):
#   > 0  collapse topics longer than N chars behind an expandable <details>
#   0    never collapse; show topics inline, in full
#  -1    always collapse, regardless of length
DEFAULT_TOPIC_COLLAPSE_LENGTH = 120
# Chars of topic shown as the teaser inside a collapsed <summary>.
TOPIC_SUMMARY_PREVIEW = 100
# Absolute cap on a single topic's length in any mode, so one pathological
# topic can't dominate a message (or blow past Matrix's per-event size limit).
TOPIC_HARD_LIMIT = 1000
# Ceiling on body + formatted_body bytes per message. Matrix's hard PDU limit
# is 65536 bytes; this leaves headroom for the event envelope and JSON
# escaping. Updates larger than this are split across multiple messages.
MAX_EVENT_CONTENT_BYTES = 48000
# Bytes reserved within the budget above for per-message header/section markup.
PAGE_HEADER_OVERHEAD = 1500
# Safety ceiling on messages emitted for a single (room, server) delivery, so
# a pathological mass-change can't flood a room with hundreds of messages.
MAX_MESSAGES_PER_DELIVERY = 50
# Maximum sleep between polling-loop iterations. The loop also wakes up
# sooner when a configured poll/delivery is due. Acts as the upper bound
# on how stale config changes can be in practice.
MAX_LOOP_SLEEP = 60
MIN_LOOP_SLEEP = 30
SETTABLE_KEYS: dict[str, type] = {
    "interval_minutes": int,
    "fetch_limit": int,
    "include_topic": bool,
    "include_members": bool,
    "notify_removals": bool,
    "max_per_message": int,
    "topic_collapse_length": int,
}

# Single source of truth for per-server columns in room_watched_servers.
# Add a new column here and every read path picks it up automatically.
WATCHED_SERVER_COLUMNS: tuple[str, ...] = (
    "server",
    "interval_minutes",
    "fetch_limit",
    "include_topic",
    "include_members",
    "notify_removals",
    "max_per_message",
    "topic_collapse_length",
)


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("allowed_users")
        helper.copy("admin_users")


class DirWatcherBot(Plugin):
    _poll_task: asyncio.Task | None
    _stop_event: asyncio.Event
    # Serializes poll/diff/enqueue and delivery so the background loop and
    # manual `!dirwatch check` can't run them concurrently for the same
    # server/room and produce duplicate rows or double-sent messages.
    _work_lock: asyncio.Lock

    # ── lifecycle ──────────────────────────────────────────────

    @classmethod
    def get_config_class(cls) -> type[BaseProxyConfig]:
        return Config

    @classmethod
    def get_db_upgrade_table(cls) -> UpgradeTable:
        return upgrade_table

    async def start(self) -> None:
        await super().start()
        self.config.load_and_update()
        self._poll_task = None
        self._stop_event = asyncio.Event()
        self._work_lock = asyncio.Lock()
        # Drop config for rooms we're no longer in (e.g. removed while the
        # bot was offline, so the live membership event was missed).
        await self._reconcile_rooms()
        self._start_polling()

    async def stop(self) -> None:
        await self._stop_polling()
        await super().stop()

    def on_external_config_update(self) -> None:
        self.config.load_and_update()
        # Restart the loop so new intervals take effect. Schedule the
        # restart on the event loop instead of doing it inline so we can
        # actually await the old task's cancellation.
        asyncio.create_task(self._restart_polling())

    async def _restart_polling(self) -> None:
        await self._stop_polling()
        self._stop_event = asyncio.Event()
        self._start_polling()

    async def _stop_polling(self) -> None:
        self._stop_event.set()
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        self._poll_task = None

    def _start_polling(self) -> None:
        self._poll_task = asyncio.create_task(self._poll_loop())

    # ── access control ─────────────────────────────────────────

    def _is_admin(self, user_id: str) -> bool:
        # Admin tier is locked until admin_users is populated.
        admins: list[str] = self.config["admin_users"] or []
        return any(fnmatch.fnmatch(user_id, pattern) for pattern in admins)

    def _is_allowed(self, user_id: str) -> bool:
        if self._is_admin(user_id):
            return True
        allowed: list[str] = self.config["allowed_users"] or []
        if not allowed:
            return True
        return any(fnmatch.fnmatch(user_id, pattern) for pattern in allowed)

    async def _check_access(self, evt: MessageEvent) -> bool:
        if not self._is_allowed(str(evt.sender)):
            await evt.reply("⛔ You don't have permission to use this command.")
            return False
        return True

    async def _check_admin(self, evt: MessageEvent) -> bool:
        if self._is_admin(str(evt.sender)):
            return True
        if not (self.config["admin_users"] or []):
            await evt.reply(
                "⛔ Admin commands are disabled. Set `admin_users` in the bot "
                "config to enable them."
            )
        else:
            await evt.reply("⛔ You don't have permission to use admin commands.")
        return False

    # ── room lifecycle ─────────────────────────────────────────

    async def _forget_room(self, room_id: str) -> bool:
        """Drop all state tied to a room: its watch config, any queued
        notifications, and its delivery schedule. Per-server data
        (directory snapshots, server poll state) is shared across rooms and
        is left untouched. Returns True if the room had any watch config."""
        async with self._work_lock:
            had = await self.database.fetchval(
                "DELETE FROM room_watched_servers WHERE matrix_room_id=$1 RETURNING 1",
                room_id,
            )
            await self.database.execute(
                "DELETE FROM pending_notifications WHERE matrix_room_id=$1", room_id,
            )
            await self.database.execute(
                "DELETE FROM poll_state WHERE poll_key=$1", f"deliver|{room_id}",
            )
        return had is not None

    async def _reconcile_rooms(self) -> None:
        """On startup, drop config for any configured room the bot is no
        longer joined to (covers removals that happened while offline)."""
        try:
            joined = {str(r) for r in await self.client.get_joined_rooms()}
        except Exception:
            self.log.warning(
                "Could not fetch joined rooms; skipping room reconciliation"
            )
            return
        configs = await self._get_all_room_configs()
        for room_id in configs:
            if room_id not in joined:
                self.log.info(
                    "No longer joined to %s; dropping its watch config", room_id
                )
                await self._forget_room(room_id)

    @event.on(EventType.ROOM_MEMBER)
    async def _on_member_event(self, evt: StateEvent) -> None:
        # Only react to the bot's own membership changing to leave/ban.
        if str(evt.state_key) != str(self.client.mxid):
            return
        if evt.content.membership in (Membership.LEAVE, Membership.BAN):
            room_id = str(evt.room_id)
            if await self._forget_room(room_id):
                self.log.info(
                    "Removed from %s (%s); dropped its watch config",
                    room_id, evt.content.membership,
                )

    # ── per-room DB helpers ────────────────────────────────────

    async def _get_room_servers(self, matrix_room_id: str) -> list[dict[str, Any]]:
        cols = ", ".join(WATCHED_SERVER_COLUMNS)
        rows = await self.database.fetch(
            f"SELECT {cols} FROM room_watched_servers WHERE matrix_room_id=$1",
            matrix_room_id,
        )
        return [self._row_to_server_dict(r) for r in rows]

    async def _add_room_server(
        self,
        matrix_room_id: str,
        server: str,
        interval_minutes: int = DEFAULT_INTERVAL_MINUTES,
        fetch_limit: int = DEFAULT_FETCH_LIMIT,
    ) -> None:
        await self.database.execute(
            "INSERT INTO room_watched_servers "
            "(matrix_room_id, server, interval_minutes, fetch_limit) "
            "VALUES ($1, $2, $3, $4) "
            "ON CONFLICT (matrix_room_id, server) DO UPDATE SET "
            "interval_minutes = EXCLUDED.interval_minutes, "
            "fetch_limit = EXCLUDED.fetch_limit",
            matrix_room_id, server, interval_minutes, fetch_limit,
        )

    async def _remove_room_server(self, matrix_room_id: str, server: str) -> bool:
        deleted = await self.database.fetchval(
            "DELETE FROM room_watched_servers "
            "WHERE matrix_room_id=$1 AND server=$2 RETURNING 1",
            matrix_room_id, server,
        )
        if deleted is None:
            return False
        # Drop any still-pending notifications for the unwatched server so
        # we don't deliver stale changes after the user opts out.
        await self.database.execute(
            "DELETE FROM pending_notifications "
            "WHERE matrix_room_id=$1 AND server=$2",
            matrix_room_id, server,
        )
        return True

    async def _set_room_server_option(
        self, matrix_room_id: str, server: str, key: str, value: str
    ) -> bool:
        if key not in SETTABLE_KEYS:
            return False

        cast = SETTABLE_KEYS[key]
        if cast is bool:
            parsed: Any = value.lower() in ("true", "1", "yes", "on")
        else:
            parsed = cast(value)
            if key == "topic_collapse_length" and parsed < -1:
                raise ValueError("topic_collapse_length must be -1, 0, or positive")

        # Column name is validated against SETTABLE_KEYS above, so it's safe
        # to interpolate into the query.
        updated = await self.database.fetchval(
            f"UPDATE room_watched_servers SET {key}=$1 "
            f"WHERE matrix_room_id=$2 AND server=$3 RETURNING 1",
            parsed, matrix_room_id, server,
        )
        return updated is not None

    async def _get_all_room_configs(self) -> dict[str, list[dict[str, Any]]]:
        cols = ", ".join(WATCHED_SERVER_COLUMNS)
        rows = await self.database.fetch(
            f"SELECT matrix_room_id, {cols} FROM room_watched_servers"
        )
        result: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            result.setdefault(r["matrix_room_id"], []).append(
                self._row_to_server_dict(r)
            )
        return result

    # ── pending notification helpers ───────────────────────────

    async def _get_rooms_watching_server(
        self, server: str
    ) -> list[tuple[str, bool]]:
        """Return (matrix_room_id, notify_removals) for rooms watching
        `server`."""
        rows = await self.database.fetch(
            "SELECT matrix_room_id, notify_removals "
            "FROM room_watched_servers WHERE server=$1",
            server,
        )
        return [(r["matrix_room_id"], bool(r["notify_removals"])) for r in rows]

    async def _enqueue_notifications(
        self,
        server: str,
        added_ids: set[str],
        removed_ids: set[str],
        current_map: dict[str, dict],
        prev_map: dict[str, dict],
    ) -> None:
        """Insert pending-notification rows for every room watching `server`."""
        target_rooms = await self._get_rooms_watching_server(server)
        if not target_rooms or not (added_ids or removed_ids):
            return

        now = int(time.time())
        records: list[tuple] = []

        for room_id, notify_removals in target_rooms:
            for rid in added_ids:
                room = current_map.get(rid, {})
                records.append((
                    server, room_id, "added", rid,
                    room.get("canonical_alias"),
                    room.get("name") or "",
                    room.get("topic") or "",
                    room.get("num_joined_members", 0),
                    now,
                ))
            # Removal tracking is server-global (snapshot/stats already
            # reflect it); here we just skip enqueuing the per-room
            # "removed" notifications when this room opted out.
            if not notify_removals:
                continue
            for rid in removed_ids:
                prev = prev_map.get(rid, {})
                records.append((
                    server, room_id, "removed", rid,
                    prev.get("alias") or "",
                    prev.get("name") or "",
                    prev.get("topic") or "",
                    prev.get("members", 0),
                    now,
                ))

        await self.database.executemany(
            "INSERT INTO pending_notifications "
            "(server, matrix_room_id, change_type, room_id, "
            "alias, name, topic, members, created_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            records,
        )

    async def _deliver_pending(self, matrix_room_id: str) -> None:
        """Deliver and clear all pending notifications for `matrix_room_id`,
        applying per-`(room, server)` formatting settings from
        `room_watched_servers`."""
        async with self._work_lock:
            rows = await self.database.fetch(
                "SELECT id, server, change_type, room_id, alias, name, topic, members "
                "FROM pending_notifications "
                "WHERE matrix_room_id=$1 ORDER BY server, change_type, created_at",
                matrix_room_id,
            )
            if not rows:
                return

            per_server_rows = await self.database.fetch(
                "SELECT server, include_topic, include_members, max_per_message, "
                "topic_collapse_length "
                "FROM room_watched_servers WHERE matrix_room_id=$1",
                matrix_room_id,
            )
            per_server_settings: dict[str, dict[str, Any]] = {
                r["server"]: {
                    "include_topic": r["include_topic"],
                    "include_members": r["include_members"],
                    "max_per_message": r["max_per_message"],
                    "topic_collapse_length": r["topic_collapse_length"],
                }
                for r in per_server_rows
            }

            by_server: dict[str, dict[str, list[dict[str, Any]]]] = {}
            ids_to_delete: list[int] = []
            for r in rows:
                ids_to_delete.append(r["id"])
                bucket = by_server.setdefault(r["server"], {"added": [], "removed": []})
                bucket[r["change_type"]].append({
                    "room_id": r["room_id"],
                    "alias": r["alias"],
                    "name": r["name"],
                    "topic": r["topic"],
                    "members": r["members"],
                })

            for server, changes in by_server.items():
                cfg = per_server_settings.get(server, {})
                include_topic = bool(cfg.get("include_topic", DEFAULT_INCLUDE_TOPIC))
                include_members = bool(cfg.get("include_members", DEFAULT_INCLUDE_MEMBERS))
                max_per = int(cfg.get("max_per_message", DEFAULT_MAX_PER_MESSAGE)) or 0
                collapse_length = int(
                    cfg.get("topic_collapse_length", DEFAULT_TOPIC_COLLAPSE_LENGTH)
                )

                added = changes["added"]
                removed = changes["removed"]
                if not added and not removed:
                    continue

                # Flatten to rows (added first, then removed) and split into
                # one-or-more messages that each stay within the per-message
                # entry count and the Matrix event-size budget.
                rows_to_send = (
                    [("added", e) for e in added]
                    + [("removed", e) for e in removed]
                )
                pages = self._paginate_rows(
                    rows_to_send, max_per,
                    include_topic, include_members, collapse_length,
                )

                omitted = 0
                if len(pages) > MAX_MESSAGES_PER_DELIVERY:
                    omitted = sum(len(p) for p in pages[MAX_MESSAGES_PER_DELIVERY:])
                    pages = pages[:MAX_MESSAGES_PER_DELIVERY]

                page_count = len(pages)
                for idx, page in enumerate(pages, 1):
                    note = omitted if idx == page_count else 0
                    body, formatted = self._render_page(
                        server, page, idx, page_count, include_members, note,
                    )
                    await self.client.send_message_event(
                        RoomID(matrix_room_id),
                        EventType.ROOM_MESSAGE,
                        {
                            "msgtype": "m.text",
                            "body": body,
                            "format": "org.matrix.custom.html",
                            "formatted_body": formatted,
                        },
                    )

            if ids_to_delete:
                # Portable IN (...) list in place of `= ANY($1::bigint[])`,
                # which SQLite can't parse. Chunk to stay under SQLite's
                # conservative 999-bound-variable limit on older builds.
                for chunk in _chunked(ids_to_delete, 900):
                    placeholders = ", ".join(
                        f"${i}" for i in range(1, 1 + len(chunk))
                    )
                    await self.database.execute(
                        f"DELETE FROM pending_notifications "
                        f"WHERE id IN ({placeholders})",
                        *chunk,
                    )

    # ── polling loop ───────────────────────────────────────────

    async def _poll_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                sleep_for = await self._run_poll_cycle()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=sleep_for)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass

    async def _run_poll_cycle(self) -> float:
        """Run one fetch+deliver cycle and return how long to sleep."""
        now = time.time()
        min_sleep: float = MAX_LOOP_SLEEP

        room_configs = await self._get_all_room_configs()

        # Per server: largest fetch_limit and shortest interval anyone asked for.
        servers_to_poll: dict[str, int] = {}
        server_intervals: dict[str, int] = {}
        for srvs in room_configs.values():
            for srv in srvs:
                servers_to_poll[srv["server"]] = max(
                    servers_to_poll.get(srv["server"], 0), srv["fetch_limit"],
                )
                iv = srv["interval_minutes"] * 60
                prior = server_intervals.get(srv["server"])
                server_intervals[srv["server"]] = (
                    iv if prior is None else min(prior, iv)
                )

        # Phase 1: poll directories whose interval has elapsed.
        for server, limit in servers_to_poll.items():
            poll_key = f"server|{server}"
            interval = server_intervals.get(server, 3600)
            last_polled = await self._get_last_polled(poll_key)

            if now - last_polled >= interval:
                try:
                    await self._poll_server(server, limit)
                except Exception:
                    self.log.exception("Error polling %s", server)
                await self._set_last_polled(poll_key, int(now))

            remaining = interval - (now - last_polled)
            if remaining > 0:
                min_sleep = min(min_sleep, remaining)

        # Phase 2: deliver pending notifications per room on its own schedule.
        for room_id, srvs in room_configs.items():
            deliver_iv = min(
                (s["interval_minutes"] * 60 for s in srvs), default=3600,
            )
            min_sleep = min(
                min_sleep, await self._maybe_deliver(room_id, deliver_iv, now),
            )

        return max(MIN_LOOP_SLEEP, min_sleep)

    async def _maybe_deliver(
        self, room_id: str, interval_s: int, now: float,
    ) -> float:
        """Deliver pending if `interval_s` has elapsed. Return remaining time."""
        poll_key = f"deliver|{room_id}"
        last_delivered = await self._get_last_polled(poll_key)
        if now - last_delivered >= interval_s:
            try:
                await self._deliver_pending(room_id)
            except Exception:
                self.log.exception("Error delivering to %s", room_id)
            await self._set_last_polled(poll_key, int(now))
        remaining = interval_s - (now - last_delivered)
        return remaining if remaining > 0 else MAX_LOOP_SLEEP

    # ── directory fetching ─────────────────────────────────────

    async def _fetch_directory(
        self, server: str, limit: int = DEFAULT_FETCH_LIMIT
    ) -> list[dict[str, Any]]:
        rooms: list[dict[str, Any]] = []
        since: str | None = None
        batch_limit = min(limit, 100)

        while len(rooms) < limit:
            body: dict[str, Any] = {"limit": batch_limit}
            if since:
                body["since"] = since

            resp = await self.client.api.request(
                method="POST",
                path="_matrix/client/v3/publicRooms",
                content=body,
                query_params={"server": server},
            )

            chunk = resp.get("chunk", [])
            rooms.extend(chunk)

            next_batch = resp.get("next_batch")
            if not next_batch or not chunk:
                break
            since = next_batch

        return rooms[:limit]

    # ── poll + diff (no direct notification) ───────────────────

    async def _poll_server(self, server: str, limit: int) -> None:
        """Fetch directory, update snapshot, enqueue notifications."""
        self.log.info("Polling directory for %s (limit=%d)", server, limit)

        # Fetch outside the lock — concurrent fetches are wasteful at worst,
        # never incorrect. The snapshot read→write→enqueue below must be
        # atomic, so it runs under the lock: that's what keeps two concurrent
        # polls of the same server from both diffing the same baseline and
        # enqueueing the same change twice.
        directory_rooms = await self._fetch_directory(server, limit)

        async with self._work_lock:
            now = int(time.time())

            current_map: dict[str, dict] = {
                room["room_id"]: room
                for room in directory_rooms
                if room.get("room_id")
            }
            current_ids = set(current_map)

            prev_rows = await self.database.fetch(
                "SELECT room_id, alias, name, topic, members "
                "FROM directory_snapshot WHERE server=$1 AND removed=FALSE",
                server,
            )
            prev_ids = {row["room_id"] for row in prev_rows}
            prev_map = {
                row["room_id"]: {
                    "alias": row["alias"],
                    "name": row["name"],
                    "topic": row["topic"],
                    "members": row["members"],
                }
                for row in prev_rows
            }

            added_ids = current_ids - prev_ids
            removed_ids = prev_ids - current_ids

            if removed_ids:
                # SQLite has no array type, so `= ANY($n::text[])` isn't
                # portable. Build IN (...) lists with positional params after
                # the fixed now/server params, chunked to stay under SQLite's
                # conservative 999-bound-variable limit on older builds.
                for chunk in _chunked(list(removed_ids), 900):
                    placeholders = ", ".join(
                        f"${i}" for i in range(3, 3 + len(chunk))
                    )
                    await self.database.execute(
                        "UPDATE directory_snapshot SET removed=TRUE, last_seen=$1 "
                        f"WHERE server=$2 AND room_id IN ({placeholders})",
                        now, server, *chunk,
                    )

            if current_ids:
                upsert_rows = [
                    (
                        server,
                        rid,
                        current_map[rid].get("canonical_alias"),
                        current_map[rid].get("name") or "",
                        current_map[rid].get("topic") or "",
                        current_map[rid].get("num_joined_members", 0),
                        now,
                        now,
                    )
                    for rid in current_ids
                ]
                await self.database.executemany(
                    "INSERT INTO directory_snapshot "
                    "(server, room_id, alias, name, topic, members, first_seen, last_seen, removed) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, FALSE) "
                    "ON CONFLICT (server, room_id) DO UPDATE SET "
                    "alias = EXCLUDED.alias, name = EXCLUDED.name, topic = EXCLUDED.topic, "
                    "members = EXCLUDED.members, last_seen = EXCLUDED.last_seen, removed = FALSE",
                    upsert_rows,
                )

            # First run for this server: store baseline only, no notifications.
            if not prev_rows:
                self.log.info(
                    "First snapshot for %s: %d rooms stored, skipping notification",
                    server, len(current_ids),
                )
                return

            if added_ids or removed_ids:
                self.log.info(
                    "Changes for %s: +%d -%d, enqueueing notifications",
                    server, len(added_ids), len(removed_ids),
                )
                await self._enqueue_notifications(
                    server, added_ids, removed_ids, current_map, prev_map,
                )
            else:
                self.log.debug("No directory changes for %s", server)

    # ── formatting helpers ─────────────────────────────────────

    @staticmethod
    def _label(entry: dict, topic: bool, members: bool) -> str:
        alias = entry.get("alias") or ""
        name = entry.get("name") or ""
        room_id = entry.get("room_id") or "???"
        display = alias or name or room_id
        parts = [display]
        if name and alias:
            parts.append(f"({name})")
        if members:
            parts.append(f"[{entry.get('members', '?')} members]")
        if topic and entry.get("topic"):
            # Plain text can't collapse, so it always carries the full topic
            # (capped) regardless of the collapse setting — the HTML body is
            # where the expandable widget lives.
            raw = str(entry["topic"]).replace("\n", " ")
            parts.append(f"— {raw[:TOPIC_HARD_LIMIT]}")
        return " ".join(parts)

    @staticmethod
    def _label_html(
        entry: dict, topic: bool, members: bool, collapse_length: int
    ) -> str:
        alias = entry.get("alias") or ""
        name = entry.get("name") or ""
        room_id = entry.get("room_id") or "???"
        link_target = alias or room_id
        display = alias or name or room_id
        href = html.escape(link_target, quote=True)
        parts = [f'<a href="https://matrix.to/#/{href}">{html.escape(display)}</a>']
        if name and alias:
            parts.append(f"({html.escape(name)})")
        if members:
            parts.append(f"<em>[{entry.get('members', '?')} members]</em>")
        if topic and entry.get("topic"):
            raw = str(entry["topic"]).replace("\n", " ")
            should_collapse = (
                collapse_length == -1
                or (collapse_length > 0 and len(raw) > collapse_length)
            )
            if should_collapse:
                suffix = "…" if len(raw) > TOPIC_SUMMARY_PREVIEW else ""
                preview = html.escape(raw[:TOPIC_SUMMARY_PREVIEW])
                full = html.escape(raw[:TOPIC_HARD_LIMIT])
                parts.append(
                    f"<details><summary>— {preview}{suffix}</summary>{full}</details>"
                )
            else:
                parts.append(f"— {html.escape(raw[:TOPIC_HARD_LIMIT])}")
        return " ".join(parts)

    def _render_entry(
        self,
        kind: str,
        entry: dict,
        include_topic: bool,
        include_members: bool,
        collapse_length: int,
    ) -> tuple[str, str]:
        """Render one change row in both plain-text and HTML form."""
        prefix = "+" if kind == "added" else "-"
        text = f"  {prefix} {self._label(entry, include_topic, include_members)}"
        html_li = (
            f"<li>{self._label_html(entry, include_topic, include_members, collapse_length)}</li>"
        )
        return text, html_li

    def _paginate_rows(
        self,
        rows: list[tuple[str, dict]],
        max_per: int,
        include_topic: bool,
        include_members: bool,
        collapse_length: int,
    ) -> list[list[tuple[str, dict, str, str]]]:
        """Pack (kind, entry) rows into pages bounded by both the per-message
        entry count (`max_per`, 0 = unlimited) and the event-size budget.

        Each page is a list of (kind, entry, text_line, html_li). A single
        entry is always small enough to fit a page on its own (topics are
        capped at TOPIC_HARD_LIMIT), so pagination always makes progress.
        """
        entry_budget = MAX_EVENT_CONTENT_BYTES - PAGE_HEADER_OVERHEAD
        pages: list[list[tuple[str, dict, str, str]]] = []
        current: list[tuple[str, dict, str, str]] = []
        current_bytes = 0

        for kind, entry in rows:
            text, html_li = self._render_entry(
                kind, entry, include_topic, include_members, collapse_length,
            )
            cost = len(text.encode("utf-8")) + len(html_li.encode("utf-8"))
            over_count = max_per > 0 and len(current) >= max_per
            over_size = bool(current) and (current_bytes + cost) > entry_budget
            if over_count or over_size:
                pages.append(current)
                current = []
                current_bytes = 0
            current.append((kind, entry, text, html_li))
            current_bytes += cost

        if current:
            pages.append(current)
        return pages

    def _render_page(
        self,
        server: str,
        page: list[tuple[str, dict, str, str]],
        page_idx: int,
        page_count: int,
        include_members: bool,
        omitted_note: int = 0,
    ) -> tuple[str, str]:
        """Render one page of rows to (body, formatted_body)."""
        part = f" (part {page_idx}/{page_count})" if page_count > 1 else ""
        lines = [f"📡 Directory changes for **{server}**{part}:"]
        html_lines = [
            f"<h4>📡 Directory changes for <strong>{html.escape(server)}</strong>"
            f"{html.escape(part)}</h4>"
        ]

        for kind, emoji, label in (("added", "🟢", "Added"), ("removed", "🔴", "Removed")):
            items = [(t, h) for (k, _e, t, h) in page if k == kind]
            if not items:
                continue
            count = len(items)
            plural = "" if count == 1 else "s"
            lines.append(f"\n{emoji} **{label}** ({count} room{plural}):")
            html_lines.append(
                f"<p>{emoji} <strong>{label}</strong> ({count} room{plural}):</p><ul>"
            )
            for text, html_li in items:
                lines.append(text)
                html_lines.append(html_li)
            html_lines.append("</ul>")

        if omitted_note > 0:
            lines.append(f"\n… and {omitted_note} more not shown")
            html_lines.append(f"<p><em>… and {omitted_note} more not shown</em></p>")

        return "\n".join(lines), "\n".join(html_lines)

    # ── commands ───────────────────────────────────────────────

    @command.new(name="dirwatch", help="Room directory watcher commands")
    async def dirwatch(self, evt: MessageEvent) -> None:
        await evt.reply(
            "**Directory watcher commands:**\n"
            "• `!dirwatch server add <server> [interval_min] [limit]`\n"
            "• `!dirwatch server remove <server>`\n"
            "• `!dirwatch server list`\n"
            "• `!dirwatch server set <server> <key> <value>`\n"
            "• `!dirwatch check [server]` — poll now and deliver pending\n"
            "• `!dirwatch status` — show what's being watched\n"
            "• `!dirwatch stats [server]` — show add/remove totals\n"
            "• `!dirwatch admin overview` — every room's config (admin only)"
        )

    # ── !dirwatch server <subcommand> ──────────────────────────

    @dirwatch.subcommand("server", help="Manage watched servers for this room")
    async def server_cmd(self, evt: MessageEvent) -> None:
        pass

    @server_cmd.subcommand("add", help="Watch a server in this room")
    @command.argument("server", required=True)
    @command.argument("interval", required=False)
    @command.argument("limit", required=False, pass_raw=True)
    async def server_add(
        self,
        evt: MessageEvent,
        server: str,
        interval: str | None = None,
        limit: str | None = None,
    ) -> None:
        if not await self._check_access(evt):
            return

        server = server.strip()
        interval_mins = (
            int(interval) if interval and interval.strip().isdigit()
            else DEFAULT_INTERVAL_MINUTES
        )
        fetch_limit = (
            int(limit.strip()) if limit and limit.strip().isdigit()
            else DEFAULT_FETCH_LIMIT
        )

        await self._add_room_server(
            str(evt.room_id), server, interval_mins, fetch_limit,
        )
        await evt.reply(
            f"✅ Now watching **{server}** in this room "
            f"(every {interval_mins}m, limit {fetch_limit})"
        )

    @server_cmd.subcommand("remove", help="Stop watching a server in this room")
    @command.argument("server", pass_raw=True, required=True)
    async def server_remove(self, evt: MessageEvent, server: str) -> None:
        if not await self._check_access(evt):
            return

        server = server.strip()
        if await self._remove_room_server(str(evt.room_id), server):
            await evt.reply(f"✅ Stopped watching **{server}** in this room.")
        else:
            await evt.reply(f"⚠️ **{server}** was not being watched in this room.")

    @staticmethod
    def _row_to_server_dict(row: Any) -> dict[str, Any]:
        """Map a `room_watched_servers` row to the per-server dict shape used
        by formatters and config readers. Pairs with WATCHED_SERVER_COLUMNS
        so all read paths share one schema."""
        return {col: row[col] for col in WATCHED_SERVER_COLUMNS}

    @staticmethod
    def _server_line(srv: dict) -> str:
        """One-line summary of a (room, server) watch config, used by both
        `server list` and `overview`."""
        topic_icon = "✓" if srv["include_topic"] else "✗"
        members_icon = "✓" if srv["include_members"] else "✗"
        removals_icon = "✓" if srv.get("notify_removals", True) else "✗"
        tcl = srv["topic_collapse_length"]
        if tcl == 0:
            collapse_desc = "off"
        elif tcl == -1:
            collapse_desc = "always"
        else:
            collapse_desc = f">{tcl}"
        return (
            f"**{srv['server']}** — every {srv['interval_minutes']}m, "
            f"limit {srv['fetch_limit']}, "
            f"topic: {topic_icon}, members: {members_icon}, "
            f"removals: {removals_icon}, "
            f"collapse: {collapse_desc}, max/msg: {srv['max_per_message']}"
        )

    @server_cmd.subcommand("list", help="List servers watched in this room")
    async def server_list(self, evt: MessageEvent) -> None:
        if not await self._check_access(evt):
            return

        room_servers = await self._get_room_servers(str(evt.room_id))
        if not room_servers:
            await evt.reply(
                "_No servers watched in this room._ "
                "Use `!dirwatch server add <server>` to start."
            )
            return

        lines = ["**Servers watched in this room:**"]
        for srv in room_servers:
            lines.append(f"• {self._server_line(srv)}")
        await evt.reply("\n".join(lines))

    @server_cmd.subcommand("set", help="Set an option for a watched server")
    @command.argument("server", required=True)
    @command.argument("key", required=True)
    @command.argument("value", pass_raw=True, required=True)
    async def server_set(
        self, evt: MessageEvent, server: str, key: str, value: str,
    ) -> None:
        if not await self._check_access(evt):
            return

        server = server.strip()
        key = key.strip()
        value = value.strip()

        if key not in SETTABLE_KEYS:
            await evt.reply(
                f"⚠️ Unknown option `{key}`. Valid options: "
                f"`{'`, `'.join(SETTABLE_KEYS)}`"
            )
            return

        try:
            ok = await self._set_room_server_option(str(evt.room_id), server, key, value)
        except (ValueError, TypeError):
            await evt.reply(f"⚠️ Invalid value `{value}` for `{key}`.")
            return

        if ok:
            await evt.reply(f"✅ Set **{key}** = `{value}` for **{server}** in this room.")
        else:
            await evt.reply(f"⚠️ **{server}** is not being watched in this room.")

    # ── !dirwatch check ────────────────────────────────────────

    @dirwatch.subcommand("check", help="Manually check a server's directory now")
    @command.argument("server", pass_raw=True, required=False)
    async def check(self, evt: MessageEvent, server: str | None = None) -> None:
        if not await self._check_access(evt):
            return

        room_id = str(evt.room_id)
        room_servers = await self._get_room_servers(room_id)

        targets: dict[str, int] = {}
        if server and server.strip():
            s = server.strip()
            room_match = next((srv for srv in room_servers if srv["server"] == s), None)
            targets[s] = room_match["fetch_limit"] if room_match else DEFAULT_FETCH_LIMIT
        else:
            for srv in room_servers:
                targets[srv["server"]] = max(
                    targets.get(srv["server"], 0), srv["fetch_limit"],
                )

        if not targets:
            await evt.reply(
                "No servers watched in this room. "
                "Use `!dirwatch server add <server>` first, or pass a server "
                "name to check ad-hoc: `!dirwatch check example.com`."
            )
            return

        await evt.reply(f"⏳ Checking directory for: {', '.join(targets)}...")

        for s, limit in targets.items():
            try:
                await self._poll_server(s, limit)
                await self._set_last_polled(f"server|{s}", int(time.time()))
            except Exception as e:
                self.log.exception("Error checking %s", s)
                await evt.reply(f"❌ Error checking **{s}**: {e}")

        try:
            await self._deliver_pending(room_id)
        except Exception:
            self.log.exception("Error delivering to %s", room_id)

        await evt.reply("✅ Directory check complete.")

    # ── !dirwatch status ───────────────────────────────────────

    @dirwatch.subcommand("status", help="Show current watch status")
    async def status(self, evt: MessageEvent) -> None:
        if not await self._check_access(evt):
            return

        room_id = str(evt.room_id)
        room_servers = await self._get_room_servers(room_id)

        if not room_servers:
            await evt.reply("No servers watched in this room.")
            return

        lines = ["**Directory Watcher Status:**"]
        for srv in room_servers:
            last = await self._get_last_polled(f"server|{srv['server']}")
            count = await self.database.fetchval(
                "SELECT COUNT(*) FROM directory_snapshot "
                "WHERE server=$1 AND removed=FALSE",
                srv["server"],
            ) or 0
            pending = await self.database.fetchval(
                "SELECT COUNT(*) FROM pending_notifications "
                "WHERE matrix_room_id=$1 AND server=$2",
                room_id, srv["server"],
            ) or 0

            if last > 0:
                mins = int(time.time() - last) // 60
                line = (
                    f"• **{srv['server']}**: {count} rooms, "
                    f"last polled {mins}m ago"
                )
                if pending:
                    line += f", {pending} pending"
                lines.append(line)
            else:
                lines.append(f"• **{srv['server']}**: not yet polled")

        await evt.reply("\n".join(lines))

    # ── !dirwatch stats ────────────────────────────────────────

    @dirwatch.subcommand("stats", help="Show add/remove stats for a server")
    @command.argument("server", pass_raw=True, required=False)
    async def stats(self, evt: MessageEvent, server: str | None = None) -> None:
        if not await self._check_access(evt):
            return

        room_servers = await self._get_room_servers(str(evt.room_id))
        all_servers = {srv["server"] for srv in room_servers}
        target = server.strip() if server else None

        if not target:
            if len(all_servers) == 1:
                target = next(iter(all_servers))
            elif not all_servers:
                await evt.reply(
                    "No servers watched in this room. "
                    "Use `!dirwatch stats <server>` for an ad-hoc lookup."
                )
                return
            else:
                await evt.reply(
                    "Specify a server: `!dirwatch stats <server>`\n"
                    f"Available: {', '.join(sorted(all_servers))}"
                )
                return

        active = await self.database.fetchval(
            "SELECT COUNT(*) FROM directory_snapshot "
            "WHERE server=$1 AND removed=FALSE",
            target,
        ) or 0
        removed = await self.database.fetchval(
            "SELECT COUNT(*) FROM directory_snapshot "
            "WHERE server=$1 AND removed=TRUE",
            target,
        ) or 0

        await evt.reply(
            f"**Stats for {target}:**\n"
            f"• Currently listed: {active}\n"
            f"• Previously removed: {removed}\n"
            f"• Total ever seen: {active + removed}"
        )

    # ── !dirwatch admin <subcommand> ───────────────────────────

    async def _room_display(self, room_id: str) -> str:
        """Best-effort human label for a room: '<name> (<linked id>)', or just
        the linked id if the name can't be resolved (bot not in room, no name
        set, etc.). The id is a matrix.to link so it's clickable."""
        name = None
        try:
            content = await self.client.get_state_event(
                RoomID(room_id), EventType.ROOM_NAME,
            )
            name = getattr(content, "name", None)
        except Exception:
            name = None
        link = f"[`{room_id}`](https://matrix.to/#/{room_id})"
        return f"**{name}** ({link})" if name else link

    @dirwatch.subcommand("admin", help="Admin commands (cross-room; admin only)")
    async def admin_cmd(self, evt: MessageEvent) -> None:
        if not await self._check_admin(evt):
            return
        await evt.reply(
            "**Admin commands:**\n"
            "• `!dirwatch admin overview` — watch config across all rooms"
        )

    @admin_cmd.subcommand(
        "overview", help="Show watch config across all rooms"
    )
    async def admin_overview(self, evt: MessageEvent) -> None:
        if not await self._check_admin(evt):
            return

        configs = await self._get_all_room_configs()
        if not configs:
            await evt.reply("No rooms have any servers configured.")
            return

        total_watches = sum(len(v) for v in configs.values())
        distinct_servers = {s["server"] for v in configs.values() for s in v}
        header = (
            f"**Directory watcher — {len(configs)} room(s), "
            f"{total_watches} watch(es), {len(distinct_servers)} distinct server(s):**"
        )

        budget = 30000
        lines = [header]
        size = len(header)
        truncated_rooms = 0
        for i, (room_id, srvs) in enumerate(sorted(configs.items())):
            block_lines = [f"\n{await self._room_display(room_id)}"]
            for srv in srvs:
                block_lines.append(f"  • {self._server_line(srv)}")
            block = "\n".join(block_lines)
            if size + len(block) > budget and len(lines) > 1:
                truncated_rooms = len(configs) - i
                break
            lines.append(block)
            size += len(block) + 1

        if truncated_rooms:
            lines.append(
                f"\n_… and {truncated_rooms} more room(s) not shown (output truncated)_"
            )

        await evt.reply("\n".join(lines))

    # ── DB helpers ─────────────────────────────────────────────

    async def _get_last_polled(self, poll_key: str) -> float:
        val = await self.database.fetchval(
            "SELECT last_polled FROM poll_state WHERE poll_key=$1", poll_key,
        )
        return float(val) if val is not None else 0.0

    async def _set_last_polled(self, poll_key: str, ts: int) -> None:
        await self.database.execute(
            "INSERT INTO poll_state (poll_key, last_polled) VALUES ($1, $2) "
            "ON CONFLICT (poll_key) DO UPDATE SET last_polled = EXCLUDED.last_polled",
            poll_key, ts,
        )