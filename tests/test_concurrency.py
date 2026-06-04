"""Exercises the real DirWatcherBot poll/enqueue/deliver code against an
in-memory fake DB to prove dedup holds and the lock kills the race."""
import asyncio
import sys
import types

# ── stub out maubot/mautrix so we can import the plugin module ──
def _mk(name):
    class _Auto:
        def __init__(self, *a, **k): pass
        def __call__(self, *a, **k):
            return a[0] if (len(a) == 1 and callable(a[0]) and not k) else self
        def __getattr__(self, _n): return _Auto()
    class _StubMod(__import__("types").ModuleType):
        def __getattr__(self, nm):
            v = _Auto(); object.__setattr__(self, nm, v); return v
    import sys as _ss
    m = _StubMod(name); _ss.modules[name] = m; return m

maubot = _mk("maubot")
class MessageEvent: ...
class Plugin: ...
maubot.MessageEvent = MessageEvent
maubot.Plugin = Plugin

handlers = _mk("maubot.handlers")
class FakeCommand:
    def __init__(self, fn=None):
        self.fn = fn
    def subcommand(self, *a, **k):
        return lambda fn: FakeCommand(fn)
class CommandNS:
    def new(self, *a, **k):
        return lambda fn: FakeCommand(fn)
    def argument(self, *a, **k):
        return lambda fn: fn
handlers.command = CommandNS()

mt = _mk("mautrix.types")
mt.EventType = types.SimpleNamespace(ROOM_MESSAGE="m.room.message")
mt.RoomID = lambda x: x
adb = _mk("mautrix.util.async_db")
class UpgradeTable:
    def register(self, *a, **k):
        return lambda fn: fn
adb.UpgradeTable = UpgradeTable
adb.Connection = object
cfgmod = _mk("mautrix.util.config")
class BaseProxyConfig: ...
class ConfigUpdateHelper: ...
cfgmod.BaseProxyConfig = BaseProxyConfig
cfgmod.ConfigUpdateHelper = ConfigUpdateHelper
_mk("mautrix")
_mk("mautrix.util")

import os
import importlib
import sys as _s
_s.modules["maubot.handlers"].event = type("E", (), {"on": staticmethod(lambda *a, **k: (lambda fn: fn))})()
_mt = _s.modules["mautrix.types"]
_mt.Membership = type("M", (), {"LEAVE":"leave","BAN":"ban","JOIN":"join","INVITE":"invite"})
_mt.StateEvent = object
_mt.EventType.ROOM_MEMBER = "m.room.member"
import os
_here = os.path.dirname(os.path.abspath(__file__))
for _cand in (_here, os.path.dirname(_here)):
    if os.path.isdir(os.path.join(_cand, "dirwatcher")):
        sys.path.insert(0, _cand)
        break
import dirwatcher as dw


class FakeDB:
    """Recognizes the handful of fixed queries the plugin issues."""
    def __init__(self):
        self.snapshot = {}            # (server, room_id) -> dict
        self.pending = []             # list of dict (with id)
        self.rws = {}                 # (room, server) -> dict
        self.poll_state = {}          # poll_key -> last_polled
        self._next_id = 1

    async def fetch(self, q, *args):
        await asyncio.sleep(0)        # yield: lets the other task interleave
        if "FROM directory_snapshot WHERE server=$1 AND removed=FALSE" in q:
            server = args[0]
            return [dict(r) for (s, _), r in self.snapshot.items()
                    if s == server and not r["removed"]]
        if "FROM pending_notifications" in q and "matrix_room_id=$1" in q:
            room = args[0]
            return sorted(
                [dict(r) for r in self.pending if r["matrix_room_id"] == room],
                key=lambda r: (r["server"], r["change_type"], r["created_at"]),
            )
        if "FROM room_watched_servers WHERE matrix_room_id=$1" in q:
            room = args[0]
            return [dict(r) for (rm, _), r in self.rws.items() if rm == room]
        if "matrix_room_id FROM room_watched_servers WHERE server=$1" in q:
            server = args[0]
            return [{"matrix_room_id": rm} for (rm, s) in self.rws if s == server]
        if "FROM room_watched_servers" in q:
            return [dict(r) for r in self.rws.values()]
        raise AssertionError(f"unhandled fetch: {q[:60]}")

    async def fetchval(self, q, *args):
        await asyncio.sleep(0)
        if "FROM poll_state WHERE poll_key=$1" in q:
            return self.poll_state.get(args[0])
        raise AssertionError(f"unhandled fetchval: {q[:60]}")

    async def execute(self, q, *args):
        await asyncio.sleep(0)
        if "UPDATE directory_snapshot SET removed=TRUE" in q:
            _, server, ids = args
            for rid in ids:
                if (server, rid) in self.snapshot:
                    self.snapshot[(server, rid)]["removed"] = True
            return
        if "DELETE FROM pending_notifications WHERE id = ANY" in q:
            ids = set(args[0])
            self.pending = [r for r in self.pending if r["id"] not in ids]
            return
        if "INSERT INTO poll_state" in q:
            self.poll_state[args[0]] = args[1]
            return
        raise AssertionError(f"unhandled execute: {q[:60]}")

    async def executemany(self, q, records):
        await asyncio.sleep(0)
        if "INSERT INTO directory_snapshot" in q:
            for rec in records:
                server, rid, alias, name, topic, members, first, last = rec
                self.snapshot[(server, rid)] = {
                    "room_id": rid, "alias": alias, "name": name,
                    "topic": topic, "members": members, "removed": False,
                }
            return
        if "INSERT INTO pending_notifications" in q:
            for rec in records:
                (server, room, ctype, rid, alias, name, topic, members, created) = rec
                self.pending.append({
                    "id": self._next_id, "server": server,
                    "matrix_room_id": room, "change_type": ctype,
                    "room_id": rid, "alias": alias, "name": name,
                    "topic": topic, "members": members, "created_at": created,
                })
                self._next_id += 1
            return
        raise AssertionError(f"unhandled executemany: {q[:60]}")


class FakeClient:
    def __init__(self):
        self.sent = []
    async def send_message_event(self, room, etype, content):
        await asyncio.sleep(0)
        self.sent.append((room, content["body"]))


class FakeLog:
    def info(self, *a, **k): pass
    def debug(self, *a, **k): pass
    def exception(self, *a, **k): pass


def make_bot(directory, real_lock=True):
    bot = dw.DirWatcherBot.__new__(dw.DirWatcherBot)
    bot.database = FakeDB()
    bot.client = FakeClient()
    bot.log = FakeLog()
    bot.config = {"allowed_users": []}
    bot._stop_event = asyncio.Event()
    bot._poll_task = None
    if real_lock:
        bot._work_lock = asyncio.Lock()
    else:
        # A no-op "lock" that doesn't actually serialize — demonstrates the
        # race the real lock prevents.
        class NoLock:
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False
        bot._work_lock = NoLock()
    bot._directory = directory
    async def fake_fetch(server, limit):
        await asyncio.sleep(0)
        return list(bot._directory)
    bot._fetch_directory = fake_fetch
    return bot


async def scenario(real_lock):
    # Baseline already exists (so it's not a first-run), room !old present.
    directory = [
        {"room_id": "!old:s", "canonical_alias": "#old:s", "name": "Old", "topic": "", "num_joined_members": 5},
        {"room_id": "!new:s", "canonical_alias": "#new:s", "name": "New", "topic": "", "num_joined_members": 3},
    ]
    bot = make_bot(directory, real_lock=real_lock)
    # seed snapshot baseline with only !old
    bot.database.snapshot[("s", "!old:s")] = {
        "room_id": "!old:s", "alias": "#old:s", "name": "Old",
        "topic": "", "members": 5, "removed": False,
    }
    # two rooms watch server "s"
    for room in ("!R1:h", "!R2:h"):
        bot.database.rws[(room, "s")] = {
            "matrix_room_id": room, "server": "s",
            "interval_minutes": 60, "fetch_limit": 500,
            "include_topic": True, "include_members": True, "max_per_message": 50,
            "topic_collapse_length": 120,
        }

    # Race: background loop poll + manual check poll, same server, concurrently.
    await asyncio.gather(bot._poll_server("s", 500), bot._poll_server("s", 500))

    pending_new = [r for r in bot.database.pending if r["room_id"] == "!new:s"]
    # Deliver to both rooms, also raced.
    await asyncio.gather(bot._deliver_pending("!R1:h"), bot._deliver_pending("!R2:h"))
    return len(pending_new), bot.client.sent


async def main():
    n_locked, sent_locked = await scenario(real_lock=True)
    n_nolock, sent_nolock = await scenario(real_lock=False)

    print(f"WITH real lock:    pending rows for !new = {n_locked} "
          f"(expect 2: one per watching room), messages sent = {len(sent_locked)}")
    print(f"WITHOUT lock:      pending rows for !new = {n_nolock} "
          f"(race inflates this), messages sent = {len(sent_nolock)}")

    ok = (n_locked == 2)
    print("\nRESULT:", "PASS — lock yields exactly one enqueue per room, no dupes"
          if ok else "FAIL")
    # Also assert no double-send with the lock: 2 rooms, 1 server each => 2 msgs
    print("double-send check (locked):",
          "PASS" if len(sent_locked) == 2 else f"FAIL ({len(sent_locked)})")
    sys.exit(0 if ok and len(sent_locked) == 2 else 1)


asyncio.run(main())
