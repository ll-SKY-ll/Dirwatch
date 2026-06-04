"""Verifies the admin-gated cross-room overview: tiers, listing, name resolution."""
import asyncio, sys, types, os
def _mk(n):
    class _Auto:
        def __init__(self, *a, **k): pass
        def __call__(self, *a, **k):
            return a[0] if (len(a) == 1 and callable(a[0]) and not k) else self
        def __getattr__(self, _n): return _Auto()
    class _StubMod(__import__("types").ModuleType):
        def __getattr__(self, name):
            v = _Auto(); object.__setattr__(self, name, v); return v
    import sys as _ss
    m = _StubMod(n); _ss.modules[n] = m; return m
maubot = _mk("maubot")
maubot.MessageEvent = type("MessageEvent", (), {})
maubot.Plugin = type("Plugin", (), {})
hh = _mk("maubot.handlers")
class FakeCommand:
    def __init__(self, fn=None): self.fn = fn
    def subcommand(self, *a, **k): return lambda fn: FakeCommand(fn)
class CommandNS:
    def new(self, *a, **k): return lambda fn: FakeCommand(fn)
    def argument(self, *a, **k): return lambda fn: fn
hh.command = CommandNS()
mt = _mk("mautrix.types")
mt.EventType = types.SimpleNamespace(ROOM_MESSAGE="m.room.message", ROOM_NAME="m.room.name")
mt.RoomID = lambda x: x
adb = _mk("mautrix.util.async_db")
adb.UpgradeTable = type("UpgradeTable", (), {"register": lambda self,*a,**k:(lambda fn: fn)})
adb.Connection = object
cm = _mk("mautrix.util.config")
cm.BaseProxyConfig = type("BaseProxyConfig", (), {})
cm.ConfigUpdateHelper = type("ConfigUpdateHelper", (), {})
_mk("mautrix"); _mk("mautrix.util")
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
    def __init__(self): self.rws={}
    async def fetch(self, q, *a):
        await asyncio.sleep(0)
        if "FROM room_watched_servers" in q and "WHERE" not in q:
            return [dict(r) for r in self.rws.values()]
        raise AssertionError(q[:50])

class FakeClient:
    def __init__(self, names): self.names=names
    async def get_state_event(self, room, et):
        await asyncio.sleep(0)
        if room in self.names: return types.SimpleNamespace(name=self.names[room])
        raise Exception("M_NOT_FOUND")

class FakeEvt:
    def __init__(self, sender): self.sender=sender; self.room_id="!cmd:h"; self.replies=[]
    async def reply(self, text): self.replies.append(text)

def make_bot(allowed, admins, names):
    b=dw.DirWatcherBot.__new__(dw.DirWatcherBot)
    b.database=FakeDB(); b.client=FakeClient(names)
    b.config={"allowed_users":allowed, "admin_users":admins}
    b.log=types.SimpleNamespace(info=lambda *a,**k:None,debug=lambda *a,**k:None,exception=lambda *a,**k:None)
    return b

def add(bot, room, server, **over):
    row={"matrix_room_id":room,"server":server,"interval_minutes":60,"fetch_limit":500,
         "include_topic":True,"include_members":True,"max_per_message":50,"topic_collapse_length":120}
    row.update(over); bot.database.rws[(room,server)]=row

fails=[]
def check(label, cond):
    print(("PASS" if cond else "FAIL"),"-",label)
    if not cond: fails.append(label)

async def main():
    # 1. admin_users empty -> admin tier disabled
    b=make_bot([], [], {}); add(b,"!a:h","matrix.org")
    e=FakeEvt("@op:h"); await b.admin_overview.fn(b, e)
    check("empty admin_users: disabled message", any("disabled" in r for r in e.replies))

    # 2. non-admin sender (admin_users set) -> permission denied
    b=make_bot([], ["@boss:h"], {}); add(b,"!a:h","matrix.org")
    e=FakeEvt("@random:h"); await b.admin_overview.fn(b, e)
    check("non-admin: permission denied", any("permission" in r for r in e.replies))

    # 3. admin sender -> overview rendered with counts and rooms
    b=make_bot([], ["@op:h"], {"!a:h":"Mod Room"})
    add(b,"!a:h","matrix.org"); add(b,"!a:h","example.org", interval_minutes=30)
    add(b,"!b:h","matrix.org", topic_collapse_length=-1)
    e=FakeEvt("@op:h"); await b.admin_overview.fn(b, e)
    out=e.replies[-1]
    check("admin: header counts", "2 room(s)" in out and "3 watch(es)" in out and "2 distinct server(s)" in out)
    check("admin: rooms present", "!a:h" in out and "!b:h" in out)
    check("admin: resolved name shown", "Mod Room" in out)
    check("admin: id fallback link", "[`!b:h`](https://matrix.to/#/!b:h)" in out)
    check("admin: per-server detail", "every 30m" in out and "collapse: always" in out)

    # 4. allowed_users gate is independent and unaffected: a normal allowed
    #    user who is NOT an admin cannot run admin overview
    b=make_bot(["@user:h"], ["@boss:h"], {}); add(b,"!a:h","matrix.org")
    e=FakeEvt("@user:h"); await b.admin_overview.fn(b, e)
    check("allowed-but-not-admin: denied from admin cmd", any("permission" in r for r in e.replies))

    # 5. admin implies allowed: an admin passes the normal access gate even
    #    when not listed in allowed_users
    b=make_bot(["@someoneelse:h"], ["@op:h"], {})
    check("admin implies allowed (_is_allowed)", b._is_allowed("@op:h") is True)
    check("non-admin honors allowed_users", b._is_allowed("@nobody:h") is False)

    # 6. bare `!dirwatch admin` -> admin help (gated)
    b=make_bot([], ["@op:h"], {})
    e=FakeEvt("@op:h"); await b.admin_cmd.fn(b, e)
    check("bare admin (admin): shows admin help", any("Admin commands" in r for r in e.replies))
    e=FakeEvt("@random:h"); await b.admin_cmd.fn(b, e)
    check("bare admin (non-admin): denied", any("permission" in r for r in e.replies))

    # 7. no configs
    b=make_bot([], ["@op:h"], {})
    e=FakeEvt("@op:h"); await b.admin_overview.fn(b, e)
    check("empty config: friendly message", any("No rooms have any servers" in r for r in e.replies))

    # 8. truncation guard
    b=make_bot([], ["@op:h"], {})
    for i in range(2000): add(b, f"!room{i:04d}:h", "matrix.org")
    e=FakeEvt("@op:h"); await b.admin_overview.fn(b, e)
    out=e.replies[-1]
    check("huge: under ~32k chars", len(out) < 32000)
    check("huge: truncation note", "output truncated" in out)

    print("\nRESULT:", "ALL PASS" if not fails else f"FAILURES: {fails}")
    sys.exit(1 if fails else 0)

asyncio.run(main())
