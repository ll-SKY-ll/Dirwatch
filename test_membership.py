"""Verifies config cleanup when the bot leaves/is removed from a room."""
import asyncio, sys, types, os
def _mk(n):
    m = types.ModuleType(n); sys.modules[n] = m; return m
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
hh.event = types.SimpleNamespace(on=lambda *a, **k: (lambda fn: fn))
mt = _mk("mautrix.types")
mt.EventType = types.SimpleNamespace(ROOM_MESSAGE="m.room.message", ROOM_NAME="m.room.name", ROOM_MEMBER="m.room.member")
mt.RoomID = lambda x: x
mt.Membership = types.SimpleNamespace(LEAVE="leave", BAN="ban", JOIN="join", INVITE="invite")
mt.StateEvent = object
adb = _mk("mautrix.util.async_db")
adb.UpgradeTable = type("UpgradeTable", (), {"register": lambda self,*a,**k:(lambda fn: fn)})
adb.Connection = object
cm = _mk("mautrix.util.config")
cm.BaseProxyConfig = type("BaseProxyConfig", (), {})
cm.ConfigUpdateHelper = type("ConfigUpdateHelper", (), {})
_mk("mautrix"); _mk("mautrix.util")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import dirwatcher as dw

class FakeDB:
    def __init__(self): self.rws={}; self.pending=[]; self.poll_state={}
    async def fetch(self, q, *a):
        await asyncio.sleep(0)
        if "FROM room_watched_servers" in q and "WHERE" not in q:
            return [dict(r) for r in self.rws.values()]
        raise AssertionError(q[:50])
    async def fetchval(self, q, *a):
        await asyncio.sleep(0)
        if "DELETE FROM room_watched_servers WHERE matrix_room_id=$1 RETURNING 1" in q:
            room=a[0]
            keys=[k for k in self.rws if k[0]==room]
            for k in keys: del self.rws[k]
            return 1 if keys else None
        raise AssertionError(q[:60])
    async def execute(self, q, *a):
        await asyncio.sleep(0)
        if "DELETE FROM pending_notifications WHERE matrix_room_id=$1" in q:
            room=a[0]; self.pending=[r for r in self.pending if r["matrix_room_id"]!=room]; return
        if "DELETE FROM poll_state WHERE poll_key=$1" in q:
            self.poll_state.pop(a[0], None); return
        raise AssertionError(q[:60])

class FakeClient:
    def __init__(self, mxid, joined): self.mxid=mxid; self._joined=joined
    async def get_joined_rooms(self):
        await asyncio.sleep(0)
        if self._joined is None: raise Exception("network")
        return list(self._joined)

def make_bot(mxid="@bot:h", joined=None):
    b=dw.DirWatcherBot.__new__(dw.DirWatcherBot)
    b.database=FakeDB(); b.client=FakeClient(mxid, joined)
    b._work_lock=asyncio.Lock()
    b.log=types.SimpleNamespace(info=lambda *a,**k:None,debug=lambda *a,**k:None,
                                 warning=lambda *a,**k:None,exception=lambda *a,**k:None)
    return b

def add(bot, room, server="matrix.org"):
    bot.database.rws[(room,server)]={"matrix_room_id":room,"server":server,
        "interval_minutes":60,"fetch_limit":500,"include_topic":True,
        "include_members":True,"max_per_message":50,"topic_collapse_length":120}

def member_evt(state_key, membership, room="!a:h"):
    return types.SimpleNamespace(state_key=state_key, room_id=room,
        content=types.SimpleNamespace(membership=membership))

fails=[]
def check(label, cond):
    print(("PASS" if cond else "FAIL"),"-",label)
    if not cond: fails.append(label)

async def main():
    # 1. _forget_room removes config + pending + deliver state, keeps shared/other
    b=make_bot(); add(b,"!a:h"); add(b,"!b:h")
    b.database.pending.append({"id":1,"matrix_room_id":"!a:h"})
    b.database.pending.append({"id":2,"matrix_room_id":"!b:h"})
    b.database.poll_state["deliver|!a:h"]=123
    b.database.poll_state["deliver|!b:h"]=456
    b.database.poll_state["server|matrix.org"]=789
    dropped=await b._forget_room("!a:h")
    check("forget: returns True when config existed", dropped is True)
    check("forget: !a config gone", not any(k[0]=="!a:h" for k in b.database.rws))
    check("forget: !a pending gone", not any(r["matrix_room_id"]=="!a:h" for r in b.database.pending))
    check("forget: !a deliver state gone", "deliver|!a:h" not in b.database.poll_state)
    check("forget: !b config preserved", any(k[0]=="!b:h" for k in b.database.rws))
    check("forget: !b pending preserved", any(r["matrix_room_id"]=="!b:h" for r in b.database.pending))
    check("forget: shared server poll_state preserved", "server|matrix.org" in b.database.poll_state)
    check("forget: returns False when nothing to drop", await b._forget_room("!ghost:h") is False)

    # 2. member event: bot leaves -> forgets
    b=make_bot(); add(b,"!a:h")
    await b._on_member_event(member_evt("@bot:h", mt.Membership.LEAVE, "!a:h"))
    check("member: bot leave drops config", not b.database.rws)

    # 3. member event: bot banned -> forgets
    b=make_bot(); add(b,"!a:h")
    await b._on_member_event(member_evt("@bot:h", mt.Membership.BAN, "!a:h"))
    check("member: bot ban drops config", not b.database.rws)

    # 4. member event: another user leaving -> no-op
    b=make_bot(); add(b,"!a:h")
    await b._on_member_event(member_evt("@someone:h", mt.Membership.LEAVE, "!a:h"))
    check("member: other user leave is ignored", any(k[0]=="!a:h" for k in b.database.rws))

    # 5. member event: bot joining -> no-op
    b=make_bot(); add(b,"!a:h")
    await b._on_member_event(member_evt("@bot:h", mt.Membership.JOIN, "!a:h"))
    check("member: bot join is ignored", any(k[0]=="!a:h" for k in b.database.rws))

    # 6. reconcile: drop rooms not joined, keep joined
    b=make_bot(joined=["!keep:h"]); add(b,"!keep:h"); add(b,"!gone:h")
    await b._reconcile_rooms()
    check("reconcile: keeps joined room", any(k[0]=="!keep:h" for k in b.database.rws))
    check("reconcile: drops un-joined room", not any(k[0]=="!gone:h" for k in b.database.rws))

    # 7. reconcile: API failure -> drop nothing (safe)
    b=make_bot(joined=None); add(b,"!a:h"); add(b,"!b:h")
    await b._reconcile_rooms()
    check("reconcile: on API error, nothing dropped", len(b.database.rws)==2)

    print("\nRESULT:", "ALL PASS" if not fails else f"FAILURES: {fails}")
    sys.exit(1 if fails else 0)

asyncio.run(main())
