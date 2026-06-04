"""Verifies oversized updates split into multiple messages with no data loss."""
import asyncio, sys, types
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
mt = _mk("mautrix.types")
mt.EventType = types.SimpleNamespace(ROOM_MESSAGE="m.room.message")
mt.RoomID = lambda x: x
adb = _mk("mautrix.util.async_db")
adb.UpgradeTable = type("UpgradeTable", (), {"register": lambda self,*a,**k:(lambda fn: fn)})
adb.Connection = object
cm = _mk("mautrix.util.config")
cm.BaseProxyConfig = type("BaseProxyConfig", (), {})
cm.ConfigUpdateHelper = type("ConfigUpdateHelper", (), {})
_mk("mautrix"); _mk("mautrix.util")
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import dirwatcher as dw

class FakeDB:
    def __init__(self): self.pending=[]; self.rws={}; self._id=1
    async def fetch(self, q, *a):
        await asyncio.sleep(0)
        if "FROM pending_notifications" in q:
            room=a[0]
            return sorted([dict(r) for r in self.pending if r["matrix_room_id"]==room],
                          key=lambda r:(r["server"],r["change_type"],r["created_at"]))
        if "FROM room_watched_servers WHERE matrix_room_id=$1" in q:
            room=a[0]; return [dict(r) for (rm,_),r in self.rws.items() if rm==room]
        raise AssertionError(q[:50])
    async def execute(self, q, *a):
        await asyncio.sleep(0)
        if "DELETE FROM pending_notifications" in q:
            ids=set(a[0]); self.pending=[r for r in self.pending if r["id"] not in ids]; return
        raise AssertionError(q[:50])

class FakeClient:
    def __init__(self): self.sent=[]
    async def send_message_event(self, room, et, content):
        await asyncio.sleep(0); self.sent.append(content)

def make_bot():
    b=dw.DirWatcherBot.__new__(dw.DirWatcherBot)
    b.database=FakeDB(); b.client=FakeClient()
    b.log=types.SimpleNamespace(info=lambda *a,**k:None,debug=lambda *a,**k:None,exception=lambda *a,**k:None)
    b._work_lock=asyncio.Lock()
    return b

def seed(bot, room, server, n_added, n_removed, topic="t", tcl=120, max_per=50):
    bot.database.rws[(room,server)]={"matrix_room_id":room,"server":server,
        "interval_minutes":60,"fetch_limit":500,"include_topic":True,
        "include_members":True,"max_per_message":max_per,"topic_collapse_length":tcl}
    for i in range(n_added):
        bot.database.pending.append({"id":bot.database._id,"server":server,
            "matrix_room_id":room,"change_type":"added","room_id":f"!a{i}:{server}",
            "alias":f"#a{i}:{server}","name":f"Room {i}","topic":topic,"members":i,"created_at":i})
        bot.database._id+=1
    for i in range(n_removed):
        bot.database.pending.append({"id":bot.database._id,"server":server,
            "matrix_room_id":room,"change_type":"removed","room_id":f"!r{i}:{server}",
            "alias":f"#r{i}:{server}","name":f"Old {i}","topic":topic,"members":i,"created_at":i})
        bot.database._id+=1

fails=[]
def check(label, cond):
    print(("PASS" if cond else "FAIL"),"-",label)
    if not cond: fails.append(label)

def count_rooms(sent):
    # count <li> entries across all html bodies
    return sum(c["formatted_body"].count("<li>") for c in sent)

def size_ok(sent):
    return all(len(c["body"].encode())+len(c["formatted_body"].encode()) <= dw.MAX_EVENT_CONTENT_BYTES for c in sent)

async def main():
    # 1. small update -> single message, no (part)
    b=make_bot(); seed(b,"!R:h","s",3,1)
    await b._deliver_pending("!R:h")
    check("small: one message", len(b.client.sent)==1)
    check("small: no part label", "part" not in b.client.sent[0]["body"])
    check("small: all 4 rooms present", count_rooms(b.client.sent)==4)
    check("small: pending cleared", len(b.database.pending)==0)

    # 2. count-based split: 120 added, max_per 50 -> 3 messages, all delivered
    b=make_bot(); seed(b,"!R:h","s",120,0,max_per=50)
    await b._deliver_pending("!R:h")
    check("count split: 3 messages", len(b.client.sent)==3)
    check("count split: part labels present", all("part" in c["body"] for c in b.client.sent))
    check("count split: all 120 delivered, none dropped", count_rooms(b.client.sent)==120)
    check("count split: every message within size budget", size_ok(b.client.sent))

    # 3. size-based split: long topics force fewer per message even under max_per
    big="Q"*900
    b=make_bot(); seed(b,"!R:h","s",200,0,topic=big,tcl=0,max_per=1000)
    await b._deliver_pending("!R:h")
    check("size split: more than one message despite high max_per", len(b.client.sent)>1)
    check("size split: all 200 delivered", count_rooms(b.client.sent)==200)
    check("size split: every message within budget", size_ok(b.client.sent))

    # 4. ordering: added rows come before removed across the stream
    b=make_bot(); seed(b,"!R:h","s",60,60,max_per=50)
    await b._deliver_pending("!R:h")
    joined="".join(c["formatted_body"] for c in b.client.sent)
    check("ordering: total 120 delivered", count_rooms(b.client.sent)==120)
    check("ordering: first Added appears before first Removed",
          joined.find(">Added<") < joined.find(">Removed<"))

    # 5. flood cap: exceed MAX_MESSAGES_PER_DELIVERY -> capped + omitted note
    over = dw.MAX_MESSAGES_PER_DELIVERY * 50 + 200   # 50 per page
    b=make_bot(); seed(b,"!R:h","s",over,0,max_per=50)
    await b._deliver_pending("!R:h")
    check("flood: capped at MAX_MESSAGES_PER_DELIVERY",
          len(b.client.sent)==dw.MAX_MESSAGES_PER_DELIVERY)
    check("flood: last message notes omission",
          "more not shown" in b.client.sent[-1]["body"])

    print("\nRESULT:", "ALL PASS" if not fails else f"FAILURES: {fails}")
    sys.exit(1 if fails else 0)

asyncio.run(main())
