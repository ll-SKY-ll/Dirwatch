"""Verifies integer topic_collapse_length semantics + HTML escaping."""
import sys, types
def _mk(n):
    m = types.ModuleType(n); sys.modules[n] = m; return m
maubot = _mk("maubot")
maubot.MessageEvent = type("MessageEvent", (), {})
maubot.Plugin = type("Plugin", (), {})
h = _mk("maubot.handlers")
class FakeCommand:
    def __init__(self, fn=None): self.fn = fn
    def subcommand(self, *a, **k): return lambda fn: FakeCommand(fn)
class CommandNS:
    def new(self, *a, **k): return lambda fn: FakeCommand(fn)
    def argument(self, *a, **k): return lambda fn: fn
h.command = CommandNS()
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

short = "short topic"                 # under any positive threshold
mid = "x" * 200                       # 200 chars
nasty = '<img src=x onerror=alert(1)> & "q"'

def E(topic): return {"alias": "#r:s", "name": "", "topic": topic, "room_id": "!r:s", "members": 7}
def html_of(topic, length): return dw.DirWatcherBot._label_html(E(topic), True, True, length)
def text_of(topic): return dw.DirWatcherBot._label(E(topic), True, True)

fails = []
def check(label, cond):
    print(("PASS" if cond else "FAIL"), "-", label)
    if not cond: fails.append(label)

# length = 0 -> never collapse, full inline
h0 = html_of(mid, 0)
check("len=0: no <details>", "<details>" not in h0)
check("len=0: full topic inline", ("x"*200) in h0)

# length = -1 -> always collapse, even short topics
hneg = html_of(short, -1)
check("len=-1: short topic collapses", "<details>" in hneg)
hneg2 = html_of(mid, -1)
check("len=-1: long topic collapses", "<details>" in hneg2 and ("x"*200) in hneg2)

# length = 120 -> collapse over 120
h120_long = html_of(mid, 120)       # 200 > 120 -> collapse
check("len=120: 200-char topic collapses", "<details>" in h120_long)
h120_short = html_of(short, 120)    # under -> inline
check("len=120: short topic inline (no details)", "<details>" not in h120_short)

# boundary: exactly 120 should NOT collapse (strictly greater)
exactly = "y" * 120
check("len=120: exactly 120 chars stays inline", "<details>" not in html_of(exactly, 120))
check("len=120: 121 chars collapses", "<details>" in html_of("y"*121, 120))

# summary preview truncates with ellipsis when topic exceeds preview len
hp = html_of(mid, -1)
check("collapsed summary has ellipsis for long topic", "…" in hp)
# short topic collapsed: summary has no ellipsis
check("collapsed short summary: no ellipsis", "…" not in html_of(short, -1))

# escaping in both collapsed and inline
for length in (0, -1):
    hh = dw.DirWatcherBot._label_html({"alias":"#r:s","name":nasty,"topic":nasty*10,"room_id":"!r:s","members":1}, True, True, length)
    check(f"escaped (len={length}): no raw <img", "<img" not in hh)
    check(f"escaped (len={length}): &amp; present", "&amp;" in hh)

# plaintext always carries full topic (capped), independent of setting
check("plaintext: full topic present", ("x"*200) in text_of(mid))

# hard cap enforced
huge = "z" * (dw.TOPIC_HARD_LIMIT + 500)
check("hard cap: html body capped", huge not in html_of(huge, -1) and ("z"*dw.TOPIC_HARD_LIMIT) in html_of(huge, -1))
check("hard cap: plaintext capped", huge not in text_of(huge))

print("\nRESULT:", "ALL PASS" if not fails else f"FAILURES: {fails}")
sys.exit(1 if fails else 0)
