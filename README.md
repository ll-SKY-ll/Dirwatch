# dirwatcher

A [maubot](https://github.com/maubot/maubot) plugin that watches one or more
Matrix homeservers' public room directories and posts a notification to a
Matrix room whenever rooms are added or removed.

Useful for spotting freshly-published rooms (good and bad), tracking what a
specific server lists publicly over time, or feeding a moderation channel
that wants to keep an eye on the wider federation.

## Features

- Watch multiple servers per room, each on its own polling interval and
  fetch limit.
- Different rooms can watch different servers with different settings,
  entirely independently.
- Per-`(room, server)` formatting overrides: toggle topic / member-count
  lines, cap entries per message.
- Persistent state in PostgreSQL so restarts don't re-notify on rooms that
  were already present.
- First poll of any new server is treated as a baseline — no notification
  storm on day one.

## Requirements

- maubot
- PostgreSQL (the plugin uses `asyncpg` features like `ON CONFLICT` and array
  parameters; SQLite is not supported)

## Installation

1. Build the `.mbp`:
   ```sh
   mbc build
   ```
2. Upload the resulting `net.codestorm.dirwatcher-*.mbp` to your maubot
   instance and create a client+instance as usual.
3. Invite the bot to a room and run `!dirwatch` for command help.

## Configuration

`base-config.yaml` only carries command access control. Everything else is
configured per-room via the bot's commands.

### Access control

There are two tiers, both lists of MXIDs or fnmatch globs:

- `allowed_users` gates the normal `!dirwatch …` commands. **Empty means
  anyone** in a room with the bot may use them.
- `admin_users` gates the `!dirwatch admin …` commands (currently the
  cross-room `overview`). **Empty means the admin commands are disabled for
  everyone** until you populate it — the elevated tier stays locked by
  default. Admins also implicitly pass the `allowed_users` gate, so you don't
  have to list someone in both.

```yaml
allowed_users:
  - "@moderator:example.com"
  - "@*:trusted-server.example"

admin_users:
  - "@you:example.com"
```

### Setting up watches

From inside the room where you want notifications:

```
!dirwatch server add example.org 30 1000
!dirwatch server set example.org include_topic false
!dirwatch server list
```

A room can watch multiple servers and each has its own interval, fetch
limit, and formatting.

### Long topics

`topic_collapse_length` controls how long room topics are handled in
notifications. It's a character count with two sentinel values:

| Value | Behavior |
| --- | --- |
| `N` (positive) | Collapse topics longer than `N` characters behind an expandable `<details>` disclosure. Shorter topics show inline. **Default: 120.** |
| `0` | Never collapse — show every topic inline, in full. |
| `-1` | Always collapse, regardless of length. |

```
!dirwatch server set example.org topic_collapse_length 200   # collapse over 200 chars
!dirwatch server set example.org topic_collapse_length 0     # never collapse
!dirwatch server set example.org topic_collapse_length -1    # always collapse
```

Collapsing is an HTML feature (rendered as a click-to-expand widget in
clients that support `<details>`, e.g. Element). The plain-text fallback
always carries the full topic, so nothing is lost for clients that don't
render HTML. Topics are capped at 1000 characters in either mode as a
message-size safeguard.

Room names and topics are HTML-escaped in notifications, so a hostile room
can't inject markup into the channel.

## Commands

| Command | Description |
| --- | --- |
| `!dirwatch` | Show help. |
| `!dirwatch server add <server> [interval_min] [limit]` | Watch `<server>` in this room. |
| `!dirwatch server remove <server>` | Stop watching `<server>` in this room. Clears any still-pending notifications for that server. |
| `!dirwatch server list` | List servers watched in this room. |
| `!dirwatch server set <server> <key> <value>` | Set one of: `interval_minutes`, `fetch_limit`, `include_topic`, `include_members`, `max_per_message` (entries per message; updates with more are split across messages), `topic_collapse_length`. |
| `!dirwatch check [server]` | Poll now and deliver pending notifications. Pass a server name (even one not watched here) for an ad-hoc lookup. |
| `!dirwatch status` | Show last-polled time and current/pending counts. |
| `!dirwatch stats [server]` | Show total added/removed counts for a server. |
| `!dirwatch admin overview` | **Admin only.** List watch config across all rooms (which servers are watched where, with their settings). Requires `admin_users`. |

## How it works

Each polling cycle:

1. Build the set of `(server, interval, fetch_limit)` tuples from every
   per-room watch across every room, taking the shortest interval and
   largest fetch limit per server. A server watched by multiple rooms is
   polled once and the result is dispatched to all of them.
2. For each server whose interval has elapsed, fetch the public room
   directory in pages of 100 and diff it against the snapshot in
   `directory_snapshot`. New rooms and removed rooms get queued in
   `pending_notifications` for every room that watches the server.
3. Per-room delivery runs on its own schedule (shortest interval among the
   room's watched servers), groups pending changes per server, and applies
   per-`(room, server)` formatting settings.

The first poll of any newly-configured server is treated as a baseline:
the snapshot is stored but no notification is sent, to avoid a storm of
"added" messages on first start.

### Message splitting

A single poll can surface more changes than fit in one Matrix event (the
protocol caps events at 64 KiB). Notifications are split across multiple
messages so nothing is dropped: each message holds at most
`max_per_message` entries and always stays within a conservative size
budget, even when individual topics are long. Split messages are labelled
`(part i/N)`. As a last-resort guard against a pathological mass-change, no
more than 50 messages are emitted per delivery; anything beyond that is
summarised as "… and N more not shown".

### Leaving rooms

When the bot leaves a room, or is kicked or banned from it, that room's watch
configuration, any queued notifications, and its delivery schedule are
dropped automatically. Shared per-server data (directory snapshots) is kept,
since other rooms may still watch the same server. If the bot is removed
while offline and misses the event, it reconciles on the next startup by
dropping config for any room it's no longer joined to. Re-inviting the bot
starts that room fresh — re-add its watches with `!dirwatch server add`.

## Tests

The `tests/` directory holds standalone regression tests for the dedup /
concurrency behavior, the message formatting (topic collapse, HTML
escaping), and message splitting. They stub out maubot/mautrix and an
in-memory database, so they need no homeserver or PostgreSQL:

```sh
python3 tests/test_concurrency.py
python3 tests/test_formatting.py
python3 tests/test_splitting.py
python3 tests/test_overview.py
python3 tests/test_membership.py
```

## License

MIT. See `LICENSE`.
