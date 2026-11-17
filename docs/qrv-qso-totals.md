# QRV QSO totals — SDRLogger+ handoff

**Status:** server is live. This is the client work. Do not invent a second API.

Presence (who is on the air) is a separate feature and already works. This document is only the opt-in QSO counts.

## What is already live

| Piece | Where |
|---|---|
| Ingest | `POST https://qrvsdr.cc/v1/qso.php` |
| Public Top 10 | `GET https://qrvsdr.cc/v1/qso_board.php?period=week` (`month`, `year`) |
| Discord | `#qsos`, one message, edited in place. Week only. |
| Presence board | `#qrv` and `heartbeat.php` / `qrt.php` / `roster.php`. Do not add QSO fields to those. |

The website leaderboard is still the empty placeholder until a later website change. A successful POST still updates `#qsos` and the public JSON.

## Rules

- SDRLogger+ is the only app that POSTs QSO totals. Lyra and STD do not.
- Opt-in, default **off**, separate from the QRV presence switch.
- Reuse the `install_key` the presence heartbeat already stored. Do not invent a second key. If no key is stored yet, a normal presence heartbeat has to succeed once before totals will be accepted (`401 bad_key`).
- Send a **replacement snapshot** of counts, not `+1` and not the logbook. No ADIF, no per-QSO rows, no frequencies, no notes, no list of calls.
- Each bucket includes `last`: the newest counted contact’s call in that UTC period. It is the worked station (`Qso.Callsign`), not the operator. Week, month, and year are counted separately. A quiet week leaves `week.last` empty while month and year still carry the newer contact inside those periods. Contest contacts stay out of the count and cannot become `last`. A call that is not a callsign is skipped for `last`; the QSO still counts and the previous valid call remains. One call per period.
- Every non-contest contact already in the SDRLogger+ log counts for the UTC week, month, and year. The path does not matter:
  - Log entry inside SDRLogger+
  - ADIF monitor (STD `std.adi`, WSJT-X, JTDX, MSHV, VARAC, or any other watched file)
  - ADIF-over-UDP
  - A manual ADIF import
- Contest contacts stay out. POTA contacts count in Voice, CW, or Digital. Lyra, STD, and Combo link are not required. The roster is not used to guess a source.
- The ADIF monitor and UDP listener also stamp new imports `Logger`. The count does not depend on that stamp. A manual ADIF import is not stamped. When the TCI session identifies itself, a log entry is still labeled **Lyra** or **STD**. Otherwise that label is **Logger**.
- Week is the ISO week (Monday 00:00 UTC). Month and year are the calendar periods in UTC. A log that already has contacts in 2026 will show those on Monthly and Yearly. This week’s board is only contacts dated since Monday 00:00 UTC.
- Presence is unchanged. Lyra’s TCI hello is `protocol:Lyra`. `protocol:ExpertSDR3` is labeled Lyra only while Combo link is on. STD is labeled STD only when STD has identified itself on the TCI session. A `{LOG}` macro is not the source.

## Periods

The server stamps the period keys in UTC. The client sends the three current buckets:

| Bucket | Window |
|---|---|
| `week` | ISO week, Monday 00:00 UTC through now |
| `month` | Calendar month, UTC |
| `year` | Calendar year, UTC |

## Mode groups

| Bucket | Modes |
|---|---|
| `voice` | LSB, USB, AM, SAM, FM, NFM, DSB, SSB |
| `cw` | CW, CWU, CWL |
| `digital` | everything else that is counted |

Each value is an integer from 0 through 100000. All three buckets are required on every POST. Omitting one is `400 invalid_fields`.

## Request

`POST https://qrvsdr.cc/v1/qso.php`

`Content-Type: application/json`

```json
{
  "call": "N8SDR",
  "app": "Logger",
  "install_key": "<key from the presence heartbeat>",
  "week":  { "voice": 0, "cw": 1, "digital": 12, "last": "N9BC" },
  "month": { "voice": 2, "cw": 4, "digital": 40, "last": "N9BC" },
  "year":  { "voice": 10, "cw": 20, "digital": 100, "last": "N9BC" }
}
```

`last` is a string. `""` means that period has no counted callsign. The server drops a `last` it cannot normalize and still stores the counts. An old build that omits `last` keeps posting totals; the column stays blank until a build that sends it.

`app` must be the string `Logger`. `call` must match the callsign bound to that install key.

Post when the operator turns the opt-in on, after a counted QSO is saved, and about every few minutes while the opt-in stays on. The server rejects a second POST for the same key inside 10 seconds (`429 rate_limited`). Back off. Do not retry in a tight loop.

When the operator turns the opt-in off, POST one snapshot of zeros and `last` of `""` for all three periods, so the call leaves the board and the Last worked column clears.

## Responses

| HTTP | `error` | What to do |
|---|---|---|
| 200 | | `{ "ok": true, "periods": { "week": "2026-W47", "month": "2026-11", "year": "2026" } }`. The server’s period keys win. |
| 400 | `invalid_fields` | Bad call, app, key, or a count. Fix the payload. Do not retry unchanged. |
| 401 | `bad_key` | No usable install key. Run a presence heartbeat first. Do not invent a key. |
| 403 | `call_mismatch` | `call` does not match the key. Send the callsign the presence heartbeat uses. |
| 403 | `forbidden` | Banned. Stop. |
| 429 | `rate_limited` | Wait and send the latest snapshot later. |
| 500 | `store_failed` | Retry later with the same snapshot. |

## Discord and the public JSON

`#qsos` shows call, total, CW, SSB, DIG for the current UTC week. Voice is labeled SSB there. No frequency.

`GET /v1/qso_board.php?period=week` returns Top 10 with `call`, `total`, `voice`, `cw`, `digital`. A total of 0 is omitted. Month and year use `period=month` and `period=year`.
