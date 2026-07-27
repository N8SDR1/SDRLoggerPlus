# Networked / shared-database — v1.0 execution plan

**Companion to** [`networked-shared-database.md`](networked-shared-database.md) (the design). That doc
covers *what and why*; this covers *the exact ordered build* for the first shippable increment, with
acceptance criteria. Written 2026-07-27, anchors re-verified against the code **after the v2.10.0 release**.

> **Nothing here is built yet.** This is the map to approve before we cut code. The first increment
> touches **auth, CORS, and remote reachability of a backend that can key the transmitter** — so it ships
> behind review, not while the operator is away.

---

## Where the code actually is today (re-verified 2026-07-27)

| Anchor | Current state | File |
|---|---|---|
| Client API base | `const API_BASE = '/api'` (relative) | `client.ts:4` |
| SignalR hub URL | `.withUrl('/hubs/log')` (relative) | `signalr.ts` |
| CORS | `SetIsOriginAllowed(_=>true).AllowAnyHeader().AllowAnyMethod().AllowCredentials()` | `Program.cs:66-69` |
| Auth | **None** — no `AddAuthentication`, no `[Authorize]`, no middleware | — |
| DB provider | Hard-forced `DatabaseProvider.Local` | `Program.cs:80` |
| Repo swap point | `AddScoped<IQsoRepository, LiteQsoRepository>()` | `DbServiceRegistration.cs:37` |
| Only credential pattern in codebase | shutdown-token, constant-time `FixedTimeEquals` | `SystemController.Shutdown` |

**Conclusion:** the design is still accurate. The security posture is unchanged since it was written —
open CORS + no auth + radio control over SignalR — so **v1.0 (auth + CORS) remains the correct, urgent
first step**, and everything downstream (configurable client, headless, TLS) layers cleanly on the
existing LiteDB with no database change.

---

## v1.0 — lock the doors (the only increment that must land before ANY remote story)

**Goal:** the backend can be safely reachable off-localhost. No new user-facing capability yet — this is
pure hardening, because the moment step v1.1 makes the client point anywhere, an unauthenticated backend
is a stranger keying your rig.

### Tasks (in order)

1. **Server auth token store** — generate a token on first server run; store it *hashed* via
   `ISecretProtector`; support a small **per-device token set** (name + hash + created/last-seen) so a
   lost phone can be revoked without locking the desktop. New `AuthTokenService` + a `auth_tokens`
   collection (or a settings blob). Reuse the `FixedTimeEquals` comparison from `SystemController`.
2. **Auth middleware** — a minimal `AuthenticationHandler`/middleware wired **between `app.UseCors()`
   (`Program.cs`) and `MapControllers()`**. Require `Authorization: Bearer <token>` on everything
   **except** `/api/health` and the first-run setup endpoints. SignalR: validate the token via
   `accessTokenFactory` (query-string `access_token` on the negotiate/ws — the standard SignalR pattern).
3. **Default-deny bind guard** — on startup, if `ASPNETCORE_URLS` binds a **non-localhost** address and no
   auth token is configured, **refuse to start** with a clear log line. Localhost-only (today's Electron
   case) stays zero-config: auth optional when bound to loopback.
4. **CORS allow-list** — replace the reflect-any policy with a config-driven origin allow-list; **drop
   `AllowCredentials`** once we're on bearer tokens (wildcard + credentials is the footgun; tokens don't
   need cookies).
5. **Local-path stays invisible** — Electron loopback path must behave exactly as today (no login prompt
   for the normal desktop user). Auth only engages when reachable remotely / token configured.

### Acceptance criteria
- Desktop Electron on loopback: **unchanged** — no prompt, everything works.
- Backend bound to `0.0.0.0` with no token: **refuses to start**, logs why.
- Backend bound to `0.0.0.0` with a token: every `/api/*` and `/hubs/*` call **401/rejects without the
  bearer token**; `/api/health` still answers (for reverse-proxy health checks).
- A revoked device token stops working; other devices keep working.
- CORS: a random origin can no longer make credentialed calls.
- **Security test:** an unauthenticated SignalR connect cannot invoke `CommandRotator` / `TuneToFrequency`
  / `SendCwKey` / `SendDxSpot`.

### Tests to write
- Middleware unit tests (valid / missing / malformed / revoked token → 200 / 401).
- Bind-guard test (non-loopback + no token → startup abort).
- SignalR auth handshake test (reject without `access_token`).
- CORS policy test (disallowed origin rejected).

---

## v1.1 — configurable client backend location

The relative-URL assumption is the only hard client coupling; this is small and mechanical.

- New `src/SDRLoggerPlus.Web/src/api/backend.ts`: `getApiBase()` / `getHubUrl()` resolving
  `localStorage override → import.meta.env.VITE_BACKEND_URL → '' (relative)`. Packaged same-origin path
  is unchanged when the override is empty.
- `client.ts:4` → `API_BASE = ` `${getApiBase()}/api` ``; route the handful of raw `fetch` sites (the
  204/blob/stream ones that bypass the `fetch<T>()` helper) through it.
- `signalr.ts` → `.withUrl(getHubUrl(), { accessTokenFactory: () => getToken() })`.
- **Settings → Server** field: host/URL + **Test** against `/api/health` + a token field. On Electron,
  when a remote server is configured, **skip `startBackend()`** and `loadURL` the remote origin instead.
- **Acceptance:** a second machine's browser/app points at the first machine's backend, authenticates, and
  logs a QSO that appears live (SignalR) on both.

## v1.2 — run the backend headless
- Add a `--server` / headless launch of the existing self-contained publish
  (`ASPNETCORE_URLS=https://0.0.0.0:5050`, `ASPNETCORE_ENVIRONMENT=Production`); ship a Windows Service
  wrapper + a `systemd` unit.
- **Overridable data dir** (env/CLI) so the DB lands on a chosen volume — **local disk only, never SMB/NFS**
  (WAL + shared-mutex semantics break over network FS).
- Re-enter secrets on the host (DPAPI→AES boundary from §2 of the design); setup flow reachable headless.

## v1.3 — TLS + edge (mandatory before any WAN story)
- Reverse proxy (Caddy auto-LE / nginx / Traefik) terminating TLS → localhost, **WebSocket upgrade enabled
  on `/hubs`**. HSTS + secure context (the WebGL globe needs it — a useful forcing function).
- Document **WireGuard/Tailscale as the preferred single-user WAN path** (no public port at all).

---

## Scaling strategy — cut off the multi-op cliff *before* it happens

The failure mode to prevent is a **word-of-mouth trap**: word spreads that SDRLogger+ does contesting +
Field Day + "multi-op", a 20-person club tries it, it buckles, and the story becomes "it can't handle
Field Day." First impressions with a club don't get a second try.

### Honest thresholds (estimates — must be load-tested, not guessed)
One backend fronting **LiteDB**, many clients over the API:
- **4–8 casual ops** (a contact every 20–60 s): **comfortable.**
- **8–15**: **gray zone** — fine at a relaxed rate, strains at a fast FT8/CW run rate.
- **20+**: **wants the SQL backend** — LiteDB's single-writer lock serializes behind this app's
  read-heavy ops (dupe-check, live scoring, awards aggregation).

### The trap: SQL alone does NOT fix 20-op. Three bottlenecks, not one:
1. **Write concurrency** → SQL fixes (real row locks/transactions).
2. **Aggregation cost** (scoring/awards scanning the whole log per change; `QsoSnapshotCache` ≈ 0.7 s per
   award over 24.5 k QSOs) → SQL fixes by pushing it into `GROUP BY`.
3. **SignalR fan-out** — every logged QSO broadcasts to all clients + triggers a score recompute. At 20
   clients this bites **even with SQL behind it**. → Needs **diff-based/throttled updates**, independent
   of the database.

So "handle a 20-op Field Day well" = a deliberate **Multi-op hardening** package =
**Postgres backend + server-side aggregation + efficient SignalR fan-out** — built as one milestone,
*before* the capability is promoted. It is NOT just "swap in MySQL."

### Two levers to cut it off
- **Lever 1 (free, now): control the promise.** Until the hardening ships, docs/wiki/landing page must
  state the current limit plainly (see next section). One honest sentence prevents the bad first
  impression at zero engineering cost. Lift the cap only when the hardening is proven.
- **Lever 2 (the build): Postgres path proactively, as a planned arc.** The `IQsoRepository` seam already
  exists, so this is scoped, not a rewrite. LiteDB stays the default for the 99% (single op / small
  groups); the big multi-op deployment is *explicitly a Postgres server* — a documented "serious Field Day
  station, set it up this way" path. Two coexisting modes; never a forced global swap, never drop LiteDB.

### Revised sequencing
1. **v1.0** auth/CORS (foundation).
2. **v1.1–1.3** shared backend on LiteDB → single-user-multi-device + small groups.
3. **Multi-op hardening** (Postgres + server-side aggregation + SignalR fan-out) → the "cut it off"
   milestone; gate promotion of 20-op capability behind it.
4. **Offline-first sync** → mobile/POTA/FD robustness.

---

## Current-release limits we MUST publish up front (before anyone builds on it)

**Operator directive (2026-07-27): users must learn the limits RIGHT AWAY — not after they've set up a
Field Day around it.** Put a clear, asterisked limitation note wherever multi-op could be *assumed*:

- **Landing page** (`website/index.html`, Contest-logging card)
- **Wiki** — `Home.md`, `Contesting.md`, `Feature-Status.md`
- **In-app Help** (`AboutDialog.tsx`, Contest section)

**The honest current-release limit (v2.10.0):**
> Contest and Field Day logging is **single-station**. Each computer runs SDRLogger+ with its **own local
> logbook** — there is **no live shared/networked log across multiple operators yet**. For a multi-op
> Field Day today, each operator logs on their own machine and you **merge the ADIF exports afterward**.
> Live networked multi-op — and support for larger concurrent groups — is **on the roadmap** (see this
> design). When it lands it will start with a comfort range (~8 ops on the built-in database) and a
> separate SQL-backed server path for larger stations.

Keep the note in lock-step with reality as each stage ships: update the cap number, don't quietly delete
the caveat.

## Load-test plan (measure the tip-over instead of guessing)
Before promoting any multi-op number, prove it:
- **Harness:** a script that opens N SignalR clients + drives M QSO writes/min through the API against a
  headless backend (reuse the `scripts/` e2e pattern). Parameterize N (5/8/12/20) × rate (slow/contest).
- **Measure:** write latency (p50/p95), score-recompute latency, SignalR delivery lag to the Nth client,
  dupe-check latency, backend CPU/RAM. On both **LiteDB** and (once built) **Postgres**.
- **Publish** the comfortable-ops number that keeps p95 write + score under a target (e.g. < 500 ms) — that
  becomes the documented cap. Re-run per backend so the wiki number is measured, not a guess.

## Deferred (explicitly NOT in v1)
- **v2 — swappable SQL repo** (Postgres/MySQL): only when concurrent multi-op writes are a demonstrated
  bottleneck. Needs 5+ sibling repo impls, `QsoSnapshotCache` rework, SignalR backplane for multi-instance.
- **Offline-first sync** (device_id + local_seq, tombstones, owner-wins-per-record): the robustness layer
  for mobile/POTA/FD — big, layer it on #1 once v1 lands.

## Convergence to honor now (so we don't repaint later)
Both this arc and contest-log-separation reduce to *"point the write at the correct backend/store."* Build
**one "profile/target" concept** = **{ backend URL + auth token + store selector }** in v1.1, and have any
future networked `IQsoRepository` accept a **contest-vs-general store selector** — otherwise contest
isolation (`IsPersonalQso` / `StationCallsign`, contest Stage 1 already shipped in v2.10.0) breaks the
instant a backend is shared for a club Field Day.

---

## Open decisions needing operator input before we build v1.0
1. **Auth model** — recommend a **per-device bearer-token set** (name a device, revoke one, no passwords to
   manage). Alternative: single shared secret (simpler, weaker — one leak = rotate everyone).
2. **v1 primary target** — recommend **Bill's single-user-multi-device** first (LiteDB unchanged, ~90% of
   the ask). Club multi-op FD (concurrent writers) pulls in v2 SQL + sync and is a separate, larger arc.
3. **WAN edge** — recommend **document Tailscale/WireGuard as the default**, reverse-proxy+TLS as the
   power-user path. Avoids ever telling anyone to open a public port.
