# Networked / shared-database (multi-machine, LAN + WAN) — design

**Status:** designed 2026-07-26 (research + code-grounded buildout). Big dedicated arc; does **not**
block the imminent SCP/grid/Flex/contest-Stage-1 release. Prompted by tester **Bill Crossley**: his
current logger (**cqrlog + MySQL**) lets many machines share one log "from anywhere with Internet"; ours
can't. Also wanted for single-user-across-machines, mobile, POTA/offsite, and club Field-Day multi-op.

---

## 1. The key insight — we're already better-positioned than cqrlog

cqrlog needs MySQL because it's a **desktop app talking directly to a database**. SDRLoggerPlus is
**already client-server**: React frontend → **.NET backend (HTTP + SignalR, :5050)** → LiteDB, and the
frontend *never* touches the DB. So Bill's ask is a **deployment + auth** problem, not a database-port
problem. The answer is **make the existing backend a hostable server with auth + TLS** — clients hit the
API, the DB stays private. This is the **Wavelog model**, proven in production for ham logging (log from
desktop/phone/POTA over the web; DB never exposed). A networked SQL DB is only needed later, for heavy
concurrent multi-op.

**Never expose a database port to the internet.** Every direct-DB logger that "works over the internet"
(cqrlog/HRD MySQL on :3306, N3FJP :1000) is either behind a VPN/tunnel or quietly vulnerable — those
ports are scanned and brute-forced within hours. Our API + auth + TLS is the safe substitute, and we
already have the API.

---

## 2. Current architecture (verified in code)

- **Client base URLs are relative + hardcoded:** `API_BASE = '/api'` (`client.ts:4`, ~20 `fetch` sites),
  SignalR `.withUrl('/hubs/log')` (`signalr.ts:931`). Works today only because packaged Electron loads the
  SPA *from the backend* (`main.js:425` `loadURL(http://localhost:${port})`) so relative paths are
  same-origin; dev uses the Vite proxy (`vite.config.ts:40-50`).
- **Backend binding is env-driven already:** `Program.cs` honors `ASPNETCORE_URLS` (Electron sets it,
  `main.js:295`); port auto-picked from 5050 (`main.js:205`).
- **Packaging already produces a headless server:** `prepare-backend.js:67` `dotnet publish
  --self-contained -r <rid>` + SPA into `backend/wwwroot`. That exe **is** a standalone web server;
  Electron is just a launcher.
- **No authentication of any kind.** No `AddAuthentication`/`UseAuthorization`/`[Authorize]`. The only
  credential check anywhere is the shutdown token (`SystemController.Shutdown`, constant-time
  `FixedTimeEquals`) — the pattern to copy.
- **CORS is dangerously open:** `SetIsOriginAllowed(_=>true).AllowAnyHeader().AllowAnyMethod().
  AllowCredentials()` (`Program.cs:62-71`) — reflecting any origin *with* credentials; any web page the
  operator visits can script authenticated calls against a reachable backend.
- **LiteDB is `Connection=shared` + WAL** (`LiteDbContext.cs:96`); writes funnel through
  `LiteQsoRepository.Commit()` → `Checkpoint()`. **One backend serving many clients is safe** (one process,
  many threads); **two backend processes on one file, or scale-out, is not.**
- **`IQsoRepository` is a clean async interface** with one swap point: `DbServiceRegistration.AddDatabase`
  (`:37`, `AddScoped<IQsoRepository, LiteQsoRepository>`); `DatabaseProvider` enum currently only `Local`;
  `Program.cs:80` hard-forces `Local`.
- **Single-backend-assuming singletons:** `QsoSnapshotCache` (whole-log cache), contest services
  (`Program.cs:129-133`), SignalR has **no backplane** — all fine for one shared backend; all blockers to
  *multiple* backend instances.
- **Secrets are OS-bound:** `SecretProtector` uses Windows DPAPI `CurrentUser` (`:111`) — QRZ/LoTW/eQSL/AI
  creds encrypted on the operator's desktop **cannot decrypt after copying the DB to a Linux/NAS host**;
  they must be re-entered on the server. QSO rows copy fine; secret columns do not.

---

## 3. Approach comparison (research)

| Approach | Examples | WAN? | Direct-DB vs API | Conflict handling | Offline | Security |
|---|---|---|---|---|---|---|
| Shared file over SMB | N3FJP File-Share, Log4OM SQLite on share | LAN-only | direct file engine | file/record lock; structural ops single-writer | none | poor; no auth/TLS |
| Cloud file-sync of single-file DB | Log4OM + Dropbox | replication, **not** concurrent | direct | **none — two writers clobber the file** | yes, no merge | silent overwrite risk |
| Direct network client-server DB | cqrlog+MySQL, HRD+MySQL, Log4OM MySQL | WAN *if you expose the port* | **direct DB** | real row locks/transactions | none | **weakest** — 3306 to internet; needs VPN/tunnel |
| App-defined peer replication | N1MM+ | LAN auto; WAN via VPN | app protocol | **owner-wins per record** (NetBIOS = keeper) | yes (full copy each node) | no built-in auth/TLS |
| **API-mediated central server** | **Wavelog/Cloudlog**, HRD "Network Server" | **WAN-safe by design** | **API-mediated, DB private** | server enforces txns + rules | optional (client cache) | **strongest** — only 443, app auth, TLS |
| Offline-first sync + conflict policy | PouchDB/CouchDB, Realm, PowerSync | WAN, resilient | API + local replica | LWW / version vectors / CRDT | **first-class** | strong if authenticated HTTPS |

**Recommended, ranked:**
1. ⭐ **Hostable API-mediated central server** (make our backend the shared host) — the Wavelog model,
   reuses everything we have, covers *all four* use cases through 443 + auth instead of 3306.
2. ⭐ **Offline-first client with sync** layered on #1 — keep LiteDB per client as a cache/queue, stamp
   `(device_id, revision, updated_at)`, sync when connected. What makes mobile/POTA/FD *robust*.
3. **Direct networked DB via the swappable repo** (Postgres/MySQL) — least engineering, satisfies the literal
   ask, but reproduces every cqrlog weakness (scanned DB port, DB creds in every client, no per-user authz,
   no offline). **Gate hard: VPN/tunnel-only, TLS-required; never publish a "forward 3306" guide.** An
   advanced-LAN escape hatch, not the WAN answer.

---

## 4. Stage v1 — shared backend + configurable client + auth + TLS
Goal: Bill runs one backend (home PC / NAS / small VPS); desktop, laptop, phone browser all point at it over
LAN and Internet. Single-user, multi-device — ~90% of the ask, **no database change**.

### v1.0 — close the current liabilities FIRST (before any remote capability)
- **Auth (mandatory):** a `TokenAuthMiddleware` / minimal `AuthenticationHandler` wired between
  `app.UseCors()` (`Program.cs:326`) and `MapControllers()` (`:333`). Validate `Authorization: Bearer <token>`
  with `FixedTimeEquals` (reuse the shutdown-token pattern). Generate on first server run, store hashed via
  `ISecretProtector`, support a small per-device token set (revoke a lost phone without locking the desktop).
  Exempt `/api/health` + first-run setup; everything else requires the token. **Default-deny: the server must
  refuse to bind to a non-localhost address without auth configured.**
- **CORS:** replace `SetIsOriginAllowed(_=>true)…AllowCredentials()` (`Program.cs:62-71`) with a config-driven
  **allow-list**; drop `AllowCredentials` if moving to bearer tokens (tokens don't need cookies, and
  wildcard+credentials is the footgun).
- **Why this is first:** SignalR exposes **radio control** — `CommandRotator`, `TuneToFrequency`, `SendCwKey`,
  connect/disconnect, `SendDxSpot` (`signalr.ts:1276-1456`). An unauth'd reachable backend lets a stranger
  **key the operator's transmitter and spot under his call.** Not a logbook-only exposure.

### v1.1 — configurable client backend location
The relative-URL assumption is the only hard client coupling.
- New `src/SDRLoggerPlus.Web/src/api/backend.ts` — `getApiBase()`/`getHubUrl()`, resolution: `localStorage`
  override → `import.meta.env.VITE_BACKEND_URL` → `''` (relative — packaged same-origin unchanged).
- `client.ts:4`: `API_BASE = \`${getBackendBase()}/api\``; route the ~20 raw `fetch` sites through it (most
  already go via the `fetch<T>()` helper `:343`; the 204/blob/stream sites are the ones to touch).
- `signalr.ts:931`: `.withUrl(getHubUrl(), { accessTokenFactory })`.
- **Settings "Server" field** (host/URL + test against `/api/health`, which exists). **Electron:** when a
  remote server is configured, skip `startBackend()` (`main.js:244`) and `loadURL` the remote origin; local
  spawn stays default.
- **Mobile/other platforms need no app** — the backend serves the SPA, so a phone browses to `https://server/`.
  The WebGL globe needs a secure context (`VITE_HTTPS`, `vite.config.ts:9`), so **TLS is mandatory over WAN**
  (nice forcing function).

### v1.2 — run the backend headless
- Reuse the self-contained publish; add a `--server`/headless mode: launch with
  `ASPNETCORE_URLS=https://0.0.0.0:5050` + `ASPNETCORE_ENVIRONMENT=Production`. Ship a Windows Service wrapper
  (NSSM/`sc.exe`) + a `systemd` unit for Linux/NAS.
- **Overridable data dir:** DB path is `configDir/sdrloggerplus.db` (`LiteDbContext.cs:177`); make the
  data/config directory env/CLI-overridable so the DB lands on a chosen volume. **DB must be on the backend
  host's local disk — never an SMB/NFS share** (WAL + shared-mutex semantics are unreliable over network FS).
- Re-enter secrets on the host (DPAPI→AES boundary); the setup flow must be reachable headless.

### v1.3 — TLS + edge (mandatory for WAN)
- **Do not expose Kestrel directly.** Reverse proxy (Caddy = auto Let's Encrypt / nginx / Traefik) terminates
  TLS, forwards to localhost, **configured for WebSocket upgrade on `/hubs`** (SignalR — mirror the Vite
  proxy's `ws:true`). HSTS + secure context for the globe.
- For operators who can't run a proxy, document **WireGuard/Tailscale** as the *preferred* WAN path (no public
  port at all) — for a single user, often better than exposing anything.

### v1 security red-team (close before any WAN story ships)
- No-auth + reachable backend = **total compromise incl. transmitter control** — auth (v1.0) is non-negotiable.
- CORS reflect-any + credentials must go, even on LAN.
- Secrets at rest: QRZ/LoTW/AI keys live in the DB; on a VPS the AES key is on the same disk — full-disk
  encryption on the host is the mitigation; a shared VPS is the wrong place for this data.
- Rate-limit / fail2ban the auth endpoint; log failures via the existing Serilog scrubbing formatter.
- Multi-tenant leakage (if clubs+individuals share a host): every query scoped server-side by owner; never
  trust a client-supplied logbook id (IDOR/BOLA — the #1 web-app bug class).
- Validate ADIF imports; parameterize all queries; the auto-updater (`updater.js`) must not try to update a
  *remote* server.

---

## 5. Stage v2 — swappable networked DB (Postgres/MySQL) for multi-op concurrency
Only when v1's single LiteDB backend is the bottleneck: **concurrent multi-operator writes**, where LiteDB's
single-writer lock and full-collection deserialization (~0.7 s per award over 24.5k QSOs, per
`QsoSnapshotCache`) don't hold up, and/or multiple backend instances are needed.

- **v2.1 — second `IQsoRepository`:** a `PostgresQsoRepository` (EF Core / Dapper) implementing the full
  surface (CRUD+bulk, `SearchAsync`, `GetStatisticsAsync`, dupe/history, the QSL/sync-ledger methods). Sibling
  repos (`ISettingsRepository`, `IContestSessionRepository`, `ICallsignImageRepository`, `IRadioConfigRepository`)
  need parallel impls. **Watch item:** `Qso.AdifExtra` is a `BsonDocument` (`LiteDbContext.cs:32-55`) → maps to
  Postgres `jsonb` (cleaner, but net-new mapping).
- **v2.2 — wire the swap:** extend `DatabaseProvider` (`DbServiceRegistration.cs:7`) with `Postgres`/`MySql`;
  branch in `AddDatabase` on `config.Provider`; relax the `Program.cs:80` `Local` force; connection string is a
  **server secret**, never in the client. Provide a migration action (read via current repo → `CreateBulkAsync`
  into the new one; ADIF export/import as fallback).
- **v2.3 — what SQL forces:** `QsoSnapshotCache` becomes wrong for multi-instance (stale cross-instance
  invalidation) — push award aggregation into SQL `GROUP BY` or keep single-instance-with-short-TTL. **Multiple
  backend instances** additionally need a **SignalR backplane** (Redis) + moving the contest singletons off
  in-process state. For Bill and even most multi-op, **one backend + SQL** is enough — stay single-instance
  unless load genuinely demands scale-out.

## 6. Offline-first sync (research recommendation for mobile/POTA/FD robustness)
Layer on #1: keep LiteDB per client as cache/queue; log offline; sync when connected.
- **Reject naïve wall-clock last-write-wins** — clock skew across FD/POTA laptops silently destroys edits.
- **Best fit: append-only + owner-wins-per-record** (N1MM-proven): the device that created a QSO owns it;
  new QSOs never collide (distinct `(device_id, local_seq)` rows) → CRDT-like convergence for the dominant
  operation without full CRDT complexity. Reserve real conflict UI for edits to the *same* existing QSO (rare).
  Use version/revision vectors to *detect* concurrent edits.
- **Idempotent, dedup-keyed sync** (`device_id + local_seq` or content hash) — mandatory, or you recreate the
  QRZ re-upload duplication class. **Tombstones, not row deletes** (else an offline device resurrects a deleted
  QSO). Persist the outbox transactionally, ack per-record — a logger must never lose or double a contact.

---

## 7. Convergence with contest-log separation
Both arcs reduce to one primitive: **"point the client/write at the correct backend/DB."**
- The `getBackendBase()` resolver (v1.1) is exactly the switch a contest-vs-general or shared-multi-op toggle
  needs — "casual server" vs "contest server" = two configured URLs, or one backend with a selectable store.
- The `DatabaseProvider`/`AddDatabase` seam (v2.2) + overridable data dir (v1.2) is the same seam that lets a
  contest run against a **separate DB/schema** (see `contest-log-separation.md` Stage 3 — the contest store as a
  second `IQsoRepository`, routed by `IsPersonalQso`/`StationCallsign`).
- **Requirement:** whatever networked `IQsoRepository` gets built **must accept a contest-vs-general store
  selector**, or contest isolation breaks the moment the backend is shared. Define **one "profile/target"
  concept** (backend URL + auth token + store selector) both features read; build the resolver once in v1.

## 8. Recommended sequencing
1. **v1.0 auth + CORS fix first** — the current wide-open, no-auth, radio-controlling backend is the real
   liability; ship nothing remote ahead of it.
2. **v1.1 configurable client + v1.2 headless + v1.3 TLS/proxy** → delivers Bill's entire single-user-multi-device
   + POTA/mobile ask on the existing LiteDB.
3. **Offline-first sync (§6)** → makes mobile/POTA/FD robust.
4. **v2 SQL** only when concurrent multi-op writes are a demonstrated need (larger, riskier: 5+ repo impls +
   snapshot-cache rework; no benefit for the single-operator case that motivated the request).
