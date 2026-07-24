import { useEffect, useRef, useState } from 'react';
import { X, Coffee, BookOpen, Info, ScrollText, Github, MessageCircle } from 'lucide-react';
import { APP_VERSION } from '../version';

interface AboutDialogProps {
  isOpen: boolean;
  onClose: () => void;
  /** Tab to show when the dialog opens (default About). Settings → About →
   *  "Open the User Guide" opens it on 'help'. */
  initialTab?: TabId;
}

export type TabId = 'about' | 'help' | 'changelog';

// Support link — the SDRLoggerPlus PayPal donation URL used by v1. Same
// account, same "Built by a fellow ham, for the community" note so the
// history of contributors reaches the same place across both versions.
const SUPPORT_URL = 'https://www.paypal.com/donate/?business=NP2ZQS4LR454L&no_recurring=0&item_name=Built+by+a+fellow+ham%2C+for+the+community.++Free+to+use%2C+free+to+share.+A+small+donation+keeps+the+code+flowing.+73+de+N8SDR&currency_code=USD';

/**
 * Tabbed About/Help/Changelog dialog. Historical AboutDialog content
 * lives in the About tab; Help is a lightweight quick-start pointing
 * at Settings + the GitHub docs; Changelog fetches the bundled
 * CHANGELOG.md at runtime so a release-time bump doesn't require a
 * frontend rebuild.
 */
export function AboutDialog({ isOpen, onClose, initialTab = 'about' }: AboutDialogProps) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const [tab, setTab] = useState<TabId>(initialTab);
  const [changelog, setChangelog] = useState<string | null>(null);
  const [changelogError, setChangelogError] = useState<string | null>(null);

  // Jump to the requested tab each time the dialog opens (so "Open the User
  // Guide" from Settings lands on Help, while the normal open lands on About).
  useEffect(() => {
    if (isOpen) setTab(initialTab);
  }, [isOpen, initialTab]);

  useEffect(() => {
    if (!isOpen) return;
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, onClose]);

  // Lazy-fetch the changelog only when the tab is first opened. The file
  // is bundled in `public/`, so this is a same-origin static fetch — no
  // backend dependency.
  useEffect(() => {
    if (tab !== 'changelog' || changelog !== null || changelogError !== null) return;
    fetch('./CHANGELOG.md')
      .then((r) => (r.ok ? r.text() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((text) => setChangelog(text))
      .catch((e) => setChangelogError(e.message ?? String(e)));
  }, [tab, changelog, changelogError]);

  if (!isOpen) return null;

  const openLink = (url: string) => {
    if (window.electronAPI && 'openExternal' in window.electronAPI) {
      window.electronAPI.openExternal(url);
    } else {
      window.open(url, '_blank', 'noopener,noreferrer');
    }
  };

  const tabs: { id: TabId; label: string; icon: React.ReactNode }[] = [
    { id: 'about', label: 'About', icon: <Info className="w-3.5 h-3.5" /> },
    { id: 'help', label: 'Help', icon: <BookOpen className="w-3.5 h-3.5" /> },
    { id: 'changelog', label: 'Changelog', icon: <ScrollText className="w-3.5 h-3.5" /> },
  ];

  return (
    <div
      className="fixed inset-0 z-[120] flex items-center justify-center bg-black/60 backdrop-blur-sm animate-fade-in"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        ref={dialogRef}
        className={`relative glass-panel border border-glass-200 rounded-xl shadow-2xl w-full mx-4 animate-scale-in flex flex-col max-h-[85vh] transition-[max-width] ${
          tab === 'help' ? 'max-w-4xl' : 'max-w-2xl'
        }`}
      >
        <button
          onClick={onClose}
          className="absolute top-3 right-3 p-1 rounded-lg hover:bg-dark-600 transition-colors text-dark-300 hover:text-white z-10"
          title="Close (Esc)"
        >
          <X className="w-4 h-4" />
        </button>

        {/* Tab bar */}
        <div className="flex items-center gap-1 px-4 pt-3 border-b border-glass-100">
          {tabs.map((t) => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-t border-b-2 font-ui text-xs font-semibold transition-colors ${
                tab === t.id
                  ? 'border-accent-primary text-accent-primary bg-accent-primary/5'
                  : 'border-transparent text-dark-300 hover:text-dark-200 hover:bg-dark-700/40'
              }`}
            >
              {t.icon}
              {t.label}
            </button>
          ))}
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {tab === 'about' && <AboutTab openLink={openLink} />}
          {tab === 'help' && <HelpTab />}
          {tab === 'changelog' && <ChangelogTab text={changelog} error={changelogError} />}
        </div>
      </div>
    </div>
  );
}

function AboutTab({ openLink }: { openLink: (url: string) => void }) {
  return (
    <div className="text-center">
      <img
        src="./sdrloggerplus-banner.png"
        alt="SDRLoggerPlus"
        className="w-40 h-40 mx-auto mb-4 rounded-lg"
      />

      <h1 className="text-2xl font-bold text-white font-display tracking-wider">
        SDRLoggerPlus
      </h1>
      <p className="text-dark-200 text-sm mt-1">Amateur Radio Logging Software</p>
      <p className="text-dark-300 text-xs font-mono mt-2">Version {APP_VERSION}</p>

      <div className="border-t border-glass-100 my-5" />

      <div className="text-left text-xs text-dark-200 space-y-2 max-w-md mx-auto">
        <p className="text-dark-100 font-medium text-sm">Credits</p>
        <p>
          Authors:{' '}
          <span className="text-dark-100">Rick Langford (N8SDR)</span> and{' '}
          <span className="text-dark-100">Brent Crier (N9BC)</span>.
        </p>
        <p>
          Built on{' '}
          <button
            onClick={() => openLink('https://github.com/brianbruff/Log4YM')}
            className="text-accent-primary hover:underline"
          >
            Log4YM
          </button>{' '}
          by Brian Keating, EI6LF (Unlicense) — the original codebase this edition
          began from.
        </p>
        <p>
          Licensed under the MIT License. Bundles{' '}
          <span className="text-dark-100">Hamlib</span> and{' '}
          <span className="text-dark-100">libusb</span> (LGPL-2.1) and the{' '}
          <span className="text-dark-100">AD1C Country Files</span>. See
          THIRD-PARTY-NOTICES for full open-source license information.
        </p>
      </div>

      {/* Support link — subtle, discoverable, not shoved in the operator's face */}
      <div className="mt-6 flex items-center justify-center">
        <button
          onClick={() => openLink(SUPPORT_URL)}
          className="flex items-center gap-2 px-4 py-2 rounded-lg bg-accent-warning/10 border border-accent-warning/30 text-accent-warning hover:bg-accent-warning/20 transition-colors text-sm font-ui"
        >
          <Coffee className="w-4 h-4" />
          Support the project
        </button>
      </div>
    </div>
  );
}

// ── Help guide ─────────────────────────────────────────────────────────
// A proper, navigable user manual: a sticky table-of-contents on the left,
// scroll-to sections on the right. Sections are sourced from the real UI —
// panel names + Settings paths match what the operator actually sees.

const GITHUB_URL = 'https://github.com/N8SDR1/SDRLoggerPlus';
const DISCORD_URL = 'https://discord.gg/r3Cuj5NA9p';
const LYRA_URL = 'https://github.com/N8SDR1/Lyra-SDR-cpp/releases';

const HELP_SECTIONS = [
  { id: 'start',    title: 'Getting Started' },
  { id: 'radio',    title: 'Your Radio' },
  { id: 'combo',    title: 'The Lyra Combo Link' },
  { id: 'logging',  title: 'Logging QSOs' },
  { id: 'contest',  title: 'Contest Logging' },
  { id: 'spots',    title: 'DX Spots & the Map' },
  { id: 'decodes',  title: 'Digital Decodes & Grid' },
  { id: 'weather',  title: 'Weather & Alerts' },
  { id: 'meters',   title: 'Meters & Panadapter' },
  { id: 'callbook', title: 'Callbook, Uploads & Import' },
  { id: 'ai',       title: 'AI Talk Points' },
  { id: 'awards',   title: 'Awards & Statistics' },
  { id: 'settings', title: 'Settings & Shortcuts' },
  { id: 'updates',  title: 'Updates & Support' },
] as const;

// A settings path chip, e.g. "Settings → Web Logbooks".
function P({ children }: { children: React.ReactNode }) {
  return <span className="font-mono text-[11px] text-accent-primary">{children}</span>;
}
function B({ children }: { children: React.ReactNode }) {
  return <strong className="text-white font-semibold">{children}</strong>;
}

function HelpTab() {
  const openLink = (url: string) => {
    if (window.electronAPI && 'openExternal' in window.electronAPI) {
      window.electronAPI.openExternal(url);
    } else {
      window.open(url, '_blank', 'noopener,noreferrer');
    }
  };
  const secRefs = useRef<Record<string, HTMLElement | null>>({});
  const [active, setActive] = useState<string>('start');
  const go = (id: string) => {
    secRefs.current[id]?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    setActive(id);
  };

  const Section = ({ id, title, children }: { id: string; title: string; children: React.ReactNode }) => (
    <section
      ref={(el) => { secRefs.current[id] = el; }}
      className="scroll-mt-1"
    >
      <h2 className="text-base font-semibold font-ui text-white mb-1.5">{title}</h2>
      <div className="space-y-2 text-[13px]">{children}</div>
    </section>
  );

  return (
    <div className="flex gap-5">
      {/* Sticky table of contents */}
      <nav className="sticky top-0 self-start shrink-0 w-40 hidden sm:block">
        <p className="text-[10px] uppercase tracking-wide text-dark-400 mb-2 px-2">Contents</p>
        <ul className="space-y-0.5">
          {HELP_SECTIONS.map((s) => (
            <li key={s.id}>
              <button
                onClick={() => go(s.id)}
                className={`w-full text-left px-2 py-1 rounded text-xs transition-colors ${
                  active === s.id
                    ? 'bg-accent-primary/10 text-accent-primary'
                    : 'text-dark-300 hover:text-dark-100 hover:bg-dark-700/40'
                }`}
              >
                {s.title}
              </button>
            </li>
          ))}
        </ul>
      </nav>

      {/* Content */}
      <div className="flex-1 min-w-0 space-y-7 text-sm text-dark-200 leading-relaxed">
        <Section id="start" title="Getting Started">
          <p>Five minutes to your first logged QSO:</p>
          <ol className="ml-4 list-decimal space-y-1.5">
            <li><B>Set your station.</B> <P>Settings → Station</P> — callsign, name, grid. Callbook lookups need your call.</li>
            <li><B>Add a callbook.</B> <P>Settings → Web Logbooks → QRZ</P> (or HamQTH). Auto-fills name / country / grid / coords on every lookup. Each has a Test Credentials button.</li>
            <li><B>Connect a radio</B> (optional) — from the <B>Rig</B> panel, pick TCI, Hamlib, or flrig (see <em>Your Radio</em>).</li>
            <li><B>Pick a log mode</B> — the Log Entry panel has General / POTA / SAT tabs (remembered across sessions).</li>
            <li><B>Log it.</B> Type a callsign, let the callbook fill the rest, adjust freq / mode / RST, and hit <span className="font-mono text-accent-secondary text-[11px]">Log QSO</span>.</li>
          </ol>
        </Section>

        <Section id="radio" title="Your Radio">
          <p>Set up radios in <B>Settings → Station</B> (or right-click the status-bar rig selector) — four connection paths, and more than one can run at once:</p>
          <ul className="ml-4 list-disc space-y-1.5">
            <li>
              <B>TCI</B> — works with{' '}
              <button onClick={() => openLink(LYRA_URL)} className="font-bold text-accent-primary hover:underline" title="Lyra SDR releases">Lyra</button>{' '}
              (built by the same team as SDRLoggerPlus), Thetis, and ExpertSDR3. TCI is the richest link — it also drives the <B>panadapter</B> (spectrum + waterfall) and the S-meter, and it's the connection the <em>Lyra Combo Link</em> rides. The panadapter is <B>TCI-only</B>.
            </li>
            <li><B>Hamlib</B> — universal (Icom / Yaesu / Kenwood / …) via rigctld. Tunes + reads the rig; no panadapter.</li>
            <li><B>flrig</B> — XML-RPC bridge to flrig's rig database (auto-detects data-mode names). Tunes + reads the rig; no panadapter.</li>
            <li><B>FlexRadio</B> <span className="text-accent-secondary">(new)</span> — native support for FlexRadio 6000-series (SmartSDR). Flex radios are <B>auto-discovered</B> on your LAN (no host/port to enter) and appear in the list; SDRLogger+ connects to the radio's control API <B>alongside SmartSDR</B> (or Aether), follows the active slice, and tunes freq/mode. <em>New in this build and still being verified on hardware — please report anything odd.</em></li>
          </ul>
          <p className="mt-3"><B>Running alongside WSJT-X / JTDX / MSHV / VarAC</B> — you do <em>not</em> need a COM-port splitter, and we'd steer you away from one: a splitter copies bytes, but CAT is request-and-response, so two apps polling one radio get each other's replies. There are two clean ways instead:</p>
          <ul className="ml-4 list-disc space-y-1.5">
            <li><B>Let the digital app own the radio (simplest, and best for FT8/FT4).</B> Give WSJT-X / JTDX / MSHV the CAT port and don't connect a rig here at all. SDRLogger+ follows along over UDP — it reads the dial frequency and mode from the decoder, so the log entry stays right — and <B>double-clicking a decode</B> asks your decoder to call that station, so <em>it</em> does the tuning on the port it already owns. Nothing to share, nothing to conflict.</li>
            <li><B>Give the port a single owner (when more than one app must command the rig).</B> Run <B>flrig</B> or Hamlib's <B>rigctld</B> as the one process holding the serial port, then point everything — WSJT-X, JTDX, MSHV, VarAC <em>and</em> SDRLogger+ — at it over the network. Both are already supported here: pick <B>flrig</B>, or <B>Hamlib</B> with the <em>NET rigctl</em> model. The owner queues each request properly, which is exactly what a splitter can't do.</li>
          </ul>
          <p className="text-xs text-dark-300">If your radio has <em>two</em> CAT interfaces (many do — USB plus a rear serial or LAN port), that's a third option: give one to the digital app and the other to SDRLogger+.</p>
          <p className="text-xs text-dark-300">Adding a TCI rig runs a quick <B>connection check</B> first — a wrong port tells you right away instead of leaving a rig that never connects. Use <B>Test</B> to probe it, or <B>Add anyway</B> to skip the check. Saved rigs have an <B>Edit</B> button to change the name, host, or port.</p>
          <p className="text-xs text-dark-300">The <B>rig selector</B> in the status bar (bottom-right) shows your connected radio at a glance: <B>left-click</B> it to switch or connect any configured rig, and <B>right-click</B> it (or use <B>Add / manage radios</B> in the popover) to jump straight to radio setup in <B>Settings → Station</B>.</p>
        </Section>

        <Section id="combo" title="The Lyra Combo Link">
          <p className="text-accent-secondary">★ The headline feature — a deep two-way link with <B>Lyra</B> (our sibling SDR) that rides the <em>same TCI connection</em> you already use. No extra setup, no bridge app.</p>
          <p><B>Turn it on in Lyra</B> (Lyra is the master): <span className="font-mono text-[11px] text-dark-100">Lyra → Settings → Network → TCI server → "SDRLogger+ Combo"</span>. SDRLogger+ shows a read-only <span className="text-accent-secondary">● Lyra Combo</span> badge in the Log Entry header while linked. When it's on:</p>
          <ul className="ml-4 list-disc space-y-1.5">
            <li><B>Grab → log entry.</B> Grab a callsign in Lyra's CW decoder and it populates here + fires the callbook lookup.</li>
            <li><B>Name back to Lyra.</B> The first name the callbook resolves flows back into Lyra's CW Console <span className="font-mono text-[11px] text-dark-100">{'{NAME}'}</span> macro token — so "TNX {'{NAME}'} 73" fills itself.</li>
            <li><B>{'{LOG}'} logs the QSO.</B> A CW macro in Lyra carrying the <span className="font-mono text-[11px] text-dark-100">{'{LOG}'}</span> tag auto-logs the current QSO here — send your 73 and log it in one keystroke.</li>
            <li><B>Call push.</B> Click a spot or select a call here and Lyra's His Call follows.</li>
            <li>
              <B>Auto received-S.</B> The <B>S</B> of your RST-Rcvd fills automatically from the shared signal meter — peak-held over the exchange, gated by Lyra's SNR so noise never reads as signal. Use the <span className="font-mono text-[11px] text-dark-100">S-auto</span> toggle by the RST-Rcvd field; it shows <span className="text-accent-success">auto</span> / manual and re-arms each new QSO (General / POTA). Typing your own value latches it to manual.
            </li>
            <li><B>Survives restarts.</B> If Lyra restarts, the link reconnects on its own.</li>
          </ul>
          <p className="text-xs text-dark-300">Transmit power, audio, and protection stay entirely in Lyra — the combo only shares logging data.</p>
        </Section>

        <Section id="logging" title="Logging QSOs">
          <p>The Log Entry panel has three modes (tabs), remembered across sessions:</p>
          <ul className="ml-4 list-disc space-y-1.5">
            <li><B>General</B> — daily logging. Type a call → callbook fills name / QTH / grid / country. With a rig connected, <B>Follow Radio</B> keeps frequency + mode tracking the dial.</li>
            <li><B>POTA</B> — park activations. Set your activating park (rides as <span className="font-mono text-[11px] text-dark-100">my_pota_ref</span>) and an optional P2P park for park-to-park. Self-spot to POTA with <P>Settings → Web Logbooks → POTA</P> credentials.</li>
            <li><B>SAT</B> — satellite QSOs. Auto-fills satellite, band, and uplink/downlink freq &amp; mode from a connected CSN S.A.T. controller (<P>Settings → S.A.T.</P>); writes ADIF sat fields for LoTW credit. The <B>S.A.T.</B> panel tracks the live pass — azimuth/elevation, range, altitude, footprint, Doppler-shifted up/downlink, sub-satellite point and signal — in both <B>miles and km</B>. It <em>follows the live (Doppler-corrected) frequency on screen but logs the nominal</em> transponder frequency, which is what LoTW expects. <B>While a pass is being tracked the controller owns the radio</B> — SDRLogger+ pauses its own rig control (a <B>🛰 rig-control-paused</B> badge shows, Band/Mode grey out) so it never fights the controller, and the Log Entry auto-switches to SAT mode for the pass and back to General after. Add the <B>S.A.T. Web</B> panel to dock your controller's <em>own</em> interface right in the app — next passes, picking a satellite to track, TLE and frequency-database updates, rotator and pass log — using the controller address from <P>Settings → S.A.T.</P></li>
            <li>
              <B>Manual or automatic?</B> By default the S.A.T. Controller panel's <B>Activate</B> button is <B>manual</B> — you press it to go on-pass and press it again afterwards. Switch on <P>Settings → S.A.T. → Follow the controller</P> and SDRLogger+ instead watches your controller and <B>activates itself</B> once AOS is inside your lead time (90 s by default), then <B>deactivates after LOS</B> — taking the Log Entry into SAT mode and back to General with it. For that to fire, your controller must actually be running passes (a satellite selected, or its <em>Continuous</em>/<em>Schedule</em> mode on) — SDRLogger+ follows the controller, it doesn't predict passes itself. <B>You can always override:</B> click Activate any time and it stays on until you turn it off, and if you switch off mid-pass it stays off — automation re-arms for the next pass rather than fighting you. While idle this only makes a light check of the controller; the UDP listener ports stay free.
            </li>
          </ul>
          <p>RST defaults sensibly per mode (599 CW / 59 phone). With the Combo link on, the received <B>S</B> can auto-fill from the meter (see above).</p>
        </Section>

        <Section id="contest" title="Contest Logging">
          <p>The <B>contest suite</B> turns SDRLogger+ into a rule-aware contest logger for the ARRL and CQ majors (CQ WW / WPX / 160 / RTTY, ARRL DX / Sweepstakes / 10 m / 160 m / RTTY Roundup / Field Day, NAQP, NA Sprint and more). Pick a contest and it drives the exchange fields, dupe checking, and scoring for you.</p>
          <ul className="ml-4 list-disc space-y-1.5">
            <li><B>Contest Entry</B> — a focused entry window with the right exchange fields for the contest (and for each worked station, e.g. a state vs a serial), live <B>dupe</B> flagging, a running <B>score</B>, serial numbers, and quick-edit / delete of a logged QSO with logbook sync. Open it from the <B>Contest</B> tab in Log Entry.</li>
            <li><B>Contest Score</B> and <B>Multipliers</B> panels — standalone running-score and needed-multiplier displays. When a contest's multiplier has a fixed universe — CQ zones, states/provinces, or <B>ARRL/RAC sections</B> — the Multipliers panel shows <em>worked / total</em> and lists the ones you still need.</li>
            <li><B>Bandmap</B> — cluster spots coloured by dupe / new-multiplier for the running contest.</li>
            <li><B>Power class &amp; bonuses.</B> Set your <B>power class</B> and the final-score power multiplier is applied where the rules use one (Field Day QRP ×5, Winter Field Day QRP ×4 / Low ×2, Stew Perry, …). For contests with self-declared objective bonuses (Winter Field Day), a <B>bonus-points</B> box adds them to your score.</li>
          </ul>
          <p className="text-xs text-dark-300">Built-in definitions are read-only — <B>clone</B> one to tweak a ruleset, or author your own. Exchanges, dupes, multipliers <em>and scoring</em> are correct across the catalog — including the band-weighted (CQ WPX low bands, VHF per-band), distance-based (Stew Perry, ARRL Digital), North-America-exception (CQ WW / WPX), per-mode-multiplier (ARRL 10 m) and Winter Field Day rules.</p>
        </Section>

        <Section id="spots" title="DX Spots & the Map">
          <p><B>Spot sources</B>:</p>
          <ul className="ml-4 list-disc space-y-1.5">
            <li><B>DX cluster (telnet)</B> — connect up to four clusters (with per-cluster call / password / auto-reconnect) from the <B>Cluster</B> panel.</li>
            <li><B>SpotHole</B> — a polled REST aggregator, the default when no cluster is connected; filter by spotter country.</li>
            <li><B>RBN band openings</B> — separate VHF/UHF opening alerts (10/6/2m, 70cm) with distance + optional voice announce: <P>Settings → Band Openings</P>.</li>
          </ul>
          <p><B>Filters</B> (Cluster panel + <P>Settings</P>): max age (1–60 min), capacity (50–300), band/mode multi-select, <B>Track Rig</B> (show only the rig's current band+mode), and status colors — new DXCC (orange), new band (green), worked (gray, dimmable).</p>
          <p><B>POTA Activators</B> panel — live park activations, now with the same filter toolbar as the Cluster: <B>Follow rig</B> (Band/Mode), Band, Mode, a <B>Region</B> filter (by park location), and search.</p>
          <p><B>Click a spot</B> to tune the radio and prefill the Log Entry. Spots can also be <B>pushed to a TCI radio's panadapter</B> (Lyra / Thetis) as click-to-tune markers.</p>
          <p><B>DXpeditions & Hot List (auto hot spots).</B> The <B>DXpeditions</B> panel lists current and upcoming operations (NG3K feed). Click any callsign to drop it on your <B>Hot List</B> — a watchlist that makes matching DX spots light up as <B>hot spots</B> the instant they appear, and, with the announce mode on, calls them out by <B>voice</B>. Cycle the pill Off → Visual → Visual + Voice; the counter shows how many you're watching, and Clear All empties the list. Manage watched calls + text-to-speech under <P>Settings → Alerts → Hot List</P>.</p>
          <p><B>The 3D globe</B> shows spots + spotter→DX arcs, lightning strikes, POTA parks, your station, satellite tracks + footprints, the day/night terminator, gray line, aurora, PSK-Reporter coverage, and cached QRZ profile photos. Click a point to focus that call. Overlays are all in <P>Settings → Map</P>.</p>
          <p><B>"Heard Me" — who's hearing you.</B> Switch on the <B>PSK</B> and <B>RBN</B> layers (on the 3D globe and the 2D map) to draw arcs from your station out to every receiver that recently spotted <em>you</em> — <B>PSK Reporter</B> for digital, <B>RBN skimmers</B> for CW/RTTY. The band follows your connected rig (or pick a band, or <B>All bands</B>), each layer with its own look-back window; click a receiver dot to see its report — frequency, mode, SNR, and how long ago.</p>
        </Section>

        <Section id="decodes" title="Digital Decodes & Grid Tracker">
          <p>Native FT8/FT4 tools that read your decoder's UDP stream — no JTAlert or GridTracker needed. Point <B>WSJT-X</B>, <B>JTDX</B>, or <B>MSHV</B> at SDRLoggerPlus: set its <B>UDP Server</B> to <span className="font-mono text-[11px] text-dark-100">127.0.0.1 : 2237</span>, turn on <B>decoded-text</B> output, then enable the source in <P>Settings → Decoder Link (UDP)</P>.</p>
          <ul className="ml-4 list-disc space-y-1.5">
            <li><B>Digital Decodes</B> panel — every decode live, coloured by what it would give you: <B>new DXCC</B>, <B>new band</B>, <B>new zone</B>, <B>new grid</B> (already worked = dim). Filter to <B>Needed only</B>, <B>CQ only</B>, or <B>Match Alerts</B> — which narrows the list to just the decodes that match your <em>Digital Decode Alert</em> rules (so a "needed grids, North America" rule finally filters the list too, not only the alerts). <B>Double-click</B> a decode and your decoder answers that CQ — WSJT-X / JTDX only, and they need <B>"Accept UDP requests"</B> turned on (MSHV doesn't accept it).</li>
            <li><B>Digital Decode Alerts</B> (<P>Settings → Digital Decode Alerts</P>) — rules that beep / speak / pop only for what you care about: an <B>award need</B> (DXCC / band / zone / grid) <em>and</em> a <B>region</B> (continent, DXCC entity, US call area, prefix, or grid field) <em>and</em> band/mode. So "needed grids, North America only, 20m" is one rule. Quick-add presets get you started in a click; voice uses your shared <P>Settings → Voice</P>.</li>
            <li><B>Grid Tracker</B> panel — a Maidenhead grid map: <span className="text-accent-success">green</span> = worked (brighter = confirmed), and with the <B>Needed</B> toggle on, un-worked land tints <span className="text-red-400">red</span>. A <B>cyan ring</B> marks a grid active <em>right now</em> from the decode stream; a needed grid that's live pulses — "chase it this cycle". Drag to pan, scroll to zoom, hover for the grid + status. <B>FDD (Follow Digital Decodes)</B> locks the map's band to what you're decoding and mirrors the Digital Decodes filters.</li>
          </ul>
          <p>It all ties together: work a station and your decoder's <B>DX Call</B> flows into the <B>Log Entry</B> and fires the callbook lookup (<B>QRZ Profile</B> + the map); the finished FT8 QSO <B>auto-logs to Log History</B>; and the grid turns green on the tracker.</p>
        </Section>

        <Section id="weather" title="Weather & Alerts">
          <p>SDRLoggerPlus watches your local weather and warns you on-screen — handy for pulling down an antenna before a storm.</p>
          <ul className="ml-4 list-disc space-y-1.5">
            <li><B>Lightning detection</B> — aggregates Blitzortung, NWS warnings, and your own Ambient or Ecowitt station; alerts within a range you set, with strike count + direction.</li>
            <li><B>High-wind alerts</B> — NWS warnings, METAR (airport) observations, and Ambient / Ecowitt data, with low / moderate / high tiers and separate sustained + gust thresholds.</li>
          </ul>
          <p>All under <P>Settings → Alerts → Weather</P> — per-source toggles, a wind-speed unit override (<B>Auto / mph / kph</B>, where Auto follows your app-wide <P>Settings → Appearance → Units</P> choice), range, cooldown, and a METAR station code, plus Preview buttons to see the alert banner without waiting for real weather. Alerts appear as an animated banner above the status bar. (Header-bar space-weather indices — SFI / K-index / SSN — live in <P>Settings → Header Bar</P>.)</p>
        </Section>

        <Section id="meters" title="Meters & Panadapter">
          <p><B>Meter</B> panel — an analog or round S-meter (switch in the panel), with mode + frequency on one line: <span className="text-white">white on receive</span>, <span className="text-red-400">red on transmit</span>. Fed by the connected radio's meter stream (TCI). S-meter calibration lives in the panel's gear menu.</p>
          <p><B>Panadapter</B> — live spectrum + waterfall, drawn from a <B>TCI</B> radio's IQ stream (TCI-only). Mouse-wheel over it to tune by the STEP you pick; Ctrl/Shift + wheel to zoom around the VFO; click to tune.</p>
          <p>Header controls tune the display (each is also mouse-wheel adjustable): <B>SM</B> spectrum smoothing, <B>SPC</B> spectrum height — turn it down if the trace rides too high in the pane — and <B>INT / FLR / CEL / WF</B> for waterfall intensity, floor, ceiling and speed. The <B>colour gear</B> holds the waterfall <B>palette</B> and the <B>spectrum line colour</B>; <B>Grid</B> toggles the overlay. On a narrow panel the sliders fold into a controls popover so nothing clips. All of these are remembered per machine.</p>
        </Section>

        <Section id="callbook" title="Callbook, Uploads & Import">
          <p><B>Callbook lookups</B>: QRZ then HamQTH (<P>Settings → Web Logbooks</P>), falling back to the bundled AD1C <B>cty.dat</B> for country + approximate coords. Keep cty.dat current with the update button under <P>Settings → Web Logbooks → Country Files</P>.</p>
          <p><B>Upload logbooks</B> (per-QSO or on demand), each in <P>Settings → Web Logbooks</P>: <B>LoTW</B> (signs via TQSL), <B>eQSL</B>, <B>Club Log</B>, <B>HRDLog</B>, and <B>QRZ Logbook</B>.</p>
          <p><B>Import</B> — <P>Settings → ADIF Monitor</P> watches external <span className="font-mono text-[11px] text-dark-100">.adi</span> files (VarAC, MSHV, … — <B>Browse</B> to each file or paste its path) and listens for ADIF-over-UDP from N1MM / Logger32 / DXKeeper; <B>WSJT-X / JTDX / MSHV</B> auto-log has its own section — <P>Settings → Decoder Link (UDP)</P> — with two independent UDP sources so you can run two decoders (say WSJT-X and JTDX) on separate ports at once.</p>
          <p><B>Backup &amp; Restore</B> (<P>Settings → Backup &amp; Restore</P>) — turn on <B>Scheduled Backups</B> to save your logbook automatically (daily, weekly, or on exit) to a folder you choose, keeping the last N copies. You can also <B>Export / Import all app settings</B> to a single file — ideal for moving your whole setup to another PC or keeping a safe copy off-machine.</p>
        </Section>

        <Section id="ai" title="AI Talk Points">
          <p><B>AI talk points</B> suggest a few friendly conversation starters for the callsign you're working — drawn from their QRZ profile and your past QSOs — so you always have something to say. Focus a callsign in the Log Entry (with auto-generate on) and they appear; the <B>Chat AI</B> panel also lets you ask follow-up questions. Your API key is stored locally and calls go straight to the provider — nothing routes through SDRLoggerPlus.</p>
          <p>Set it up in <P>Settings → Chat AI</P>: pick a <B>provider</B>, paste a key (if needed), choose a model, and hit <B>Test connection</B>. Providers:</p>
          <ul className="ml-4 list-disc space-y-1.5">
            <li><B>Ollama</B> — a model running <B>locally on your PC</B>: free, private, offline, <B>no API key</B>. Setup below.</li>
            <li><B>Groq</B> — free cloud key, very fast (Llama 3.3); generous free limits — plenty for talk points.</li>
            <li><B>OpenRouter</B> — one key, many models including free ones.</li>
            <li><B>OpenAI</B> / <B>Anthropic</B> — paid, top-tier. <B>Custom</B> — any other OpenAI-compatible endpoint (enter its Base URL).</li>
          </ul>
          <p className="pt-1"><B>Set up Ollama (free, local, no key):</B></p>
          <ol className="ml-4 list-decimal space-y-1.5">
            <li>Install Ollama from{' '}
              <button onClick={() => openLink('https://ollama.com')} className="font-bold text-accent-primary hover:underline" title="Download Ollama">ollama.com</button>{' '}
              (Windows / macOS / Linux).</li>
            <li>Pull a model — in a terminal run <span className="font-mono text-[11px] text-dark-100">ollama pull llama3.2</span> (a good small default; <span className="font-mono text-[11px] text-dark-100">phi3</span>, <span className="font-mono text-[11px] text-dark-100">gemma2</span>, <span className="font-mono text-[11px] text-dark-100">mistral</span> also work).</li>
            <li>Ollama then serves it locally at <span className="font-mono text-[11px] text-dark-100">http://localhost:11434</span> (started automatically).</li>
            <li>In <P>Settings → Chat AI</P> set <B>Provider → Ollama</B> — no key needed, model defaults to <span className="font-mono text-[11px] text-dark-100">llama3.2</span> — then <B>Test connection</B>.</li>
          </ol>
          <p className="text-xs text-dark-300">A mid-range PC handles small models (a few GB) fine; larger models want more RAM/VRAM. It runs on your machine, so there are no usage limits or costs.</p>
        </Section>

        <Section id="awards" title="Awards & Statistics">
          <ul className="ml-4 list-disc space-y-1.5">
            <li><B>Statistics</B> panel — DXCC, WAS, WAZ, WPX, WAC, 5-band awards, VUCC, POTA, IOTA; worked vs. confirmed, filterable by band / continent.</li>
            <li><B>Propagation</B> panel — HF band conditions (from N0NBH) as a 24-hour heatmap by band and UTC hour.</li>
            <li><B>Rotator</B> panel — azimuth / elevation readout + preset headings; configure the hamlib rotctld / serial connection in <P>Settings → Rotator</P>.</li>
          </ul>
        </Section>

        <Section id="settings" title="Settings & Shortcuts">
          <p><B>Arrange your workspace.</B> Every panel <B>docks and drags</B> — grab a panel's title bar to move it, split the view, or tab panels together however you like, and the arrangement is remembered across sessions. Once you've built an operating position you like, save it: <P>Settings → Appearance → Layout Presets</P> holds up to <B>3 named layouts</B> to switch between (say, one for casual logging and one for a DX pileup).</p>
          <p>Every panel has its own <B>gear</B> (top-right of the header) for panel-specific tuning. The main <P>Settings</P> sections: Station · Web Logbooks · Alerts · ADIF Monitor · Decoder Link (UDP) · Digital Decode Alerts · Band Openings · Rotator · Backup &amp; Restore · S.A.T. · Appearance · Map · Header Bar · Chat AI · About.</p>
          <p><B>Starter layouts.</B> The desktop app's <B>View → Layouts</B> menu has five ready-made workspaces — <B>General, Contest, POTA, Satellite, Digital</B> — plus save / apply / reset, on top of your own 3 named presets.</p>
          <p><B>Mute.</B> The speaker button in the status bar silences all spoken announcements — green when audible, red when muted; it also cuts off whatever is mid-sentence.</p>
          <p><B>UI scale — fit more on screen.</B> Two independent, down-only (70–100%) zoom controls for smaller or high-density displays. <B>Whole-app zoom</B> (desktop app): the <B>View</B> menu or <span className="font-mono text-accent-primary">Ctrl +</span> / <span className="font-mono text-accent-primary">Ctrl −</span> / <span className="font-mono text-accent-primary">Ctrl 0</span>, also as a stepper in <P>Settings → Appearance → UI Scale</P>. <B>Per-panel scale</B>: the small <span className="font-mono">− % +</span> stepper in each panel's tabset header shrinks just that panel (click the % to reset); it's saved with your layout. The Map, Globe and Panadapter don't scale (their canvas geometry needs true pixels).</p>
          <p><B>Units (imperial / metric).</B> One master switch — <P>Settings → Appearance → Units</P> — sets how every physical value is shown app-wide: distance to DX, satellite range / altitude / footprint, header temperature &amp; wind, and lightning proximity. A few features can override it: the Weather <B>wind</B> switch defaults to <B>Auto</B> (follow the master) but can be pinned to mph or kph on its own, and the RBN Band-Openings and the lightning <B>Alert range</B> keep their own mi / km pickers — the lightning banner shows the strike distance in that same unit.</p>
          <p className="text-xs text-dark-300">Tip: the <B>Appearance</B> section has the theme picker (dark, night-ops, midnight, and more).</p>
          <p className="pt-1"><B>Keyboard &amp; mouse:</B></p>
          <ul className="ml-4 list-disc space-y-1 text-xs">
            <li><span className="font-mono text-accent-primary">Esc</span> — close this dialog / clear the Log Entry form.</li>
            <li><span className="font-mono text-accent-primary">Mouse wheel</span> over the panadapter or the Rig VFO — tune by the STEP dropdown.</li>
            <li><span className="font-mono text-accent-primary">Ctrl / Shift + wheel</span> — zoom the panadapter / globe.</li>
            <li><span className="font-mono text-accent-primary">Ctrl + / Ctrl − / Ctrl 0</span> — zoom the whole app out / in / reset (desktop app).</li>
          </ul>
        </Section>

        <Section id="updates" title="Updates & Support">
          <p>SDRLoggerPlus checks GitHub for new releases and lets you know when one is available (also on startup); the update prompt opens the download page.</p>
          <div className="flex flex-wrap gap-2 pt-1">
            <button
              onClick={() => openLink(GITHUB_URL)}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded bg-dark-700 border border-glass-200 text-accent-primary hover:bg-dark-600 text-xs font-ui transition-colors"
            >
              <Github className="w-3.5 h-3.5" /> GitHub — releases, issues, docs
            </button>
            <button
              onClick={() => openLink(DISCORD_URL)}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded bg-[#5865F2]/15 border border-[#5865F2]/40 text-[#a3abff] hover:bg-[#5865F2]/25 text-xs font-ui transition-colors"
            >
              <MessageCircle className="w-3.5 h-3.5" /> Discord — community &amp; support
            </button>
          </div>
          <p className="text-xs text-dark-300 pt-1">Enjoying it? There's a “Support the project” link on the About tab. 73!</p>
        </Section>
      </div>
    </div>
  );
}

// How many "## " session headings to show inline in the dialog before
// truncating and linking off to GitHub for the rest. Three keeps the
// dialog scannable while still covering recent development.
const CHANGELOG_INLINE_SECTIONS = 3;
const CHANGELOG_GITHUB_URL = 'https://github.com/N8SDR1/SDRLoggerPlus/blob/v2-alpha/CHANGELOG.md';

/**
 * Trim the bundled CHANGELOG.md to the last N `## ` sessions. Everything
 * before the first `## ` (top-level intro paragraphs) is kept as-is, then
 * the first N `## ` blocks, then we stop. Returns { text, truncated }
 * so the caller can render a "see full history on GitHub" footer.
 */
function trimChangelog(full: string, keepSections: number): { text: string; truncated: boolean } {
  const lines = full.split('\n');
  let seen = 0;
  const kept: string[] = [];
  for (const line of lines) {
    if (line.startsWith('## ')) {
      seen++;
      if (seen > keepSections) return { text: kept.join('\n'), truncated: true };
    }
    kept.push(line);
  }
  return { text: kept.join('\n'), truncated: false };
}

function ChangelogTab({ text, error }: { text: string | null; error: string | null }) {
  if (error) {
    return (
      <div className="text-sm text-red-400">
        <p>Failed to load changelog: {error}</p>
        <p className="text-dark-300 text-xs mt-2">
          The bundled CHANGELOG.md file may be missing from this build.
        </p>
      </div>
    );
  }
  if (text === null) {
    return <p className="text-sm text-dark-300">Loading changelog…</p>;
  }
  const { text: shown, truncated } = trimChangelog(text, CHANGELOG_INLINE_SECTIONS);

  const openLink = (url: string) => {
    if (window.electronAPI && 'openExternal' in window.electronAPI) {
      window.electronAPI.openExternal(url);
    } else {
      window.open(url, '_blank', 'noopener,noreferrer');
    }
  };

  // Very light Markdown rendering — no full parser needed. Headings pop,
  // lists render as lists, everything else is preformatted for readability.
  return (
    <div className="text-sm text-dark-200 space-y-1 leading-relaxed">
      {shown.split('\n').map((line, i) => {
        if (line.startsWith('## ')) {
          return <h2 key={i} className="text-base font-semibold font-ui text-accent-primary mt-4 mb-1 first:mt-0">{line.replace(/^## /, '')}</h2>;
        }
        if (line.startsWith('### ')) {
          return <h3 key={i} className="text-sm font-semibold font-ui text-white mt-3 mb-0.5">{line.replace(/^### /, '')}</h3>;
        }
        if (line.startsWith('# ')) {
          return <h1 key={i} className="text-xl font-bold font-display text-white mb-2">{line.replace(/^# /, '')}</h1>;
        }
        if (line.startsWith('- ')) {
          return <p key={i} className="ml-4 text-xs text-dark-200">• {line.replace(/^- /, '').replace(/\*\*(.+?)\*\*/g, '$1')}</p>;
        }
        if (line.startsWith('  - ')) {
          return <p key={i} className="ml-8 text-xs text-dark-300">◦ {line.replace(/^  - /, '').replace(/\*\*(.+?)\*\*/g, '$1')}</p>;
        }
        if (line.trim() === '') {
          return <div key={i} className="h-1" />;
        }
        // Body paragraph.
        return <p key={i} className="text-xs text-dark-200 leading-relaxed">{line.replace(/\*\*(.+?)\*\*/g, '$1').replace(/`(.+?)`/g, '$1')}</p>;
      })}
      {truncated && (
        <div className="mt-6 pt-4 border-t border-glass-100 text-center">
          <p className="text-xs text-dark-300 mb-2">
            Only the {CHANGELOG_INLINE_SECTIONS} most recent sessions are shown here to keep the dialog focused.
          </p>
          <button
            onClick={() => openLink(CHANGELOG_GITHUB_URL)}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded bg-dark-700 border border-glass-200 text-accent-primary hover:bg-dark-600 text-xs font-ui transition-colors"
          >
            <ScrollText className="w-3 h-3" />
            Full changelog on GitHub
          </button>
        </div>
      )}
    </div>
  );
}
