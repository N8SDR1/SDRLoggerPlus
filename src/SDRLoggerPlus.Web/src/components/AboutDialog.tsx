import { useEffect, useRef, useState } from 'react';
import { X, Coffee, BookOpen, Info, ScrollText } from 'lucide-react';
import { APP_VERSION } from '../version';

interface AboutDialogProps {
  isOpen: boolean;
  onClose: () => void;
}

type TabId = 'about' | 'help' | 'changelog';

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
export function AboutDialog({ isOpen, onClose }: AboutDialogProps) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const [tab, setTab] = useState<TabId>('about');
  const [changelog, setChangelog] = useState<string | null>(null);
  const [changelogError, setChangelogError] = useState<string | null>(null);

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
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm animate-fade-in"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        ref={dialogRef}
        className="relative glass-panel border border-glass-200 rounded-xl shadow-2xl max-w-2xl w-full mx-4 animate-scale-in flex flex-col max-h-[85vh]"
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

function HelpTab() {
  const openLink = (url: string) => {
    if (window.electronAPI && 'openExternal' in window.electronAPI) {
      window.electronAPI.openExternal(url);
    } else {
      window.open(url, '_blank', 'noopener,noreferrer');
    }
  };
  return (
    <div className="text-sm text-dark-200 space-y-5 leading-relaxed">
      <div>
        <h2 className="text-lg font-semibold font-ui text-white mb-1">Quick start</h2>
        <p className="text-dark-300 text-xs">
          Five minutes to your first QSO in the log.
        </p>
      </div>

      <ol className="space-y-3 list-decimal list-inside">
        <li>
          <strong className="text-white">Set your callsign + grid.</strong>{' '}
          Open <span className="font-mono text-accent-primary">Settings → Station</span> and fill in
          your callsign, name, and grid square. QRZ / HamQTH lookups won't work
          without your call.
        </li>
        <li>
          <strong className="text-white">Connect a radio (optional but recommended).</strong>{' '}
          Any of three paths works and multiple can run at once:
          <ul className="mt-1 ml-6 list-disc space-y-0.5 text-dark-300 text-xs">
            <li>
              <span className="text-dark-100">TCI</span> — works with{' '}
              <button
                onClick={() => openLink('https://github.com/N8SDR1/Lyra-SDR-cpp/releases')}
                className="font-bold text-accent-primary hover:underline"
                title="Lyra SDR — open the GitHub releases page"
              >
                Lyra
              </button>{' '}
              (built by the same team as SDRLoggerPlus), Thetis, and ExpertSDR3.
              The panadapter panel is <span className="text-dark-100">TCI-only</span> —
              it draws its spectrum from the TCI IQ stream, so a TCI radio is
              required for the waterfall to light up. Enable at Settings → Radio → TCI.
            </li>
            <li>
              <span className="text-dark-100">Hamlib</span> — universal (Icom /
              Yaesu / Kenwood / etc.) via rigctld. Tunes the radio but doesn't
              feed the panadapter.
            </li>
            <li>
              <span className="text-dark-100">flrig</span> — XML-RPC bridge to
              flrig's rig database. Tunes the radio but doesn't feed the
              panadapter.
            </li>
          </ul>
        </li>
        <li>
          <strong className="text-white">Add a QRZ / HamQTH account.</strong>{' '}
          Settings → Web Logbooks → QRZ (or HamQTH). Auto-fills operator name,
          country, grid, and coords on every callsign lookup. Each has a Test
          Credentials button so you know the login works before saving.
        </li>
        <li>
          <strong className="text-white">Pick a log-entry mode.</strong>{' '}
          The Log Entry panel has three tabs — General for daily logging,
          POTA for park activations, SAT for satellite QSOs. The tab remembers
          across sessions.
        </li>
        <li>
          <strong className="text-white">Log a QSO.</strong>{' '}
          Type a callsign. QRZ / HamQTH fill everything else. Adjust freq / mode /
          RST as needed and hit <span className="font-mono text-accent-secondary">Log QSO</span>.
        </li>
      </ol>

      <div>
        <h3 className="text-white font-semibold font-ui mb-1">Where to find more</h3>
        <ul className="ml-4 list-disc space-y-1 text-xs">
          <li>
            Every panel has its own <em>Settings</em> button (the gear icon in
            the top-right of the panel header) — start there for panel-specific
            tuning.
          </li>
          <li>
            Weather, POTA, SAT, DX-cluster, awards, statistics, propagation,
            and rotator settings each live in <span className="font-mono text-accent-primary">Settings</span> under
            their own sections.
          </li>
          <li>
            Full documentation, release notes, and issue tracker live on the
            GitHub repository — see the About tab.
          </li>
        </ul>
      </div>

      <div className="border-t border-glass-100 pt-4">
        <h3 className="text-white font-semibold font-ui mb-1">Keyboard shortcuts</h3>
        <ul className="ml-4 list-disc space-y-1 text-xs">
          <li><span className="font-mono text-accent-primary">Esc</span> — close this dialog / cancel a form.</li>
          <li><span className="font-mono text-accent-primary">Ctrl/Shift + Mouse wheel</span> — zoom the panadapter around the VFO.</li>
          <li><span className="font-mono text-accent-primary">Mouse wheel</span> (over panadapter or Rig VFO) — tune by the step you pick in the STEP dropdown.</li>
        </ul>
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
