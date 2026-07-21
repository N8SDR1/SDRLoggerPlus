import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { Keyboard, X } from 'lucide-react';
import { useShortcutStore } from '../store/shortcutStore';
import { useRegisterShortcuts } from '../hooks/useRegisterShortcuts';
import { groupShortcuts, isHelpChord, GLOBAL_SCOPE, type Shortcut } from '../utils/shortcuts';

// The overlay documents its own keys, so it appears in its own list.
const GLOBAL_SHORTCUTS: Shortcut[] = [
  { keys: '?', label: 'Show or hide this shortcut list' },
  { keys: 'Ctrl+/', label: 'Show or hide this list (works while typing)' },
  { keys: 'Esc', label: 'Close the shortcut list' },
];

/**
 * The `?` keyboard reference. Renders whatever panels have registered, so it
 * can never drift out of date the way a hand-written help page would.
 */
export function ShortcutOverlay() {
  const [open, setOpen] = useState(false);
  const registrations = useShortcutStore((s) => s.registrations);

  useRegisterShortcuts(GLOBAL_SCOPE, GLOBAL_SHORTCUTS);

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        setOpen(false);
        return;
      }
      if (isHelpChord(e)) {
        e.preventDefault();
        setOpen((v) => !v);
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  if (!open) return null;

  const groups = groupShortcuts(registrations);

  return createPortal(
    <div
      className="fixed inset-0 bg-dark-900/85 backdrop-blur-sm flex items-center justify-center z-50"
      onClick={() => setOpen(false)}
    >
      <div
        className="glass-panel w-[34rem] max-h-[80vh] flex flex-col animate-fade-in"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-4 py-3 border-b border-glass-100">
          <div className="flex items-center gap-2">
            <Keyboard className="w-5 h-5 text-accent-secondary" />
            <h3 className="font-semibold text-accent-success font-ui text-sm tracking-wide uppercase">
              Keyboard Shortcuts
            </h3>
          </div>
          <button
            onClick={() => setOpen(false)}
            className="p-1 hover:bg-dark-600 rounded transition-colors text-dark-300 hover:text-accent-danger"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto px-4 py-3">
          {groups.length === 0 ? (
            <div className="text-center py-6 text-dark-300 font-ui text-sm">
              No shortcuts registered.
            </div>
          ) : (
            groups.map(({ scope, shortcuts }) => (
              <div key={scope} className="mt-4 first:mt-0">
                <h4 className="text-xs font-ui font-semibold text-dark-300 uppercase tracking-wider mb-2">
                  {scope}
                </h4>
                <div className="space-y-1">
                  {shortcuts.map((s) => (
                    <div key={`${s.keys}-${s.label}`} className="flex items-baseline gap-3 text-sm">
                      <kbd className="font-mono text-xs bg-dark-700/70 border border-glass-100 rounded px-1.5 py-0.5 text-accent-secondary whitespace-nowrap min-w-[5rem] text-center">
                        {s.keys}
                      </kbd>
                      <span className="font-ui text-gray-100">{s.label}</span>
                    </div>
                  ))}
                </div>
              </div>
            ))
          )}
        </div>

        <div className="px-4 py-2 border-t border-glass-100 text-xs font-ui text-dark-300">
          Shortcuts for panels that are currently open.
        </div>
      </div>
    </div>,
    document.body,
  );
}
