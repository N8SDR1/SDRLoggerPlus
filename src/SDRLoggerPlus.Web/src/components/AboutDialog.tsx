import { useEffect, useRef } from 'react';
import { X } from 'lucide-react';
import { APP_VERSION } from '../version';

interface AboutDialogProps {
  isOpen: boolean;
  onClose: () => void;
}

export function AboutDialog({ isOpen, onClose }: AboutDialogProps) {
  const dialogRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!isOpen) return;
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const openLink = (url: string) => {
    if (window.electronAPI && 'openExternal' in window.electronAPI) {
      window.electronAPI.openExternal(url);
    } else {
      window.open(url, '_blank', 'noopener,noreferrer');
    }
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm animate-fade-in"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        ref={dialogRef}
        className="relative glass-panel border border-glass-200 rounded-xl shadow-2xl p-8 max-w-sm w-full mx-4 animate-scale-in text-center"
      >
        <button
          onClick={onClose}
          className="absolute top-3 right-3 p-1 rounded-lg hover:bg-dark-600 transition-colors text-dark-300 hover:text-white"
          title="Close"
        >
          <X className="w-4 h-4" />
        </button>

        <img
          src="./sdrloggerplus-banner.png"
          alt="SDRLoggerPlus"
          className="w-full h-auto mx-auto mb-4 rounded-lg"
        />

        <h1 className="text-2xl font-bold text-white font-display tracking-wider">
          SDRLoggerPlus
        </h1>
        <p className="text-dark-300 text-sm mt-1">
          Amateur Radio Logging Software
        </p>
        <p className="text-dark-400 text-xs font-mono mt-2">
          Version {APP_VERSION}
        </p>

        <div className="border-t border-glass-100 my-5" />

        <div className="text-left text-xs text-dark-400 space-y-1.5">
          <p className="text-dark-300 font-medium">Credits</p>
          <p>
            Built on{' '}
            <button
              onClick={() => openLink('https://github.com/brianbruff/Log4YM')}
              className="text-accent-primary hover:underline"
            >
              Log4YM
            </button>{' '}
            by Brian Keating, EI6LF (public domain) — the origin of this codebase.
          </p>
          <p>
            Feature designs ported from <span className="text-dark-300">SDRLogger+</span>:
            awards tracking, weather alerts, S.A.T. integration, Hot List, and scheduled backups.
          </p>
          <p>
            Licensed under the MIT License. Bundles{' '}
            <span className="text-dark-300">Hamlib</span> and{' '}
            <span className="text-dark-300">libusb</span> (LGPL-2.1) and the{' '}
            <span className="text-dark-300">AD1C Country Files</span>. See
            THIRD-PARTY-NOTICES for full open-source license information.
          </p>
        </div>

        <button
          onClick={onClose}
          className="mt-5 px-6 py-2 rounded-lg bg-dark-600 hover:bg-dark-500 text-white text-sm font-medium transition-colors"
        >
          Close
        </button>
      </div>
    </div>
  );
}
