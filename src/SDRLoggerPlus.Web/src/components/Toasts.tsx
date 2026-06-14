import { X } from 'lucide-react';
import { useToastStore } from '../store/toastStore';

const KIND_STYLES: Record<string, string> = {
  info: 'border-accent-info/50 text-dark-100',
  success: 'border-accent-success/60 text-accent-success',
  warn: 'border-accent-warning/60 text-accent-warning',
  error: 'border-accent-danger/60 text-accent-danger',
};

/** Bottom-right transient notification stack. */
export function Toasts() {
  const { toasts, dismiss } = useToastStore();
  if (toasts.length === 0) return null;

  return (
    <div className="fixed bottom-10 right-4 z-[60] flex flex-col gap-2 max-w-md">
      {toasts.map((t) => (
        <div
          key={t.id}
          className={`glass-panel border px-4 py-3 rounded-lg shadow-glass flex items-start gap-3 animate-fade-in text-sm ${KIND_STYLES[t.kind]}`}
        >
          <span className="flex-1">{t.text}</span>
          <button onClick={() => dismiss(t.id)} className="text-dark-400 hover:text-dark-200 shrink-0" title="Dismiss">
            <X className="w-4 h-4" />
          </button>
        </div>
      ))}
    </div>
  );
}
