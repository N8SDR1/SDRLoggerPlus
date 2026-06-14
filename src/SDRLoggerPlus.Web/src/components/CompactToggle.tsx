import { Minimize2, Maximize2 } from 'lucide-react';

interface CompactToggleProps {
  compact: boolean;
  onToggle: () => void;
}

// Title-bar button that flips a panel between normal and compact density.
export function CompactToggle({ compact, onToggle }: CompactToggleProps) {
  return (
    <button
      onClick={onToggle}
      className={`glass-button p-1.5 ${compact ? 'text-accent-primary' : 'text-dark-300'}`}
      title={compact ? 'Expand to normal density' : 'Compact density'}
    >
      {compact ? <Maximize2 className="w-4 h-4" /> : <Minimize2 className="w-4 h-4" />}
    </button>
  );
}
