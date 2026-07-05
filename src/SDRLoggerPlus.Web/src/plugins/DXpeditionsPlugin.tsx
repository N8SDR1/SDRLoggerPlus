import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Plane, X } from 'lucide-react';
import { api, DXpedition } from '../api/client';
import { GlassPanel } from '../components/GlassPanel';
import { useSettingsStore } from '../store/settingsStore';

/**
 * DXpeditions panel with a Hot-List control header, ported from v1.x
 * SDRLogger+'s /feeds page. The v1.x behavior we're mirroring:
 *
 *   - "Visual + Voice" pill toggles the Hot List announce mode
 *     (Off → Visual → Visual + Voice, cycling on click).
 *   - Watchlist counter shows how many callsigns are on the list; a
 *     hover tooltip peeks at the first ~60.
 *   - Clear All button empties the list in one click (hidden when the
 *     list is empty).
 *   - Clicking a callsign chip TOGGLES that call on the Hot List:
 *     off → on if it wasn't there, on → off if it was. Matches v1.x
 *     `toggleCall()`.
 *
 * Uses the existing backend endpoints (HotListController). Panel state
 * lives in the settings store so any other tab / panel / component
 * observing `settings.hotList` re-renders with the updated list.
 */
export function DXpeditionsPlugin() {
  const hotListSettings = useSettingsStore(state => state.settings.hotList);
  const updateHotListSettings = useSettingsStore(state => state.updateHotListSettings);
  const [pendingCall, setPendingCall] = useState<string | null>(null);
  const queryClient = useQueryClient();

  const { data, isLoading } = useQuery({
    queryKey: ['dxpeditions'],
    queryFn: () => api.getDXpeditions(),
    refetchInterval: 60 * 60 * 1000, // NG3K feed changes slowly — hourly is plenty
  });

  const upperCalls = new Set(hotListSettings.callsigns.map(c => c.toUpperCase()));
  const isOnHotList = (callsign: string) => upperCalls.has(callsign.toUpperCase());

  const toggleCall = async (callsign: string) => {
    const upper = callsign.toUpperCase();
    setPendingCall(upper);
    try {
      let next: string[];
      if (upperCalls.has(upper)) {
        await api.removeHotListCall(upper);
        next = hotListSettings.callsigns.filter(c => c.toUpperCase() !== upper);
      } else {
        await api.addHotListCalls([upper]);
        next = [...hotListSettings.callsigns, upper];
      }
      // Optimistic update so the chip flips immediately without waiting
      // for the settings store to re-hydrate from the backend.
      updateHotListSettings({ callsigns: next });
    } catch {
      // Backend unavailable — leave state as-is. User can retry.
    } finally {
      setPendingCall(null);
    }
  };

  const clearAll = async () => {
    if (!hotListSettings.callsigns.length) return;
    if (!window.confirm(`Clear all ${hotListSettings.callsigns.length} calls from the Hot List?`)) return;
    try {
      await api.clearHotList();
      updateHotListSettings({ callsigns: [] });
    } catch {
      // no-op — user can retry
    }
  };

  // Cycle: Off → Visual → Visual + Voice → Off. Matches the pill in the
  // v1.x /feeds screenshot the operator flagged for the port.
  const cyclePillMode = async () => {
    const { enabled, ttsEnabled } = hotListSettings;
    let next = { enabled: false, ttsEnabled: false };
    if (!enabled && !ttsEnabled) next = { enabled: true, ttsEnabled: false };
    else if (enabled && !ttsEnabled) next = { enabled: true, ttsEnabled: true };
    else next = { enabled: false, ttsEnabled: false };
    try {
      await api.setHotListFlags(next);
      updateHotListSettings(next);
    } catch {
      // no-op
    }
  };

  const pillLabel = hotListSettings.enabled && hotListSettings.ttsEnabled
    ? 'VISUAL + VOICE'
    : hotListSettings.enabled
    ? 'VISUAL ONLY'
    : 'OFF';
  const pillClass = hotListSettings.enabled
    ? 'bg-green-500/25 border-green-400 text-green-300'
    : 'bg-dark-600 border-dark-400 text-dark-300';
  const pillTitle = hotListSettings.enabled && hotListSettings.ttsEnabled
    ? 'Matching spots will glow red on the panadapter AND be announced by voice. Click to disable.'
    : hotListSettings.enabled
    ? 'Matching spots will glow red on the panadapter (voice off). Click to enable voice.'
    : 'Hot List alerts are OFF. Click to enable visual glow on matching spots.';

  const counterTitle = hotListSettings.callsigns.length === 0
    ? 'Hot List is empty. Click any DXpedition callsign chip below to add.'
    : hotListSettings.callsigns.length <= 60
    ? 'Current Hot List:\n  ' + hotListSettings.callsigns.join(', ')
    : `${hotListSettings.callsigns.length} calls — first 60:\n  ${hotListSettings.callsigns.slice(0, 60).join(', ')}\n  …`;

  return (
    <GlassPanel
      title="DXpeditions"
      icon={<Plane className="w-5 h-5" />}
      actions={
        data && data.active > 0 && (
          <span className="text-xs text-green-400">
            {data.active} active
          </span>
        )
      }
    >
      {/* Hot List control strip — ported from v1.x /feeds header */}
      <div className="px-3 py-2 border-b border-glass-100 flex items-center gap-3 flex-wrap text-xs">
        <span className="text-orange-400 font-medium">🔥 Hot List:</span>
        <button
          onClick={cyclePillMode}
          title={pillTitle}
          className={`px-2 py-1 rounded border font-mono text-[10px] tracking-wider ${pillClass}`}
        >
          {pillLabel}
        </button>
        <span
          className="text-dark-300 flex-1 truncate"
          title={counterTitle}
        >
          {hotListSettings.enabled && hotListSettings.ttsEnabled
            ? '— matching spots glow red AND are announced'
            : hotListSettings.enabled
            ? '— matching spots glow red on the panadapter'
            : '— click Off pill above to enable alerts'}
        </span>
        <span
          className="text-dark-200 font-mono"
          title={counterTitle}
        >
          Watchlist: <span className="text-accent-primary font-bold">{hotListSettings.callsigns.length}</span>
        </span>
        {hotListSettings.callsigns.length > 0 && (
          <button
            onClick={clearAll}
            className="px-2 py-1 rounded border border-accent-danger/60 text-accent-danger hover:bg-accent-danger/10 font-mono text-[10px] flex items-center gap-1"
            title="Clear all callsigns from the Hot List"
          >
            <X className="w-3 h-3" /> CLEAR ALL
          </button>
        )}
        <button
          onClick={() => queryClient.invalidateQueries({ queryKey: ['dxpeditions'] })}
          className="px-2 py-1 rounded border border-dark-500 text-dark-200 hover:bg-dark-700 font-mono text-[10px]"
          title="Force refresh from NG3K"
        >
          ⟳ Refresh
        </button>
      </div>

      <div className="h-full overflow-y-auto p-4">
        {isLoading ? (
          <div className="flex items-center justify-center py-8">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-accent-primary"></div>
          </div>
        ) : data?.dxpeditions && data.dxpeditions.length > 0 ? (
          <div className="space-y-2">
            {data.dxpeditions.map((expedition: DXpedition, idx: number) => {
              const style = getStatusStyle(expedition);
              const onList = isOnHotList(expedition.callsign);
              const pending = pendingCall === expedition.callsign.toUpperCase();
              return (
                <div
                  key={idx}
                  className="p-3 rounded-lg transition-all duration-200 hover:scale-[1.01]"
                  style={{
                    background: style.bg,
                    borderLeft: `3px solid ${style.border}`,
                  }}
                >
                  <div className="flex justify-between items-start mb-1">
                    <button
                      onClick={() => toggleCall(expedition.callsign)}
                      disabled={pending}
                      title={onList
                        ? `On your Hot List — click to remove`
                        : `Click to add ${expedition.callsign} to the Hot List`}
                      className={`font-mono font-bold text-base px-2 py-0.5 rounded border transition-colors ${
                        onList
                          ? 'bg-orange-500/25 border-orange-400 text-orange-300 hover:bg-orange-500/35'
                          : 'bg-dark-700 border-dark-500 text-accent-primary hover:bg-dark-600 hover:border-accent-primary'
                      } ${pending ? 'opacity-60 cursor-wait' : 'cursor-pointer'}`}
                    >
                      {onList ? '🔥 ' : ''}{expedition.callsign}
                    </button>
                    {style.badge && (
                      <span
                        className="text-xs font-bold px-2 py-0.5 rounded"
                        style={{
                          color: style.color,
                          backgroundColor: `${style.color}20`,
                        }}
                      >
                        {style.badge}
                      </span>
                    )}
                  </div>
                  <div className="text-sm text-gray-300 mb-1">
                    {expedition.entity}
                  </div>
                  {expedition.dates && (
                    <div className="text-xs text-gray-500 font-mono">
                      {expedition.dates}
                    </div>
                  )}
                  {expedition.info && (
                    <div className="text-xs text-gray-600 mt-1 line-clamp-2">
                      {expedition.info}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center py-12 text-gray-500">
            <Plane className="w-12 h-12 mb-3 opacity-50" />
            <p>No DXpeditions found</p>
            <p className="text-xs mt-1">Data from NG3K ADXO</p>
          </div>
        )}
      </div>
    </GlassPanel>
  );
}

function getStatusStyle(expedition: DXpedition) {
  if (expedition.isActive) {
    return {
      bg: 'rgba(0, 255, 136, 0.15)',
      border: 'rgb(0, 255, 136)',
      color: 'rgb(0, 255, 136)',
      badge: 'NOW',
    };
  }
  if (expedition.isUpcoming) {
    return {
      bg: 'rgba(0, 170, 255, 0.15)',
      border: 'rgb(0, 170, 255)',
      color: 'rgb(0, 170, 255)',
      badge: 'SOON',
    };
  }
  return {
    bg: 'var(--bg-tertiary)',
    border: 'var(--border-color)',
    color: 'var(--text-muted)',
    badge: '',
  };
}
