import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Plane, Flame } from 'lucide-react';
import { api, DXpedition } from '../api/client';
import { GlassPanel } from '../components/GlassPanel';
import { useSettingsStore } from '../store/settingsStore';

export function DXpeditionsPlugin() {
  const hotListSettings = useSettingsStore(state => state.settings.hotList);
  const [addedCalls, setAddedCalls] = useState<Set<string>>(new Set());

  const { data, isLoading } = useQuery({
    queryKey: ['dxpeditions'],
    queryFn: () => api.getDXpeditions(),
    refetchInterval: 60 * 60 * 1000, // Refresh every hour
  });

  const addToHotList = async (callsign: string) => {
    try {
      await api.addHotListCalls([callsign]);
      setAddedCalls(prev => new Set(prev).add(callsign.toUpperCase()));
    } catch {
      // Backend unavailable — chip stays unmarked so the user can retry
    }
  };

  const isOnHotList = (callsign: string) =>
    addedCalls.has(callsign.toUpperCase()) ||
    hotListSettings.callsigns.includes(callsign.toUpperCase());

  const getStatusStyle = (expedition: DXpedition) => {
    if (expedition.isActive) {
      return {
        bg: 'rgba(0, 255, 136, 0.15)',
        border: 'rgb(0, 255, 136)',
        color: 'rgb(0, 255, 136)',
        badge: 'NOW'
      };
    }
    if (expedition.isUpcoming) {
      return {
        bg: 'rgba(0, 170, 255, 0.15)',
        border: 'rgb(0, 170, 255)',
        color: 'rgb(0, 170, 255)',
        badge: 'SOON'
      };
    }
    return {
      bg: 'var(--bg-tertiary)',
      border: 'var(--border-color)',
      color: 'var(--text-muted)',
      badge: ''
    };
  };

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
      <div className="h-full overflow-y-auto p-4">
        {isLoading ? (
          <div className="flex items-center justify-center py-8">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-accent-primary"></div>
          </div>
        ) : data?.dxpeditions && data.dxpeditions.length > 0 ? (
          <div className="space-y-2">
            {data.dxpeditions.slice(0, 20).map((expedition, idx) => {
              const style = getStatusStyle(expedition);
              const onHotList = isOnHotList(expedition.callsign);
              return (
                <div
                  key={idx}
                  className="p-3 rounded-lg transition-all duration-200 hover:scale-[1.02]"
                  style={{
                    background: style.bg,
                    borderLeft: `3px solid ${style.border}`,
                  }}
                >
                  <div className="flex justify-between items-start mb-1">
                    <span className="flex items-center gap-1.5">
                      <span className="font-mono font-bold text-accent-primary text-base">
                        {expedition.callsign}
                      </span>
                      <button
                        onClick={() => !onHotList && addToHotList(expedition.callsign)}
                        title={onHotList ? 'On your Hot List' : 'Add to Hot List'}
                        className={`p-0.5 rounded transition-colors ${
                          onHotList
                            ? 'text-orange-400 cursor-default'
                            : 'text-dark-400 hover:text-orange-400'
                        }`}
                      >
                        <Flame className="w-3.5 h-3.5" />
                      </button>
                    </span>
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
