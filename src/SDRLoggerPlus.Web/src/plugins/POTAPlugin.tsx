import { useMemo, useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { TentTree, Radio, Map, Crosshair, Search, X } from 'lucide-react';
import { AgGridReact } from 'ag-grid-react';
import { ColDef, ICellRendererParams, RowClickedEvent } from 'ag-grid-community';
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-alpine.css';
import { api, PotaSpot } from '../api/client';
import { useSignalR } from '../hooks/useSignalR';
import { GlassPanel } from '../components/GlassPanel';
import { MultiSelectDropdown, MultiSelectOption } from '../components/MultiSelectDropdown';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { useAgGridState } from '../hooks/useAgGridState';
import { getBandFromFrequency, BAND_OPTIONS, MODE_OPTIONS } from '../utils/spotBands';
import { rigModeToSpotModes } from '../utils/rigTracking';

const formatTime = (dateStr: string) => {
  if (!dateStr) return '--:--';
  try {
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) return '--:--';
    return date.toISOString().slice(11, 16);
  } catch {
    return '--:--';
  }
};

const getAge = (dateStr: string) => {
  if (!dateStr) return '-';
  try {
    const now = new Date();
    const spotted = new Date(dateStr);
    if (isNaN(spotted.getTime())) return '-';
    const minutes = Math.floor((now.getTime() - spotted.getTime()) / 60000);
    if (minutes < 1) return 'now';
    if (minutes < 60) return `${minutes}m`;
    return `${Math.floor(minutes / 60)}h`;
  } catch {
    return '-';
  }
};

// Custom cell renderer for activator callsign (green)
const ActivatorCellRenderer = (props: ICellRendererParams<PotaSpot>) => {
  return <span className="font-mono font-bold text-accent-success">{props.value}</span>;
};

// Custom cell renderer for park reference
const ReferenceCellRenderer = (props: ICellRendererParams<PotaSpot>) => {
  return <span className="font-mono text-accent-info">{props.value}</span>;
};

// Custom cell renderer for frequency
const FrequencyCellRenderer = (props: ICellRendererParams<PotaSpot>) => {
  const freq = parseFloat(props.value);
  if (isNaN(freq)) return <span className="text-gray-500">{props.value}</span>;
  return <span className="font-mono text-accent-warning">{(freq / 1000).toFixed(3)}</span>;
};

// Custom cell renderer for time with age
const TimeCellRenderer = (props: ICellRendererParams<PotaSpot>) => {
  const time = props.data?.spotTime || '';
  const age = getAge(time);
  return (
    <div className="flex items-center gap-2 text-gray-400">
      <span className="font-mono">{formatTime(time)}</span>
      <span className="text-xs text-gray-600">({age})</span>
    </div>
  );
};

export function POTAPlugin() {
  const { onGridReady, onColumnChanged, onSortChanged } = useAgGridState('pota');
  const { selectSpot } = useSignalR();
  const { setPotaSpots } = useAppStore();
  const { settings, updateMapSettings, updatePotaSettings, saveSettings } = useSettingsStore();

  const { data: spots, isLoading } = useQuery({
    queryKey: ['pota-spots'],
    queryFn: () => api.getPotaSpots(),
    refetchInterval: 60000, // Refresh every minute
  });

  // Rig state for "follow rig" — read the selected radio (independent from the
  // DX Cluster panel's own follow-rig toggles).
  const selectedRadioId = useAppStore((state) => state.selectedRadioId);
  const radioStates = useAppStore((state) => state.radioStates);
  const rigState = selectedRadioId ? radioStates.get(selectedRadioId) : undefined;
  const rigConnected = !!rigState;
  const rigFreqHz = rigState?.frequencyHz;
  const rigMode = rigState?.mode;

  // Filter state (local to this panel).
  // Follow-rig persists in settings (like the DX Cluster) so it survives a
  // restart or panel re-dock.
  const followBand = settings.pota.followRigBand;
  const followMode = settings.pota.followRigMode;
  const toggleFollowBand = () => { updatePotaSettings({ followRigBand: !followBand }); saveSettings(); };
  const toggleFollowMode = () => { updatePotaSettings({ followRigMode: !followMode }); saveSettings(); };
  const [selectedBands, setSelectedBands] = useState<string[]>([]);
  const [selectedModes, setSelectedModes] = useState<string[]>([]);
  const [selectedRegions, setSelectedRegions] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState('');

  const bandTracking = followBand && rigConnected;
  const modeTracking = followMode && rigConnected;

  // Region dropdown options — the park locations currently present in the feed.
  const regionOptions = useMemo<MultiSelectOption[]>(() => {
    const set = new Set<string>();
    for (const s of spots ?? []) {
      if (!s.invalid && s.locationDesc) set.add(s.locationDesc);
    }
    return Array.from(set).sort().map((r) => ({ value: r, label: r }));
  }, [spots]);

  // Filter and sort spots. Follow-rig (when a rig is connected) overrides the
  // manual Band / Mode dropdowns, mirroring the DX Cluster panel.
  const filteredSpots = useMemo(() => {
    if (!spots) return [];

    const oneHourAgo = new Date().getTime() - 60 * 60 * 1000;
    const query = searchQuery.trim().toLowerCase();

    const rigBand = bandTracking ? getBandFromFrequency((rigFreqHz ?? 0) / 1000) : null;
    const rigModes = modeTracking ? rigModeToSpotModes(rigMode) : null;
    const effectiveBands = bandTracking
      ? (rigBand && rigBand !== '?' ? [rigBand] : [])
      : selectedBands;
    const effectiveModes = modeTracking ? (rigModes ?? []) : selectedModes;

    return spots
      .filter(spot => {
        if (spot.invalid) return false;

        // 1-hour age cutoff
        try {
          if (new Date(spot.spotTime).getTime() < oneHourAgo) return false;
        } catch {
          return false;
        }

        // Search
        if (query) {
          const hay = [spot.activator, spot.reference, spot.parkName, spot.locationDesc, spot.mode, spot.spotter, spot.comments]
            .filter(Boolean).join(' ').toLowerCase();
          if (!hay.includes(query)) return false;
        }

        // Band (derived from frequency in kHz)
        if (effectiveBands.length > 0) {
          const band = getBandFromFrequency(parseFloat(spot.frequency));
          if (!effectiveBands.includes(band)) return false;
        }

        // Mode (USB/LSB normalize to SSB)
        if (effectiveModes.length > 0) {
          let m = spot.mode?.toUpperCase();
          if (m === 'USB' || m === 'LSB') m = 'SSB';
          if (!m || !effectiveModes.includes(m)) return false;
        }

        // Region (park location)
        if (selectedRegions.length > 0) {
          if (!spot.locationDesc || !selectedRegions.includes(spot.locationDesc)) return false;
        }

        return true;
      })
      .sort((a, b) => {
        try {
          return new Date(b.spotTime).getTime() - new Date(a.spotTime).getTime();
        } catch {
          return 0;
        }
      });
  }, [spots, searchQuery, selectedBands, selectedModes, selectedRegions, bandTracking, modeTracking, rigFreqHz, rigMode]);

  // Sync filtered spots to app store for map visualization
  useEffect(() => {
    setPotaSpots(filteredSpots);
  }, [filteredSpots, setPotaSpots]);

  const hasActiveFilters = selectedBands.length > 0 || selectedModes.length > 0 || selectedRegions.length > 0 || searchQuery.trim().length > 0;
  const totalActiveFilters = selectedBands.length + selectedModes.length + selectedRegions.length + (searchQuery.trim() ? 1 : 0);
  const clearAllFilters = () => {
    setSelectedBands([]); setSelectedModes([]); setSelectedRegions([]); setSearchQuery('');
  };

  const handleRowClick = async (event: RowClickedEvent<PotaSpot>) => {
    const spot = event.data;
    if (spot) {
      const freqKhz = parseFloat(spot.frequency);
      // Carry the park reference so the log entry can fill the worked-park field (POTA mode) or a
      // Remarks note (General mode) — #59.
      await selectSpot(spot.activator, freqKhz, spot.mode, spot.reference);
    }
  };

  const columnDefs = useMemo<ColDef<PotaSpot>[]>(() => [
    {
      headerName: 'Time',
      field: 'spotTime',
      cellRenderer: TimeCellRenderer,
      width: 110,
      resizable: true,
    },
    {
      headerName: 'Activator',
      field: 'activator',
      cellRenderer: ActivatorCellRenderer,
      width: 120,
      resizable: true,
    },
    {
      headerName: 'Park',
      field: 'reference',
      cellRenderer: ReferenceCellRenderer,
      width: 100,
      resizable: true,
    },
    {
      headerName: 'Freq',
      field: 'frequency',
      cellRenderer: FrequencyCellRenderer,
      width: 90,
      resizable: true,
    },
    {
      headerName: 'Mode',
      field: 'mode',
      cellClass: 'font-mono text-gray-400',
      width: 70,
      resizable: true,
    },
    {
      headerName: 'Location',
      field: 'locationDesc',
      cellClass: 'text-gray-500 truncate',
      flex: 1,
      minWidth: 150,
      resizable: true,
    },
    {
      headerName: 'Comments',
      field: 'comments',
      cellClass: 'text-gray-500 truncate',
      flex: 1,
      minWidth: 100,
      resizable: true,
    },
  ], []);

  const defaultColDef = useMemo<ColDef>(() => ({
    sortable: true,
    resizable: true,
    filter: true,
    enableRowGroup: false,
  }), []);

  return (
    <GlassPanel
      title="POTA Activators"
      icon={<TentTree className="w-5 h-5" />}
      actions={
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-400">
            {filteredSpots?.length || 0} active
          </span>
          <button
            onClick={() => {
              updateMapSettings({ showPotaOverlay: !settings.map.showPotaOverlay });
              saveSettings();
            }}
            className={`glass-button p-1.5 ${settings.map.showPotaOverlay ? 'text-accent-info' : 'text-dark-300'}`}
            title={settings.map.showPotaOverlay ? 'Hide map overlay' : 'Show map overlay'}
          >
            <Map className="w-4 h-4" />
          </button>
        </div>
      }
    >
      <div className="flex flex-col h-full">
        {/* Filter toolbar — mirrors the DX Cluster panel (Region replaces Status) */}
        <div className="flex items-center gap-2 flex-wrap px-4 pt-3 pb-2">
          <div className="relative">
            <Search className="w-4 h-4 absolute left-2 top-1/2 -translate-y-1/2 text-dark-400 pointer-events-none" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search call, park, spotter…"
              className="glass-input pl-8 pr-2 py-1.5 text-sm w-48"
            />
          </div>
          <div className="flex items-center gap-1.5 whitespace-nowrap">
            <span className="text-xs text-dark-300 font-ui flex items-center gap-1"><Crosshair className="w-4 h-4" /> Follow rig:</span>
            {([
              { key: 'band', label: 'Band', on: followBand, active: bandTracking, onClick: toggleFollowBand },
              { key: 'mode', label: 'Mode', on: followMode, active: modeTracking, onClick: toggleFollowMode },
            ] as const).map((p) => (
              <button
                key={p.key}
                onClick={p.onClick}
                title={p.on ? (rigConnected ? `Following rig ${p.key} — click to stop` : `Follow ${p.key} armed — waiting for a connected rig`) : `Follow the rig's ${p.key}`}
                className={`px-2.5 py-1.5 rounded-lg text-sm font-ui border transition-colors ${
                  p.active
                    ? 'bg-accent-primary/20 text-accent-primary border-accent-primary/30'
                    : p.on
                      ? 'bg-accent-warning/15 text-accent-warning border-accent-warning/40'
                      : 'bg-dark-800 text-dark-300 border-glass-100 hover:text-dark-200'
                }`}
              >
                {p.label}
              </button>
            ))}
          </div>
          <MultiSelectDropdown options={BAND_OPTIONS} selected={selectedBands} onChange={setSelectedBands} placeholder="All Bands" className="w-32" disabled={bandTracking} title={bandTracking ? 'Following rig band' : undefined} />
          <MultiSelectDropdown options={MODE_OPTIONS} selected={selectedModes} onChange={setSelectedModes} placeholder="All Modes" className="w-32" disabled={modeTracking} title={modeTracking ? 'Following rig mode' : undefined} />
          <MultiSelectDropdown options={regionOptions} selected={selectedRegions} onChange={setSelectedRegions} placeholder="All Regions" className="w-36" />
          {hasActiveFilters && (
            <button onClick={clearAllFilters} className="flex items-center gap-1.5 px-3 py-1.5 bg-accent-warning/20 text-accent-warning rounded-lg text-sm font-ui hover:bg-accent-warning/30 transition-colors whitespace-nowrap" title="Clear all filters">
              <X className="w-4 h-4" /> <span>Clear ({totalActiveFilters})</span>
            </button>
          )}
        </div>
        {/* AG Grid Table */}
        <div className="flex-1 px-4 pb-4 min-h-0">
          <div className="ag-theme-alpine-dark h-full">
            {isLoading ? (
              <div className="flex items-center justify-center py-8 text-gray-500">
                <Radio className="w-4 h-4 animate-spin mr-2" />
                Loading activators...
              </div>
            ) : filteredSpots?.length === 0 ? (
              <div className="text-center py-8 text-gray-500">
                {hasActiveFilters ? (
                  <>
                    <p>No activators match your filters</p>
                    <button onClick={clearAllFilters} className="mt-2 text-accent-primary hover:underline font-ui">Clear filters</button>
                  </>
                ) : 'No active POTA activators'}
              </div>
            ) : (
              <AgGridReact<PotaSpot>
                rowData={filteredSpots}
                columnDefs={columnDefs}
                defaultColDef={defaultColDef}
                rowHeight={36}
                headerHeight={40}
                suppressCellFocus={true}
                animateRows={true}
                onRowClicked={handleRowClick}
                rowClass="cursor-pointer hover:bg-dark-600/50"
                getRowId={(params) => params.data.spotId.toString()}
                suppressMenuHide={true}
                onGridReady={onGridReady}
                onColumnMoved={onColumnChanged}
                onColumnResized={onColumnChanged}
                onColumnVisible={onColumnChanged}
                onColumnPinned={onColumnChanged}
                onSortChanged={onSortChanged}
              />
            )}
          </div>
        </div>
      </div>
    </GlassPanel>
  );
}
