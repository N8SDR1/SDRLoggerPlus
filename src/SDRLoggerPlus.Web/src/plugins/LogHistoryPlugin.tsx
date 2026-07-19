import { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { ScrollText, Search, Calendar, Radio, Filter, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight, X, CloudUpload, Loader2, Pencil, Trash2, Upload, Download, FileText, CheckCircle, AlertTriangle, XCircle } from 'lucide-react';
import { AgGridReact } from 'ag-grid-react';
import { ColDef, ICellRendererParams } from 'ag-grid-community';
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-alpine.css';
import { api, QsoResponse, UpdateQsoRequest, AdifImportResponse, ConfirmationSource, ConfirmationMergeResponse } from '../api/client';
import { GlassPanel } from '../components/GlassPanel';
import { getCountryFlag } from '../core/countryFlags';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { useAgGridState } from '../hooks/useAgGridState';
import { utcDatePart, toUtcInstant } from '../utils/qsoDateTime';

// Common RST values for phone modes (SSB, AM, FM)
const RST_PHONE = ['59', '58', '57', '56', '55', '54', '53', '52', '51'];
// Common RST values for CW and digital modes
const RST_CW_DIGITAL = ['599', '589', '579', '569', '559', '549', '539', '529', '519'];

// Custom cell renderer for mode badges
// QSL column — a green badge per confirmed channel (LoTW / eQSL / card).
const QslCellRenderer = (props: ICellRendererParams<QsoResponse>) => {
  const q = props.data;
  const channels: Array<{ on?: boolean; letter: string; title: string }> = [
    { on: q?.confirmedLotw, letter: 'L', title: 'LoTW' },
    { on: q?.confirmedEqsl, letter: 'E', title: 'eQSL' },
    { on: q?.confirmedQrz, letter: 'Q', title: 'QRZ Logbook' },
    { on: q?.confirmedCard, letter: 'C', title: 'Card / paper QSL' },
  ];
  const confirmed = channels.filter((c) => c.on);
  return (
    <div className="flex items-center gap-1 h-full">
      {confirmed.length === 0 ? (
        <span className="text-dark-500 text-xs">—</span>
      ) : (
        confirmed.map((c) => (
          <span
            key={c.letter}
            title={`${c.title} confirmed`}
            className="inline-flex items-center justify-center w-4 h-4 rounded-sm bg-accent-success/20 text-accent-success text-[10px] font-bold font-ui"
          >
            {c.letter}
          </span>
        ))
      )}
    </div>
  );
};

const ModeCellRenderer = (props: ICellRendererParams<QsoResponse>) => {
  const mode = props.value;
  const getModeClass = (mode: string) => {
    switch (mode) {
      case 'CW': return 'badge-cw';
      case 'SSB': return 'badge-ssb';
      case 'FT8':
      case 'FT4': return 'badge-ft8';
      case 'RTTY':
      case 'PSK31': return 'badge-rtty';
      default: return 'bg-dark-600 text-dark-200';
    }
  };
  return <span className={`badge text-xs ${getModeClass(mode)}`}>{mode}</span>;
};

// Custom cell renderer for callsign
const CallsignCellRenderer = (props: ICellRendererParams<QsoResponse>) => {
  return <span className="font-mono font-bold text-accent-primary">{props.value}</span>;
};

// Custom cell renderer for country flag
const FlagCellRenderer = (props: ICellRendererParams<QsoResponse>) => {
  const country = props.data?.station?.country || props.data?.country;
  return <span className="text-lg">{getCountryFlag(country)}</span>;
};

// Custom cell renderer for date with calendar icon
const DateCellRenderer = (props: ICellRendererParams<QsoResponse>) => {
  // Rendered in UTC to match the Time column beside it, which shows the raw UTC
  // TimeOn. Formatting locally put an evening QSO's date a day behind its own
  // time, e.g. "Jun 26 · 0452" for a contact made 04:52 UTC on the 27th.
  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', {
      timeZone: 'UTC',
      month: 'short',
      day: 'numeric',
      year: '2-digit'
    });
  };
  return (
    <div className="flex items-center gap-1 text-dark-300">
      <Calendar className="w-3 h-3" />
      <span className="font-mono">{formatDate(props.value)}</span>
    </div>
  );
};

// Custom cell renderer for actions
const ActionCellRenderer = (props: ICellRendererParams<QsoResponse> & {
  onEdit: (qso: QsoResponse) => void;
  onDelete: (qso: QsoResponse) => void;
}) => {
  if (!props.data) return null;
  return (
    <div className="flex items-center gap-1">
      <button
        onClick={() => props.onEdit(props.data!)}
        className="p-1 text-dark-300 hover:text-accent-primary transition-colors"
        title="Edit QSO"
      >
        <Pencil className="w-3.5 h-3.5" />
      </button>
      <button
        onClick={() => props.onDelete(props.data!)}
        className="p-1 text-dark-300 hover:text-accent-danger transition-colors"
        title="Delete QSO"
      >
        <Trash2 className="w-3.5 h-3.5" />
      </button>
    </div>
  );
};

export function LogHistoryPlugin() {
  const { onGridReady, onColumnChanged, onSortChanged } = useAgGridState('logHistory');
  const [callsignSearch, setCallsignSearch] = useState('');
  const [nameSearch, setNameSearch] = useState('');
  const [selectedBand, setSelectedBand] = useState<string>('');
  const [selectedMode, setSelectedMode] = useState<string>('');
  const [fromDate, setFromDate] = useState<string>('');
  const [toDate, setToDate] = useState<string>('');
  const [currentPage, setCurrentPage] = useState(1);
  const [showFilters, setShowFilters] = useState(false);
  const [isSyncing, setIsSyncing] = useState(false);
  const [editingQso, setEditingQso] = useState<QsoResponse | null>(null);
  const [deletingQso, setDeletingQso] = useState<QsoResponse | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const pageSize = 50;

  // ADIF Import/Export state
  const [showImportModal, setShowImportModal] = useState(false);
  const [importResult, setImportResult] = useState<AdifImportResponse | null>(null);
  const [skipDuplicates, setSkipDuplicates] = useState(true);
  const [markAsSyncedToQrz, setMarkAsSyncedToQrz] = useState(true);
  const [clearExistingLogs, setClearExistingLogs] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const mergeFileInputRef = useRef<HTMLInputElement>(null);
  const [showMergeModal, setShowMergeModal] = useState(false);
  const [mergeSource, setMergeSource] = useState<ConfirmationSource>('lotw');
  const [mergeResult, setMergeResult] = useState<ConfirmationMergeResponse | null>(null);
  const [mergeError, setMergeError] = useState<string | null>(null);
  const [isMerging, setIsMerging] = useState(false);

  const queryClient = useQueryClient();
  const { qrzSyncProgress, setQrzSyncProgress, adifImportProgress, setAdifImportProgress, logHistoryCallsignFilter, lotwUploadProgress, setLotwUploadProgress } = useAppStore();
  const lotwEnabled = useSettingsStore((s) => s.settings.lotw.enabled);
  const [isLotwUploading, setIsLotwUploading] = useState(false);

  const handleSyncToQrz = useCallback(async () => {
    if (isSyncing) return;
    setIsSyncing(true);
    setQrzSyncProgress(null);
    try {
      await api.syncToQrz();
    } catch (error) {
      console.error('Failed to sync to QRZ:', error);
    } finally {
      setIsSyncing(false);
    }
  }, [isSyncing, setQrzSyncProgress]);

  const handlePushToLotw = useCallback(async () => {
    if (isLotwUploading) return;
    setIsLotwUploading(true);
    setLotwUploadProgress(null);
    try {
      const result = await api.uploadToLotw({});
      // Belt-and-suspenders: surface the final result via the progress banner even
      // if SignalR didn't deliver (disconnected, slow, whatever). Same shape as the
      // LotwUploadProgressEvent the backend broadcasts.
      setLotwUploadProgress({
        stage: result.success ? 'done' : 'error',
        qsoCount: result.qsoCount,
        isComplete: true,
        tqslExitCode: result.tqslExitCode,
        message: result.message,
      });
      if (result.success) {
        // Marked-sent flags may have flipped — refresh the grid.
        queryClient.invalidateQueries({ queryKey: ['qsos'] });
      }
    } catch (error) {
      console.error('Failed to push to LOTW:', error);
      setLotwUploadProgress({
        stage: 'error',
        qsoCount: 0,
        isComplete: true,
        tqslExitCode: -1,
        message: error instanceof Error ? error.message : 'Request failed',
      });
    } finally {
      setIsLotwUploading(false);
    }
  }, [isLotwUploading, setLotwUploadProgress, queryClient]);

  const handleCancelSync = useCallback(async () => {
    try {
      await api.cancelQrzSync();
    } catch (error) {
      console.error('Failed to cancel sync:', error);
    }
  }, []);

  const handleCancelImport = useCallback(async () => {
    try {
      await api.cancelImport();
      setAdifImportProgress(null);
    } catch (error) {
      console.error('Failed to cancel import:', error);
    }
  }, [setAdifImportProgress]);

  // Sync callsign filter from LogEntryPlugin
  useEffect(() => {
    setCallsignSearch(logHistoryCallsignFilter || '');
    setCurrentPage(1);
  }, [logHistoryCallsignFilter]);

  // ADIF Import mutation
  const importMutation = useMutation({
    mutationFn: (file: File) => {
      // Clear any previous progress state
      setAdifImportProgress(null);
      return api.importAdif(file, { skipDuplicates, markAsSyncedToQrz, clearExistingLogs });
    },
    onSuccess: (data) => {
      setImportResult(data);
      queryClient.invalidateQueries({ queryKey: ['qsos'] });
      queryClient.invalidateQueries({ queryKey: ['statistics'] });
    },
  });

  // ADIF Export mutation
  const exportMutation = useMutation({
    mutationFn: () => api.exportAdif(),
    onSuccess: (blob) => {
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `sdrloggerplus_export_${new Date().toISOString().slice(0, 10)}.adi`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    },
  });

  const handleFileSelect = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      setShowImportModal(false);
      importMutation.mutate(file);
    }
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  }, [importMutation]);

  const handleMergeFileSelect = useCallback(async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (mergeFileInputRef.current) mergeFileInputRef.current.value = '';
    if (!file) return;
    setIsMerging(true);
    setMergeResult(null);
    setMergeError(null);
    try {
      const result = await api.mergeConfirmations(file, mergeSource);
      setMergeResult(result);
      queryClient.invalidateQueries({ queryKey: ['qsos'] });
      queryClient.invalidateQueries({ queryKey: ['statistics'] });
    } catch (error) {
      console.error('Failed to merge confirmations:', error);
      setMergeError(error instanceof Error ? error.message : 'Merge failed');
    } finally {
      setIsMerging(false);
    }
  }, [mergeSource, queryClient]);

  const handleDownloadSync = useCallback(async (kind: 'lotw' | 'eqsl' | 'qrz') => {
    setIsMerging(true);
    setMergeResult(null);
    setMergeError(null);
    try {
      const result =
        kind === 'lotw'
          ? await api.downloadLotwConfirmations()
          : kind === 'eqsl'
          ? await api.downloadEqslConfirmations()
          : await api.downloadQrzConfirmations();
      setMergeResult(result);
      queryClient.invalidateQueries({ queryKey: ['qsos'] });
      queryClient.invalidateQueries({ queryKey: ['statistics'] });
    } catch (error) {
      console.error(`Failed to download ${kind} confirmations:`, error);
      setMergeError(error instanceof Error ? error.message : `${kind} download failed`);
    } finally {
      setIsMerging(false);
    }
  }, [queryClient]);

  const handleDelete = useCallback(async () => {
    if (!deletingQso || isDeleting) return;
    setIsDeleting(true);
    try {
      await api.deleteQso(deletingQso.id);
      queryClient.invalidateQueries({ queryKey: ['qsos'] });
      queryClient.invalidateQueries({ queryKey: ['statistics'] });
      setDeletingQso(null);
    } catch (error) {
      console.error('Failed to delete QSO:', error);
    } finally {
      setIsDeleting(false);
    }
  }, [deletingQso, isDeleting, queryClient]);

  const handleSaveEdit = useCallback(async (updates: UpdateQsoRequest) => {
    if (!editingQso || isSaving) return;
    setIsSaving(true);
    try {
      await api.updateQso(editingQso.id, updates);
      queryClient.invalidateQueries({ queryKey: ['qsos'] });
      queryClient.invalidateQueries({ queryKey: ['statistics'] });
      setEditingQso(null);
    } catch (error) {
      console.error('Failed to update QSO:', error);
    } finally {
      setIsSaving(false);
    }
  }, [editingQso, isSaving, queryClient]);

  const { data: response, isLoading } = useQuery({
    queryKey: ['qsos', callsignSearch, nameSearch, selectedBand, selectedMode, fromDate, toDate, currentPage],
    queryFn: () => api.getQsos({
      callsign: callsignSearch || undefined,
      name: nameSearch || undefined,
      band: selectedBand || undefined,
      mode: selectedMode || undefined,
      fromDate: fromDate || undefined,
      toDate: toDate || undefined,
      page: currentPage,
      pageSize,
    }),
  });

  const { data: stats } = useQuery({
    queryKey: ['statistics'],
    queryFn: () => api.getStatistics(),
  });

  // Resolve a QSO's contestId → the definition's display name for the Contest column.
  const { data: contestDefs } = useQuery({
    queryKey: ['contest-definitions'],
    queryFn: () => api.getContestDefinitions(),
    staleTime: 5 * 60 * 1000,
  });
  const contestNameById = useMemo(() => {
    const map = new Map<string, string>();
    contestDefs?.forEach((d) => map.set(d.id, d.name));
    return map;
  }, [contestDefs]);

  const qsos = response?.items || [];
  const totalCount = response?.totalCount || 0;
  const totalPages = response?.totalPages || 1;

  const handlePageChange = useCallback((page: number) => {
    setCurrentPage(Math.max(1, Math.min(page, totalPages)));
  }, [totalPages]);

  const clearFilters = useCallback(() => {
    setCallsignSearch('');
    setNameSearch('');
    setSelectedBand('');
    setSelectedMode('');
    setFromDate('');
    setToDate('');
    setCurrentPage(1);
  }, []);

  const hasActiveFilters = callsignSearch || nameSearch || selectedBand || selectedMode || fromDate || toDate;

  const formatTime = (timeStr: string) => {
    if (timeStr && timeStr.length >= 4) {
      return `${timeStr.slice(0, 2)}:${timeStr.slice(2, 4)}`;
    }
    return timeStr || '';
  };

  const columnDefs = useMemo<ColDef<QsoResponse>[]>(() => [
    {
      headerName: 'Date',
      field: 'qsoDate',
      cellRenderer: DateCellRenderer,
      width: 110,
      resizable: true,
    },
    {
      headerName: 'Time',
      field: 'timeOn',
      valueFormatter: (params) => formatTime(params.value),
      cellClass: 'font-mono text-dark-300',
      width: 70,
      resizable: true,
    },
    {
      headerName: 'Callsign',
      field: 'callsign',
      cellRenderer: CallsignCellRenderer,
      width: 110,
      resizable: true,
    },
    {
      headerName: 'Band',
      field: 'band',
      cellClass: 'font-mono text-accent-info',
      width: 70,
      resizable: true,
    },
    {
      headerName: 'Mode',
      field: 'mode',
      cellRenderer: ModeCellRenderer,
      width: 80,
      resizable: true,
    },
    {
      headerName: 'RST S/R',
      valueGetter: (params) => `${params.data?.rstSent || '-'}/${params.data?.rstRcvd || '-'}`,
      cellClass: 'font-mono text-dark-300',
      width: 80,
      resizable: true,
    },
    {
      headerName: 'QSL',
      cellRenderer: QslCellRenderer,
      // Numeric sort key so the column sorts by confirmation strength.
      valueGetter: (params) =>
        (params.data?.confirmedLotw ? 8 : 0) +
        (params.data?.confirmedEqsl ? 4 : 0) +
        (params.data?.confirmedQrz ? 2 : 0) +
        (params.data?.confirmedCard ? 1 : 0),
      width: 70,
      resizable: true,
      headerTooltip: 'Confirmations — L: LoTW, E: eQSL, Q: card / paper QSL',
    },
    {
      headerName: 'Name',
      valueGetter: (params) => params.data?.station?.name || params.data?.name || '-',
      cellClass: 'text-dark-200',
      width: 120,
      resizable: true,
    },
    {
      headerName: '',
      field: 'country',
      cellRenderer: FlagCellRenderer,
      width: 50,
      resizable: false,
      sortable: false,
    },
    {
      headerName: 'Country',
      valueGetter: (params) => params.data?.station?.country || params.data?.country || '-',
      cellClass: 'text-dark-300',
      width: 120,
      resizable: true,
    },
    {
      // Contest column — which contest this QSO was logged under. Resolves the
      // stored ContestDefinition id to its display name, falling back to the raw
      // id if the definition is gone. Blank for casual QSOs.
      headerName: 'Contest',
      field: 'contestId',
      valueGetter: (params) => {
        const id = params.data?.contestId;
        if (!id) return '';
        return contestNameById.get(id) ?? id;
      },
      cellClass: 'text-dark-300 truncate',
      width: 130,
      resizable: true,
    },
    {
      // Remarks column — v1.x parity. Log-entry writes the "Remarks" field
      // into Qso.Comment (the ADIF-exported field), so we surface it here.
      // Fills the previously-blank gap between Country and the action icons.
      headerName: 'Remarks',
      field: 'comment',
      valueGetter: (params) => params.data?.comment || '',
      cellClass: 'text-dark-200 truncate',
      tooltipValueGetter: (params) => params.data?.comment ?? '',
      width: 220,
      resizable: true,
      flex: 1,
    },
    {
      headerName: '',
      field: 'id',
      cellRenderer: ActionCellRenderer,
      cellRendererParams: {
        onEdit: (qso: QsoResponse) => setEditingQso(qso),
        onDelete: (qso: QsoResponse) => setDeletingQso(qso),
      },
      width: 70,
      resizable: false,
      sortable: false,
      pinned: 'right',
    },
  ], [contestNameById]);

  const defaultColDef = useMemo<ColDef>(() => ({
    sortable: true,
    resizable: true,
    filter: true,
    enableRowGroup: false,
  }), []);

  return (
    <GlassPanel
      title="Log History"
      icon={<ScrollText className="w-5 h-5" />}
      actions={
        <div className="flex items-center gap-3 text-sm text-dark-300 font-ui">
          <span>{stats?.uniqueCountries || 0} DXCC</span>
          <div className="flex items-center gap-1">
            <button
              onClick={() => setShowImportModal(true)}
              className="glass-button p-1.5 flex items-center gap-1.5 text-accent-success hover:text-accent-primary"
              title="Import ADIF file"
            >
              <Upload className="w-4 h-4" />
              <span className="text-xs">Import</span>
            </button>
            <button
              onClick={() => exportMutation.mutate()}
              disabled={exportMutation.isPending}
              className="glass-button p-1.5 flex items-center gap-1.5 text-accent-info hover:text-accent-primary disabled:opacity-50"
              title="Export all QSOs to ADIF"
            >
              {exportMutation.isPending ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Download className="w-4 h-4" />
              )}
              <span className="text-xs">Export</span>
            </button>
            <span className="text-glass-100 mx-1">|</span>
            <button
              onClick={handleSyncToQrz}
              disabled={isSyncing}
              className="glass-button p-1.5 flex items-center gap-1.5 text-accent-warning hover:text-accent-primary disabled:opacity-50 disabled:cursor-not-allowed"
              title="Upload unsynced QSOs to QRZ.com (one-way push)"
            >
              {isSyncing ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <CloudUpload className="w-4 h-4" />
              )}
              <span className="text-xs">Push to QRZ</span>
            </button>
            {lotwEnabled && (
              <button
                onClick={handlePushToLotw}
                disabled={isLotwUploading}
                className="glass-button p-1.5 flex items-center gap-1.5 text-accent-warning hover:text-accent-primary disabled:opacity-50 disabled:cursor-not-allowed"
                title="Sign and upload eligible QSOs to ARRL LOTW via TQSL"
              >
                {isLotwUploading ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <CloudUpload className="w-4 h-4" />
                )}
                <span className="text-xs">
                  {lotwUploadProgress && !lotwUploadProgress.isComplete
                    ? `LOTW: ${lotwUploadProgress.stage}`
                    : 'Push to LOTW'}
                </span>
              </button>
            )}
            <button
              onClick={() => { setMergeResult(null); setMergeError(null); setShowMergeModal(true); }}
              className="glass-button p-1.5 flex items-center gap-1.5 text-accent-success hover:text-accent-primary"
              title="Sync confirmations — download from LoTW, or import a LoTW / eQSL / QRZ / card report"
            >
              <CheckCircle className="w-4 h-4" />
              <span className="text-xs">Confirmations</span>
            </button>
          </div>
        </div>
      }
    >
      {/* Fill the panel rather than the viewport: h-full + a flex column lets the
          grid take the leftover space and keeps the pagination row on screen at
          any panel height. */}
      <div className="p-4 space-y-4 h-full flex flex-col min-h-0">
        {/* QRZ Sync Starting (before first progress event) */}
        {isSyncing && !qrzSyncProgress && (
          <div className="bg-dark-700/80 rounded-lg p-3 border border-accent-info/30">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Loader2 className="w-4 h-4 text-accent-info animate-spin" />
                <span className="text-sm text-dark-200 font-ui">
                  Connecting to QRZ.com...
                </span>
              </div>
              <button
                onClick={handleCancelSync}
                className="text-xs text-accent-danger hover:text-accent-danger/80 flex items-center gap-1 px-2 py-1 rounded hover:bg-accent-danger/10 transition-colors"
                title="Cancel sync"
              >
                <X className="w-3 h-3" />
                Cancel
              </button>
            </div>
          </div>
        )}

        {/* QRZ Sync Progress */}
        {qrzSyncProgress && !qrzSyncProgress.isComplete && qrzSyncProgress.total > 0 && (
          <div className="bg-dark-700/80 rounded-lg p-3 border border-accent-info/30">
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <Loader2 className="w-4 h-4 text-accent-info animate-spin" />
                <span className="text-sm text-dark-200 font-ui">
                  {qrzSyncProgress.message || `Syncing to QRZ.com...`}
                </span>
              </div>
              <div className="flex items-center gap-3">
                <span className="text-xs text-dark-300 font-mono">
                  {qrzSyncProgress.completed} / {qrzSyncProgress.total}
                </span>
                <button
                  onClick={handleCancelSync}
                  className="text-xs text-accent-danger hover:text-accent-danger/80 flex items-center gap-1 px-2 py-1 rounded hover:bg-accent-danger/10 transition-colors"
                  title="Cancel sync"
                >
                  <X className="w-3 h-3" />
                  Cancel
                </button>
              </div>
            </div>
            <div className="w-full bg-dark-600 rounded-full h-2 overflow-hidden">
              <div
                className="bg-accent-info h-full transition-all duration-300 ease-out"
                style={{ width: `${(qrzSyncProgress.completed / qrzSyncProgress.total) * 100}%` }}
              />
            </div>
            <div className="flex justify-between text-xs text-dark-300 mt-1 font-mono">
              <span className="text-accent-success">{qrzSyncProgress.successful} successful</span>
              {qrzSyncProgress.failed > 0 && (
                <span className="text-accent-danger">{qrzSyncProgress.failed} failed</span>
              )}
            </div>
          </div>
        )}

        {/* QRZ Sync Complete */}
        {qrzSyncProgress?.isComplete && (
          <div className={`bg-dark-700/80 rounded-lg p-3 border ${
            qrzSyncProgress.message?.includes('cancelled')
              ? 'border-accent-primary/30'
              : 'border-accent-success/30'
          }`}>
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <CloudUpload className={`w-4 h-4 ${
                  qrzSyncProgress.message?.includes('cancelled')
                    ? 'text-accent-primary'
                    : 'text-accent-success'
                }`} />
                <span className="text-sm text-dark-200 font-ui">
                  {qrzSyncProgress.message?.includes('cancelled')
                    ? `Sync cancelled: ${qrzSyncProgress.successful} of ${qrzSyncProgress.total} uploaded`
                    : qrzSyncProgress.total === 0
                      ? 'All QSOs already synced to QRZ'
                      : `QRZ Sync Complete: ${qrzSyncProgress.successful} uploaded${qrzSyncProgress.failed > 0 ? `, ${qrzSyncProgress.failed} failed` : ''}`
                  }
                </span>
              </div>
              <button
                onClick={() => setQrzSyncProgress(null)}
                className="text-dark-300 hover:text-dark-200"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
          </div>
        )}

        {/* LOTW Upload status — live progress or final result (error or success).
            Message may be multi-line (e.g. includes TQSL's own stdout); we split so the
            "TQSL output:" block renders as a monospace block underneath the summary. */}
        {lotwUploadProgress && (() => {
          const raw = lotwUploadProgress.message
            || `LOTW: ${lotwUploadProgress.stage}${lotwUploadProgress.qsoCount > 0 ? ` (${lotwUploadProgress.qsoCount} QSOs)` : ''}`;
          const splitIdx = raw.indexOf('\n\nTQSL output:\n');
          const summary = splitIdx >= 0 ? raw.slice(0, splitIdx) : raw;
          const tqslOutput = splitIdx >= 0 ? raw.slice(splitIdx + '\n\nTQSL output:\n'.length) : '';
          return (
            <div className={`bg-dark-700/80 rounded-lg p-3 border ${
              lotwUploadProgress.stage === 'error'
                ? 'border-accent-danger/30'
                : lotwUploadProgress.isComplete
                  ? 'border-accent-success/30'
                  : 'border-accent-info/30'
            }`}>
              <div className="flex items-start justify-between gap-3">
                <div className="flex items-start gap-2 flex-1 min-w-0">
                  <div className="mt-0.5 shrink-0">
                    {!lotwUploadProgress.isComplete && (
                      <Loader2 className="w-4 h-4 text-accent-info animate-spin" />
                    )}
                    {lotwUploadProgress.isComplete && lotwUploadProgress.stage === 'error' && (
                      <AlertTriangle className="w-4 h-4 text-accent-danger" />
                    )}
                    {lotwUploadProgress.isComplete && lotwUploadProgress.stage !== 'error' && (
                      <CheckCircle className="w-4 h-4 text-accent-success" />
                    )}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="text-sm text-dark-200 font-ui whitespace-pre-wrap">{summary}</div>
                    {tqslOutput && (
                      <pre className="mt-2 text-xs text-dark-300 font-mono bg-dark-800/60 rounded px-2 py-1.5 overflow-x-auto whitespace-pre-wrap">{tqslOutput}</pre>
                    )}
                  </div>
                </div>
                {lotwUploadProgress.isComplete && (
                  <button
                    onClick={() => setLotwUploadProgress(null)}
                    className="text-dark-300 hover:text-dark-200 shrink-0"
                    title="Dismiss"
                  >
                    <X className="w-4 h-4" />
                  </button>
                )}
              </div>
            </div>
          );
        })()}

        {/* ADIF Import Starting (before first progress event) */}
        {importMutation.isPending && !adifImportProgress && (
          <div className="bg-dark-700/80 rounded-lg p-3 border border-accent-success/30">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Loader2 className="w-4 h-4 text-accent-success animate-spin" />
                <span className="text-sm text-dark-200 font-ui">
                  Starting import...
                </span>
              </div>
              <button
                onClick={handleCancelImport}
                className="text-xs text-accent-danger hover:text-accent-danger/80 flex items-center gap-1 px-2 py-1 rounded hover:bg-accent-danger/10 transition-colors"
                title="Cancel import"
              >
                <X className="w-3 h-3" />
                Cancel
              </button>
            </div>
          </div>
        )}

        {/* ADIF Import Progress */}
        {adifImportProgress && !adifImportProgress.isComplete && adifImportProgress.total > 0 && (
          <div className="bg-dark-700/80 rounded-lg p-3 border border-accent-success/30">
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <Loader2 className="w-4 h-4 text-accent-success animate-spin" />
                <span className="text-sm text-dark-200 font-ui">
                  {adifImportProgress.message || `Importing ADIF...`}
                </span>
              </div>
              <div className="flex items-center gap-3">
                <span className="text-xs text-dark-300 font-mono">
                  {adifImportProgress.processed} / {adifImportProgress.total}
                </span>
                <button
                  onClick={handleCancelImport}
                  className="text-xs text-accent-danger hover:text-accent-danger/80 flex items-center gap-1 px-2 py-1 rounded hover:bg-accent-danger/10 transition-colors"
                  title="Cancel import"
                >
                  <X className="w-3 h-3" />
                  Cancel
                </button>
              </div>
            </div>
            <div className="w-full bg-dark-600 rounded-full h-2 overflow-hidden">
              <div
                className="bg-accent-success h-full transition-all duration-300 ease-out"
                style={{ width: `${(adifImportProgress.processed / adifImportProgress.total) * 100}%` }}
              />
            </div>
            <div className="flex justify-between text-xs text-dark-300 mt-1 font-mono">
              <span className="text-accent-success">{adifImportProgress.imported} imported</span>
              <span className="text-accent-primary">{adifImportProgress.skipped} skipped</span>
              {adifImportProgress.failed > 0 && (
                <span className="text-accent-danger">{adifImportProgress.failed} failed</span>
              )}
            </div>
          </div>
        )}

        {/* ADIF Import Complete */}
        {adifImportProgress?.isComplete && (
          <div className={`bg-dark-700/80 rounded-lg p-3 border ${
            adifImportProgress.message?.includes('cancelled')
              ? 'border-accent-primary/30'
              : 'border-accent-success/30'
          }`}>
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Upload className={`w-4 h-4 ${
                  adifImportProgress.message?.includes('cancelled')
                    ? 'text-accent-primary'
                    : 'text-accent-success'
                }`} />
                <span className="text-sm text-dark-200 font-ui">
                  {adifImportProgress.message?.includes('cancelled')
                    ? `Import cancelled: ${adifImportProgress.imported} of ${adifImportProgress.total} imported`
                    : adifImportProgress.total === 0
                      ? 'No records to import'
                      : `Import Complete: ${adifImportProgress.imported} imported${adifImportProgress.skipped > 0 ? `, ${adifImportProgress.skipped} skipped` : ''}${adifImportProgress.failed > 0 ? `, ${adifImportProgress.failed} failed` : ''}`
                  }
                </span>
              </div>
              <button
                onClick={() => setAdifImportProgress(null)}
                className="text-dark-300 hover:text-dark-200"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
          </div>
        )}

        {/* Summary — single line, counts spread across the panel to save vertical space */}
        {stats && (
          <div className="shrink-0 bg-dark-700/50 rounded-lg px-5 py-1.5 flex items-center justify-between flex-nowrap gap-x-6 overflow-x-auto whitespace-nowrap font-ui leading-none">
            <span className="font-medium text-dark-200 text-sm">Summary</span>
            <span className="flex items-baseline gap-2">
              <span className="font-display font-bold text-accent-primary text-xl leading-none">{stats.totalQsos.toLocaleString()}</span>
              {hasActiveFilters && totalCount !== stats.totalQsos && (
                <span className="font-display font-bold text-accent-info text-xl leading-none">({totalCount.toLocaleString()})</span>
              )}
              <span className="text-[13px] text-dark-300">QSOs</span>
            </span>
            <span className="flex items-baseline gap-2">
              <span className="font-display font-bold text-accent-success text-xl leading-none">{stats.uniqueCountries}</span>
              <span className="text-[13px] text-dark-300">Countries</span>
            </span>
            <span className="flex items-baseline gap-2">
              <span className="font-display font-bold text-accent-info text-xl leading-none">{stats.uniqueGrids}</span>
              <span className="text-[13px] text-dark-300">Grids</span>
            </span>
            <span className="flex items-baseline gap-2">
              <span className="font-display font-bold text-accent-warning text-xl leading-none">{stats.qsosToday}</span>
              <span className="text-[13px] text-dark-300">Today</span>
            </span>
          </div>
        )}

        {/* Search and Filters Row */}
        <div className="shrink-0 flex gap-3 flex-wrap">
          {/* Callsign Search */}
          <div className="flex-1 min-w-[150px] relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-dark-300" />
            <input
              type="text"
              value={callsignSearch}
              onChange={(e) => {
                setCallsignSearch(e.target.value.toUpperCase());
                setCurrentPage(1);
              }}
              placeholder="Search callsign..."
              className="glass-input w-full pl-10 font-mono"
            />
          </div>

          {/* Name Search */}
          <div className="flex-1 min-w-[150px] relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-dark-300" />
            <input
              type="text"
              value={nameSearch}
              onChange={(e) => {
                setNameSearch(e.target.value);
                setCurrentPage(1);
              }}
              placeholder="Search name..."
              className="glass-input w-full pl-10"
            />
          </div>

          {/* Band Select */}
          <select
            value={selectedBand}
            onChange={(e) => {
              setSelectedBand(e.target.value);
              setCurrentPage(1);
            }}
            className="glass-input w-24 font-mono"
          >
            <option value="">Band</option>
            <option value="160m">160m</option>
            <option value="80m">80m</option>
            <option value="40m">40m</option>
            <option value="30m">30m</option>
            <option value="20m">20m</option>
            <option value="17m">17m</option>
            <option value="15m">15m</option>
            <option value="12m">12m</option>
            <option value="10m">10m</option>
            <option value="6m">6m</option>
            <option value="2m">2m</option>
          </select>

          {/* Mode Select */}
          <select
            value={selectedMode}
            onChange={(e) => {
              setSelectedMode(e.target.value);
              setCurrentPage(1);
            }}
            className="glass-input w-24 font-mono"
          >
            <option value="">Mode</option>
            <option value="SSB">SSB</option>
            <option value="CW">CW</option>
            <option value="FT8">FT8</option>
            <option value="FT4">FT4</option>
            <option value="RTTY">RTTY</option>
            <option value="PSK31">PSK31</option>
            <option value="AM">AM</option>
            <option value="FM">FM</option>
          </select>

          {/* Filter Toggle */}
          <button
            onClick={() => setShowFilters(!showFilters)}
            className={`glass-button p-2 ${showFilters ? 'bg-accent-primary/20' : ''}`}
            title="Date filters"
          >
            <Filter className="w-4 h-4" />
          </button>

          {/* Clear Filters */}
          {hasActiveFilters && (
            <button
              onClick={clearFilters}
              className="glass-button p-2 text-accent-danger hover:text-accent-danger/80"
              title="Clear all filters"
            >
              <X className="w-4 h-4" />
            </button>
          )}
        </div>

        {/* Date Range Filters (Collapsible) */}
        {showFilters && (
          <div className="flex gap-3 items-center p-3 bg-dark-700/50 rounded-lg">
            <Calendar className="w-4 h-4 text-dark-300" />
            <div className="flex items-center gap-2">
              <label className="text-sm text-dark-300 font-ui">From:</label>
              <input
                type="date"
                value={fromDate}
                onChange={(e) => {
                  setFromDate(e.target.value);
                  setCurrentPage(1);
                }}
                className="glass-input px-2 py-1 text-sm font-mono"
              />
            </div>
            <div className="flex items-center gap-2">
              <label className="text-sm text-dark-300 font-ui">To:</label>
              <input
                type="date"
                value={toDate}
                onChange={(e) => {
                  setToDate(e.target.value);
                  setCurrentPage(1);
                }}
                className="glass-input px-2 py-1 text-sm font-mono"
              />
            </div>
          </div>
        )}

        {/* AG Grid Table */}
        <div className="ag-theme-alpine-dark flex-1 min-h-0">
          {isLoading ? (
            <div className="flex items-center justify-center py-8 text-dark-300">
              <Radio className="w-4 h-4 animate-spin mr-2" />
              Loading...
            </div>
          ) : (
            <AgGridReact<QsoResponse>
              rowData={qsos}
              columnDefs={columnDefs}
              defaultColDef={defaultColDef}
              rowHeight={36}
              headerHeight={40}
              suppressCellFocus={true}
              suppressRowClickSelection={true}
              animateRows={true}
              getRowId={(params) => params.data.id}
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

        {/* Pagination — shrink-0 so it's never squeezed out by the grid above */}
        {totalPages > 1 && (
          <div className="shrink-0 flex items-center justify-between pt-2 border-t border-glass-100">
            <div className="text-sm text-dark-300 font-mono">
              Showing {((currentPage - 1) * pageSize) + 1} - {Math.min(currentPage * pageSize, totalCount)} of {totalCount.toLocaleString()}
            </div>
            <div className="flex items-center gap-1">
              <button
                onClick={() => handlePageChange(1)}
                disabled={currentPage === 1}
                className="glass-button p-1.5 disabled:opacity-30 disabled:cursor-not-allowed"
                title="First page"
              >
                <ChevronsLeft className="w-4 h-4" />
              </button>
              <button
                onClick={() => handlePageChange(currentPage - 1)}
                disabled={currentPage === 1}
                className="glass-button p-1.5 disabled:opacity-30 disabled:cursor-not-allowed"
                title="Previous page"
              >
                <ChevronLeft className="w-4 h-4" />
              </button>
              <div className="flex items-center gap-1 px-2">
                <input
                  type="number"
                  value={currentPage}
                  onChange={(e) => {
                    const page = parseInt(e.target.value) || 1;
                    handlePageChange(page);
                  }}
                  min={1}
                  max={totalPages}
                  className="glass-input w-16 text-center text-sm py-1 font-mono"
                />
                <span className="text-dark-300 text-sm font-mono">/ {totalPages}</span>
              </div>
              <button
                onClick={() => handlePageChange(currentPage + 1)}
                disabled={currentPage === totalPages}
                className="glass-button p-1.5 disabled:opacity-30 disabled:cursor-not-allowed"
                title="Next page"
              >
                <ChevronRight className="w-4 h-4" />
              </button>
              <button
                onClick={() => handlePageChange(totalPages)}
                disabled={currentPage === totalPages}
                className="glass-button p-1.5 disabled:opacity-30 disabled:cursor-not-allowed"
                title="Last page"
              >
                <ChevronsRight className="w-4 h-4" />
              </button>
            </div>
          </div>
        )}

      </div>

      {/* Edit QSO Modal */}
      {editingQso && (
        <EditQsoModal
          qso={editingQso}
          onSave={handleSaveEdit}
          onCancel={() => setEditingQso(null)}
          isSaving={isSaving}
        />
      )}

      {/* Delete Confirmation Modal */}
      {deletingQso && createPortal(
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
          <div className="bg-dark-800 rounded-lg p-6 max-w-md w-full mx-4 border border-glass-200">
            <h3 className="text-lg font-ui font-semibold text-white mb-4">Delete QSO?</h3>
            <p className="text-dark-300 mb-2">
              Are you sure you want to delete this QSO?
            </p>
            <div className="bg-dark-700/50 rounded p-3 mb-6">
              <p className="text-accent-primary font-mono font-bold">{deletingQso.callsign}</p>
              <p className="text-sm text-dark-300 font-mono">
                {new Date(deletingQso.qsoDate).toLocaleDateString()} • {deletingQso.band} • {deletingQso.mode}
              </p>
            </div>
            <p className="text-sm text-accent-danger mb-4">
              This action cannot be undone.
            </p>
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => setDeletingQso(null)}
                disabled={isDeleting}
                className="glass-button px-4 py-2"
              >
                Cancel
              </button>
              <button
                onClick={handleDelete}
                disabled={isDeleting}
                className="bg-accent-danger hover:bg-accent-danger/80 text-white px-4 py-2 rounded-lg flex items-center gap-2 disabled:opacity-50"
              >
                {isDeleting ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Trash2 className="w-4 h-4" />
                )}
                Delete
              </button>
            </div>
          </div>
        </div>,
        document.body
      )}

      {/* ADIF Import Modal */}
      {showImportModal && createPortal(
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
          <div className="bg-dark-800 rounded-lg p-6 max-w-lg w-full mx-4 border border-glass-200">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-ui font-semibold text-white flex items-center gap-2">
                <Upload className="w-5 h-5 text-accent-success" />
                Import ADIF
              </h3>
              <button
                onClick={() => setShowImportModal(false)}
                className="text-dark-300 hover:text-white"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            <p className="text-sm text-dark-300 mb-4">
              Import QSOs from an ADIF file. Supports .adi, .adif, and .xml formats.
            </p>

            {/* Import Options */}
            <div className="space-y-3 mb-6">
              <label className="flex items-center gap-3 p-3 bg-dark-700/50 rounded-lg cursor-pointer hover:bg-dark-700">
                <input
                  type="checkbox"
                  checked={markAsSyncedToQrz}
                  onChange={e => setMarkAsSyncedToQrz(e.target.checked)}
                  className="w-4 h-4 rounded border-glass-200 text-accent-primary focus:ring-accent-primary/40"
                />
                <div>
                  <p className="text-sm text-dark-200 font-ui">Mark as already synced to QRZ</p>
                  <p className="text-xs text-dark-300">
                    Recommended when importing from QRZ.com export (prevents re-uploading)
                  </p>
                </div>
              </label>

              <label className="flex items-center gap-3 p-3 bg-dark-700/50 rounded-lg cursor-pointer hover:bg-dark-700">
                <input
                  type="checkbox"
                  checked={skipDuplicates}
                  onChange={e => setSkipDuplicates(e.target.checked)}
                  className="w-4 h-4 rounded border-glass-200 text-accent-primary focus:ring-accent-primary/40"
                />
                <div>
                  <p className="text-sm text-dark-200 font-ui">Skip duplicate QSOs</p>
                  <p className="text-xs text-dark-300">
                    Skip QSOs that match existing records (callsign, date, time, band, mode)
                  </p>
                </div>
              </label>

              <label className="flex items-center gap-3 p-3 bg-accent-danger/10 border border-accent-danger/30 rounded-lg cursor-pointer hover:bg-accent-danger/20">
                <input
                  type="checkbox"
                  checked={clearExistingLogs}
                  onChange={e => setClearExistingLogs(e.target.checked)}
                  className="w-4 h-4 rounded border-accent-danger/30 text-accent-danger focus:ring-accent-danger/40"
                />
                <div>
                  <p className="text-sm text-accent-danger font-ui">Clear existing log before import</p>
                  <p className="text-xs text-accent-danger/70">
                    Warning: This will delete ALL existing QSOs before importing
                  </p>
                </div>
              </label>
            </div>

            {/* File Input */}
            <input
              ref={fileInputRef}
              type="file"
              accept=".adi,.adif,.xml"
              onChange={handleFileSelect}
              className="hidden"
            />

            <div className="flex gap-3">
              <button
                onClick={() => setShowImportModal(false)}
                className="flex-1 glass-button py-3"
              >
                Cancel
              </button>
              <button
                onClick={() => fileInputRef.current?.click()}
                disabled={importMutation.isPending}
                className="flex-1 bg-accent-success/20 hover:bg-accent-success/30 text-accent-success border border-accent-success/30 py-3 rounded-lg flex items-center justify-center gap-2 disabled:opacity-50"
              >
                {importMutation.isPending ? (
                  <>
                    <Loader2 className="w-5 h-5 animate-spin" />
                    Importing...
                  </>
                ) : (
                  <>
                    <FileText className="w-5 h-5" />
                    Select ADIF File
                  </>
                )}
              </button>
            </div>
          </div>
        </div>,
        document.body
      )}

      {/* Confirmation merge modal */}
      {showMergeModal && createPortal(
        <div
          className="fixed inset-0 bg-black/60 flex items-center justify-center z-50"
          onClick={() => !isMerging && setShowMergeModal(false)}
        >
          <div
            className="bg-dark-800 rounded-lg p-6 max-w-md w-full mx-4 border border-glass-200"
            onClick={(e) => e.stopPropagation()}
          >
            <h3 className="text-lg font-ui font-semibold text-white flex items-center gap-2 mb-1">
              <CheckCircle className="w-5 h-5 text-accent-success" />
              Merge Confirmations
            </h3>
            <p className="text-sm text-dark-300 mb-4">
              Download your confirmation report from LoTW, eQSL, or QRZ, pick the source, then select the file.
              Matching QSOs are marked <span className="text-accent-success font-medium">Confirmed</span> — no
              duplicates are created.
            </p>

            {!mergeResult ? (
              <>
                <label className="text-sm font-medium text-dark-200">Confirmation source</label>
                <div className="grid grid-cols-4 gap-2 mt-2 mb-4">
                  {([['lotw', 'LoTW'], ['eqsl', 'eQSL'], ['qrz', 'QRZ'], ['card', 'Card']] as const).map(([val, label]) => (
                    <button
                      key={val}
                      onClick={() => setMergeSource(val)}
                      className={`rounded-lg border px-3 py-2 text-sm font-ui transition-colors ${
                        mergeSource === val
                          ? 'border-accent-primary/50 bg-accent-primary/15 text-accent-primary'
                          : 'border-glass-100 bg-dark-700/40 text-dark-300 hover:text-dark-100'
                      }`}
                    >
                      {label}
                    </button>
                  ))}
                </div>
                {(mergeSource === 'lotw' || mergeSource === 'eqsl' || mergeSource === 'qrz') && (
                  <div className="mb-4">
                    {(() => {
                      const label = mergeSource === 'lotw' ? 'LoTW' : mergeSource === 'eqsl' ? 'eQSL' : 'QRZ';
                      const note =
                        mergeSource === 'lotw'
                          ? 'Uses your LoTW website login from Settings → LoTW. Pulls only new confirmations since the last sync.'
                          : mergeSource === 'eqsl'
                          ? 'Uses your eQSL login from Settings → eQSL. Requires an Authenticity-Guaranteed account. Pulls only new confirmations since the last sync.'
                          : 'Uses your QRZ Logbook API key from Settings → QRZ. Fetches your logbook and confirms matching QSOs.';
                      return (
                        <>
                          <button
                            onClick={() => handleDownloadSync(mergeSource as 'lotw' | 'eqsl' | 'qrz')}
                            disabled={isMerging}
                            className="w-full bg-accent-primary/15 hover:bg-accent-primary/25 text-accent-primary border border-accent-primary/40 py-2.5 rounded-lg flex items-center justify-center gap-2 text-sm font-medium disabled:opacity-50"
                          >
                            {isMerging ? (
                              <><Loader2 className="w-4 h-4 animate-spin" />Downloading from {label}…</>
                            ) : (
                              <><CloudUpload className="w-4 h-4 rotate-180" />Download from {label} (one-click)</>
                            )}
                          </button>
                          <p className="text-[11px] text-dark-400 mt-1.5">{note}</p>
                        </>
                      );
                    })()}
                    <div className="flex items-center gap-2 my-3">
                      <div className="flex-1 h-px bg-glass-100" />
                      <span className="text-[10px] text-dark-500 uppercase tracking-wide">or import a file</span>
                      <div className="flex-1 h-px bg-glass-100" />
                    </div>
                  </div>
                )}

                {mergeError && (
                  <div className="mb-3 p-2.5 rounded-lg bg-accent-danger/10 border border-accent-danger/30 text-xs text-accent-danger">
                    {mergeError}
                  </div>
                )}

                <input
                  ref={mergeFileInputRef}
                  type="file"
                  accept=".adi,.adif"
                  onChange={handleMergeFileSelect}
                  className="hidden"
                />
                <div className="flex gap-3">
                  <button
                    onClick={() => setShowMergeModal(false)}
                    disabled={isMerging}
                    className="flex-1 glass-button py-3"
                  >
                    Cancel
                  </button>
                  <button
                    onClick={() => mergeFileInputRef.current?.click()}
                    disabled={isMerging}
                    className="flex-1 bg-accent-success/20 hover:bg-accent-success/30 text-accent-success border border-accent-success/30 py-3 rounded-lg flex items-center justify-center gap-2 disabled:opacity-50"
                  >
                    {isMerging ? (
                      <>
                        <Loader2 className="w-5 h-5 animate-spin" />
                        Merging…
                      </>
                    ) : (
                      <>
                        <FileText className="w-5 h-5" />
                        Select {mergeSource === 'lotw' ? 'LoTW' : mergeSource === 'eqsl' ? 'eQSL' : mergeSource === 'qrz' ? 'QRZ' : 'card'} ADIF
                      </>
                    )}
                  </button>
                </div>
              </>
            ) : (
              <>
                <div className="grid grid-cols-4 gap-3 mb-4">
                  <div className="text-center">
                    <p className="text-2xl font-display font-bold text-accent-primary">{mergeResult.totalRecords}</p>
                    <p className="text-xs text-dark-300 font-ui">In file</p>
                  </div>
                  <div className="text-center">
                    <p className="text-2xl font-display font-bold text-accent-success">{mergeResult.updated}</p>
                    <p className="text-xs text-dark-300 font-ui">Confirmed</p>
                  </div>
                  <div className="text-center">
                    <p className="text-2xl font-display font-bold text-dark-200">{mergeResult.alreadyConfirmed}</p>
                    <p className="text-xs text-dark-300 font-ui">Already</p>
                  </div>
                  <div className="text-center">
                    <p className="text-2xl font-display font-bold text-accent-warning">{mergeResult.unmatched}</p>
                    <p className="text-xs text-dark-300 font-ui">No match</p>
                  </div>
                </div>
                <p className="text-xs text-dark-400 mb-4">
                  {mergeResult.updated} QSO{mergeResult.updated === 1 ? '' : 's'} newly confirmed via{' '}
                  {mergeSource === 'lotw' ? 'LoTW' : mergeSource === 'eqsl' ? 'eQSL' : mergeSource === 'qrz' ? 'QRZ' : 'card'}. Unmatched records aren't
                  in your log (call / band / mode / date didn't line up). The Statistics panel now reflects the new
                  Confirmed counts.
                </p>
                <button
                  onClick={() => { setMergeResult(null); setShowMergeModal(false); }}
                  className="w-full glass-button py-3"
                >
                  Done
                </button>
              </>
            )}
          </div>
        </div>,
        document.body
      )}

      {/* Import Results */}
      {importResult && createPortal(
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
          <div className="bg-dark-800 rounded-lg p-6 max-w-md w-full mx-4 border border-glass-200">
            <h3 className="text-lg font-ui font-semibold text-white flex items-center gap-2 mb-4">
              {importResult.errorCount === 0 && importResult.importedCount > 0 ? (
                <CheckCircle className="w-5 h-5 text-accent-success" />
              ) : importResult.errorCount > 0 ? (
                <AlertTriangle className="w-5 h-5 text-accent-primary" />
              ) : (
                <XCircle className="w-5 h-5 text-accent-danger" />
              )}
              Import Results
            </h3>

            <div className="grid grid-cols-4 gap-4 mb-4">
              <div className="text-center">
                <p className="text-2xl font-display font-bold text-accent-primary">{importResult.totalRecords}</p>
                <p className="text-xs text-dark-300 font-ui">Total</p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-display font-bold text-accent-success">{importResult.importedCount}</p>
                <p className="text-xs text-dark-300 font-ui">Imported</p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-display font-bold text-accent-primary">{importResult.skippedDuplicates}</p>
                <p className="text-xs text-dark-300 font-ui">Duplicates</p>
              </div>
              <div className="text-center">
                <p className="text-2xl font-display font-bold text-accent-danger">{importResult.errorCount}</p>
                <p className="text-xs text-dark-300 font-ui">Errors</p>
              </div>
            </div>

            {importResult.errors.length > 0 && (
              <div className="max-h-24 overflow-auto bg-dark-700/50 rounded p-2 mb-4">
                {importResult.errors.map((error, i) => (
                  <p key={i} className="text-sm text-accent-danger flex items-start gap-2 py-1">
                    <XCircle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                    {error}
                  </p>
                ))}
              </div>
            )}

            <button
              onClick={() => setImportResult(null)}
              className="w-full glass-button py-2"
            >
              Close
            </button>
          </div>
        </div>,
        document.body
      )}
    </GlassPanel>
  );
}

// Edit QSO Modal Component
function EditQsoModal({
  qso,
  onSave,
  onCancel,
  isSaving,
}: {
  qso: QsoResponse;
  onSave: (updates: UpdateQsoRequest) => void;
  onCancel: () => void;
  isSaving: boolean;
}) {
  const [formData, setFormData] = useState({
    callsign: qso.callsign,
    // UTC date to match the "Time (UTC)" field beside it. Slicing the raw string
    // would show the local date, which is a day behind for evening QSOs.
    qsoDate: utcDatePart(qso.qsoDate),
    timeOn: qso.timeOn,
    band: qso.band,
    mode: qso.mode,
    frequency: qso.frequency?.toString() || '',
    rstSent: qso.rstSent || '',
    rstRcvd: qso.rstRcvd || '',
    name: qso.station?.name || '',
    grid: qso.station?.grid || '',
    country: qso.station?.country || '',
    comment: qso.comment || '',
  });

  // Get RST options based on mode
  const getRstOptions = (mode: string): string[] => {
    return mode === 'CW' ? RST_CW_DIGITAL : RST_PHONE;
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSave({
      callsign: formData.callsign,
      // Send the full instant: a bare date deserialises to midnight server-side
      // and would throw away the time of day.
      qsoDate: toUtcInstant(formData.qsoDate, formData.timeOn),
      timeOn: formData.timeOn,
      band: formData.band,
      mode: formData.mode,
      frequency: formData.frequency ? parseFloat(formData.frequency) : undefined,
      rstSent: formData.rstSent || undefined,
      rstRcvd: formData.rstRcvd || undefined,
      name: formData.name || undefined,
      grid: formData.grid || undefined,
      country: formData.country || undefined,
      comment: formData.comment || undefined,
    });
  };

  return createPortal(
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50">
      <div className="bg-dark-800 rounded-lg p-6 max-w-2xl w-full mx-4 border border-glass-200 max-h-[90vh] overflow-y-auto">
        <h3 className="text-lg font-ui font-semibold text-white mb-4">Edit QSO</h3>
        <form onSubmit={handleSubmit} className="space-y-4">
          {/* Row 1: Callsign, Date, Time */}
          <div className="grid grid-cols-3 gap-4">
            <div>
              <label className="block text-sm text-dark-300 mb-1 font-ui">Callsign</label>
              <input
                type="text"
                value={formData.callsign}
                onChange={(e) => setFormData({ ...formData, callsign: e.target.value.toUpperCase() })}
                className="glass-input w-full font-mono"
                required
              />
            </div>
            <div>
              <label className="block text-sm text-dark-300 mb-1 font-ui">Date (UTC)</label>
              <input
                type="date"
                value={formData.qsoDate}
                onChange={(e) => setFormData({ ...formData, qsoDate: e.target.value })}
                className="glass-input w-full font-mono"
                required
              />
            </div>
            <div>
              <label className="block text-sm text-dark-300 mb-1 font-ui">Time (UTC)</label>
              <input
                type="text"
                value={formData.timeOn}
                onChange={(e) => setFormData({ ...formData, timeOn: e.target.value })}
                className="glass-input w-full font-mono"
                placeholder="1234"
                required
              />
            </div>
          </div>

          {/* Row 2: Band, Mode, Frequency */}
          <div className="grid grid-cols-3 gap-4">
            <div>
              <label className="block text-sm text-dark-300 mb-1 font-ui">Band</label>
              <select
                value={formData.band}
                onChange={(e) => setFormData({ ...formData, band: e.target.value })}
                className="glass-input w-full font-mono"
                required
              >
                <option value="">Select</option>
                <option value="160m">160m</option>
                <option value="80m">80m</option>
                <option value="40m">40m</option>
                <option value="30m">30m</option>
                <option value="20m">20m</option>
                <option value="17m">17m</option>
                <option value="15m">15m</option>
                <option value="12m">12m</option>
                <option value="10m">10m</option>
                <option value="6m">6m</option>
                <option value="2m">2m</option>
              </select>
            </div>
            <div>
              <label className="block text-sm text-dark-300 mb-1 font-ui">Mode</label>
              <select
                value={formData.mode}
                onChange={(e) => setFormData({ ...formData, mode: e.target.value })}
                className="glass-input w-full font-mono"
                required
              >
                <option value="">Select</option>
                <option value="SSB">SSB</option>
                <option value="CW">CW</option>
                <option value="FT8">FT8</option>
                <option value="FT4">FT4</option>
                <option value="RTTY">RTTY</option>
                <option value="PSK31">PSK31</option>
                <option value="AM">AM</option>
                <option value="FM">FM</option>
              </select>
            </div>
            <div>
              <label className="block text-sm text-dark-300 mb-1 font-ui">Frequency (MHz)</label>
              <input
                type="number"
                step="0.001"
                value={formData.frequency}
                onChange={(e) => setFormData({ ...formData, frequency: e.target.value })}
                className="glass-input w-full font-mono"
                placeholder="14.074"
              />
            </div>
          </div>

          {/* Row 3: RST Sent, RST Rcvd */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm text-dark-300 mb-1 font-ui">RST Sent</label>
              <input
                type="text"
                list="edit-rst-sent-options"
                value={formData.rstSent}
                onChange={(e) => setFormData({ ...formData, rstSent: e.target.value })}
                className="glass-input w-full font-mono"
              />
              <datalist id="edit-rst-sent-options">
                {getRstOptions(formData.mode).map(rst => (
                  <option key={rst} value={rst} />
                ))}
              </datalist>
            </div>
            <div>
              <label className="block text-sm text-dark-300 mb-1 font-ui">RST Rcvd</label>
              <input
                type="text"
                list="edit-rst-rcvd-options"
                value={formData.rstRcvd}
                onChange={(e) => setFormData({ ...formData, rstRcvd: e.target.value })}
                className="glass-input w-full font-mono"
              />
              <datalist id="edit-rst-rcvd-options">
                {getRstOptions(formData.mode).map(rst => (
                  <option key={rst} value={rst} />
                ))}
              </datalist>
            </div>
          </div>

          {/* Row 4: Name, Grid, Country */}
          <div className="grid grid-cols-3 gap-4">
            <div>
              <label className="block text-sm text-dark-300 mb-1 font-ui">Name</label>
              <input
                type="text"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                className="glass-input w-full"
              />
            </div>
            <div>
              <label className="block text-sm text-dark-300 mb-1 font-ui">Grid</label>
              <input
                type="text"
                value={formData.grid}
                onChange={(e) => setFormData({ ...formData, grid: e.target.value.toUpperCase() })}
                className="glass-input w-full font-mono"
                placeholder="FN31"
              />
            </div>
            <div>
              <label className="block text-sm text-dark-300 mb-1 font-ui">Country</label>
              <input
                type="text"
                value={formData.country}
                onChange={(e) => setFormData({ ...formData, country: e.target.value })}
                className="glass-input w-full"
              />
            </div>
          </div>

          {/* Row 5: Comment */}
          <div>
            <label className="block text-sm text-dark-300 mb-1 font-ui">Comment</label>
            <textarea
              value={formData.comment}
              onChange={(e) => setFormData({ ...formData, comment: e.target.value })}
              className="glass-input w-full h-20 resize-none"
            />
          </div>

          {/* Actions */}
          <div className="flex gap-3 justify-end pt-4 border-t border-glass-100">
            <button
              type="button"
              onClick={onCancel}
              disabled={isSaving}
              className="glass-button px-4 py-2"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isSaving}
              className="bg-accent-primary hover:bg-accent-primary/80 text-white px-4 py-2 rounded-lg flex items-center gap-2 disabled:opacity-50"
            >
              {isSaving ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Pencil className="w-4 h-4" />
              )}
              Save Changes
            </button>
          </div>
        </form>
      </div>
    </div>,
    document.body
  );
}
