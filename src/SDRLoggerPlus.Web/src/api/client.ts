import type { SatState, ContestStateEvent, WsjtxDecodeEvent } from './signalr';
export type { SatState } from './signalr';

const API_BASE = '/api';

export interface QsoResponse {
  id: string;
  callsign: string;
  qsoDate: string;
  timeOn: string;
  timeOff?: string;
  band: string;
  mode: string;
  frequency?: number;
  rstSent?: string;
  rstRcvd?: string;
  name?: string;
  grid?: string;
  country?: string;
  qth?: string;
  station?: StationInfo;
  comment?: string;
  createdAt: string;
  // Contest this QSO was logged under (ContestDefinition id), null for casual QSOs.
  contestId?: string;
  // Operating call this QSO was made under (contest sessions); null = personal. When set and
  // different from the station call, Log History badges it (excluded from personal upload/awards).
  stationCallsign?: string;
  // The bird, for satellite QSOs (ADIF SAT_NAME).
  satellite?: string | null;
  confirmedLotw?: boolean;
  confirmedEqsl?: boolean;
  confirmedQrz?: boolean;
  confirmedCard?: boolean;
  // Upload state per QSL service. Undefined means the QSO predates upload
  // tracking — deliberately different from "nothing sent yet".
  qslSync?: QslSync;
}

export interface QslServiceSync {
  status: string;
  syncedAt?: string;
  lastAttemptAt?: string;
  lastError?: string;
  failureKind: string;
  attempts: number;
  retryable: boolean;
}

export interface QslSync {
  clubLog?: QslServiceSync;
  hrdLog?: QslServiceSync;
  eqsl?: QslServiceSync;
}

export interface StationInfo {
  name?: string;
  grid?: string;
  country?: string;
  dxcc?: number;
  state?: string;
  county?: string;
  continent?: string;
  latitude?: number;
  longitude?: number;
}

export interface CreateQsoRequest {
  callsign: string;
  qsoDate: string;
  timeOn: string;
  band: string;
  mode: string;
  frequency?: number;
  rstSent?: string;
  rstRcvd?: string;
  name?: string;
  grid?: string;
  country?: string;
  comment?: string;
  notes?: string;
  // v1.x General-mode fields — Qth is the worked-station QTH (e.g.
  // "New York, NY"), Contest is a free-text contest/event/park
  // identifier that lands on Qso.Contest.ContestId server-side.
  qth?: string;
  contest?: string;
  // Worked-station US state + county (bare name), usually carried from the
  // callbook lookup. County feeds USA-CA county tracking.
  state?: string;
  county?: string;
  // v1.x POTA-mode fields — myPotaRef is the park YOU'RE activating,
  // potaRef is the WORKED station's park for park-to-park contacts.
  // Both stored on the QSO via AdifExtra so PotaStatistics picks them
  // up automatically and ADIF export round-trips cleanly.
  myPotaRef?: string;
  potaRef?: string;
  // v1.x SAT-mode fields — satellite name + per-leg freq/mode. Backend
  // promotes uplinkFreq→Frequency and upMode→Mode, and writes sat_name /
  // prop_mode=SAT / freq_rx / down_mode into AdifExtra so LoTW satellite
  // credit survives ADIF export.
  satellite?: string;
  uplinkFreq?: number;
  downlinkFreq?: number;
  upMode?: string;
  downMode?: string;
}

export interface UpdateQsoRequest {
  callsign?: string;
  qsoDate?: string;
  timeOn?: string;
  band?: string;
  mode?: string;
  frequency?: number;
  rstSent?: string;
  rstRcvd?: string;
  name?: string;
  grid?: string;
  country?: string;
  state?: string;
  county?: string;
  comment?: string;
}

export interface BulkDeleteQsosResponse {
  /** How many of the requested QSOs actually existed and were removed. */
  deleted: number;
  /** How many were asked for — lower than `deleted` never happens; higher means some were already gone. */
  requested: number;
}

export interface QsoStatistics {
  totalQsos: number;
  uniqueCallsigns: number;
  uniqueCountries: number;
  uniqueGrids: number;
  qsosToday: number;
  qsosByBand: Record<string, number>;
  qsosByMode: Record<string, number>;
}

// Statistics / Awards Types
export interface DxccBandStatus {
  worked: boolean;
  confirmed: boolean;
  qsoCount: number;
}

export interface DxccEntityStatus {
  dxccCode?: number;
  entityName: string;
  continent?: string;
  bandStatus: Record<string, DxccBandStatus>;
  firstWorked?: string;
  lastWorked?: string;
  totalQsos: number;
}

export interface BandSummary {
  entitiesWorked: number;
  entitiesConfirmed: number;
}

export interface DxccStatistics {
  totalEntitiesWorked: number;
  totalEntitiesConfirmed: number;
  challengeScore: number;
  entities: DxccEntityStatus[];
  bandSummaries: Record<string, BandSummary>;
}

export interface DxccFilters {
  band?: string;
  mode?: string;
  continent?: string;
  status?: string;
  fromDate?: string;
  toDate?: string;
}

// VUCC Types
export interface VuccStatistics {
  totalUniqueGrids: number;
  bandSummaries: Record<string, GridBandSummary>;
  grids: GridDetail[];
}

export interface GridBandSummary {
  band: string;
  uniqueGrids: number;
  confirmedGrids: number;
  awardThreshold: number;
  qsoCount: number;
}

export interface GridDetail {
  grid: string;
  band: string;
  qsoCount: number;
  confirmed: boolean;
  firstWorked?: string;
  lastWorked?: string;
}

export interface VuccFilters {
  band?: string;
  mode?: string;
  status?: string;
  fromDate?: string;
  toDate?: string;
}

// FFMA (Fred Fish Memorial Award) — 488 grids on 6m, confirmed by LoTW/paper QSL.
export interface FfmaStatistics {
  totalRequired: number;
  worked: number;
  confirmed: number;
  listComplete: boolean;
  grids: FfmaGridStatus[];
}

export interface FfmaGridStatus {
  grid: string;
  status: 'confirmed' | 'worked' | 'needed';
  qsoCount: number;
  lastWorked?: string | null;
}

// POTA Statistics Types
export interface PotaStatistics {
  uniqueParksActivated: number;
  uniqueParksHunted: number;
  totalActivationQsos: number;
  totalHuntQsos: number;
  parks: PotaParkDetail[];
}

export interface PotaParkDetail {
  parkReference: string;
  activityType: string;
  qsoCount: number;
  firstQso?: string;
  lastQso?: string;
}

export interface PotaFilters {
  activityType?: string;
  fromDate?: string;
  toDate?: string;
}

// IOTA Statistics Types
export interface IotaStatistics {
  totalGroupsWorked: number;
  totalGroupsConfirmed: number;
  totalQsos: number;
  groupsByContinent: Record<string, number>;
  groups: IotaGroupDetail[];
}

export interface IotaGroupDetail {
  iotaReference: string;
  continent: string;
  qsoCount: number;
  confirmed: boolean;
  firstWorked?: string;
  lastWorked?: string;
}

export interface IotaFilters {
  continent?: string;
  status?: string;
  fromDate?: string;
  toDate?: string;
}

export interface RbnSpot {
  callsign: string;      // Skimmer callsign
  dx: string;            // Spotted station
  frequency: number;     // kHz
  band: string;
  mode: string;
  snr?: number;          // Signal-to-noise ratio in dB
  speed?: number;        // CW speed in WPM
  timestamp: string;
  grid?: string;
  skimmerLat?: number;
  skimmerLon?: number;
  skimmerCountry?: string;
}

export interface PskReceptionReport {
  senderCallsign: string;
  senderLocator: string;
  receiverCallsign: string;
  receiverLocator: string;
  frequencyHz: number;
  mode: string;
  snr: number;
  flowStartSeconds: number;
}

export interface RbnHeardMeReport {
  skimmer: string;
  lat: number;
  lon: number;
  freqKhz: number;
  band: string;
  mode: string;
  snr: number;
  ageSeconds: number;
}

export interface AuroraPoint {
  lat: number;
  lon: number;
  aurora: number; // probability percentage 0-100
}

export interface AuroraForecast {
  observationTime: string;
  forecastTime: string;
  points: AuroraPoint[];
}

export interface QsoQuery {
  callsign?: string;
  name?: string;
  band?: string;
  mode?: string;
  fromDate?: string;
  toDate?: string;
  page?: number;
  pageSize?: number;
}

export interface PaginatedQsoResponse {
  items: QsoResponse[];
  totalCount: number;
  page: number;
  pageSize: number;
  totalPages: number;
}

class ApiClient {
  private async fetch<T>(url: string, options?: RequestInit): Promise<T> {
    const response = await fetch(`${API_BASE}${url}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      },
    });

    if (!response.ok) {
      throw new Error(`API error: ${response.status}`);
    }

    return response.json();
  }

  // QSOs
  async getQsos(query?: QsoQuery): Promise<PaginatedQsoResponse> {
    const params = new URLSearchParams();
    if (query?.callsign) params.append('callsign', query.callsign);
    if (query?.name) params.append('name', query.name);
    if (query?.band) params.append('band', query.band);
    if (query?.mode) params.append('mode', query.mode);
    if (query?.fromDate) params.append('fromDate', query.fromDate);
    if (query?.toDate) params.append('toDate', query.toDate);
    if (query?.page) params.append('page', query.page.toString());
    if (query?.pageSize) params.append('pageSize', query.pageSize.toString());
    const qs = params.toString();
    return this.fetch<PaginatedQsoResponse>(`/qsos${qs ? `?${qs}` : ''}`);
  }

  async getQso(id: string): Promise<QsoResponse> {
    return this.fetch<QsoResponse>(`/qsos/${id}`);
  }

  /**
   * Probable-duplicate check for the Log Entry form. 200 + the prior QSO when
   * the same call+band+mode was logged within the backend's dupe window,
   * 204 (→ null) when the entry looks clean. Advisory — never blocks logging.
   * Uses fetch directly because the shared helper can't express a 204.
   */
  async checkQsoDupe(callsign: string, band: string, mode: string): Promise<QsoResponse | null> {
    const params = new URLSearchParams({ callsign, band, mode });
    const response = await fetch(`${API_BASE}/qsos/dupe-check?${params}`);
    if (response.status === 204) return null;
    if (!response.ok) throw new Error(`API error: ${response.status}`);
    return response.json();
  }

  async createQso(qso: CreateQsoRequest): Promise<QsoResponse> {
    return this.fetch<QsoResponse>('/qsos', {
      method: 'POST',
      body: JSON.stringify(qso),
    });
  }

  async updateQso(id: string, qso: UpdateQsoRequest): Promise<QsoResponse> {
    return this.fetch<QsoResponse>(`/qsos/${id}`, {
      method: 'PUT',
      body: JSON.stringify(qso),
    });
  }

  async deleteQso(id: string): Promise<void> {
    await fetch(`${API_BASE}/qsos/${id}`, { method: 'DELETE' });
  }

  /**
   * Delete several QSOs in one request — the Log History multi-select delete.
   * A single call rather than one DELETE per row: a full page is 50 QSOs, and
   * the server also only has to recompute contest state once.
   */
  async deleteQsos(ids: string[]): Promise<BulkDeleteQsosResponse> {
    return this.fetch<BulkDeleteQsosResponse>('/qsos/bulk-delete', {
      method: 'POST',
      body: JSON.stringify({ ids }),
    });
  }

  async getStatistics(): Promise<QsoStatistics> {
    return this.fetch<QsoStatistics>('/qsos/statistics');
  }

  async getDxccStatistics(filters?: DxccFilters): Promise<DxccStatistics> {
    const params = new URLSearchParams();
    if (filters?.band) params.append('band', filters.band);
    if (filters?.mode) params.append('mode', filters.mode);
    if (filters?.continent) params.append('continent', filters.continent);
    if (filters?.status) params.append('status', filters.status);
    if (filters?.fromDate) params.append('fromDate', filters.fromDate);
    if (filters?.toDate) params.append('toDate', filters.toDate);
    const qs = params.toString();
    return this.fetch<DxccStatistics>(`/statistics/dxcc${qs ? `?${qs}` : ''}`);
  }

  async getVuccStatistics(filters?: VuccFilters): Promise<VuccStatistics> {
    const params = new URLSearchParams();
    if (filters?.band) params.append('band', filters.band);
    if (filters?.mode) params.append('mode', filters.mode);
    if (filters?.status) params.append('status', filters.status);
    if (filters?.fromDate) params.append('fromDate', filters.fromDate);
    if (filters?.toDate) params.append('toDate', filters.toDate);
    const qs = params.toString();
    return this.fetch<VuccStatistics>(`/statistics/vucc${qs ? `?${qs}` : ''}`);
  }

  /** Current amateur satellites from the live TLE feed (name + NORAD). Empty if the feed is unreachable. */
  async getAvailableSatellites(): Promise<{ name: string; noradId: number }[]> {
    return this.fetch('/satellites/list');
  }

  async getFfmaStatistics(): Promise<FfmaStatistics> {
    return this.fetch<FfmaStatistics>('/statistics/ffma');
  }

  async getPotaStatistics(filters?: PotaFilters): Promise<PotaStatistics> {
    const params = new URLSearchParams();
    if (filters?.activityType) params.append('activityType', filters.activityType);
    if (filters?.fromDate) params.append('fromDate', filters.fromDate);
    if (filters?.toDate) params.append('toDate', filters.toDate);
    const qs = params.toString();
    return this.fetch<PotaStatistics>(`/statistics/pota${qs ? `?${qs}` : ''}`);
  }

  async getIotaStatistics(filters?: IotaFilters): Promise<IotaStatistics> {
    const params = new URLSearchParams();
    if (filters?.continent) params.append('continent', filters.continent);
    if (filters?.status) params.append('status', filters.status);
    if (filters?.fromDate) params.append('fromDate', filters.fromDate);
    if (filters?.toDate) params.append('toDate', filters.toDate);
    const qs = params.toString();
    return this.fetch<IotaStatistics>(`/statistics/iota${qs ? `?${qs}` : ''}`);
  }

  // SDRLogger+ ported awards
  async getWasStatistics(filters?: AwardFilters): Promise<WasStatistics> {
    return this.fetch<WasStatistics>(`/statistics/was${awardQs(filters)}`);
  }

  async getWazStatistics(filters?: AwardFilters): Promise<WazStatistics> {
    return this.fetch<WazStatistics>(`/statistics/waz${awardQs(filters)}`);
  }

  async getWpxStatistics(filters?: AwardFilters): Promise<WpxStatistics> {
    return this.fetch<WpxStatistics>(`/statistics/wpx${awardQs(filters)}`);
  }

  async getWacStatistics(filters?: AwardFilters): Promise<WacStatistics> {
    return this.fetch<WacStatistics>(`/statistics/wac${awardQs(filters)}`);
  }

  async getSatelliteStatistics(filters?: AwardFilters): Promise<SatelliteStatistics> {
    return this.fetch<SatelliteStatistics>(`/statistics/satellites${awardQs(filters)}`);
  }

  async getCountiesStatistics(filters?: AwardFilters): Promise<CountiesStatistics> {
    return this.fetch<CountiesStatistics>(`/statistics/counties${awardQs(filters)}`);
  }

  async getCountyDetails(state: string, filters?: AwardFilters): Promise<CountyDetail[]> {
    return this.fetch<CountyDetail[]>(`/statistics/counties/${encodeURIComponent(state)}${awardQs(filters)}`);
  }

  async get5BWasStatistics(mode?: string): Promise<FiveBandStatistics> {
    return this.fetch<FiveBandStatistics>(`/statistics/5bwas${awardQs({ mode })}`);
  }

  async get5BDxccStatistics(mode?: string): Promise<FiveBandStatistics> {
    return this.fetch<FiveBandStatistics>(`/statistics/5bdxcc${awardQs({ mode })}`);
  }

  // RBN
  async getRbnSpots(minutes: number = 5): Promise<{ count: number; spots: RbnSpot[] }> {
    return this.fetch(`/rbn/spots?minutes=${minutes}`);
  }

  async getRbnHeardMe(callsign: string, band: string | null, minutes = 30): Promise<RbnHeardMeReport[]> {
    const bandParam = band ? `&band=${encodeURIComponent(band)}` : '';
    return this.fetch<RbnHeardMeReport[]>(
      `/rbn/heardme?callsign=${encodeURIComponent(callsign)}${bandParam}&minutes=${minutes}`);
  }

  async getRbnSkimmerLocation(callsign: string): Promise<{
    callsign: string;
    grid: string;
    lat: number;
    lon: number;
    country?: string;
  }> {
    return this.fetch(`/rbn/location/${encodeURIComponent(callsign)}`);
  }

  // Health
  async getHealth(): Promise<{ status: string; timestamp: string }> {
    return this.fetch('/health');
  }

  // Plugins
  async getPlugins(): Promise<Array<{ id: string; name: string; version: string; enabled: boolean }>> {
    return this.fetch('/plugins');
  }

  // Space Weather
  async getSpaceWeather(): Promise<SpaceWeatherData> {
    return this.fetch('/spaceweather');
  }

  // DXpeditions
  async getDXpeditions(): Promise<DXpeditionData> {
    return this.fetch('/dxpeditions');
  }

  // QRZ
  async getQrzSubscription(): Promise<QrzSubscriptionResponse> {
    return this.fetch('/qrz/subscription');
  }

  async updateQrzSettings(settings: QrzSettingsRequest): Promise<{ success: boolean; message: string; hasXmlSubscription: boolean }> {
    return this.fetch('/qrz/settings', {
      method: 'PUT',
      body: JSON.stringify(settings),
    });
  }

  async uploadToQrz(qsoIds: string[]): Promise<QrzUploadResponse> {
    return this.fetch('/qrz/upload', {
      method: 'POST',
      body: JSON.stringify({ qsoIds }),
    });
  }

  async syncToQrz(): Promise<QrzUploadResponse> {
    return this.fetch('/qrz/sync', {
      method: 'POST',
    });
  }

  async cancelQrzSync(): Promise<{ message: string }> {
    return this.fetch('/qrz/sync/cancel', {
      method: 'POST',
    });
  }

  async getQrzPendingCount(): Promise<{ pending: number }> {
    return this.fetch('/qrz/pending-count');
  }

  async markAllQrzSynced(): Promise<{ marked: number }> {
    return this.fetch('/qrz/mark-all-synced', {
      method: 'POST',
    });
  }

  async uploadToLotw(filter: LotwUploadFilter = {}): Promise<LotwUploadResult> {
    return this.fetch('/lotw/upload', {
      method: 'POST',
      body: JSON.stringify(filter),
    });
  }

  async previewLotw(filter: LotwUploadFilter = {}): Promise<LotwPreviewResponse> {
    return this.fetch('/lotw/preview', {
      method: 'POST',
      body: JSON.stringify(filter),
    });
  }

  async testTqsl(path: string): Promise<LotwTestTqslResponse> {
    return this.fetch('/lotw/test-tqsl', {
      method: 'POST',
      body: JSON.stringify({ path }),
    });
  }

  // Hot List
  async addHotListCalls(callsigns: string[]): Promise<void> {
    await this.fetch('/hotlist/calls', {
      method: 'POST',
      body: JSON.stringify({ callsigns }),
    });
  }

  async removeHotListCall(callsign: string): Promise<void> {
    await this.fetch(`/hotlist/calls/${encodeURIComponent(callsign)}`, {
      method: 'DELETE',
    });
  }

  async clearHotList(): Promise<void> {
    await this.fetch('/hotlist/calls', { method: 'DELETE' });
  }

  async setHotListFlags(flags: { enabled?: boolean; ttsEnabled?: boolean }): Promise<void> {
    await this.fetch('/hotlist', {
      method: 'PUT',
      body: JSON.stringify(flags),
    });
  }

  // Named layout presets — up to 3 slots persisted server-side. Load flow
  // is client-driven: fetch the list, pick one, apply its layoutJson via
  // the layoutStore's setLayout so it flows through the normal auto-save
  // path.
  async getSavedLayouts(): Promise<SavedLayoutSlot[]> {
    return this.fetch<SavedLayoutSlot[]>('/settings/layouts');
  }

  async saveNamedLayout(name: string, layoutJson: string): Promise<SavedLayoutSlot[]> {
    return this.fetch<SavedLayoutSlot[]>('/settings/layouts', {
      method: 'POST',
      body: JSON.stringify({ name, layoutJson }),
    });
  }

  async deleteNamedLayout(name: string): Promise<SavedLayoutSlot[]> {
    return this.fetch<SavedLayoutSlot[]>(`/settings/layouts/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    });
  }

  // Settings export/import — full-blob backup/restore. Export just calls
  // the general getSettings and hands the caller a raw object; the caller
  // wraps it in a downloadable blob. Import posts to the dedicated
  // /settings/import endpoint which does a full-replace on the DB row
  // (unlike the general POST /settings, which preserves SavedLayouts +
  // LayoutJson from the DB and would otherwise ignore the imported
  // versions of those fields).
  async exportSettings(): Promise<unknown> {
    return this.fetch<unknown>('/settings');
  }

  async importSettings(payload: unknown): Promise<unknown> {
    return this.fetch<unknown>('/settings/import', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
  }

  // WSJT-X
  async getWsjtxStatus(): Promise<WsjtxStatus[]> {
    return this.fetch('/wsjtx/status');
  }

  async getWsjtxDecodes(): Promise<WsjtxDecodeEvent[]> {
    return this.fetch('/wsjtx/decodes');
  }

  /** Worked grids (all bands, or a band/mode) for the Grid Tracker panel. */
  async getGridMap(band?: string, mode?: string): Promise<GridMapStatistics> {
    const p = new URLSearchParams();
    if (band) p.set('band', band);
    if (mode) p.set('mode', mode);
    const q = p.toString();
    return this.fetch(`/statistics/gridmap${q ? `?${q}` : ''}`);
  }

  /** Answer a decoded CQ ("call this station") via a WSJT-X Reply message. */
  async sendWsjtxReply(d: WsjtxDecodeEvent): Promise<{ sent: boolean }> {
    return this.fetch('/wsjtx/reply', {
      method: 'POST',
      body: JSON.stringify({
        source: d.source,
        clientId: d.clientId,
        time: d.timeMsSinceMidnight ?? 0,
        snr: d.snr,
        deltaTime: d.deltaTimeSeconds,
        deltaFreq: d.audioOffsetHz,
        // Echo the ORIGINAL wire mode code — a Reply must mirror the decode WSJT-X/JTDX
        // sent, or it silently ignores it. Fall back to the display mode for older events.
        mode: d.rawMode ?? d.mode,
        message: d.rawMessage,
        lowConfidence: d.lowConfidence ?? false,
      }),
    });
  }

  // S.A.T. controller
  async getSatStatus(): Promise<SatState> {
    return this.fetch('/sat/status');
  }

  async setSatActive(active: boolean): Promise<void> {
    await this.fetch('/sat/active', {
      method: 'POST',
      body: JSON.stringify({ active }),
    });
  }


  // Weather alerts
  async getLightningStatus(): Promise<LightningStatus> {
    return this.fetch('/weather/lightning');
  }

  async getLightningStrikes(): Promise<{ lat: number; lon: number; timestampUtc: string; local: boolean }[]> {
    return this.fetch('/weather/lightning/strikes');
  }

  async getWindStatus(): Promise<WindStatus> {
    return this.fetch('/weather/wind');
  }

  // Backup
  async getBackupStatus(): Promise<BackupStatus> {
    return this.fetch('/backup/status');
  }

  async runBackupNow(): Promise<BackupRunResult> {
    return this.fetch('/backup/run', { method: 'POST' });
  }

  async lookupCallsignQrz(callsign: string): Promise<QrzCallsignResponse> {
    return this.fetch(`/qrz/lookup/${encodeURIComponent(callsign)}`);
  }

  // POTA
  async getPotaSpots(): Promise<PotaSpot[]> {
    return this.fetch<PotaSpot[]>('/pota/spots');
  }

  // PSK Reporter — stations currently hearing the given callsign (last hour).
  async getPskReports(callsign: string, minutes = 60): Promise<PskReceptionReport[]> {
    return this.fetch<PskReceptionReport[]>(
      `/pskreporter/reports?callsign=${encodeURIComponent(callsign)}&minutes=${minutes}`);
  }

  // Aurora — latest NOAA OVATION auroral-oval forecast (sparse grid).
  async getAuroraOvation(): Promise<AuroraForecast> {
    return this.fetch<AuroraForecast>('/aurora/ovation');
  }

  // ADIF
  async importAdif(
    file: File,
    options: {
      skipDuplicates?: boolean;
      markAsSyncedToQrz?: boolean;
      clearExistingLogs?: boolean;
    } = {}
  ): Promise<AdifImportResponse> {
    const {
      skipDuplicates = true,
      markAsSyncedToQrz = true,
      clearExistingLogs = false,
    } = options;

    const formData = new FormData();
    formData.append('file', file);

    const params = new URLSearchParams();
    params.append('skipDuplicates', String(skipDuplicates));
    params.append('markAsSyncedToQrz', String(markAsSyncedToQrz));
    params.append('clearExistingLogs', String(clearExistingLogs));

    const response = await fetch(`${API_BASE}/adif/import?${params}`, {
      method: 'POST',
      body: formData,
    });

    if (!response.ok) {
      throw new Error(`API error: ${response.status}`);
    }

    return response.json();
  }

  async cancelImport(): Promise<{ message: string }> {
    return this.fetch('/adif/import/cancel', {
      method: 'POST',
    });
  }

  /**
   * Merge a confirmation report (LoTW / eQSL / card ADIF) into the log — marks
   * matching QSOs Confirmed without creating duplicates.
   */
  async mergeConfirmations(file: File, source: ConfirmationSource): Promise<ConfirmationMergeResponse> {
    const formData = new FormData();
    formData.append('file', file);
    // Enum binds by name on the server: Lotw / Eqsl / Card.
    const srcName =
      source === 'lotw' ? 'Lotw' : source === 'eqsl' ? 'Eqsl' : source === 'qrz' ? 'Qrz' : 'Card';
    const response = await fetch(`${API_BASE}/adif/merge-confirmations?source=${srcName}`, {
      method: 'POST',
      body: formData,
    });
    if (!response.ok) {
      throw new Error(`API error: ${response.status}`);
    }
    return response.json();
  }

  /**
   * Download the LoTW confirmation report (using the stored LoTW website login)
   * and merge it into the log. One-click "sync from LoTW".
   */
  async downloadLotwConfirmations(): Promise<ConfirmationMergeResponse> {
    const response = await fetch(`${API_BASE}/lotw/download-confirmations`, { method: 'POST' });
    if (!response.ok) {
      const msg = await response.text();
      throw new Error(msg || `API error: ${response.status}`);
    }
    return response.json();
  }

  /** Download the eQSL inbox (received QSLs) and merge into the log. */
  async downloadEqslConfirmations(): Promise<ConfirmationMergeResponse> {
    const response = await fetch(`${API_BASE}/eqsl/download-confirmations`, { method: 'POST' });
    if (!response.ok) {
      const msg = await response.text();
      throw new Error(msg || `API error: ${response.status}`);
    }
    return response.json();
  }

  /** Fetch the QRZ logbook and merge its confirmations into the log. */
  async downloadQrzConfirmations(): Promise<ConfirmationMergeResponse> {
    const response = await fetch(`${API_BASE}/qrz/download-confirmations`, { method: 'POST' });
    if (!response.ok) {
      const msg = await response.text();
      throw new Error(msg || `API error: ${response.status}`);
    }
    return response.json();
  }

  async exportAdif(request?: AdifExportRequest): Promise<Blob> {
    const params = new URLSearchParams();
    if (request?.callsign) params.append('callsign', request.callsign);
    if (request?.band) params.append('band', request.band);
    if (request?.mode) params.append('mode', request.mode);
    if (request?.fromDate) params.append('fromDate', request.fromDate);
    if (request?.toDate) params.append('toDate', request.toDate);
    const qs = params.toString();

    const response = await fetch(`${API_BASE}/adif/export${qs ? `?${qs}` : ''}`, {
      method: 'GET',
    });

    if (!response.ok) {
      throw new Error(`API error: ${response.status}`);
    }

    return response.blob();
  }

  async exportSelectedQsos(qsoIds: string[]): Promise<Blob> {
    const response = await fetch(`${API_BASE}/adif/export`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ qsoIds }),
    });

    if (!response.ok) {
      throw new Error(`API error: ${response.status}`);
    }

    return response.blob();
  }

  // AI
  async generateTalkPoints(request: GenerateTalkPointsRequest): Promise<GenerateTalkPointsResponse> {
    return this.fetch<GenerateTalkPointsResponse>('/ai/talk-points', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async chat(request: ChatRequest): Promise<ChatResponse> {
    return this.fetch<ChatResponse>('/ai/chat', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  async chatStream(request: ChatRequest, onToken: (token: string) => void): Promise<void> {
    const response = await fetch(`${API_BASE}/ai/chat/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      const text = await response.text().catch(() => '');
      throw new Error(`API error: ${response.status}${text ? ` - ${text}` : ''}`);
    }

    const reader = response.body?.getReader();
    if (!reader) throw new Error('No response body');

    const decoder = new TextDecoder();
    let buffer = '';

    try {
      // eslint-disable-next-line no-constant-condition
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          const data = line.slice(6);
          if (data === '[DONE]') return;

          try {
            const parsed = JSON.parse(data);
            if (parsed.token) {
              onToken(parsed.token);
            }
            if (parsed.error) {
              throw new Error(parsed.error);
            }
          } catch (e) {
            if (e instanceof SyntaxError) continue;
            throw e;
          }
        }
      }
    } finally {
      reader.releaseLock();
    }
  }

  async testApiKey(request: TestApiKeyRequest): Promise<TestApiKeyResponse> {
    return this.fetch<TestApiKeyResponse>('/ai/test-key', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  }

  // Contests
  async getContests(days: number = 7): Promise<Contest[]> {
    return this.fetch<Contest[]>(`/contests?days=${days}`);
  }

  async getLiveContests(): Promise<Contest[]> {
    return this.fetch<Contest[]>('/contests/live');
  }

  // Contest suite (/api/contest — rule presets, sessions, live operating)
  async getContestDefinitions(): Promise<ContestDefinition[]> {
    return this.fetch<ContestDefinition[]>('/contest/definitions');
  }

  async saveContestDefinition(def: ContestDefinition): Promise<ContestDefinition> {
    return this.fetch<ContestDefinition>('/contest/definitions', {
      method: 'POST',
      body: JSON.stringify(def),
    });
  }

  async cloneContestDefinition(id: string, newName: string): Promise<ContestDefinition> {
    return this.fetch<ContestDefinition>(
      `/contest/definitions/${encodeURIComponent(id)}/clone?newName=${encodeURIComponent(newName)}`,
      { method: 'POST' });
  }

  async deleteContestDefinition(id: string): Promise<void> {
    await fetch(`${API_BASE}/contest/definitions/${encodeURIComponent(id)}`, { method: 'DELETE' });
  }

  async getContestSessions(): Promise<ContestSession[]> {
    return this.fetch<ContestSession[]>('/contest/sessions');
  }

  // The active session (with the operator's own MyExchange), or null when none.
  async getActiveContestSession(): Promise<ContestSession | null> {
    const response = await fetch(`${API_BASE}/contest/sessions/active`);
    if (response.status === 204) return null;
    if (!response.ok) throw new Error(`API error: ${response.status}`);
    return response.json();
  }

  async startContestSession(req: StartContestSessionRequest): Promise<ContestSession> {
    return this.fetch<ContestSession>('/contest/sessions', {
      method: 'POST',
      body: JSON.stringify(req),
    });
  }

  async activateContestSession(id: string): Promise<ContestSession> {
    return this.fetch<ContestSession>(`/contest/sessions/${encodeURIComponent(id)}/activate`, { method: 'POST' });
  }

  async stopContestSession(id: string): Promise<void> {
    await fetch(`${API_BASE}/contest/sessions/${encodeURIComponent(id)}/stop`, { method: 'POST' });
  }

  // Returns the Cabrillo file as a Blob + suggested filename (or throws with the
  // server's error message, e.g. missing station callsign).
  async downloadCabrillo(sessionId: string): Promise<{ blob: Blob; fileName: string }> {
    const response = await fetch(`${API_BASE}/contest/sessions/${encodeURIComponent(sessionId)}/cabrillo`);
    if (!response.ok) {
      let msg = `API error: ${response.status}`;
      try { msg = (await response.json()).error ?? msg; } catch { /* non-JSON */ }
      throw new Error(msg);
    }
    const disposition = response.headers.get('content-disposition') ?? '';
    const match = /filename="?([^"]+)"?/.exec(disposition);
    const fileName = match?.[1] ?? `${sessionId}.cbr`;
    return { blob: await response.blob(), fileName };
  }

  async getContestState(): Promise<ContestStateEvent | null> {
    const response = await fetch(`${API_BASE}/contest/state`);
    if (response.status === 204) return null;
    if (!response.ok) throw new Error(`API error: ${response.status}`);
    return response.json();
  }

  async checkContestCall(callsign: string, band: string, mode: string): Promise<ContestCheckResponse> {
    const params = new URLSearchParams({ callsign, band, mode });
    return this.fetch<ContestCheckResponse>(`/contest/check?${params}`);
  }

  // Super Check Partial call set (master.scp ∪ your logged calls); cache + match locally.
  async getScpCalls(): Promise<string[]> {
    return this.fetch<string[]>('/contest/scp');
  }

  // The operator's distinct worked calls — lets the Log Entry flag/rank worked-before suggestions.
  async getWorkedCalls(): Promise<string[]> {
    return this.fetch<string[]>('/callsigns/worked');
  }

  // SCP master-list maintenance: current status + one-click update from supercheckpartial.com.
  async getScpStatus(): Promise<{ count: number; updatedUtc: string | null }> {
    return this.fetch('/callsigns/scp/status');
  }
  async updateScpMaster(): Promise<{ count: number; updatedUtc: string | null }> {
    return this.fetch('/callsigns/scp/update', { method: 'POST' });
  }

  // Most recent QSO with a callsign (any band/mode) — Log Entry call-history prefill. Null if never worked.
  async getRecentByCallsign(callsign: string): Promise<QsoResponse | null> {
    const res = await fetch(`${API_BASE}/qsos/recent-by-callsign?callsign=${encodeURIComponent(callsign)}`);
    if (res.status === 204) return null;
    if (!res.ok) return null;
    return res.json();
  }

  // Batch dupe/new-mult check for bandmap spots.
  async checkContestBatch(items: { call: string; band: string; mode: string }[]): Promise<BatchCheckEntry[]> {
    return this.fetch<BatchCheckEntry[]>('/contest/check-batch', {
      method: 'POST',
      body: JSON.stringify({ items }),
    });
  }

  async logContestQso(req: LogContestQsoRequest): Promise<ContestLogResult> {
    return this.fetch<ContestLogResult>('/contest/qso', {
      method: 'POST',
      body: JSON.stringify(req),
    });
  }

  // Recent QSOs of the active session (for the entry window's edit strip).
  async getContestQsos(limit = 8): Promise<ContestQso[]> {
    return this.fetch<ContestQso[]>(`/contest/qsos?limit=${limit}`);
  }

  // Correct a busted call / exchange; returns the recomputed session state.
  async updateContestQso(id: string, req: UpdateContestQsoRequest): Promise<ContestStateEvent> {
    return this.fetch<ContestStateEvent>(`/contest/qso/${encodeURIComponent(id)}`, {
      method: 'PUT',
      body: JSON.stringify(req),
    });
  }

  // Delete a busted QSO from the active session; returns the recomputed state.
  async deleteContestQso(id: string): Promise<ContestStateEvent> {
    return this.fetch<ContestStateEvent>(`/contest/qso/${encodeURIComponent(id)}`, {
      method: 'DELETE',
    });
  }

  // Change the operator's own exchange (county / power class / state…) on a running
  // session; re-derives role and returns the recomputed state.
  async updateContestExchange(sessionId: string, exchange: ContestMyExchange): Promise<ContestStateEvent | null> {
    return this.fetch<ContestStateEvent | null>(`/contest/sessions/${encodeURIComponent(sessionId)}/exchange`, {
      method: 'PUT',
      body: JSON.stringify(exchange),
    });
  }

  // DX News
  async getDXNews(): Promise<DXNewsItem[]> {
    return this.fetch<DXNewsItem[]>('/dxnews');
  }

  // Propagation
  async getPropagation(dxLat: number, dxLon: number): Promise<PropagationPrediction> {
    return this.fetch<PropagationPrediction>(`/propagation?dxLat=${dxLat}&dxLon=${dxLon}`);
  }

  async getGenericConditions(): Promise<GenericBandConditions> {
    return this.fetch<GenericBandConditions>('/propagation/conditions');
  }

  // Callsign Map Images
  async getCallsignMapImages(limit: number = 100): Promise<CallsignMapImage[]> {
    return this.fetch<CallsignMapImage[]>(`/callsign-images?limit=${limit}`);
  }
}

// QRZ Types
export interface QrzSubscriptionResponse {
  isValid: boolean;
  hasXmlSubscription: boolean;
  username?: string;
  message?: string;
  expirationDate?: string;
}

export interface QrzSettingsRequest {
  username: string;
  password: string;
  apiKey?: string;
  enabled?: boolean;
}

export interface QrzUploadResponse {
  totalCount: number;
  successCount: number;
  failedCount: number;
  results: QrzUploadResult[];
}

export interface QrzUploadResult {
  success: boolean;
  logId?: string;
  message?: string;
  qsoId?: string;
}

export interface QrzCallsignResponse {
  callsign: string;
  name?: string;
  firstName?: string;
  address?: string;
  city?: string;
  state?: string;
  country?: string;
  grid?: string;
  latitude?: number;
  longitude?: number;
  dxcc?: number;
  cqZone?: number;
  ituZone?: number;
  email?: string;
  qslManager?: string;
  imageUrl?: string;
  licenseExpiration?: string;
}

// POTA Types
export interface PotaSpot {
  spotId: number;
  activator: string;
  frequency: string;
  mode: string;
  reference: string;
  parkName: string;
  spotTime: string;
  spotter: string;
  comments: string;
  source: string;
  invalid?: boolean;
  name?: string;
  locationDesc?: string;
  grid4?: string;
  grid6?: string;
  latitude?: number;
  longitude?: number;
}

// ADIF Types
export interface AdifImportResponse {
  totalRecords: number;
  importedCount: number;
  skippedDuplicates: number;
  errorCount: number;
  errors: string[];
  /** Band/mode values the importer corrected or could not recognise. Absent on older servers. */
  issues?: AdifImportIssue[];
}

/** One distinct import problem, with how many records carried it. */
export interface AdifImportIssue {
  field: string;
  originalValue: string;
  storedValue: string;
  /** 'Corrected' — rewritten with no meaning lost. 'Flagged' — kept exactly as it arrived. */
  action: 'Corrected' | 'Flagged' | string;
  count: number;
  note: string;
}

export type ConfirmationSource = 'lotw' | 'eqsl' | 'qrz' | 'card';

export interface ConfirmationMergeResponse {
  totalRecords: number;
  matched: number;
  updated: number;
  alreadyConfirmed: number;
  unmatched: number;
}

export interface AdifExportRequest {
  callsign?: string;
  band?: string;
  mode?: string;
  fromDate?: string;
  toDate?: string;
  qsoIds?: string[];
}

export interface LotwUploadFilter {
  dateFrom?: string;
  dateTo?: string;
  stationCallsign?: string;
  bands?: string[];
  modes?: string[];
  includeIgnored?: boolean;
  includeNotSent?: boolean;
  /** Hand-picked QSO ids — upload exactly these, bypassing the other filters. */
  qsoIds?: string[];
}

export interface LotwUploadResult {
  qsoCount: number;
  success: boolean;
  tqslExitCode: number;
  message: string;
  markedAsSent: number;
}

export interface LotwPreviewItem {
  id: string;
  callsign: string;
  qsoDate: string;
  band: string;
  mode: string;
  lotwSent: string | null;
}

export interface LotwPreviewResponse {
  count: number;
  sample: LotwPreviewItem[];
}

export interface LotwTestTqslResponse {
  ok: boolean;
  version: string | null;
  error: string | null;
}

// Named layout preset (v2 — operator can save/name up to 3 arrangements)
export interface SavedLayoutSlot {
  name: string;
  layoutJson: string;
  savedAt: string;
}

// Contest suite types (rule presets / sessions / live operating)
export type { ContestStateEvent } from './signalr';

export interface ContestField {
  key: string;
  label: string;
  type: 'text' | 'rst' | 'serial' | 'zone' | 'state' | 'section' | 'grid' | 'name' | 'power' | 'check' | 'precedence' | string;
  width: number;
  required?: boolean;
  validate?: string;
  prefillFrom?: string;
  // Show/collect only when the worked station is in this class ('InArea' = W/VE
  // or in-state; 'Dx' = DX, for a WVE-kind contest). Absent/'All' ⇒ always. Drives
  // per-QSO exchange branching.
  appliesTo?: 'All' | 'InArea' | 'OutArea' | 'Dx';
}

export interface ContestDefinition {
  id: string;
  name: string;
  cabrilloName: string;
  builtin: boolean;
  bands: string[];
  modes: string[];
  sentExchange: ContestField[];
  rcvdExchange: ContestField[];
  qsoPoints: {
    sameCountry?: number;
    sameContinent?: number;
    otherContinent?: number;
    sameZone?: number;
    default: number;
    // Base points per mode class ("CW", "PH", "RTTY", "DIGI") when no relation
    // override matches; falls back to `default` for an unlisted mode.
    byMode?: Record<string, number>;
  };
  multiplierRules: { source: string; perBand: boolean; perMode?: boolean }[];
  dupeRule: 'PerBand' | 'PerBandMode' | 'PerContest';
  serial: 'None' | 'PerBand' | 'AllBand';
  // Present for role-split contests (QSO parties, ARRL DX). Drives the setup
  // My-state/My-county inputs; kind 'None' or absent ⇒ no role split.
  homeArea?: { kind: 'None' | 'StateCounty' | 'WVE'; states: string[] };
  // Final-score multiplier by power class (e.g. { QRP: 2, LOW: 1.5, HIGH: 1 }).
  // Present ⇒ setup shows a power-class picker.
  powerMultipliers?: Record<string, number>;
  scoringStrategyId?: string;
  // Present ⇒ setup shows a "claimed bonus points" input (self-declared objective
  // bonuses, e.g. Winter Field Day); the string is the input's hint.
  bonusPointsHint?: string;
}

export interface ContestMyExchange {
  dxcc?: number;
  country?: string;
  continent?: string;
  cqZone?: number;
  ituZone?: number;
  state?: string;
  county?: string;
  section?: string;
  grid?: string;
  category?: string;
  power?: string;
  name?: string;
  // Sent CLASS for Field Day / Winter Field Day ("1E", "2F", …).
  class?: string;
  // Operator-declared bonus/objective points, added to the final score (WFD).
  bonusPoints?: number;
  // Operator-declared role, overriding the location-based guess: "InArea"/"OutArea"
  // for a StateCounty-kind contest, "InArea"/"Dx" for a WVE-kind one. Undefined =
  // auto-derive from state.
  roleOverride?: string;
}

export interface ContestSession {
  id: string;
  definitionId: string;
  label: string;
  myExchange: ContestMyExchange;
  startedAt: string;
  endedAt?: string;
  active: boolean;
}

export interface StartContestSessionRequest {
  definitionId: string;
  myExchange: ContestMyExchange;
  label?: string;
  // Callsign operated under this session (own / /P / club / special). Omit → the station call.
  operatingCallsign?: string;
}

export interface LogContestQsoRequest {
  callsign: string;
  band: string;
  mode: string;
  frequency?: number;
  rstSent?: string;
  exchange?: Record<string, string>;
}

export interface ContestCheckResponse {
  isDupe: boolean;
  workedCount: number;
  newMults: string[];
  // Prefill for received-exchange fields (by field key), from call-history / prior QSO.
  prefill?: Record<string, string> | null;
  // How the engine classified the worked station: 'InArea' (W/VE / in-state),
  // 'Dx' (WVE-kind: DX), 'OutArea' (StateCounty-kind: another US/VE station), or
  // 'All' (no home-area split). Drives which received field the entry window
  // shows (state vs serial).
  workedClass?: 'All' | 'InArea' | 'OutArea' | 'Dx';
}

export interface ContestLogResult {
  qsoId: string;
  isDupe: boolean;
  points: number;
  newMults: string[];
  state: ContestStateEvent;
}

export interface ContestQso {
  id: string;
  callsign: string;
  band: string;
  mode: string;
  timeOn: string;
  points: number;
  isDupe: boolean;
  exchange?: Record<string, string> | null;
}

export interface UpdateContestQsoRequest {
  callsign: string;
  exchange?: Record<string, string>;
}

export interface BatchCheckEntry {
  call: string;
  isDupe: boolean;
  isNewMult: boolean;
}

// Contest Types
export interface Contest {
  name: string;
  mode: string;
  startTime: string;
  endTime: string;
  url: string;
  isLive: boolean;
  isStartingSoon: boolean;
  timeRemaining?: string;
}

// Space Weather Types
export interface SpaceWeatherData {
  solarFluxIndex: number;
  kIndex: number;
  sunspotNumber: number;
  timestamp: string;
}

// DXpedition Types
export interface DXpedition {
  callsign: string;
  entity: string;
  dates: string;
  qsl: string;
  info: string;
  bands: string;
  modes: string;
  startDate?: string;
  endDate?: string;
  isActive: boolean;
  isUpcoming: boolean;
}

export interface DXpeditionData {
  dxpeditions: DXpedition[];
  active: number;
  upcoming: number;
  source: string;
  timestamp: string;
}

// AI Types
export interface GenerateTalkPointsRequest {
  callsign: string;
  currentBand?: string;
  currentMode?: string;
}

export interface GenerateTalkPointsResponse {
  callsign: string;
  previousQsos: PreviousQsoSummary[];
  qrzProfile?: QrzProfileSummary;
  talkPoints: string[];
  generatedText: string;
}

export interface PreviousQsoSummary {
  qsoDate: string;
  band: string;
  mode: string;
  rstSent?: string;
  rstRcvd?: string;
  comment?: string;
}

export interface QrzProfileSummary {
  name?: string;
  location?: string;
  grid?: string;
  bio?: string;
  interests?: string;
}

export interface ChatRequest {
  callsign: string;
  question: string;
  conversationHistory?: ChatMessage[];
}

export interface ChatResponse {
  answer: string;
}

export interface ChatMessage {
  role: string; // "user" or "assistant"
  content: string;
}

export interface TestApiKeyRequest {
  provider: string; // "anthropic" or "openai"
  apiKey: string;
  model: string;
}

export interface TestApiKeyResponse {
  isValid: boolean;
  errorMessage?: string;
}

// DX News Types
export interface DXNewsItem {
  title: string;
  description: string;
  link: string;
  publishedDate: string;
}

// Propagation Types
export interface BandPrediction {
  band: string;
  freqMHz: number;
  reliability: number;
  status: 'EXCELLENT' | 'GOOD' | 'FAIR' | 'POOR' | 'CLOSED';
}

export interface PropagationPrediction {
  deLat: number;
  deLon: number;
  dxLat: number;
  dxLon: number;
  distanceKm: number;
  bearingDeg: number;
  mufMHz: number;
  lufMHz: number;
  sfi: number;
  kIndex: number;
  currentBands: BandPrediction[];
  heatmapData: number[][];
  bandNames: string[];
  timestamp: string;
}

export interface BandConditionEntry {
  band: string;
  dayStatus: string;
  nightStatus: string;
}

export interface GenericBandConditions {
  bands: BandConditionEntry[];
  sfi: number;
  kIndex: number;
  ssn: number;
  source: string;
  timestamp: string;
}

// Callsign Map Image Types
export interface CallsignMapImage {
  callsign: string;
  imageUrl?: string;
  latitude: number;
  longitude: number;
  name?: string;
  country?: string;
  grid?: string;
  savedAt: string;
}

// Backup Types
export interface BackupStatus {
  enabled: boolean;
  interval: string;
  retention: number;
  destination: string;
  lastRunUtc?: string | null;
  ok?: boolean | null;
  message?: string | null;
  path?: string | null;
  nextDueUtc?: string | null;
}

export interface BackupRunResult {
  ok: boolean;
  message: string;
  path?: string | null;
}

// SDRLogger+ ported award types (worked-based counting)
export interface AwardFilters {
  band?: string;
  mode?: string;
}

function awardQs(filters?: AwardFilters): string {
  const params = new URLSearchParams();
  if (filters?.band) params.append('band', filters.band);
  if (filters?.mode) params.append('mode', filters.mode);
  const qs = params.toString();
  return qs ? `?${qs}` : '';
}

export interface WasStateStatus {
  state: string;
  bands: Record<string, string[]>;
  qsoCount: number;
}

// USA-CA. A county counts once regardless of band/mode, and worked/confirmed
// are reported separately because confirmation is the point of the award.
export interface CountiesStateStatus {
  state: string;
  worked: number;
  confirmed: number;
  target: number;
  qsoCount: number;
}

// Satellite operating. Grids/states/entities are counted over satellite QSOs
// only — ARRL runs VUCC Satellite as its own award at 100 grids, and the same
// contacts chase WAS and DXCC via satellite.
export interface SatelliteDetail {
  satellite: string;
  qsoCount: number;
  confirmedQsos: number;
  uniqueGrids: number;
  firstWorked: string | null;
  lastWorked: string | null;
}

export interface SatelliteStatistics {
  totalSatellites: number;
  totalQsos: number;
  uniqueGrids: number;
  confirmedGrids: number;
  vuccThreshold: number;
  uniqueStates: number;
  uniqueEntities: number;
  satellites: SatelliteDetail[];
}

export interface CountiesStatistics {
  totalWorked: number;
  totalConfirmed: number;
  totalTarget: number;
  states: CountiesStateStatus[];
}

export interface CountyDetail {
  state: string;
  county: string;
  qsoCount: number;
  confirmed: boolean;
  firstWorked: string | null;
  lastWorked: string | null;
}

export interface WasStatistics {
  totalWorked: number;
  totalNeeded: number;
  states: WasStateStatus[];
}

export interface WazZoneStatus {
  zone: number;
  bands: Record<string, string[]>;
  entities: string[];
}

export interface WazStatistics {
  totalWorked: number;
  totalNeeded: number;
  zones: WazZoneStatus[];
}

export interface WpxPrefixStatus {
  prefix: string;
  bands: Record<string, string[]>;
  calls: string[];
  bandCount: number;
}

export interface WpxStatistics {
  totalWorked: number;
  prefixes: WpxPrefixStatus[];
}

export interface WacContinentStatus {
  code: string;
  name: string;
  isExtra: boolean;
  bands: Record<string, string[]>;
  entities: string[];
}

export interface WacStatistics {
  baseWorked: number;
  extraWorked: number;
  achieved: boolean;
  continents: WacContinentStatus[];
}

export interface FiveBandBandStatus {
  band: string;
  count: number;
  threshold: number;
  achieved: boolean;
  items: string[];
}

export interface FiveBandStatistics {
  achieved: boolean;
  unionCount: number;
  bands: FiveBandBandStatus[];
}

export interface WorkedGrid {
  grid: string;
  confirmed: boolean;
  qsoCount: number;
}

export interface GridMapStatistics {
  totalGrids: number;
  confirmedGrids: number;
  grids: WorkedGrid[];
}

export interface WsjtxClientInfo {
  id: string;
  version?: string | null;
  lastHeardUtc: string;
}

export interface WsjtxStatus {
  source: number; // 1 = primary, 2 = secondary
  listening: boolean;
  port: number;
  multicastAddress?: string | null;
  error?: string | null;
  clients: WsjtxClientInfo[];
  lastQsoCall?: string | null;
  lastQsoAtUtc?: string | null;
}

export interface LightningStatus {
  active: boolean;
  closestKm?: number | null;
  closestMi?: number | null;
  direction: string;
  // Aggregate across sources, each over its own window (Blitzortung ~10 min,
  // Ambient last hour, Ecowitt firmware-defined) — a magnitude, not a rate.
  strikeCount: number;
  sources: string[];
  nwsWarning?: string | null;
  lastUpdateUtc?: string | null;
}

export interface WindStatus {
  active: boolean;
  severity: string;
  sustainedMph?: number | null;
  gustMph?: number | null;
  sustainedKph?: number | null;
  gustKph?: number | null;
  direction: string;
  sources: string[];
  nwsAlert?: string | null;
  lastUpdateUtc?: string | null;
  unit: string;
  threshSustMph: number;
  threshGustMph: number;
}

export const api = new ApiClient();
