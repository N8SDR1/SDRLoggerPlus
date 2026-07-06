import type { SatState } from './signalr';
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
  station?: StationInfo;
  comment?: string;
  createdAt: string;
}

export interface StationInfo {
  name?: string;
  grid?: string;
  country?: string;
  dxcc?: number;
  state?: string;
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
  comment?: string;
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

  // WSJT-X
  async getWsjtxStatus(): Promise<WsjtxStatus> {
    return this.fetch('/wsjtx/status');
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
  async getPskReports(callsign: string): Promise<PskReceptionReport[]> {
    return this.fetch<PskReceptionReport[]>(`/pskreporter/reports?callsign=${encodeURIComponent(callsign)}`);
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

export interface WsjtxClientInfo {
  id: string;
  version?: string | null;
  lastHeardUtc: string;
}

export interface WsjtxStatus {
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
  strikesLastHour: number;
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
