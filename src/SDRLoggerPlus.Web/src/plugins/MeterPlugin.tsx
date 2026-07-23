import { useEffect, useRef, useState } from 'react';
import { Gauge, Settings as SettingsIcon, X } from 'lucide-react';
import { GlassPanel } from '../components/GlassPanel';
import { RoundMeter, type RoundMeterConfig } from '../components/RoundMeter';
import { setTciMetersCallback, clearTciMetersCallback, signalRService, type TciMetersEvent } from '../api/signalr';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { dbmToSUnit } from '../utils/smeter';

const NO_DATA_MSG = 'No meter data — connect a supported TCI radio in Settings → Station';
const ROUND_CONFIG_STORAGE_KEY = 'sdrloggerplus-round-meter-config';
// S-meter calibration: TCI reports a dBm that can sit well above the rig's own
// S-meter (e.g. a Hermes Lite 2 / Thetis reads ~20 dB high). This offset is added
// to the RX dBm before display so the needle/readout match the radio. Adjust in
// the Meters settings; persisted per install.
const SMETER_CAL_STORAGE_KEY = 'sdrloggerplus-smeter-cal-db';
// Default tuned against a Hermes Lite 2 / Thetis (TCI dBm reads ~15 dB above the
// rig's S-meter); other radios can adjust it in the Meters settings.
const DEFAULT_SMETER_CAL_DB = -15;
const DEFAULT_ROUND_CONFIG: RoundMeterConfig = {
  peakHold: true,
  centerSpectrum: true,
  // Narrow (kHz) windows are resolution-limited by the 192 kHz / 4096-pt FFT
  // (~47 Hz/bin), so the default span is wide enough to stay smooth.
  spectrumBandwidthHz: 50000,
  centerWaterfall: false,
  minDbm: -130,
  maxDbm: -10,
};

// Needle ballistics: fast attack so peaks register, slow decay like a real
// movement. Applied per animation frame against the latest target fraction.
const ATTACK_PER_SEC = 18; // ~50ms to reach a higher reading
const DECAY_PER_SEC = 2.2; // ~450ms to fall back

const STALE_AFTER_MS = 2000;

// Canvas backing-store multiplier: at least 2x for supersampling on 1x
// displays, matching the device ratio on 3x phones/tablets.
const BACKING_SCALE = Math.max(
  2,
  Math.ceil(typeof window !== 'undefined' ? window.devicePixelRatio || 1 : 1),
);

// Meter face geometry (canvas units; canvas is scaled to device pixels)
const FACE_W = 300;
const FACE_H = 150;
const PIVOT_X = FACE_W / 2;
const PIVOT_Y = FACE_H - 10;
const NEEDLE_LEN = 118;
const ARC_R = 124;
const SWEEP_START = -Math.PI * 0.78; // radians from vertical
const SWEEP_END = Math.PI * 0.78;

interface MeterState {
  rxDbm: number | null;
  rxAvgDbm: number | null;
  txPowerW: number | null;
  txPeakW: number | null;
  swr: number | null;
  micDbm: number | null;
  isTransmitting: boolean;
  lastUpdate: number;
}

type MeterView = 'analog' | 'round';

const EMPTY_STATE: MeterState = {
  rxDbm: null,
  rxAvgDbm: null,
  txPowerW: null,
  txPeakW: null,
  swr: null,
  micDbm: null,
  isTransmitting: false,
  lastUpdate: 0,
};

function sweepAngle(fraction: number): number {
  return SWEEP_START + (SWEEP_END - SWEEP_START) * fraction;
}


/** Major tick fractions and labels for the S-meter face. */
const S_TICKS: Array<{ frac: number; label: string; red: boolean }> = [
  { frac: 0, label: 'S0', red: false },
  { frac: dbmToSUnit(-115).fraction, label: '2', red: false },
  { frac: dbmToSUnit(-103).fraction, label: '4', red: false },
  { frac: dbmToSUnit(-91).fraction, label: '6', red: false },
  { frac: dbmToSUnit(-79).fraction, label: '8', red: false },
  { frac: dbmToSUnit(-73).fraction, label: '9', red: false },
  { frac: dbmToSUnit(-53).fraction, label: '+20', red: true },
  { frac: 1, label: '+40', red: true },
];

export function MeterPlugin() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const stateRef = useRef<MeterState>({ ...EMPTY_STATE });
  const needleFracRef = useRef(0);
  const rafIdRef = useRef(0);
  const lastFrameRef = useRef(0);
  const [hasEverReceived, setHasEverReceived] = useState(false);
  const rigStatus = useAppStore((s) => s.rigStatus);
  const radioStates = useAppStore((s) => s.radioStates);
  const selectedRadioId = useAppStore((s) => s.selectedRadioId);
  const [view, setView] = useState<MeterView>('analog');
  const [roundConfig, setRoundConfig] = useState(loadRoundMeterConfig);
  const [showSettings, setShowSettings] = useState(false);
  const [calibrationDb, setCalibrationDb] = useState<number>(loadSMeterCalibration);
  // The meters callback closure (mounted once) reads the latest calibration
  // through a ref so changes take effect without re-subscribing.
  const calibrationRef = useRef(calibrationDb);

  // Tile values come through React state at most 10 Hz (server-coalesced);
  // only the needle bypasses state via refs + rAF.
  const [tiles, setTiles] = useState<MeterState>({ ...EMPTY_STATE });

  const radioIdRef = useRef<string | null>(null);

  useEffect(() => {
    localStorage.setItem(ROUND_CONFIG_STORAGE_KEY, JSON.stringify(roundConfig));
  }, [roundConfig]);

  useEffect(() => {
    calibrationRef.current = calibrationDb;
    localStorage.setItem(SMETER_CAL_STORAGE_KEY, String(calibrationDb));
  }, [calibrationDb]);

  useEffect(() => {
    // Non-finite numbers can't come from our server (parser drops them),
    // but a defensive normalize keeps NaN out of the canvas math no matter
    // what feeds the callback.
    const num = (v: number | null) => (v !== null && Number.isFinite(v) ? v : null);

    const cb = (evt: TciMetersEvent) => {
      const s = stateRef.current;

      // Follow one radio at a time: adopt the first radioId seen; switch
      // only if the current source has gone stale (e.g. it disconnected).
      if (radioIdRef.current === null) radioIdRef.current = evt.radioId;
      if (evt.radioId !== radioIdRef.current) {
        if (performance.now() - s.lastUpdate < STALE_AFTER_MS) return;
        radioIdRef.current = evt.radioId;
      }

      // Calibration offset aligns the RX signal with the rig's S-meter; TX
      // power/SWR/mic are unaffected.
      const cal = calibrationRef.current;
      const rx = num(evt.rxSignalDbm);
      const rxAvg = num(evt.rxAvgSignalDbm);
      const pwr = num(evt.txPowerWatts);
      const peak = num(evt.txPeakPowerWatts);
      const swr = num(evt.txSwr);
      const mic = num(evt.txMicDbm);
      if (rx !== null) s.rxDbm = rx + cal;
      if (rxAvg !== null) s.rxAvgDbm = rxAvg + cal;
      if (pwr !== null) s.txPowerW = pwr;
      if (peak !== null) s.txPeakW = peak;
      if (swr !== null) s.swr = swr;
      if (mic !== null) s.micDbm = mic;
      s.isTransmitting = evt.isTransmitting;
      s.lastUpdate = performance.now();
      setHasEverReceived(true);
      setTiles({ ...s });
    };

    setTciMetersCallback(cb);
    return () => clearTciMetersCallback(cb);
  }, []);

  // The tiles' stale dimming needs a re-render after data stops arriving —
  // no event will trigger one, so poll at a low rate.
  const [, forceStaleCheck] = useState(0);
  useEffect(() => {
    if (!hasEverReceived) return;
    const id = setInterval(() => forceStaleCheck((n) => n + 1), 500);
    return () => clearInterval(id);
  }, [hasEverReceived]);

  // Needle + face render loop
  useEffect(() => {
    if (!hasEverReceived || view !== 'analog') return;
    lastFrameRef.current = 0;

    const render = (now: number) => {
      rafIdRef.current = requestAnimationFrame(render);
      const canvas = canvasRef.current;
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      if (!ctx) return;

      const dt = lastFrameRef.current ? (now - lastFrameRef.current) / 1000 : 0.016;
      lastFrameRef.current = now;

      const s = stateRef.current;
      const stale = performance.now() - s.lastUpdate > STALE_AFTER_MS;

      // Target needle position from latest reading; true exponential
      // smoothing so ballistics are frame-rate independent.
      const target = s.rxDbm !== null ? dbmToSUnit(s.rxDbm).fraction : 0;
      const current = needleFracRef.current;
      const rate = target > current ? ATTACK_PER_SEC : DECAY_PER_SEC;
      const step = 1 - Math.exp(-rate * dt);
      needleFracRef.current = current + (target - current) * step;

      drawFace(ctx, canvas, needleFracRef.current, s, stale);
    };

    rafIdRef.current = requestAnimationFrame(render);
    return () => cancelAnimationFrame(rafIdRef.current);
  }, [hasEverReceived, view]);

  const stale = hasEverReceived && performance.now() - tiles.lastUpdate > STALE_AFTER_MS;
  const dimmed = stale ? { opacity: 0.45 } : undefined;
  const meterRadioState = radioIdRef.current ? radioStates.get(radioIdRef.current) : undefined;
  const selectedRadioState = selectedRadioId ? radioStates.get(selectedRadioId) : undefined;
  const activeRadioState = meterRadioState
    ?? selectedRadioState
    ?? radioStates.values().next().value
    ?? null;
  const frequencyHz = activeRadioState?.frequencyHz ?? rigStatus?.frequency ?? null;
  const mode = activeRadioState?.mode ?? rigStatus?.mode ?? null;
  const isTransmitting = tiles.isTransmitting
    || activeRadioState?.isTransmitting
    || rigStatus?.isTransmitting
    || false;

  const scrollTuneStepHz = useSettingsStore((s) => s.settings.radio.scrollTuneStepHz);

  return (
    <GlassPanel
      title="Meters"
      icon={<Gauge className="w-4 h-4" />}
      actions={
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setShowSettings((s) => !s)}
            className={`glass-button p-1.5 ${showSettings ? 'text-accent-primary' : 'text-dark-300'}`}
            title="Meter settings"
          >
            <SettingsIcon className="w-4 h-4" />
          </button>
          <div className="flex rounded bg-black/25 p-0.5 text-[10px] font-ui uppercase">
            <button
              type="button"
              onClick={() => setView('analog')}
              className={`rounded px-2 py-1 ${view === 'analog' ? 'bg-white/10 text-accent-primary' : 'opacity-60'}`}
            >
              Analog
            </button>
            <button
              type="button"
              onClick={() => setView('round')}
              className={`rounded px-2 py-1 ${view === 'round' ? 'bg-white/10 text-accent-primary' : 'opacity-60'}`}
            >
              Round
            </button>
          </div>
        </div>
      }
    >
      {!hasEverReceived ? (
        <div className="flex items-center justify-center h-full text-sm opacity-60 p-4 text-center">
          {NO_DATA_MSG}
        </div>
      ) : view === 'round' ? (
        <div className="flex flex-col h-full p-2 gap-2 relative" style={dimmed} data-testid="meter-content">
          <div className="flex-1 min-h-0">
            <RoundMeter
              rxDbm={tiles.rxDbm}
              frequencyHz={frequencyHz}
              mode={mode}
              isTransmitting={isTransmitting}
              config={roundConfig}
              scrollStepHz={scrollTuneStepHz}
              onTune={(hz) => signalRService.tuneToFrequency(hz)}
            />
          </div>
          {showSettings && (
            <MeterSettingsPopover
              calibrationDb={calibrationDb}
              onCalibrationChange={setCalibrationDb}
              showRoundControls
              roundConfig={roundConfig}
              onRoundConfigChange={setRoundConfig}
              onClose={() => setShowSettings(false)}
            />
          )}
        </div>
      ) : (
        <div className="flex flex-col h-full p-2 gap-2 relative" style={dimmed} data-testid="meter-content">
          {showSettings && (
            <MeterSettingsPopover
              calibrationDb={calibrationDb}
              onCalibrationChange={setCalibrationDb}
              showRoundControls={false}
              roundConfig={roundConfig}
              onRoundConfigChange={setRoundConfig}
              onClose={() => setShowSettings(false)}
            />
          )}
          <div className="flex-1 min-h-0 flex items-center justify-center">
            <canvas
              ref={canvasRef}
              width={FACE_W * BACKING_SCALE}
              height={FACE_H * BACKING_SCALE}
              style={{ width: '100%', maxWidth: FACE_W, aspectRatio: '2 / 1' }}
            />
          </div>
          {/* Mode + frequency on one horizontal line. White on RX, red (with a
              soft glow) on TX. Power / SWR / protection / faults are TX-side and
              live in Lyra (the radio owns them), so those tiles were removed.
              Uses the unified frequencyHz/mode (TCI radio state first, then
              rigStatus) so it populates on a TCI radio (Lyra) where rigStatus
              is null. */}
          <div
            className={`flex items-baseline justify-center gap-2 rounded bg-black/30 px-2 py-1 font-mono transition-colors ${
              isTransmitting ? 'text-red-500' : 'text-white'
            }`}
            style={isTransmitting ? { textShadow: '0 0 8px rgba(239,68,68,0.7)' } : undefined}
            data-testid="meter-freq-line"
          >
            <span className="text-[11px] uppercase tracking-wide opacity-80">
              {mode ?? 'Freq'}
            </span>
            <span className="text-lg font-semibold tabular-nums leading-none" data-testid="meter-freq">
              {frequencyHz !== null ? (frequencyHz / 1e6).toFixed(4) : '—'}
            </span>
            <span className="text-[10px] uppercase opacity-50">MHz</span>
            {isTransmitting && (
              <span className="ml-1 rounded bg-red-500/20 px-1 text-[9px] font-bold uppercase tracking-wider">
                TX
              </span>
            )}
          </div>
        </div>
      )}
    </GlassPanel>
  );
}

function loadSMeterCalibration(): number {
  const raw = typeof localStorage !== 'undefined' ? localStorage.getItem(SMETER_CAL_STORAGE_KEY) : null;
  if (raw === null) return DEFAULT_SMETER_CAL_DB;
  const v = Number(raw);
  return Number.isFinite(v) ? v : DEFAULT_SMETER_CAL_DB;
}

function MeterSettingsPopover({
  calibrationDb,
  onCalibrationChange,
  showRoundControls,
  roundConfig,
  onRoundConfigChange,
  onClose,
}: {
  calibrationDb: number;
  onCalibrationChange: (db: number) => void;
  showRoundControls: boolean;
  roundConfig: RoundMeterConfig;
  onRoundConfigChange: (config: RoundMeterConfig) => void;
  onClose: () => void;
}) {
  return (
    <div className="absolute top-2 right-2 z-20 w-56 rounded-lg border border-glass-200 bg-dark-800/95 shadow-lg p-2">
      <div className="flex items-center justify-between mb-1.5 px-0.5">
        <span className="text-[10px] font-ui uppercase tracking-wide text-dark-300">Meter Settings</span>
        <button type="button" onClick={onClose} className="text-dark-300 hover:text-dark-100" title="Close">
          <X className="w-3.5 h-3.5" />
        </button>
      </div>
      <label className="flex items-center justify-between gap-2 text-[10px] font-ui uppercase mb-2">
        <span>S-meter cal</span>
        <span className="flex items-center gap-1">
          <input
            type="number"
            step={1}
            value={calibrationDb}
            onChange={(event) => {
              const value = Number(event.target.value);
              if (Number.isFinite(value)) onCalibrationChange(value);
            }}
            className="w-16 rounded border border-glass-100 bg-black/30 px-1 py-0.5 font-mono normal-case text-right"
          />
          <span className="opacity-50 normal-case">dB</span>
        </span>
      </label>
      {showRoundControls && <RoundMeterControls config={roundConfig} onChange={onRoundConfigChange} />}
    </div>
  );
}

function loadRoundMeterConfig(): RoundMeterConfig {
  try {
    const stored = JSON.parse(localStorage.getItem(ROUND_CONFIG_STORAGE_KEY) ?? '{}') as Partial<RoundMeterConfig>;
    const minDbm = typeof stored.minDbm === 'number' && Number.isFinite(stored.minDbm)
      ? stored.minDbm
      : DEFAULT_ROUND_CONFIG.minDbm;
    const maxDbm = typeof stored.maxDbm === 'number' && Number.isFinite(stored.maxDbm)
      ? stored.maxDbm
      : DEFAULT_ROUND_CONFIG.maxDbm;
    const rangeIsValid = minDbm < maxDbm;
    return {
      peakHold: typeof stored.peakHold === 'boolean' ? stored.peakHold : DEFAULT_ROUND_CONFIG.peakHold,
      centerSpectrum: typeof stored.centerSpectrum === 'boolean'
        ? stored.centerSpectrum
        : DEFAULT_ROUND_CONFIG.centerSpectrum,
      // Old builds stored narrow kHz spans (2–10 kHz); migrate those to the
      // wider default since narrow windows are too chunky at this resolution.
      spectrumBandwidthHz: typeof stored.spectrumBandwidthHz === 'number' && Number.isFinite(stored.spectrumBandwidthHz) && stored.spectrumBandwidthHz >= 20000
        ? stored.spectrumBandwidthHz
        : DEFAULT_ROUND_CONFIG.spectrumBandwidthHz,
      centerWaterfall: typeof stored.centerWaterfall === 'boolean'
        ? stored.centerWaterfall
        : DEFAULT_ROUND_CONFIG.centerWaterfall,
      minDbm: rangeIsValid ? minDbm : DEFAULT_ROUND_CONFIG.minDbm,
      maxDbm: rangeIsValid ? maxDbm : DEFAULT_ROUND_CONFIG.maxDbm,
    };
  } catch {
    return { ...DEFAULT_ROUND_CONFIG };
  }
}

function RoundMeterControls({
  config,
  onChange,
}: {
  config: RoundMeterConfig;
  onChange: (config: RoundMeterConfig) => void;
}) {
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5 text-[10px] font-ui uppercase">
      <label className="flex items-center gap-1 cursor-pointer">
        <input
          type="checkbox"
          checked={config.peakHold}
          onChange={(event) => onChange({ ...config, peakHold: event.target.checked })}
          className="accent-[rgb(var(--accent-primary))]"
        />
        Peak
      </label>
      <label className="flex items-center gap-1 cursor-pointer">
        <input
          type="checkbox"
          checked={config.centerSpectrum}
          onChange={(event) => onChange({ ...config, centerSpectrum: event.target.checked })}
          className="accent-[rgb(var(--accent-primary))]"
        />
        Spectrum
      </label>
      <label className="flex items-center gap-1 cursor-pointer">
        <input
          type="checkbox"
          checked={config.centerWaterfall}
          onChange={(event) => onChange({ ...config, centerWaterfall: event.target.checked })}
          className="accent-[rgb(var(--accent-primary))]"
        />
        Waterfall
      </label>
      <label className="flex items-center gap-1">
        BW
        <select
          value={config.spectrumBandwidthHz}
          onChange={(event) => onChange({ ...config, spectrumBandwidthHz: Number(event.target.value) })}
          className="rounded border border-glass-100 bg-black/30 px-1 py-0.5 font-mono normal-case"
        >
          <option value={40000}>40 kHz</option>
          <option value={50000}>50 kHz</option>
          <option value={60000}>60 kHz</option>
          <option value={100000}>100 kHz</option>
        </select>
      </label>
      <label className="flex items-center gap-1">
        Min
        <input
          type="number"
          step={5}
          value={config.minDbm}
          onChange={(event) => {
            const value = Number(event.target.value);
            if (Number.isFinite(value)) onChange({ ...config, minDbm: Math.min(value, config.maxDbm - 1) });
          }}
          className="w-14 rounded border border-glass-100 bg-black/30 px-1 py-0.5 font-mono normal-case"
        />
      </label>
      <label className="flex items-center gap-1">
        Max
        <input
          type="number"
          step={5}
          value={config.maxDbm}
          onChange={(event) => {
            const value = Number(event.target.value);
            if (Number.isFinite(value)) onChange({ ...config, maxDbm: Math.max(value, config.minDbm + 1) });
          }}
          className="w-14 rounded border border-glass-100 bg-black/30 px-1 py-0.5 font-mono normal-case"
        />
      </label>
      <span className="opacity-50 normal-case">dBm</span>
    </div>
  );
}

function drawFace(
  ctx: CanvasRenderingContext2D,
  canvas: HTMLCanvasElement,
  needleFrac: number,
  s: MeterState,
  stale: boolean,
) {
  const scaleX = canvas.width / FACE_W;
  const scaleY = canvas.height / FACE_H;
  ctx.save();
  ctx.scale(scaleX, scaleY);
  ctx.clearRect(0, 0, FACE_W, FACE_H);

  const tx = s.isTransmitting;

  // Arc
  ctx.lineWidth = 2;
  for (const seg of [
    { from: 0, to: 0.6, color: '#3a4452' },
    { from: 0.6, to: 1, color: tx ? '#3a4452' : '#7a3030' },
  ]) {
    ctx.beginPath();
    ctx.strokeStyle = stale ? '#333' : seg.color;
    ctx.arc(
      PIVOT_X, PIVOT_Y, ARC_R,
      sweepAngle(seg.from) - Math.PI / 2,
      sweepAngle(seg.to) - Math.PI / 2,
    );
    ctx.stroke();
  }

  // Ticks + labels
  ctx.font = '9px ui-monospace, monospace';
  ctx.textAlign = 'center';
  for (const tick of S_TICKS) {
    const a = sweepAngle(tick.frac) - Math.PI / 2;
    const x1 = PIVOT_X + Math.cos(a) * (ARC_R - 7);
    const y1 = PIVOT_Y + Math.sin(a) * (ARC_R - 7);
    const x2 = PIVOT_X + Math.cos(a) * ARC_R;
    const y2 = PIVOT_Y + Math.sin(a) * ARC_R;
    ctx.beginPath();
    ctx.strokeStyle = stale ? '#444' : tick.red ? '#c0524a' : '#8b96a5';
    ctx.moveTo(x1, y1);
    ctx.lineTo(x2, y2);
    ctx.stroke();
    const lx = PIVOT_X + Math.cos(a) * (ARC_R - 17);
    const ly = PIVOT_Y + Math.sin(a) * (ARC_R - 17) + 3;
    ctx.fillStyle = stale ? '#444' : tick.red ? '#c0524a' : '#8b96a5';
    ctx.fillText(tick.label, lx, ly);
  }

  // Average marker (small dot on the arc) when available
  if (!tx && s.rxAvgDbm !== null && !stale) {
    const a = sweepAngle(dbmToSUnit(s.rxAvgDbm).fraction) - Math.PI / 2;
    ctx.beginPath();
    ctx.fillStyle = '#5dade2';
    ctx.arc(PIVOT_X + Math.cos(a) * (ARC_R + 4), PIVOT_Y + Math.sin(a) * (ARC_R + 4), 2.5, 0, Math.PI * 2);
    ctx.fill();
  }

  // Needle
  const na = sweepAngle(needleFrac) - Math.PI / 2;
  ctx.beginPath();
  ctx.strokeStyle = stale ? '#555' : tx ? '#e74c3c' : '#e8c14d';
  ctx.lineWidth = 2;
  ctx.shadowColor = stale ? 'transparent' : tx ? '#e74c3c' : '#e8c14d';
  ctx.shadowBlur = 6;
  ctx.moveTo(PIVOT_X, PIVOT_Y);
  ctx.lineTo(PIVOT_X + Math.cos(na) * NEEDLE_LEN, PIVOT_Y + Math.sin(na) * NEEDLE_LEN);
  ctx.stroke();
  ctx.shadowBlur = 0;

  // Pivot cap
  ctx.beginPath();
  ctx.fillStyle = '#2c3442';
  ctx.arc(PIVOT_X, PIVOT_Y, 5, 0, Math.PI * 2);
  ctx.fill();

  // Readout
  ctx.font = 'bold 13px ui-monospace, monospace';
  ctx.fillStyle = stale ? '#555' : tx ? '#e74c3c' : '#e8c14d';
  const readout = tx
    ? `TX ${s.txPowerW !== null ? s.txPowerW.toFixed(1) + ' W' : ''}`
    : s.rxDbm !== null
      ? `${dbmToSUnit(s.rxDbm).label}  ${s.rxDbm.toFixed(1)} dBm`
      : '—';
  ctx.fillText(readout, PIVOT_X, PIVOT_Y - 24);

  ctx.restore();
}
