import { useEffect, useRef, useCallback, useState } from 'react';
import { Activity, Pause, Play, ZoomIn, ZoomOut } from 'lucide-react';
import { GlassPanel } from '../components/GlassPanel';
import {
  clearSpectrumDataCallback,
  setSpectrumDataCallback,
  type SpectrumDataEvent,
} from '../api/signalr';
import { signalRService } from '../api/signalr';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';

// Pre-compute a 256-entry colormap: black -> blue -> cyan -> green -> yellow -> red -> white
const COLORMAP = buildColormap();

function buildColormap(): Uint8Array {
  const lut = new Uint8Array(256 * 3);
  const stops = [
    { pos: 0, r: 0, g: 0, b: 0 },       // black
    { pos: 36, r: 0, g: 0, b: 180 },     // blue
    { pos: 72, r: 0, g: 180, b: 220 },   // cyan
    { pos: 120, r: 0, g: 200, b: 0 },    // green
    { pos: 170, r: 240, g: 240, b: 0 },  // yellow
    { pos: 210, r: 255, g: 60, b: 0 },   // red
    { pos: 255, r: 255, g: 255, b: 255 },// white
  ];
  for (let s = 0; s < stops.length - 1; s++) {
    const a = stops[s];
    const b = stops[s + 1];
    for (let i = a.pos; i <= b.pos; i++) {
      const t = (i - a.pos) / (b.pos - a.pos);
      lut[i * 3] = Math.round(a.r + (b.r - a.r) * t);
      lut[i * 3 + 1] = Math.round(a.g + (b.g - a.g) * t);
      lut[i * 3 + 2] = Math.round(a.b + (b.b - a.b) * t);
    }
  }
  return lut;
}

const DEFAULT_SPECTRUM_RATIO = 0.3; // top 30% for spectrum line (user-adjustable by dragging the axis bar)
const MIN_SPECTRUM_RATIO = 0.1;
const MAX_SPECTRUM_RATIO = 0.85;
const SPLIT_STORAGE_KEY = 'sdrloggerplus-panadapter-split';
const SMOOTH_STORAGE_KEY = 'sdrloggerplus-panadapter-smoothing';      // 0..0.95 EMA factor
const INTENSITY_STORAGE_KEY = 'sdrloggerplus-panadapter-wf-intensity'; // 0.2..3.0 waterfall gain
const ZOOM_STORAGE_KEY = 'sdrloggerplus-panadapter-zoom';             // 1..MAX_ZOOM
const AXIS_HEIGHT = 20;     // pixels for frequency axis between spectrum and waterfall
const MAX_SMOOTH = 0.95;    // cap so the trace never fully freezes
const MIN_ZOOM = 1;
const MAX_ZOOM = 32;

function loadNumber(key: string, fallback: number, min: number, max: number): number {
  const stored = parseFloat(localStorage.getItem(key) ?? '');
  return Number.isFinite(stored) ? Math.min(max, Math.max(min, stored)) : fallback;
}
function loadSplitRatio(): number {
  return loadNumber(SPLIT_STORAGE_KEY, DEFAULT_SPECTRUM_RATIO, MIN_SPECTRUM_RATIO, MAX_SPECTRUM_RATIO);
}

const NO_DATA_MSG = 'No spectrum — connect a TCI radio (Thetis) with IQ streaming';

export function PanadapterPlugin() {
  const containerRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const spectrumRef = useRef<SpectrumDataEvent | null>(null);
  const smoothedRef = useRef<Float64Array | null>(null); // EMA-smoothed display data
  const rafIdRef = useRef<number>(0);
  const waterfallBufRef = useRef<HTMLCanvasElement | null>(null);
  const [paused, setPaused] = useState(false);
  const pausedRef = useRef(false);
  const hasDataRef = useRef(false);
  const peakRef = useRef(100); // rolling peak for auto-scaling amplitude
  const splitRatioRef = useRef(loadSplitRatio()); // spectrum/waterfall split, dragged via the axis bar
  const draggingSplitRef = useRef(false);
  const didDragRef = useRef(false); // suppress click-to-tune after a divider drag

  // Waterfall scroll speed: 1 (slowest, 1 row per 10 frames) … 10 (full speed).
  const [wfSpeed, setWfSpeed] = useState(() => loadNumber('sdrloggerplus-waterfall-speed', 10, 1, 10));
  const wfSpeedRef = useRef(wfSpeed);
  useEffect(() => {
    wfSpeedRef.current = wfSpeed;
    localStorage.setItem('sdrloggerplus-waterfall-speed', String(wfSpeed));
  }, [wfSpeed]);
  const wfFrameCounterRef = useRef(0);

  // Spectrum smoothing (exponential moving average across frames).
  const [smoothing, setSmoothing] = useState(() => loadNumber(SMOOTH_STORAGE_KEY, 0.5, 0, MAX_SMOOTH));
  const smoothingRef = useRef(smoothing);
  useEffect(() => {
    smoothingRef.current = smoothing;
    localStorage.setItem(SMOOTH_STORAGE_KEY, smoothing.toFixed(2));
    if (smoothing <= 0) smoothedRef.current = null;
  }, [smoothing]);

  // Waterfall intensity (gain applied to the value→color mapping).
  const [wfIntensity, setWfIntensity] = useState(() => loadNumber(INTENSITY_STORAGE_KEY, 1.0, 0.2, 3.0));
  const wfIntensityRef = useRef(wfIntensity);
  useEffect(() => {
    wfIntensityRef.current = wfIntensity;
    localStorage.setItem(INTENSITY_STORAGE_KEY, wfIntensity.toFixed(2));
  }, [wfIntensity]);

  // Zoom: 1× = full IQ span; higher zooms into the RX (window centered on the VFO).
  const [zoom, setZoom] = useState(() => loadNumber(ZOOM_STORAGE_KEY, 1, MIN_ZOOM, MAX_ZOOM));
  const zoomRef = useRef(zoom);
  useEffect(() => {
    zoomRef.current = zoom;
    localStorage.setItem(ZOOM_STORAGE_KEY, zoom.toFixed(2));
    // Waterfall history was binned at the old window — rebuild cleanly.
    waterfallBufRef.current = null;
  }, [zoom]);

  // Scroll-tune step (shared with the Rig panel).
  const scrollTuneStepHz = useSettingsStore((s) => s.settings.radio.scrollTuneStepHz);
  const scrollStepRef = useRef(scrollTuneStepHz);
  useEffect(() => { scrollStepRef.current = scrollTuneStepHz; }, [scrollTuneStepHz]);

  // Keep pausedRef in sync
  useEffect(() => {
    pausedRef.current = paused;
  }, [paused]);

  /** Current VFO frequency from the connected radio (Hz), or null. */
  const currentVfoHz = (): number | null => {
    const radioStates = useAppStore.getState().radioStates;
    for (const [, state] of radioStates) {
      if (state.frequencyHz) return state.frequencyHz;
    }
    return null;
  };

  /**
   * Current radio state, or null. Bundles VFO + mode + filter passband edges
   * + CW pitch so the render loop can draw the passband rectangle exactly
   * where the radio actually is (per TCI's rx_filter_band + cw_pitch),
   * with no per-mode default guessing.
   */
  const currentRadioState = () => {
    const radioStates = useAppStore.getState().radioStates;
    for (const [, state] of radioStates) {
      if (state.frequencyHz) return state;
    }
    return null;
  };

  /**
   * Compute the panadapter passband window in absolute Hz for the current
   * radio state. USB reports positive edges (100..2700), LSB negative,
   * CW narrow around ±cwPitch. Returns null when the radio hasn't
   * reported filter edges yet (Lyra sends rx_filter_band on connect +
   * every mode/width change, so this is only null during the first
   * fraction of a second).
   */
  const passbandAbsHz = (state: ReturnType<typeof currentRadioState>): { lo: number; hi: number } | null => {
    if (!state) return null;
    const vfo = state.frequencyHz;
    const lo = state.filterLowHz ?? 0;
    const hi = state.filterHighHz ?? 0;
    if (lo === 0 && hi === 0) return null;
    // TCI reports CW filter edges around 0 Hz (e.g. -250..+250 for a
    // 500 Hz filter). The audible tone is at +cwPitch for CWU, -cwPitch
    // for CWL. Offset the passband so it centers on the pitch tone —
    // matching how Thetis and Lyra render CW passbands.
    const mode = state.mode?.toUpperCase() ?? '';
    const pitch = state.cwPitchHz ?? 700;
    let offset = 0;
    if (mode === 'CWU' || mode === 'CW') offset = pitch;
    else if (mode === 'CWL') offset = -pitch;
    return { lo: vfo + lo + offset, hi: vfo + hi + offset };
  };

  // Render loop
  const render = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const dpr = window.devicePixelRatio || 1;
    const w = Math.round(canvas.width / dpr);
    const h = Math.round(canvas.height / dpr);
    if (w === 0 || h === 0) {
      // Tab is hidden (FlexLayout display:none) — keep the loop alive
      rafIdRef.current = requestAnimationFrame(render);
      return;
    }

    const spectrumH = Math.floor((h - AXIS_HEIGHT) * splitRatioRef.current);
    const waterfallY = spectrumH + AXIS_HEIGHT;
    const waterfallH = h - waterfallY;

    const data = spectrumRef.current;

    // Clear whole canvas
    ctx.fillStyle = '#0a0e14';
    ctx.fillRect(0, 0, w, h);

    if (!data || data.data.length === 0) {
      // No-data message
      ctx.fillStyle = '#6b7280';
      ctx.font = '13px system-ui, sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText(NO_DATA_MSG, w / 2, h / 2);
      rafIdRef.current = requestAnimationFrame(render);
      return;
    }

    hasDataRef.current = true;
    // Use the smoothed trace when smoothing is on, else the raw frame.
    const points = (smoothingRef.current > 0 && smoothedRef.current && smoothedRef.current.length === data.data.length)
      ? smoothedRef.current
      : data.data;
    const len = points.length;

    // Auto-scale: find max in this frame, decay peak slowly for stable display
    let frameMax = 1;
    for (let i = 0; i < len; i++) {
      if (points[i] > frameMax) frameMax = points[i];
    }
    // Peak hold with slow decay
    if (frameMax > peakRef.current) {
      peakRef.current = frameMax;
    } else {
      peakRef.current = peakRef.current * 0.995 + frameMax * 0.005;
    }
    const scale = Math.max(peakRef.current * 1.1, 1); // 10% headroom

    // --- Zoom window: full span at 1×, narrowing around the VFO as zoom rises ---
    const lowHz = data.lowFrequencyHz;
    const highHz = data.highFrequencyHz;
    const fullRange = highHz - lowHz;

    const zoomLevel = Math.max(MIN_ZOOM, zoomRef.current);
    const vfoHz = currentVfoHz();
    let centerHz = (vfoHz !== null && vfoHz >= lowHz && vfoHz <= highHz) ? vfoHz : (lowHz + highHz) / 2;
    const dispSpan = fullRange / zoomLevel;
    let zLow = centerHz - dispSpan / 2;
    let zHigh = centerHz + dispSpan / 2;
    // Keep the window inside the available data.
    if (zLow < lowHz) { zLow = lowHz; zHigh = Math.min(highHz, zLow + dispSpan); }
    if (zHigh > highHz) { zHigh = highHz; zLow = Math.max(lowHz, zHigh - dispSpan); }
    const dispRange = Math.max(1, zHigh - zLow);

    // Map a display x (0..w) to the source bin index for that frequency.
    const xToIdx = (x: number): number => {
      const freq = zLow + (x / w) * dispRange;
      const idx = Math.round(((freq - lowHz) / fullRange) * (len - 1));
      return idx < 0 ? 0 : idx > len - 1 ? len - 1 : idx;
    };

    // --- Spectrum line graph ---
    ctx.save();
    ctx.beginPath();
    for (let x = 0; x <= w; x++) {
      const normalized = Math.min(points[xToIdx(x)] / scale, 1);
      const y = spectrumH - normalized * spectrumH;
      if (x === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = '#00e5ff';
    ctx.lineWidth = 1;
    ctx.stroke();

    // Fill under the curve
    ctx.lineTo(w, spectrumH);
    ctx.lineTo(0, spectrumH);
    ctx.closePath();
    ctx.fillStyle = 'rgba(0, 229, 255, 0.08)';
    ctx.fill();
    ctx.restore();

    // --- Frequency axis (labels follow the zoom window) ---
    ctx.fillStyle = '#1a1f2e';
    ctx.fillRect(0, spectrumH, w, AXIS_HEIGHT);

    if (dispRange > 0) {
      const targetTicks = Math.floor(w / 100);
      const rawStep = dispRange / Math.max(targetTicks, 1);
      const magnitude = Math.pow(10, Math.floor(Math.log10(rawStep)));
      const nice = [1, 2, 5, 10].find(m => m * magnitude >= rawStep) ?? 10;
      const stepHz = nice * magnitude;

      const firstTick = Math.ceil(zLow / stepHz) * stepHz;
      ctx.fillStyle = '#9ca3af';
      ctx.font = '10px system-ui, sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'top';

      for (let freq = firstTick; freq <= zHigh; freq += stepHz) {
        const x = ((freq - zLow) / dispRange) * w;
        ctx.strokeStyle = '#4b5563';
        ctx.beginPath();
        ctx.moveTo(x, spectrumH);
        ctx.lineTo(x, spectrumH + 4);
        ctx.stroke();
        const mhz = freq / 1e6;
        ctx.fillText(mhz.toFixed(mhz >= 100 ? 2 : 3), x, spectrumH + 5);
      }
    }

    // --- Waterfall ---
    wfFrameCounterRef.current = (wfFrameCounterRef.current + 1) % 1000;
    const wfAdvance = wfFrameCounterRef.current % (11 - wfSpeedRef.current) === 0;
    const intensity = wfIntensityRef.current;

    if (!pausedRef.current && waterfallH > 0 && wfAdvance) {
      let wfBuf = waterfallBufRef.current;
      if (!wfBuf || wfBuf.width !== w || wfBuf.height !== waterfallH) {
        wfBuf = document.createElement('canvas');
        wfBuf.width = w;
        wfBuf.height = waterfallH;
        waterfallBufRef.current = wfBuf;
      }
      const wfCtx = wfBuf.getContext('2d')!;

      // Scroll existing content down by 1 pixel
      wfCtx.drawImage(wfBuf, 0, 0, w, waterfallH - 1, 0, 1, w, waterfallH - 1);

      // Draw new row at y=0 (value → colormap, scaled by intensity)
      const imgData = wfCtx.createImageData(w, 1);
      const pixels = imgData.data;
      for (let x = 0; x < w; x++) {
        const v = (points[xToIdx(x)] / scale) * intensity;
        const val = Math.min(Math.max(Math.floor(v * 255), 0), 255);
        const ci = val * 3;
        const pi = x * 4;
        pixels[pi] = COLORMAP[ci];
        pixels[pi + 1] = COLORMAP[ci + 1];
        pixels[pi + 2] = COLORMAP[ci + 2];
        pixels[pi + 3] = 255;
      }
      wfCtx.putImageData(imgData, 0, 0);

      ctx.drawImage(wfBuf, 0, waterfallY);
    } else if (waterfallBufRef.current && waterfallH > 0) {
      ctx.drawImage(waterfallBufRef.current, 0, waterfallY);
    }

    // --- Filter passband rectangle ---
    // Drawn BEFORE the VFO marker so the marker line sits on top and
    // stays visible when the passband overlaps the carrier position.
    // Uses the exact edges the radio reports via TCI's rx_filter_band —
    // no per-mode default guessing.
    const state = currentRadioState();
    const pass = passbandAbsHz(state);
    if (pass && pass.hi > zLow && pass.lo < zHigh) {
      const loClamped = Math.max(pass.lo, zLow);
      const hiClamped = Math.min(pass.hi, zHigh);
      const x0 = ((loClamped - zLow) / dispRange) * w;
      const x1 = ((hiClamped - zLow) / dispRange) * w;
      ctx.save();
      // Translucent green fill over the spectrum + waterfall — same colour
      // Thetis/Lyra use for the RX passband. Waterfall gets a lighter
      // tint so the scrolling signal is still readable underneath.
      ctx.fillStyle = 'rgba(0, 200, 100, 0.15)';
      ctx.fillRect(x0, 0, Math.max(1, x1 - x0), spectrumH);
      ctx.fillStyle = 'rgba(0, 200, 100, 0.08)';
      ctx.fillRect(x0, spectrumH + AXIS_HEIGHT, Math.max(1, x1 - x0), waterfallH);
      // Thin edge lines mark the filter cutoffs precisely.
      ctx.strokeStyle = 'rgba(0, 230, 120, 0.6)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x0, 0);
      ctx.lineTo(x0, spectrumH);
      ctx.moveTo(x1, 0);
      ctx.lineTo(x1, spectrumH);
      ctx.stroke();
      ctx.restore();
    }

    // --- VFO indicator ---
    if (vfoHz !== null && vfoHz >= zLow && vfoHz <= zHigh) {
      const vfoX = ((vfoHz - zLow) / dispRange) * w;
      ctx.save();
      ctx.strokeStyle = '#ff4444';
      ctx.lineWidth = 1;
      ctx.setLineDash([4, 3]);
      ctx.beginPath();
      ctx.moveTo(vfoX, 0);
      ctx.lineTo(vfoX, h);
      ctx.stroke();
      ctx.restore();
    }

    rafIdRef.current = requestAnimationFrame(render);
  }, []);

  // ResizeObserver to keep canvas pixel-perfect
  useEffect(() => {
    const container = containerRef.current;
    const canvas = canvasRef.current;
    if (!container || !canvas) return;

    const resize = () => {
      const rect = container.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      canvas.width = Math.floor(rect.width * dpr);
      canvas.height = Math.floor(rect.height * dpr);
      canvas.style.width = `${rect.width}px`;
      canvas.style.height = `${rect.height}px`;
      const ctx = canvas.getContext('2d');
      if (ctx) ctx.scale(dpr, dpr);
      // Reset waterfall buffer on resize
      waterfallBufRef.current = null;
    };

    const observer = new ResizeObserver(resize);
    observer.observe(container);
    resize();

    return () => observer.disconnect();
  }, []);

  // Spectrum data callback (applies EMA smoothing per frame) + render loop
  useEffect(() => {
    const onSpectrum = (evt: SpectrumDataEvent) => {
      if (pausedRef.current) return;
      spectrumRef.current = evt;

      const a = smoothingRef.current;
      if (a > 0) {
        const raw = evt.data;
        const len = raw.length;
        let sm = smoothedRef.current;
        if (!sm || sm.length !== len) {
          sm = new Float64Array(len);
          for (let i = 0; i < len; i++) sm[i] = raw[i];
        } else {
          for (let i = 0; i < len; i++) sm[i] = sm[i] * a + raw[i] * (1 - a);
        }
        smoothedRef.current = sm;
      }
    };
    setSpectrumDataCallback(onSpectrum);

    rafIdRef.current = requestAnimationFrame(render);

    return () => {
      clearSpectrumDataCallback(onSpectrum);
      if (rafIdRef.current) cancelAnimationFrame(rafIdRef.current);
    };
  }, [render]);

  // ── Mouse wheel: tune (plain) or zoom (Ctrl/Shift) ──
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    let pendingHz: number | null = null;
    let tuneTimer: ReturnType<typeof setTimeout> | null = null;

    const onWheel = (e: WheelEvent) => {
      e.preventDefault();

      // Ctrl/Shift + wheel = zoom around the RX.
      if (e.ctrlKey || e.shiftKey) {
        setZoom((z) => {
          const next = e.deltaY < 0 ? z * 2 : z / 2;
          return Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, next));
        });
        return;
      }

      // Plain wheel = tune the VFO by the configured step (like the Rig panel).
      const vfo = currentVfoHz();
      if (vfo === null) return;
      if (pendingHz === null) pendingHz = vfo;
      const step = scrollStepRef.current || 100;
      pendingHz += e.deltaY < 0 ? step : -step;
      pendingHz = Math.round(Math.min(500_000_000, Math.max(100_000, pendingHz)));
      if (tuneTimer) clearTimeout(tuneTimer);
      tuneTimer = setTimeout(() => {
        if (pendingHz !== null) signalRService.tuneToFrequency(pendingHz);
        pendingHz = null;
        tuneTimer = null;
      }, 30);
    };

    el.addEventListener('wheel', onWheel, { passive: false });
    return () => el.removeEventListener('wheel', onWheel);
  }, []);

  // ── Spectrum/waterfall divider drag (the frequency-axis bar is the handle) ──

  /** Canvas-local y of the axis bar's top edge, from the current split ratio. */
  const axisTop = (canvas: HTMLCanvasElement): number => {
    const h = canvas.getBoundingClientRect().height;
    return Math.floor((h - AXIS_HEIGHT) * splitRatioRef.current);
  };

  const handlePointerDown = useCallback((e: React.PointerEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const y = e.clientY - canvas.getBoundingClientRect().top;
    const top = axisTop(canvas);
    if (y >= top && y <= top + AXIS_HEIGHT) {
      draggingSplitRef.current = true;
      didDragRef.current = false;
      canvas.setPointerCapture(e.pointerId);
    }
  }, []);

  const handlePointerMove = useCallback((e: React.PointerEvent<HTMLCanvasElement>) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const y = e.clientY - rect.top;

    if (draggingSplitRef.current) {
      didDragRef.current = true;
      const ratio = (y - AXIS_HEIGHT / 2) / Math.max(1, rect.height - AXIS_HEIGHT);
      splitRatioRef.current = Math.min(MAX_SPECTRUM_RATIO, Math.max(MIN_SPECTRUM_RATIO, ratio));
      // Waterfall history is height-dependent — drop the buffer so it rebuilds cleanly
      waterfallBufRef.current = null;
      return;
    }

    const top = axisTop(canvas);
    canvas.style.cursor = y >= top && y <= top + AXIS_HEIGHT ? 'row-resize' : 'crosshair';
  }, []);

  const handlePointerUp = useCallback((e: React.PointerEvent<HTMLCanvasElement>) => {
    if (draggingSplitRef.current) {
      draggingSplitRef.current = false;
      canvasRef.current?.releasePointerCapture(e.pointerId);
      localStorage.setItem(SPLIT_STORAGE_KEY, splitRatioRef.current.toFixed(3));
    }
  }, []);

  // Click-to-tune handler (honors the zoom window)
  const handleClick = useCallback((e: React.MouseEvent<HTMLCanvasElement>) => {
    if (didDragRef.current) {
      didDragRef.current = false; // this click was the tail of a divider drag
      return;
    }
    const data = spectrumRef.current;
    if (!data) return;

    const canvas = canvasRef.current;
    if (!canvas) return;

    const rect = canvas.getBoundingClientRect();
    const xRatio = (e.clientX - rect.left) / rect.width;
    const fullRange = data.highFrequencyHz - data.lowFrequencyHz;
    if (fullRange <= 0) return;

    // Reconstruct the same zoom window the render used.
    const zoomLevel = Math.max(MIN_ZOOM, zoomRef.current);
    const vfoHz = currentVfoHz();
    let centerHz = (vfoHz !== null && vfoHz >= data.lowFrequencyHz && vfoHz <= data.highFrequencyHz)
      ? vfoHz : (data.lowFrequencyHz + data.highFrequencyHz) / 2;
    const dispSpan = fullRange / zoomLevel;
    let zLow = centerHz - dispSpan / 2;
    let zHigh = centerHz + dispSpan / 2;
    if (zLow < data.lowFrequencyHz) { zLow = data.lowFrequencyHz; zHigh = Math.min(data.highFrequencyHz, zLow + dispSpan); }
    if (zHigh > data.highFrequencyHz) { zHigh = data.highFrequencyHz; zLow = Math.max(data.lowFrequencyHz, zHigh - dispSpan); }

    const freqHz = Math.round(zLow + xRatio * (zHigh - zLow));
    signalRService.tuneToFrequency(freqHz);
  }, []);

  const zoomLabel = zoom < 2 ? '1×' : `${Math.round(zoom)}×`;

  // Mode + filter width readout for the panel header. Subscribes to
  // radioStates so the chip re-renders when the radio reports a new
  // mode / filter width via TCI. Falls back to null (chip hidden) when
  // the radio hasn't reported filter edges yet.
  const modeChip = useAppStore((s) => {
    for (const [, state] of s.radioStates) {
      if (!state.frequencyHz) continue;
      const lo = state.filterLowHz ?? 0;
      const hi = state.filterHighHz ?? 0;
      if (lo === 0 && hi === 0) {
        return state.mode ? { mode: state.mode, width: null as string | null } : null;
      }
      const width = Math.round(hi - lo);
      // Show CW as ±width around pitch (matches how operators think of
      // narrow filters); SSB/AM/FM as low..high in Hz.
      const mode = (state.mode ?? '').toUpperCase();
      const w = mode.startsWith('CW')
        ? `${width} Hz`
        : `${lo >= 0 ? lo : lo} to ${hi >= 0 ? '+' + hi : hi} Hz`;
      return { mode: state.mode, width: w };
    }
    return null;
  });

  return (
    <GlassPanel
      title="Panadapter"
      icon={<Activity className="w-4 h-4" />}
      actions={
        <div className="flex items-center gap-3">
          {modeChip && (
            <span
              className="text-[10px] font-ui text-accent-secondary/90 border border-accent-secondary/30 rounded px-1.5 py-0.5 flex items-center gap-1"
              title="RX mode + filter passband width (from TCI)"
            >
              <span className="font-bold tracking-wider">{modeChip.mode}</span>
              {modeChip.width && (
                <span className="text-dark-100 font-mono">{modeChip.width}</span>
              )}
            </span>
          )}
          {/* Spectrum smoothing */}
          <div className="flex items-center gap-1.5" title={`Spectrum smoothing: ${Math.round(smoothing * 100)}%`}>
            <span className="text-[10px] font-ui text-dark-300 uppercase tracking-wide">SM</span>
            <input
              type="range"
              min={0}
              max={MAX_SMOOTH}
              step={0.05}
              value={smoothing}
              onChange={(e) => setSmoothing(parseFloat(e.target.value))}
              className="w-16 accent-[rgb(var(--accent-primary))] cursor-pointer"
            />
          </div>
          {/* Waterfall intensity */}
          <div className="flex items-center gap-1.5" title={`Waterfall intensity: ${wfIntensity.toFixed(1)}×`}>
            <span className="text-[10px] font-ui text-dark-300 uppercase tracking-wide">INT</span>
            <input
              type="range"
              min={0.2}
              max={3.0}
              step={0.1}
              value={wfIntensity}
              onChange={(e) => setWfIntensity(parseFloat(e.target.value))}
              className="w-16 accent-[rgb(var(--accent-primary))] cursor-pointer"
            />
          </div>
          {/* Waterfall speed */}
          <div className="flex items-center gap-1.5" title={`Waterfall speed: ${wfSpeed}/10`}>
            <span className="text-[10px] font-ui text-dark-300 uppercase tracking-wide">WF</span>
            <input
              type="range"
              min={1}
              max={10}
              value={wfSpeed}
              onChange={(e) => setWfSpeed(parseInt(e.target.value))}
              className="w-16 accent-[rgb(var(--accent-primary))] cursor-pointer"
            />
          </div>
          {/* Zoom */}
          <div className="flex items-center gap-1" title="Zoom (or Ctrl/Shift + scroll)">
            <button
              onClick={() => setZoom((z) => Math.max(MIN_ZOOM, z / 2))}
              className="glass-button p-1.5"
              title="Zoom out"
            >
              <ZoomOut className="w-3.5 h-3.5" />
            </button>
            <span className="text-[10px] font-mono text-dark-300 w-7 text-center">{zoomLabel}</span>
            <button
              onClick={() => setZoom((z) => Math.min(MAX_ZOOM, z * 2))}
              className="glass-button p-1.5"
              title="Zoom in to RX"
            >
              <ZoomIn className="w-3.5 h-3.5" />
            </button>
          </div>
          <button
            onClick={() => setPaused(p => !p)}
            className="glass-button p-1.5"
            title={paused ? 'Resume' : 'Pause'}
          >
            {paused ? <Play className="w-3.5 h-3.5" /> : <Pause className="w-3.5 h-3.5" />}
          </button>
        </div>
      }
    >
      <div ref={containerRef} className="w-full h-full relative overflow-hidden" style={{ minHeight: 120 }}>
        <canvas
          ref={canvasRef}
          className="absolute inset-0 cursor-crosshair"
          onClick={handleClick}
          onPointerDown={handlePointerDown}
          onPointerMove={handlePointerMove}
          onPointerUp={handlePointerUp}
        />
      </div>
    </GlassPanel>
  );
}
