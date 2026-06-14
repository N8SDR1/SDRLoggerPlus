import { useEffect, useRef, type MutableRefObject } from 'react';
import {
  clearSpectrumDataCallback,
  setSpectrumDataCallback,
  type SpectrumDataEvent,
} from '../api/signalr';
import { dbmToSUnit } from '../utils/smeter';

// Waterfall colormap (black → blue → cyan → green → yellow → red → white).
const WF_COLORMAP = buildWaterfallColormap();
function buildWaterfallColormap(): Uint8Array {
  const lut = new Uint8Array(256 * 3);
  const stops = [
    { pos: 0, r: 0, g: 0, b: 0 },
    { pos: 36, r: 0, g: 0, b: 180 },
    { pos: 72, r: 0, g: 180, b: 220 },
    { pos: 120, r: 0, g: 200, b: 0 },
    { pos: 170, r: 240, g: 240, b: 0 },
    { pos: 210, r: 255, g: 60, b: 0 },
    { pos: 255, r: 255, g: 255, b: 255 },
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

const ATTACK_PER_SEC = 18;
const DECAY_PER_SEC = 2.2;
const PEAK_HOLD_MS = 900;
const PEAK_DECAY_PER_SEC = 0.08;
const TOP_ARC_START = Math.PI * 0.8;
const TOP_ARC_END = Math.PI * 2.2;

const S_TICKS: ReadonlyArray<{ dbm: number; label: string; overNine?: boolean }> = [
  { dbm: -121, label: 'S1' },
  { dbm: -115, label: '2' },
  { dbm: -109, label: '3' },
  { dbm: -103, label: '4' },
  { dbm: -97, label: '5' },
  { dbm: -91, label: '6' },
  { dbm: -85, label: '7' },
  { dbm: -79, label: '8' },
  { dbm: -73, label: '9' },
  { dbm: -53, label: '+20', overNine: true },
  { dbm: -33, label: '+40', overNine: true },
  { dbm: -13, label: '+60', overNine: true },
];

export interface RoundMeterConfig {
  peakHold: boolean;
  centerSpectrum: boolean;
  spectrumBandwidthHz: number; // width of the center spectrum window around the VFO
  centerWaterfall: boolean;    // show a mini waterfall under the center spectrum
  minDbm: number;
  maxDbm: number;
}

export interface RoundMeterSecondary {
  active: boolean;
  rxDbm: number | null;
}

interface RoundMeterProps {
  rxDbm: number | null;
  frequencyHz: number | null;
  mode: string | null;
  isTransmitting: boolean;
  config: RoundMeterConfig;
  secondary?: RoundMeterSecondary;
  // Optional interaction: scroll over the meter to tune the VFO.
  scrollStepHz?: number;
  onTune?: (frequencyHz: number) => void;
}

export function dbmToArcFraction(dbm: number, minDbm: number, maxDbm: number): number {
  if (maxDbm <= minDbm) return 0;
  return Math.min(1, Math.max(0, (dbm - minDbm) / (maxDbm - minDbm)));
}

export function formatFrequencyHz(frequencyHz: number): string {
  const digits = Math.max(0, Math.round(frequencyHz)).toString();
  return digits.replace(/\B(?=(\d{3})+(?!\d))/g, '.');
}

export function RoundMeter(props: RoundMeterProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const propsRef = useRef(props);
  const spectrumRef = useRef<SpectrumDataEvent | null>(null);
  const spectrumRangeRef = useRef<{ min: number; max: number } | null>(null);
  const wfBufRef = useRef<HTMLCanvasElement | null>(null);
  const wfLastAdvanceRef = useRef(0);
  const meterFracRef = useRef(0);
  const secondaryFracRef = useRef(0);
  const peakFracRef = useRef(0);
  const peakHoldUntilRef = useRef(0);
  const lastFrameRef = useRef(0);
  const rafIdRef = useRef(0);

  propsRef.current = props;

  useEffect(() => {
    const container = containerRef.current;
    const canvas = canvasRef.current;
    if (!container || !canvas) return;

    const resize = () => {
      const rect = container.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      canvas.width = Math.max(1, Math.floor(rect.width * dpr));
      canvas.height = Math.max(1, Math.floor(rect.height * dpr));
      canvas.style.width = `${rect.width}px`;
      canvas.style.height = `${rect.height}px`;
    };

    const observer = new ResizeObserver(resize);
    observer.observe(container);
    resize();
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const onSpectrum = (event: SpectrumDataEvent) => {
      spectrumRef.current = event;
    };
    setSpectrumDataCallback(onSpectrum);
    return () => clearSpectrumDataCallback(onSpectrum);
  }, []);

  // Scroll over the meter to tune the VFO (like the Rig panel / panadapter).
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    let pending: number | null = null;
    let timer: ReturnType<typeof setTimeout> | null = null;

    const onWheel = (e: WheelEvent) => {
      const p = propsRef.current;
      if (!p.onTune || p.frequencyHz === null) return; // read-only meter → let the page scroll
      e.preventDefault();
      const step = p.scrollStepHz && p.scrollStepHz > 0 ? p.scrollStepHz : 100;
      if (pending === null) pending = p.frequencyHz;
      pending += e.deltaY < 0 ? step : -step;
      pending = Math.round(Math.min(500_000_000, Math.max(100_000, pending)));
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => {
        if (pending !== null) p.onTune?.(pending);
        pending = null;
        timer = null;
      }, 30);
    };

    el.addEventListener('wheel', onWheel, { passive: false });
    return () => el.removeEventListener('wheel', onWheel);
  }, []);

  useEffect(() => {
    const render = (now: number) => {
      rafIdRef.current = requestAnimationFrame(render);
      const canvas = canvasRef.current;
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      if (!ctx) return;

      const dpr = window.devicePixelRatio || 1;
      const width = canvas.width / dpr;
      const height = canvas.height / dpr;
      if (width <= 1 || height <= 1) return;

      const currentProps = propsRef.current;
      const dt = lastFrameRef.current ? Math.min((now - lastFrameRef.current) / 1000, 0.1) : 0.016;
      lastFrameRef.current = now;

      const target = currentProps.rxDbm === null
        ? 0
        : dbmToArcFraction(currentProps.rxDbm, currentProps.config.minDbm, currentProps.config.maxDbm);
      meterFracRef.current = applyBallistics(meterFracRef.current, target, dt);

      const secondary = currentProps.secondary;
      const secondaryTarget = secondary?.active && secondary.rxDbm !== null
        ? dbmToArcFraction(
          secondary.rxDbm,
          currentProps.config.minDbm,
          currentProps.config.maxDbm,
        )
        : 0;
      secondaryFracRef.current = applyBallistics(secondaryFracRef.current, secondaryTarget, dt);

      if (!currentProps.config.peakHold) {
        peakFracRef.current = meterFracRef.current;
      } else if (meterFracRef.current >= peakFracRef.current) {
        peakFracRef.current = meterFracRef.current;
        peakHoldUntilRef.current = now + PEAK_HOLD_MS;
      } else if (now > peakHoldUntilRef.current) {
        peakFracRef.current = Math.max(
          meterFracRef.current,
          peakFracRef.current - PEAK_DECAY_PER_SEC * dt,
        );
      }

      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      drawRoundMeter(
        ctx,
        width,
        height,
        currentProps,
        meterFracRef.current,
        peakFracRef.current,
        secondaryFracRef.current,
        spectrumRef.current,
        spectrumRangeRef,
        wfBufRef,
        wfLastAdvanceRef,
        now,
      );
    };

    rafIdRef.current = requestAnimationFrame(render);
    return () => cancelAnimationFrame(rafIdRef.current);
  }, []);

  return (
    <div
      ref={containerRef}
      className="h-full w-full min-h-[180px] relative"
      title={props.onTune ? 'Scroll to tune' : undefined}
    >
      <canvas
        ref={canvasRef}
        className="absolute inset-0"
        data-testid="round-meter-canvas"
        aria-label="Round S-meter"
      />
    </div>
  );
}

function applyBallistics(current: number, target: number, dt: number): number {
  const rate = target > current ? ATTACK_PER_SEC : DECAY_PER_SEC;
  return current + (target - current) * (1 - Math.exp(-rate * dt));
}

function drawRoundMeter(
  ctx: CanvasRenderingContext2D,
  width: number,
  height: number,
  props: RoundMeterProps,
  meterFraction: number,
  peakFraction: number,
  secondaryFraction: number,
  spectrum: SpectrumDataEvent | null,
  spectrumRangeRef: MutableRefObject<{ min: number; max: number } | null>,
  wfBufRef: MutableRefObject<HTMLCanvasElement | null>,
  wfLastAdvanceRef: MutableRefObject<number>,
  now: number,
) {
  const size = Math.min(width, height);
  const cx = width / 2;
  const cy = height / 2;
  const radius = size * 0.46;

  ctx.clearRect(0, 0, width, height);

  const face = ctx.createRadialGradient(cx, cy - radius * 0.2, radius * 0.08, cx, cy, radius);
  face.addColorStop(0, '#18212d');
  face.addColorStop(0.72, '#0e151e');
  face.addColorStop(1, '#070a0f');
  ctx.fillStyle = face;
  ctx.beginPath();
  ctx.arc(cx, cy, radius, 0, Math.PI * 2);
  ctx.fill();

  ctx.strokeStyle = '#344151';
  ctx.lineWidth = Math.max(1, size * 0.008);
  ctx.beginPath();
  ctx.arc(cx, cy, radius - ctx.lineWidth / 2, 0, Math.PI * 2);
  ctx.stroke();

  drawTopArc(ctx, cx, cy, radius, size, props.config, meterFraction, peakFraction);

  if (props.secondary?.active && props.secondary.rxDbm !== null) {
    drawSecondaryArc(ctx, cx, cy, radius, size, secondaryFraction);
  }

  if (props.config.centerSpectrum) {
    drawSpectrumInset(ctx, cx, cy, size, spectrum, spectrumRangeRef, props, wfBufRef, wfLastAdvanceRef, now);
  }

  drawCenterReadout(ctx, cx, cy, size, props);
}

function drawTopArc(
  ctx: CanvasRenderingContext2D,
  cx: number,
  cy: number,
  radius: number,
  size: number,
  config: RoundMeterConfig,
  meterFraction: number,
  peakFraction: number,
) {
  const arcRadius = radius * 0.82;
  const lineWidth = Math.max(3, size * 0.018);

  ctx.lineCap = 'round';
  ctx.strokeStyle = '#273443';
  ctx.lineWidth = lineWidth;
  ctx.beginPath();
  ctx.arc(cx, cy, arcRadius, TOP_ARC_START, TOP_ARC_END);
  ctx.stroke();

  const s9Fraction = dbmToArcFraction(-73, config.minDbm, config.maxDbm);
  const redStart = fractionToAngle(s9Fraction);
  if (redStart < TOP_ARC_END) {
    ctx.strokeStyle = 'rgba(213, 73, 73, 0.45)';
    ctx.lineWidth = Math.max(1, lineWidth * 0.38);
    ctx.beginPath();
    ctx.arc(cx, cy, arcRadius, redStart, TOP_ARC_END);
    ctx.stroke();
  }

  ctx.strokeStyle = '#00d9ff';
  ctx.shadowColor = '#00d9ff';
  ctx.shadowBlur = size * 0.025;
  ctx.lineWidth = lineWidth;
  ctx.beginPath();
  ctx.arc(cx, cy, arcRadius, TOP_ARC_START, fractionToAngle(meterFraction));
  ctx.stroke();
  ctx.shadowBlur = 0;

  ctx.font = `${Math.max(7, size * 0.026)}px ui-monospace, monospace`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillStyle = '#657386';
  ctx.fillText('VFO A', cx, cy - size * 0.23);

  for (const tick of S_TICKS) {
    const fraction = dbmToArcFraction(tick.dbm, config.minDbm, config.maxDbm);
    if (fraction <= 0 || fraction >= 1) continue;
    const angle = fractionToAngle(fraction);
    const tickInner = arcRadius - size * 0.025;
    const tickOuter = arcRadius + size * 0.018;
    ctx.strokeStyle = tick.overNine ? '#c45b55' : '#8593a4';
    ctx.lineWidth = tick.overNine ? 1.5 : 1;
    ctx.beginPath();
    ctx.moveTo(cx + Math.cos(angle) * tickInner, cy + Math.sin(angle) * tickInner);
    ctx.lineTo(cx + Math.cos(angle) * tickOuter, cy + Math.sin(angle) * tickOuter);
    ctx.stroke();

    const labelRadius = arcRadius - size * 0.064;
    ctx.fillStyle = tick.overNine ? '#d27870' : '#a6b0bd';
    ctx.fillText(
      tick.label,
      cx + Math.cos(angle) * labelRadius,
      cy + Math.sin(angle) * labelRadius,
    );
  }

  if (config.peakHold && peakFraction > 0.003) {
    const peakAngle = fractionToAngle(peakFraction);
    ctx.strokeStyle = '#f4d35e';
    ctx.lineWidth = Math.max(2, size * 0.009);
    ctx.beginPath();
    ctx.moveTo(
      cx + Math.cos(peakAngle) * (arcRadius - lineWidth),
      cy + Math.sin(peakAngle) * (arcRadius - lineWidth),
    );
    ctx.lineTo(
      cx + Math.cos(peakAngle) * (arcRadius + lineWidth),
      cy + Math.sin(peakAngle) * (arcRadius + lineWidth),
    );
    ctx.stroke();
  }
}

function drawSecondaryArc(
  ctx: CanvasRenderingContext2D,
  cx: number,
  cy: number,
  radius: number,
  size: number,
  fraction: number,
) {
  const start = Math.PI * 0.18;
  const end = Math.PI * 0.82;
  const arcRadius = radius * 0.72;

  ctx.strokeStyle = '#273443';
  ctx.lineWidth = Math.max(3, size * 0.014);
  ctx.beginPath();
  ctx.arc(cx, cy, arcRadius, start, end);
  ctx.stroke();

  ctx.strokeStyle = '#8b7cff';
  ctx.beginPath();
  ctx.arc(cx, cy, arcRadius, start, start + (end - start) * fraction);
  ctx.stroke();

  ctx.fillStyle = '#8f9baa';
  ctx.font = `${Math.max(7, size * 0.025)}px ui-monospace, monospace`;
  ctx.textAlign = 'center';
  ctx.fillText('VFO B', cx, cy + arcRadius + size * 0.045);
}

function drawSpectrumInset(
  ctx: CanvasRenderingContext2D,
  cx: number,
  cy: number,
  size: number,
  spectrum: SpectrumDataEvent | null,
  rangeRef: MutableRefObject<{ min: number; max: number } | null>,
  props: RoundMeterProps,
  wfBufRef: MutableRefObject<HTMLCanvasElement | null>,
  wfLastAdvanceRef: MutableRefObject<number>,
  now: number,
) {
  const insetW = size * 0.58;
  const insetH = size * 0.165;
  const x = cx - insetW / 2;
  const y = cy - size * 0.085;

  ctx.fillStyle = 'rgba(3, 8, 13, 0.78)';
  ctx.strokeStyle = '#263646';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.rect(x, y, insetW, insetH);
  ctx.fill();
  ctx.stroke();

  const points = spectrum?.data;
  if (!spectrum || !points || points.length < 2) {
    ctx.fillStyle = '#566373';
    ctx.font = `${Math.max(8, size * 0.026)}px ui-monospace, monospace`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText('NO RX SPECTRUM', cx, y + insetH / 2);
    if (wfBufRef.current) wfBufRef.current = null;
    return;
  }

  // Window the full IQ span down to the configured bandwidth around the VFO.
  const low = spectrum.lowFrequencyHz;
  const high = spectrum.highFrequencyHz;
  const full = high - low;
  if (full <= 0) return;
  const len = points.length;
  const bw = props.config.spectrumBandwidthHz > 0 ? props.config.spectrumBandwidthHz : 4000;
  const vfo = props.frequencyHz;
  const center = (vfo !== null && vfo >= low && vfo <= high) ? vfo : (low + high) / 2;
  let wLow = center - bw / 2;
  let wHigh = center + bw / 2;
  if (wLow < low) { wLow = low; wHigh = Math.min(high, wLow + bw); }
  if (wHigh > high) { wHigh = high; wLow = Math.max(low, wHigh - bw); }
  const wRange = Math.max(1, wHigh - wLow);
  const idxForFreq = (freq: number): number => {
    const idx = Math.round(((freq - low) / full) * (len - 1));
    return idx < 0 ? 0 : idx > len - 1 ? len - 1 : idx;
  };

  const showWf = props.config.centerWaterfall;
  const specH = showWf ? insetH * 0.55 : insetH;
  const wfY = y + specH;
  const wfH = insetH - specH;

  // Auto-scale level range across the windowed samples (smoothed).
  let frameMin = Number.POSITIVE_INFINITY;
  let frameMax = Number.NEGATIVE_INFINITY;
  for (let i = idxForFreq(wLow); i <= idxForFreq(wHigh); i++) {
    const p = points[i];
    if (!Number.isFinite(p)) continue;
    if (p < frameMin) frameMin = p;
    if (p > frameMax) frameMax = p;
  }
  if (!Number.isFinite(frameMin) || !Number.isFinite(frameMax)) return;
  if (frameMax <= frameMin) frameMax = frameMin + 1;

  const previous = rangeRef.current;
  const range = previous
    ? { min: previous.min * 0.86 + frameMin * 0.14, max: previous.max * 0.86 + frameMax * 0.14 }
    : { min: frameMin, max: frameMax };
  rangeRef.current = range;
  const amplitude = Math.max(1, range.max - range.min);

  const iw = Math.max(2, Math.round(insetW));
  const sampleAt = (px: number): number => points[idxForFreq(wLow + (px / (iw - 1)) * wRange)];

  // Spectrum line (windowed)
  ctx.save();
  ctx.beginPath();
  ctx.rect(x + 1, y + 1, insetW - 2, specH - 2);
  ctx.clip();
  ctx.beginPath();
  for (let px = 0; px < iw; px++) {
    const v = sampleAt(px);
    const point = Number.isFinite(v) ? v : range.min;
    const cxp = x + (px / (iw - 1)) * insetW;
    const normalized = Math.min(1, Math.max(0, (point - range.min) / amplitude));
    const py = y + specH - normalized * specH;
    if (px === 0) ctx.moveTo(cxp, py);
    else ctx.lineTo(cxp, py);
  }
  ctx.strokeStyle = '#00d9ff';
  ctx.lineWidth = 1.25;
  ctx.shadowColor = '#00d9ff';
  ctx.shadowBlur = 4;
  ctx.stroke();
  ctx.shadowBlur = 0;
  ctx.lineTo(x + insetW, y + specH);
  ctx.lineTo(x, y + specH);
  ctx.closePath();
  ctx.fillStyle = 'rgba(0, 217, 255, 0.09)';
  ctx.fill();
  ctx.restore();

  // Optional mini waterfall under the spectrum (same windowed data).
  if (showWf && wfH > 2) {
    const bufW = iw;
    const bufH = Math.max(2, Math.round(wfH));
    let buf = wfBufRef.current;
    if (!buf || buf.width !== bufW || buf.height !== bufH) {
      buf = document.createElement('canvas');
      buf.width = bufW;
      buf.height = bufH;
      wfBufRef.current = buf;
      wfLastAdvanceRef.current = 0;
    }
    const bctx = buf.getContext('2d');
    if (bctx) {
      if (now - wfLastAdvanceRef.current >= 50) { // ~20 fps
        wfLastAdvanceRef.current = now;
        bctx.drawImage(buf, 0, 0, bufW, bufH - 1, 0, 1, bufW, bufH - 1);
        const rowImg = bctx.createImageData(bufW, 1);
        const pix = rowImg.data;
        for (let px = 0; px < bufW; px++) {
          const v = sampleAt(px);
          const n = Number.isFinite(v) ? Math.min(1, Math.max(0, (v - range.min) / amplitude)) : 0;
          const ci = Math.min(255, Math.floor(n * 255)) * 3;
          const pi = px * 4;
          pix[pi] = WF_COLORMAP[ci];
          pix[pi + 1] = WF_COLORMAP[ci + 1];
          pix[pi + 2] = WF_COLORMAP[ci + 2];
          pix[pi + 3] = 255;
        }
        bctx.putImageData(rowImg, 0, 0);
      }
      ctx.save();
      ctx.beginPath();
      ctx.rect(x + 1, wfY, insetW - 2, wfH);
      ctx.clip();
      ctx.imageSmoothingEnabled = false;
      ctx.drawImage(buf, x, wfY, insetW, wfH);
      ctx.restore();
      ctx.strokeStyle = '#1c2a38';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x, wfY);
      ctx.lineTo(x + insetW, wfY);
      ctx.stroke();
    }
  } else if (wfBufRef.current) {
    wfBufRef.current = null;
  }
}

function drawCenterReadout(
  ctx: CanvasRenderingContext2D,
  cx: number,
  cy: number,
  size: number,
  props: RoundMeterProps,
) {
  const statusY = cy + size * 0.105;
  const mode = props.mode?.trim() || '--';

  ctx.textBaseline = 'middle';
  ctx.textAlign = 'center';
  ctx.font = `600 ${Math.max(10, size * 0.042)}px ui-monospace, monospace`;
  ctx.fillStyle = '#d9e2ec';

  if (props.isTransmitting) {
    const badgeWidth = size * 0.13;
    const badgeHeight = size * 0.056;
    ctx.fillStyle = '#b73535';
    ctx.fillRect(cx - badgeWidth / 2, statusY - badgeHeight / 2, badgeWidth, badgeHeight);
    ctx.fillStyle = '#ffffff';
    ctx.font = `700 ${Math.max(9, size * 0.035)}px ui-monospace, monospace`;
    ctx.fillText('TX', cx, statusY);
    ctx.fillStyle = '#d9e2ec';
    ctx.font = `600 ${Math.max(10, size * 0.042)}px ui-monospace, monospace`;
    ctx.fillText(mode, cx, statusY + size * 0.062);
  } else {
    ctx.fillText(mode, cx, statusY);
  }

  const frequencyY = props.isTransmitting ? statusY + size * 0.125 : statusY + size * 0.068;
  ctx.font = `700 ${Math.max(12, size * 0.055)}px ui-monospace, monospace`;
  ctx.fillStyle = '#f1f5f9';
  ctx.fillText(
    props.frequencyHz === null ? '--.---.---' : formatFrequencyHz(props.frequencyHz),
    cx,
    frequencyY,
  );

  ctx.font = `600 ${Math.max(9, size * 0.034)}px ui-monospace, monospace`;
  ctx.fillStyle = props.isTransmitting ? '#e85d5d' : '#e8c14d';
  const meterText = props.rxDbm === null
    ? '—'
    : `${dbmToSUnit(props.rxDbm, 60).label}  ${props.rxDbm.toFixed(1)} dBm`;
  ctx.fillText(meterText, cx, frequencyY + size * 0.065);
}

function fractionToAngle(fraction: number): number {
  return TOP_ARC_START + (TOP_ARC_END - TOP_ARC_START) * Math.min(1, Math.max(0, fraction));
}
