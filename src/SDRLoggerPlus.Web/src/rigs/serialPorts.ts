import type { SerialPortDetail } from '../api/signalr';

/**
 * How a serial port reads in a dropdown.
 *
 * "COM5" on its own tells you nothing, and choosing wrongly is the most common way rig
 * setup fails: modern Icom and Yaesu radios present TWO ports over a single USB cable,
 * and the wrong one behaves exactly like a broken cable. The device name is what makes
 * them distinguishable, so it goes in the label.
 */
export function portLabel(p: SerialPortDetail): string {
  const name = (p.description ?? '').trim();
  if (!name) return p.port;
  // Vendor is usually already inside the description ("Silicon Labs CP210x …"); only add
  // it when it isn't, so the label doesn't stutter.
  const vendor = (p.vendor ?? '').trim();
  const needsVendor = vendor && !name.toLowerCase().includes(vendor.toLowerCase());
  return `${p.port} — ${needsVendor ? `${vendor} ` : ''}${name}`;
}
