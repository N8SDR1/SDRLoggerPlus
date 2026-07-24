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

/**
 * Port numbers that more than one device claims.
 *
 * Windows assigns Bluetooth serial ports the next number IT believes is free, but
 * virtual-port software (VSPE/ELTIMA, com0com, …) often doesn't register with the COM
 * name arbiter — so a paired radio can land on a number already in use. Opening it then
 * reaches whichever driver wins, which looks exactly like the radio refusing to connect.
 */
export function duplicatePorts(ports: SerialPortDetail[]): Set<string> {
  const seen = new Set<string>();
  const dupes = new Set<string>();
  for (const p of ports) {
    if (seen.has(p.port)) dupes.add(p.port);
    seen.add(p.port);
  }
  return dupes;
}
