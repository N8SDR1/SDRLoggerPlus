import { describe, it, expect } from 'vitest';
import { portLabel, duplicatePorts } from './serialPorts';
import type { SerialPortDetail } from '../api/signalr';

const port = (p: Partial<SerialPortDetail> & { port: string }): SerialPortDetail =>
  ({ isUsb: false, ...p });

describe('portLabel', () => {
  it('falls back to the bare port when there is no device name', () => {
    expect(portLabel(port({ port: 'COM3' }))).toBe('COM3');
  });

  it('names the device behind the port', () => {
    expect(portLabel(port({ port: 'COM3', description: 'Silicon Labs CP210x USB to UART Bridge' })))
      .toBe('COM3 — Silicon Labs CP210x USB to UART Bridge');
  });

  it("doesn't repeat a vendor already inside the description", () => {
    const label = portLabel(port({ port: 'COM3', description: 'Silicon Labs CP210x', vendor: 'Silicon Labs' }));
    expect(label).toBe('COM3 — Silicon Labs CP210x');
  });

  it('adds the vendor when the description omits it', () => {
    expect(portLabel(port({ port: 'COM1', description: 'USB Serial Port', vendor: 'FTDI' })))
      .toBe('COM1 — FTDI USB Serial Port');
  });
});

describe('duplicatePorts', () => {
  it('finds a COM number claimed by two devices', () => {
    // Real case: Windows gave a paired IC-705 COM5/COM6, numbers virtual-port software
    // already held — so opening the port reached the wrong driver and the radio
    // appeared to refuse the connection.
    const dupes = duplicatePorts([
      port({ port: 'COM5', description: 'Standard Serial over Bluetooth link' }),
      port({ port: 'COM6', description: 'Standard Serial over Bluetooth link' }),
      port({ port: 'COM5', description: 'ELTIMA Virtual Serial Port (COM5->COM6)' }),
      port({ port: 'COM6', description: 'ELTIMA Virtual Serial Port (COM6->COM5)' }),
      port({ port: 'COM3', description: 'Silicon Labs CP210x' }),
    ]);
    expect([...dupes].sort()).toEqual(['COM5', 'COM6']);
  });

  it('reports nothing when every port is distinct', () => {
    expect(duplicatePorts([port({ port: 'COM1' }), port({ port: 'COM2' })]).size).toBe(0);
  });

  it('copes with an empty list', () => {
    expect(duplicatePorts([]).size).toBe(0);
  });
});
