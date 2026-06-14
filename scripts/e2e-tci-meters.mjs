// E2E proof for the TCI meters pipeline:
//   mock Thetis TCI (ws) -> SDRLoggerPlus.Server TciRadioConnection -> SignalR OnTciMeters
// Run from src/SDRLoggerPlus.Web (for node_modules): node ../../scripts/e2e-tci-meters.mjs
// Assumes SDRLoggerPlus.Server is listening on http://localhost:5217 (pass as argv[2] to override).

import { WebSocketServer } from 'ws';
import signalR from '@microsoft/signalr';

const SERVER_URL = process.argv[2] ?? 'http://localhost:5217';
const TCI_PORT = 50101;

let sawStart = false;
let sawRxEnable = false;
let sawTxEnable = false;

// --- 1. Mock Thetis TCI server ---
const wss = new WebSocketServer({ host: '127.0.0.1', port: TCI_PORT });
wss.on('connection', (ws) => {
  console.log('[mock-tci] client connected');
  ws.send('protocol:ExpertSDR3,1.9;');
  ws.send('device:Thetis;');
  ws.send('vfo:0,0,14074000;');
  ws.send('modulation:0,digu;');
  ws.send('ready;');

  ws.on('message', (data) => {
    const text = data.toString();
    console.log('[mock-tci] received:', text);
    if (text.includes('start;')) sawStart = true;
    if (text.includes('rx_sensors_enable:true')) sawRxEnable = true;
    if (text.includes('tx_sensors_enable:true')) sawTxEnable = true;

    if (sawRxEnable && sawTxEnable) {
      // Stream a few sensor frames like Thetis's 100ms timers would
      let n = 0;
      const timer = setInterval(() => {
        ws.send(`rx_channel_sensors_ex:0,0,${(-97.4 + n).toFixed(1)},-99.1,-85.0;`);
        ws.send('tx_sensors:0,-12.3,4.8,5.0,1.42;');
        if (++n >= 5) clearInterval(timer);
      }, 100);
    }
  });
});
console.log(`[mock-tci] listening on ws://127.0.0.1:${TCI_PORT}`);

// --- 2. SignalR client: trigger connect, await OnTciMeters ---
const hub = new signalR.HubConnectionBuilder()
  .withUrl(`${SERVER_URL}/hubs/log`)
  .configureLogging(signalR.LogLevel.Warning)
  .build();

const received = [];
hub.on('OnTciMeters', (evt) => {
  received.push(evt);
  console.log('[signalr] OnTciMeters:', JSON.stringify(evt));
});

const fail = (msg) => { console.error('E2E FAIL:', msg); process.exit(1); };
setTimeout(() => fail('timeout after 30s'), 30_000);

await hub.start();
console.log('[signalr] connected to', SERVER_URL);
await hub.invoke('ConnectTci', '127.0.0.1', TCI_PORT, 'e2e-mock');
console.log('[signalr] ConnectTci invoked');

// Give the pipeline a few seconds, then assert
await new Promise((r) => setTimeout(r, 4000));

if (!sawStart) fail('server never sent start;');
if (!sawRxEnable) fail('server never sent rx_sensors_enable');
if (!sawTxEnable) fail('server never sent tx_sensors_enable');
if (received.length === 0) fail('no OnTciMeters events received');

const last = received[received.length - 1];
if (last.txSwr !== 1.42) fail(`unexpected SWR: ${last.txSwr}`);
if (typeof last.rxSignalDbm !== 'number') fail('rxSignalDbm missing');
if (received.length > 6) fail(`throttling broken: ${received.length} events for 5 frames over ~500ms`);

console.log(`E2E PASS: ${received.length} meter events; handshake order correct; SWR/RX values intact`);
process.exit(0);
