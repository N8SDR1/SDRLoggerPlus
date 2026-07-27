import { useCallback, useEffect, useState } from 'react';
import { Server, Wifi, Copy, Trash2, Plus, CheckCircle2, XCircle, AlertTriangle } from 'lucide-react';
import { api, type ServerConfig, type AuthDevice } from '../../api/client';

/**
 * Settings → Server — the multi-op "easy connect" front door.
 *   • Host this log: show the address clients dial + manage per-device tokens.
 *   • Connect to a host: point this station's shared log at a host URL + token (with a Test).
 * Provider changes are saved to config and take effect on the next app start.
 */
export function ServerSection() {
  const [cfg, setCfg] = useState<ServerConfig | null>(null);
  const [mode, setMode] = useState<'host' | 'client'>('host');
  const [hostUrl, setHostUrl] = useState('');
  const [token, setToken] = useState('');
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<{ ok: boolean; detail: string } | null>(null);
  const [saved, setSaved] = useState(false);
  const [devices, setDevices] = useState<AuthDevice[]>([]);
  const [newDeviceName, setNewDeviceName] = useState('');
  const [issuedToken, setIssuedToken] = useState<{ name: string; token: string } | null>(null);

  const load = useCallback(async () => {
    const c = await api.getServerConfig();
    setCfg(c);
    setMode(c.mode);
    setHostUrl(c.hostUrl ?? '');
    if (c.mode === 'host') setDevices(await api.listServerDevices());
  }, []);

  useEffect(() => { load(); }, [load]);

  const test = async () => {
    setTesting(true); setTestResult(null);
    try { setTestResult(await api.testServerHost(hostUrl, token || undefined)); }
    finally { setTesting(false); }
  };

  const save = async () => {
    await api.saveServerConfig({ mode, hostUrl: hostUrl || undefined, token: token || undefined });
    setSaved(true);
  };

  const addDevice = async () => {
    const { token: t, device } = await api.addServerDevice(newDeviceName || 'device');
    setIssuedToken({ name: device.name, token: t });
    setNewDeviceName('');
    setDevices(await api.listServerDevices());
  };

  const revoke = async (id: string) => {
    await api.revokeServerDevice(id);
    setDevices(await api.listServerDevices());
  };

  const copy = (text: string) => navigator.clipboard?.writeText(text);

  return (
    <div className="space-y-6 max-w-2xl">
      <div>
        <h2 className="text-lg font-semibold text-white flex items-center gap-2">
          <Server className="w-5 h-5 text-accent-secondary" /> Server / Multi-op
        </h2>
        <p className="text-sm text-dark-300 mt-1">
          Share one log across several stations — each runs its own app and radio, all logging into,
          and dupe-checking against, one host. <strong className="text-amber-300">Field-Day / multi-op is
          still in development;</strong> the pieces below are the connection setup.
        </p>
      </div>

      {/* Mode selector */}
      <div className="flex gap-2">
        {(['host', 'client'] as const).map((m) => (
          <button key={m} onClick={() => { setMode(m); setSaved(false); }}
            className={`flex-1 px-4 py-3 rounded-lg border text-left transition-colors ${
              mode === m ? 'border-accent-secondary bg-accent-secondary/10 text-white'
                         : 'border-glass-100 text-dark-300 hover:border-glass-200'}`}>
            <div className="font-medium">{m === 'host' ? 'Host this log' : 'Connect to a host'}</div>
            <div className="text-xs text-dark-400 mt-0.5">
              {m === 'host' ? 'This machine owns the shared log' : 'Log into another machine’s shared log'}
            </div>
          </button>
        ))}
      </div>

      {/* HOST mode */}
      {mode === 'host' && cfg && (
        <div className="space-y-4">
          <div className="p-3 rounded-lg bg-glass-50 border border-glass-100">
            {cfg.remotelyBound ? (
              <div className="text-sm text-gray-200">
                <div className="flex items-center gap-2 text-green-400 font-medium">
                  <Wifi className="w-4 h-4" /> Reachable on your network
                </div>
                <div className="mt-2 text-dark-300">Other stations connect to:</div>
                {cfg.lanAddresses.length ? cfg.lanAddresses.map((ip) => (
                  <div key={ip} className="mt-1 flex items-center gap-2">
                    <code className="text-accent-primary text-[13px]">http://{ip}:{cfg.port}</code>
                    <button onClick={() => copy(`http://${ip}:${cfg.port}`)} title="Copy"
                      className="text-dark-400 hover:text-white"><Copy className="w-3.5 h-3.5" /></button>
                  </div>
                )) : <div className="text-dark-400 text-xs mt-1">No LAN address detected.</div>}
              </div>
            ) : (
              <div className="text-sm text-amber-200 flex items-start gap-2">
                <AlertTriangle className="w-4 h-4 mt-0.5 shrink-0" />
                <span>Running on this computer only (localhost). Hosting for other stations needs the
                  backend to listen on your network — that launch option is coming with the multi-op release.</span>
              </div>
            )}
          </div>

          {/* Device tokens */}
          <div>
            <div className="text-sm font-medium text-gray-200 mb-2">Allowed devices</div>
            <p className="text-xs text-dark-400 mb-2">
              Each station gets its own token. Add one per laptop and hand it over; revoke a lost one
              without affecting the others.
            </p>
            <div className="flex gap-2 mb-2">
              <input value={newDeviceName} onChange={(e) => setNewDeviceName(e.target.value)}
                placeholder="Device name (e.g. Station B)"
                className="glass-input flex-1 text-sm px-3 py-2" />
              <button onClick={addDevice}
                className="px-3 py-2 rounded-lg bg-accent-secondary/20 text-accent-secondary border border-accent-secondary/40 text-sm flex items-center gap-1 hover:bg-accent-secondary/30">
                <Plus className="w-4 h-4" /> Add
              </button>
            </div>

            {issuedToken && (
              <div className="p-3 rounded-lg bg-green-500/10 border border-green-500/30 mb-2">
                <div className="text-xs text-green-300 mb-1">
                  Token for <strong>{issuedToken.name}</strong> — copy it now, it won’t be shown again:
                </div>
                <div className="flex items-center gap-2">
                  <code className="text-[12px] text-white break-all flex-1">{issuedToken.token}</code>
                  <button onClick={() => copy(issuedToken.token)} title="Copy"
                    className="text-dark-300 hover:text-white shrink-0"><Copy className="w-4 h-4" /></button>
                </div>
              </div>
            )}

            <div className="space-y-1">
              {devices.map((d) => (
                <div key={d.id} className="flex items-center justify-between px-3 py-2 rounded bg-glass-50 text-sm">
                  <div>
                    <span className="text-gray-200">{d.name}</span>
                    <span className="text-dark-400 text-xs ml-2">
                      {d.lastSeenUtc ? `last seen ${new Date(d.lastSeenUtc).toLocaleString()}` : 'never connected'}
                    </span>
                  </div>
                  <button onClick={() => revoke(d.id)} title="Revoke"
                    className="text-dark-400 hover:text-red-400"><Trash2 className="w-4 h-4" /></button>
                </div>
              ))}
              {!devices.length && <div className="text-dark-400 text-xs px-1">No devices yet.</div>}
            </div>
          </div>
        </div>
      )}

      {/* CLIENT mode */}
      {mode === 'client' && (
        <div className="space-y-3">
          <div>
            <label className="text-sm text-gray-200">Host address</label>
            <input value={hostUrl} onChange={(e) => { setHostUrl(e.target.value); setTestResult(null); setSaved(false); }}
              placeholder="http://192.168.1.50:5050"
              className="glass-input w-full text-sm px-3 py-2 mt-1" />
          </div>
          <div>
            <label className="text-sm text-gray-200">Access token</label>
            <input value={token} onChange={(e) => { setToken(e.target.value); setTestResult(null); setSaved(false); }}
              placeholder="paste the token the host gave you"
              className="glass-input w-full text-sm px-3 py-2 mt-1" />
          </div>

          <div className="flex items-center gap-2">
            <button onClick={test} disabled={testing || !hostUrl}
              className="px-3 py-2 rounded-lg border border-glass-200 text-sm text-gray-200 hover:bg-glass-100 disabled:opacity-50">
              {testing ? 'Testing…' : 'Test connection'}
            </button>
            {testResult && (
              <span className={`text-sm flex items-center gap-1 ${testResult.ok ? 'text-green-400' : 'text-red-400'}`}>
                {testResult.ok ? <CheckCircle2 className="w-4 h-4" /> : <XCircle className="w-4 h-4" />}
                {testResult.detail}
              </span>
            )}
          </div>
        </div>
      )}

      {/* Save */}
      <div className="pt-2 border-t border-glass-100">
        <button onClick={save}
          className="px-4 py-2 rounded-lg bg-accent-secondary text-black font-medium text-sm hover:bg-accent-secondary/90">
          Save
        </button>
        {saved && (
          <p className="text-xs text-amber-300 mt-2 flex items-center gap-1">
            <AlertTriangle className="w-3.5 h-3.5" /> Saved — restart SDRLogger+ for the change to take effect.
          </p>
        )}
      </div>
    </div>
  );
}
