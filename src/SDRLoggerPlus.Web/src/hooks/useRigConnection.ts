import { useCallback } from 'react';
import { useAppStore } from '../store/appStore';
import { useSettingsStore } from '../store/settingsStore';
import { useSignalR } from './useSignalR';
import type { RadioConnectionState, RadioDiscoveredEvent } from '../api/signalr';

export interface RigListItem {
  id: string;
  /** Display name — the configured nickname (e.g. "Lyra"), else the model. */
  name: string;
  type: RadioDiscoveredEvent['type'];
  /** "host:port" detail line. */
  detail: string;
  state: RadioConnectionState | undefined;
  connected: boolean;
  connecting: boolean;
  isSelected: boolean;
}

/**
 * Shared rig connect/disconnect/select logic, used by both the RIG panel and the
 * status-bar quick switcher so the two can't drift. "Connected" mirrors the panel:
 * a Connected/Monitoring connection state, or an already-streaming radio state.
 * Connecting is uniform (`connectRadio`); disconnecting is type-specific.
 */
export function useRigConnection() {
  const {
    discoveredRadios,
    radioConnectionStates,
    radioStates,
    selectedRadioId,
    setSelectedRadio,
  } = useAppStore();
  const { connectRadio, disconnectRadio, disconnectTci, disconnectHamlibRig } = useSignalR();
  const autoConnectRigId = useSettingsStore((s) => s.settings.radio?.autoConnectRigId);

  const isRadioConnected = useCallback(
    (id: string) => {
      const s = radioConnectionStates.get(id);
      return s === 'Connected' || s === 'Monitoring' || radioStates.has(id);
    },
    [radioConnectionStates, radioStates],
  );

  const rigs: RigListItem[] = [...discoveredRadios.values()].map((r) => {
    const state = radioConnectionStates.get(r.id);
    return {
      id: r.id,
      name: r.nickname || r.model,
      type: r.type,
      detail: `${r.ipAddress}${r.port ? `:${r.port}` : ''}`,
      state,
      connected: isRadioConnected(r.id),
      connecting: state === 'Connecting',
      isSelected: r.id === selectedRadioId,
    };
  });

  /** Select + connect a radio (connect path is uniform across rig types). */
  const connect = useCallback(
    async (radioId: string) => {
      setSelectedRadio(radioId);
      await connectRadio(radioId);
    },
    [setSelectedRadio, connectRadio],
  );

  /** Disconnect a radio using its type-specific teardown. */
  const disconnect = useCallback(
    async (radioId: string) => {
      const radio = discoveredRadios.get(radioId);
      const isHamlib = radio?.type === 'Hamlib' || radioId.startsWith('hamlib-');
      const isTci = radio?.type === 'Tci' || radioId.startsWith('tci-');
      if (isHamlib) await disconnectHamlibRig();
      else if (isTci) await disconnectTci(radioId);
      else await disconnectRadio(radioId);
    },
    [discoveredRadios, disconnectHamlibRig, disconnectTci, disconnectRadio],
  );

  /** Switch to another rig, disconnecting the currently-connected one first. */
  const switchTo = useCallback(
    async (radioId: string) => {
      if (selectedRadioId && selectedRadioId !== radioId && isRadioConnected(selectedRadioId)) {
        await disconnect(selectedRadioId);
      }
      await connect(radioId);
    },
    [selectedRadioId, isRadioConnected, disconnect, connect],
  );

  const selectedRadio = selectedRadioId ? discoveredRadios.get(selectedRadioId) : undefined;

  // The radio the status pill represents: the selected one if any, else the
  // auto-connect target, else the first discovered rig. This keeps the pill's
  // name identical to what the popover shows for that same rig (nickname → model)
  // in every state, rather than a separate settings-derived label.
  const pillRadio =
    selectedRadio ??
    (autoConnectRigId ? discoveredRadios.get(autoConnectRigId) : undefined) ??
    [...discoveredRadios.values()][0];

  return {
    rigs,
    connect,
    disconnect,
    switchTo,
    isRadioConnected,
    selectedRadioId,
    /** Name shown on the status pill (nickname → model), or undefined if no rig configured. */
    pillRigName: pillRadio ? pillRadio.nickname || pillRadio.model : undefined,
    /** Whether the pill's rig is connected. */
    pillConnected: pillRadio ? isRadioConnected(pillRadio.id) : false,
  };
}
