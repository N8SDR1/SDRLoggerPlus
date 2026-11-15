import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import { gridToLatLon } from '../utils/maidenhead';
import { toReceptionReports, type ReceptionReport } from '../utils/dxCoach';

// Window sizes match the Heard-Me map layers' defaults so both share the
// server's per-(call, window) cache — no extra load on PSKReporter.
const PSK_WINDOW_MIN = 60;
const RBN_WINDOW_MIN = 15;

/**
 * Who has heard the operator's own signal recently, all bands — PSKReporter
 * (digital) + RBN (CW skimmers). Used by the DX Coach as its strongest
 * evidence: if your signal is reaching a DX's area on a band, call them.
 */
export function useMyReceptionReports(callsign: string | null | undefined): ReceptionReport[] {
  const call = (callsign ?? '').trim();

  // PSKReporter asks clients not to query more than every ~5 minutes; the
  // server caches per call for 5 minutes as well.
  const { data: psk } = useQuery({
    queryKey: ['psk-reports', call, PSK_WINDOW_MIN],
    queryFn: () => api.getPskReports(call, PSK_WINDOW_MIN),
    enabled: call.length > 0,
    refetchInterval: 5 * 60 * 1000,
    staleTime: 5 * 60 * 1000,
  });

  const { data: rbn } = useQuery({
    queryKey: ['rbn-heardme', call, 'all', RBN_WINDOW_MIN],
    queryFn: () => api.getRbnHeardMe(call, null, RBN_WINDOW_MIN),
    enabled: call.length > 0,
    refetchInterval: 45 * 1000,
    staleTime: 30 * 1000,
  });

  return useMemo(() => toReceptionReports(psk ?? [], rbn ?? [], gridToLatLon), [psk, rbn]);
}
