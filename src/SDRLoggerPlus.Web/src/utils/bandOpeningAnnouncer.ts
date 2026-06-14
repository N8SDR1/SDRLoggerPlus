import { spellCallsign } from './hotSpotAnnouncer';

/**
 * Voice announcement for RBN band-opening alerts (SDRLogger+ port):
 * "Band opening! 6 meters. K 5 X Y Z. CW. 320 miles away. 24 dB."
 */
export function announceBandOpening(
  band: string,
  dxCall: string,
  mode: string,
  distance: number,
  unit: string,
  snr: number
): void {
  if (typeof speechSynthesis === 'undefined') return;
  const bandSpoken = band.replace('cm', ' centimeters').replace('m', ' meters');
  const unitSpoken = unit === 'km' ? 'kilometers' : 'miles';
  const utterance = new SpeechSynthesisUtterance(
    `Band opening! ${bandSpoken}. ${spellCallsign(dxCall)}. ${mode}. ${Math.round(distance)} ${unitSpoken} away. ${snr} dB.`
  );
  utterance.rate = 0.9;
  // Chromium requires cancel() before speak() or it silently drops the request
  speechSynthesis.cancel();
  speechSynthesis.speak(utterance);
}
