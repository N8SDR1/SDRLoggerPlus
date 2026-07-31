import { useConfirmationRuleStore } from '../store/confirmationRuleStore';

/**
 * Segmented LoTW+Card / All-confirmations switch for the grid views. Bound to
 * the shared store, so flipping it in one panel flips every grid surface at
 * once — the whole point is that they agree (issue #46).
 */
export function ConfirmationRuleToggle() {
  const rule = useConfirmationRuleStore((s) => s.rule);
  const setRule = useConfirmationRuleStore((s) => s.setRule);

  const btn = (active: boolean) =>
    `px-2 py-1 text-xs ${active ? 'bg-cyan-500/20 text-cyan-300' : 'bg-dark-700 text-dark-300 hover:bg-dark-600'}`;

  return (
    <div className="flex rounded border border-dark-500 overflow-hidden" role="group" aria-label="Confirmation counting">
      <button
        onClick={() => setRule('awardRules')}
        title="Count only LoTW and paper-card confirmations — what ARRL credits for VUCC/FFMA"
        className={btn(rule === 'awardRules')}
      >
        LoTW+Card
      </button>
      <button
        onClick={() => setRule('any')}
        title="Count any confirmation, including eQSL and QRZ Logbook"
        className={`border-l border-dark-500 ${btn(rule === 'any')}`}
      >
        All conf
      </button>
    </div>
  );
}
