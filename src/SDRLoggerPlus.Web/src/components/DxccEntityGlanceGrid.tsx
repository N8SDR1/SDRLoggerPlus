import type { DxccEntityGlance } from '../api/client';

const ROW_LABEL: Record<string, string> = {
  phone: 'Phone',
  cw: 'CW',
  digital: 'Digital',
};

function cellTitle(qsoCount: number, confirmed: boolean): string {
  return `${qsoCount} QSO${qsoCount === 1 ? '' : 's'}${confirmed ? ' · Confirmed' : ''}`;
}

/**
 * Log Entry DXCC grid. Not a control: a cell does not tune the radio.
 * Stage 1 does not read callsignWorked.
 */
export function DxccEntityGlanceGrid({ glance }: { glance: DxccEntityGlance }) {
  return (
    <div
      className="rounded-lg border border-glass-100 bg-dark-700/30 p-2"
      data-testid="dxcc-entity-glance"
    >
      <div className="mb-1 text-xs font-ui text-gray-200">
        <span className="mr-1.5 font-mono text-accent-primary">{glance.primaryPrefix}</span>
        {glance.entityName}
      </div>
      <div className="overflow-x-auto">
        <table className="border-collapse text-xs font-ui">
          <thead>
            <tr>
              <th className="pr-2 text-left font-semibold text-dark-300" />
              {glance.bands.map(band => (
                <th key={band} className="px-1 text-center font-semibold text-dark-300">
                  {band}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {glance.rows.map(row => (
              <tr key={row.modeGroup}>
                <td className="whitespace-nowrap pr-2 text-dark-300">
                  {ROW_LABEL[row.modeGroup] ?? row.modeGroup}
                </td>
                {row.cells.map((cell, index) => {
                  const band = glance.bands[index] ?? String(index);
                  if (!cell.worked) {
                    return (
                      <td key={band} className="px-1 py-0.5 text-center text-xs text-dark-400">
                        -
                      </td>
                    );
                  }
                  return (
                    <td key={band} className="px-1 py-0.5 text-center">
                      <span
                        title={cellTitle(cell.qsoCount, cell.confirmed)}
                        className={`inline-block h-3 w-3 rounded-sm ${cell.confirmed ? 'bg-accent-success' : 'bg-accent-warning'}`}
                      />
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="mt-1 flex items-center gap-2 text-[10px]">
        <span className="inline-block h-3 w-3 rounded-sm bg-accent-success" />
        <span className="text-dark-300">Confirmed</span>
        <span className="inline-block h-3 w-3 rounded-sm bg-accent-warning" />
        <span className="text-dark-300">Worked</span>
        <span className="inline-block h-3 w-3 rounded-sm border border-glass-100 bg-dark-600" />
        <span className="text-dark-300">Not worked</span>
      </div>
    </div>
  );
}
