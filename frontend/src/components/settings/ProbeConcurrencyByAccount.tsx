import './ProbeConcurrencyByAccount.css';

/** One row of the editor. Both halves stay text so a half-typed row survives a re-render. */
export interface ProbeAccountConcurrency {
  accountId: string;
  concurrency: string;
}

interface ProbeConcurrencyByAccountProps {
  rows: ProbeAccountConcurrency[];
  onChange: (rows: ProbeAccountConcurrency[]) => void;
}

export function ProbeConcurrencyByAccount({ rows, onChange }: ProbeConcurrencyByAccountProps) {
  function updateRow(index: number, field: keyof ProbeAccountConcurrency, value: string): void {
    onChange(rows.map((row, i) => (i === index ? { ...row, [field]: value } : row)));
  }

  function removeRow(index: number): void {
    onChange(rows.filter((_, i) => i !== index));
  }

  function addRow(): void {
    onChange([...rows, { accountId: '', concurrency: '' }]);
  }

  return (
    <div className="form-group-vertical probe-concurrency">
      <label id="probeConcurrencyLabel">Per-account probe limit</label>
      <span className="form-description">
        An account listed here is probed no more than this many streams at a time.
        The figure can only lower an account's limit, never raise it past the global
        one above. Use it when one provider is stricter than the rest. The account id
        is the number in its Dispatcharr URL.
      </span>

      {rows.length === 0 ? (
        <p className="probe-concurrency-empty">
          <span className="material-icons" aria-hidden="true">tune</span>
          No accounts are capped, so every provider uses the global limit.
        </p>
      ) : (
        <div className="probe-concurrency-list" role="group" aria-labelledby="probeConcurrencyLabel">
          <div className="probe-concurrency-row probe-concurrency-head" aria-hidden="true">
            <span>Account id</span>
            <span>At a time</span>
            <span />
          </div>
          {rows.map((row, index) => (
            <div className="probe-concurrency-row" key={index}>
              <input
                type="text"
                inputMode="numeric"
                value={row.accountId}
                placeholder="4"
                aria-label={`Account id ${index + 1}`}
                onChange={(e) => updateRow(index, 'accountId', e.target.value)}
              />
              <input
                type="text"
                inputMode="numeric"
                value={row.concurrency}
                placeholder="1"
                aria-label={`Streams at a time ${index + 1}`}
                onChange={(e) => updateRow(index, 'concurrency', e.target.value)}
              />
              <button
                type="button"
                className="btn-secondary btn-small probe-concurrency-remove"
                aria-label={`Remove account limit ${index + 1}`}
                onClick={() => removeRow(index)}
              >
                <span className="material-icons" aria-hidden="true">delete</span>
              </button>
            </div>
          ))}
        </div>
      )}

      <button
        type="button"
        className="btn-secondary btn-small probe-concurrency-add"
        onClick={addRow}
      >
        <span className="material-icons" aria-hidden="true">add</span>
        Add account
      </button>
    </div>
  );
}
