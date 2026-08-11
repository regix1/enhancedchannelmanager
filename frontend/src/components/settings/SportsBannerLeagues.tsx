import type { SportsBannerLeagueRule } from '../../services/api';
import './SportsBannerLeagues.css';

interface SportsBannerLeaguesProps {
  rules: SportsBannerLeagueRule[];
  onChange: (rules: SportsBannerLeagueRule[]) => void;
  disabled?: boolean;
}

export function SportsBannerLeagues({ rules, onChange, disabled = false }: SportsBannerLeaguesProps) {
  function updateRule(index: number, field: keyof SportsBannerLeagueRule, value: string): void {
    onChange(rules.map((rule, i) => (i === index ? { ...rule, [field]: value } : rule)));
  }

  function removeRule(index: number): void {
    onChange(rules.filter((_, i) => i !== index));
  }

  function addRule(): void {
    onChange([...rules, { match: '', league: '' }]);
  }

  return (
    <div className="settings-group banner-league-rules">
      <span className="form-description">
        Programme titles matching the first rule in this list decide which league
        the banner is built for. Order matters, because the first match wins: a
        narrower name has to sit above one it contains, which is why WNBA comes
        before NBA. A title matching no rule keeps the artwork the guide sent,
        and that is what stops shows like Divorce Court, which air a
        &quot;X vs. Y&quot; subtitle, from being given a sports banner.
      </span>

      {rules.length === 0 ? (
        <p className="banner-league-empty">
          No rules, so no matchup banners are built. Add one, or clear the server
          URL above to turn the feature off entirely.
        </p>
      ) : (
        <div className="banner-league-list">
          <div className="banner-league-row banner-league-head" aria-hidden="true">
            <span>Title matches</span>
            <span>League</span>
            <span />
          </div>
          {rules.map((rule, index) => (
            <div className="banner-league-row" key={index}>
              <input
                type="text"
                value={rule.match}
                disabled={disabled}
                placeholder="College Football|CFP"
                aria-label={`Title pattern ${index + 1}`}
                onChange={(e) => updateRule(index, 'match', e.target.value)}
              />
              <input
                type="text"
                value={rule.league}
                disabled={disabled}
                placeholder="ncaaf"
                aria-label={`League ${index + 1}`}
                onChange={(e) => updateRule(index, 'league', e.target.value)}
              />
              <button
                type="button"
                className="btn-secondary btn-small"
                disabled={disabled}
                aria-label={`Remove rule ${index + 1}`}
                onClick={() => removeRule(index)}
              >
                <span className="material-icons" aria-hidden="true">delete</span>
              </button>
            </div>
          ))}
        </div>
      )}

      <button
        type="button"
        className="btn-secondary btn-small banner-league-add"
        disabled={disabled}
        onClick={addRule}
      >
        <span className="material-icons" aria-hidden="true">add</span>
        Add rule
      </button>
    </div>
  );
}
