/**
 * Tests for the Event Sync rule editor (bead ti939.1.5 — Phase 1A).
 *
 * Pins: the attach-threshold input clamp (>= 0.80 hard floor), live
 * auto-sync guidance (guidance only — this phase never toggles Dispatcharr
 * settings), the placeholder conditions/actions save convention, the
 * omit-patterns-when-builtin-defaults behavior, and the absence of any
 * apply/attach control.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from 'vitest';
import { act, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { http, HttpResponse } from 'msw';
import {
  server,
  resetMockDataStore,
  mockDataStore,
  createMockChannelGroup,
} from '../../test/mocks/server';
import { EventSyncRuleEditor } from './EventSyncRuleEditor';
import type { ChannelPipelineRule } from '../../types/channelPipeline';
import type { ProviderGroupScopeRow } from '../../services/api';
import type { EventSyncConfig, EventSyncPreviewResponse } from '../../types/eventSync';

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => {
  server.resetHandlers();
  resetMockDataStore();
});
afterAll(() => server.close());

/** Stub GET /api/providers/group-settings/by-provider with (provider, group)
 * junction rows (bead 38dzi). The picker groups rows by channel_group_id; a
 * single row per group renders as a flat WHOLE-GROUP (m3u_account_id null)
 * option with data-testid `psg-<role>-<groupId>-any`. */
function stubJunctions(rows: ProviderGroupScopeRow[]) {
  server.use(
    http.get('/api/providers/group-settings/by-provider', () =>
      HttpResponse.json(rows)
    )
  );
}

/** Convenience: one single-provider junction per group ('Provider A', id 1,
 * enabled) with the given auto-sync flag. Use `stubJunctionsFull` for tests
 * that need to control the `enabled` flag itself (bead x82s3). */
function stubGroupSettings(autoSyncByGroupId: Record<number, boolean>) {
  stubJunctions(
    Object.entries(autoSyncByGroupId).map(([groupId, autoSync]) => ({
      m3u_account_id: 1,
      m3u_account_name: 'Provider A',
      channel_group_id: Number(groupId),
      auto_channel_sync: autoSync,
      enabled: true,
      stream_count: 10,
    }))
  );
}

/** One single-provider junction per group with explicit `enabled` +
 * `auto_channel_sync` (bead x82s3 — enabled-groups filter). */
function stubGroupSettingsFull(
  settings: Record<number, { enabled: boolean; auto_channel_sync: boolean }>
) {
  stubJunctions(
    Object.entries(settings).map(([groupId, s]) => ({
      m3u_account_id: 1,
      m3u_account_name: 'Provider A',
      channel_group_id: Number(groupId),
      auto_channel_sync: s.auto_channel_sync,
      enabled: s.enabled,
      stream_count: 10,
    }))
  );
}

/** Click a wizard step pill (bead m1s38.1). Controls that live on a step
 * other than the current one are `hidden`, so role/visibility queries only
 * reach them after navigating to that step. */
async function goToStep(
  user: ReturnType<typeof userEvent.setup>,
  n: 1 | 2 | 3 | 4
) {
  await user.click(screen.getByTestId(`event-sync-step-${n}`));
}

function seedGroups() {
  mockDataStore.channelGroups.push(
    createMockChannelGroup({ id: 1, name: 'Master Events' }),
    createMockChannelGroup({ id: 2, name: 'Secondary Events' })
  );
}

/** seedGroups() plus a third group (id 3) for the enabled-groups-filter tests. */
function seedGroupsWithDisabled() {
  seedGroups();
  mockDataStore.channelGroups.push(createMockChannelGroup({ id: 3, name: 'Disabled Group' }));
}

const EXISTING_RULE: Partial<ChannelPipelineRule> = {
  id: 5,
  name: 'PPV Events',
  enabled: true,
  conditions: [{ type: 'always' }],
  actions: [{ type: 'skip' }],
  event_sync_config: {
    master_group_id: 1,
    secondary_group_ids: [2],
    time_window_minutes: 30,
    attach_threshold: 0.8,
    enabled: true,
  },
};

describe('EventSyncRuleEditor', () => {
  describe('attach threshold clamp', () => {
    it('preserves a value below the 0.80 default on blur (operator-authoritative)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Score threshold'));
      const input = screen.getByLabelText(/attach threshold/i);
      await user.clear(input);
      await user.type(input, '0.5');
      await user.tab();

      // 0.80 is the default, not a hard floor — the operator's 0.50 stands.
      expect(input).toHaveValue(0.5);
    });

    it('clamps a value above 1.0 down to 1.00 on blur', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Score threshold'));
      const input = screen.getByLabelText(/attach threshold/i);
      await user.clear(input);
      await user.type(input, '1.5');
      await user.tab();

      expect(input).toHaveValue(1);
    });

    it('clamps the threshold to the [0,1] bounds in the saved config', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Score threshold'));
      const input = screen.getByLabelText(/attach threshold/i);
      await user.clear(input);
      // A sub-default value is honored (only out-of-[0,1] values are clamped).
      await user.type(input, '0.65');
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config.attach_threshold).toBe(0.65);
    });
  });

  describe('ignore time window toggle (krkm4)', () => {
    it('disables the time-window input and saves enforce_time_window=false when checked', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Time tuning'));
      const windowInput = screen.getByLabelText(/time window \(minutes\)/i);
      expect(windowInput).not.toBeDisabled();

      await user.click(screen.getByTestId('event-sync-ignore-time-window'));
      expect(windowInput).toBeDisabled();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.enforce_time_window
      ).toBe(false);
    });

    it('omits enforce_time_window when left enforced (absent means true)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config
      ).not.toHaveProperty('enforce_time_window');
    });
  });

  describe('max_attach_per_run pass-through (ti939.2.1)', () => {
    it('preserves an API-set attach cap across a UI edit save', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          max_attach_per_run: 25,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config.max_attach_per_run).toBe(25);
    });

    it('omits the cap when the rule never had one (backend default applies)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config).not.toHaveProperty(
        'max_attach_per_run'
      );
    });
  });

  describe('master-group self-attach (bead 6xxmp)', () => {
    it('saves with an EMPTY secondary list when the flag is on (bead 3ux85)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          secondary_group_ids: [],
          include_master_group_streams: true,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      // No secondary selected + flag on => save is allowed.
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const cfg = onSave.mock.calls[0][0].event_sync_config;
      expect(cfg.secondary).toEqual([]);
      expect(cfg).not.toHaveProperty('secondary_group_ids');
      expect(cfg.include_master_group_streams).toBe(true);
    });

    it('blocks save with an empty secondary list when the flag is OFF', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          secondary_group_ids: [],
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('button', { name: 'Save' }));
      expect(onSave).not.toHaveBeenCalled();
    });

    it('defaults OFF and omits include_master_group_streams when never set', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Scope extension'));
      expect(
        screen.getByTestId('event-sync-include-master-group-streams')
      ).not.toBeChecked();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config).not.toHaveProperty(
        'include_master_group_streams'
      );
    });

    it('emits include_master_group_streams: true when the operator checks the box', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Scope extension'));
      await user.click(
        screen.getByTestId('event-sync-include-master-group-streams')
      );
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.include_master_group_streams
      ).toBe(true);
    });

    it('initializes checked from a stored true and round-trips it', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          include_master_group_streams: true,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Scope extension'));
      expect(
        screen.getByTestId('event-sync-include-master-group-streams')
      ).toBeChecked();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.include_master_group_streams
      ).toBe(true);
    });
  });

  describe('parse-master-from-stream opt-in', () => {
    it('defaults OFF and omits the key when never set', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Scope extension'));
      expect(
        screen.getByTestId('event-sync-parse-master-from-stream')
      ).not.toBeChecked();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config).not.toHaveProperty(
        'parse_master_from_stream'
      );
    });

    it('emits parse_master_from_stream: true when checked', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Scope extension'));
      await user.click(
        screen.getByTestId('event-sync-parse-master-from-stream')
      );
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.parse_master_from_stream
      ).toBe(true);
    });
  });

  describe('assume-current-date opt-in (dateless listings)', () => {
    it('defaults OFF and omits assume_current_date when never set', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Date handling'));
      expect(
        screen.getByTestId('event-sync-assume-current-date')
      ).not.toBeChecked();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config).not.toHaveProperty(
        'assume_current_date'
      );
    });

    it('emits assume_current_date: true when checked', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Date handling'));
      await user.click(screen.getByTestId('event-sync-assume-current-date'));
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.assume_current_date
      ).toBe(true);
    });
  });

  describe('stale-dateless demote guard (bead jqwfq)', () => {
    it('defaults ON, is disabled without the date assumption, and omits the key', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Date handling'));
      const guard = screen.getByTestId('event-sync-demote-stale-dateless');
      expect(guard).toBeChecked();
      // Inert without assume_current_date — the control says so.
      expect(guard).toBeDisabled();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      // Guard untouched → key omitted (absent means true on the backend).
      expect(onSave.mock.calls[0][0].event_sync_config).not.toHaveProperty(
        'demote_stale_dateless'
      );
    });

    it('emits demote_stale_dateless: false when unchecked under assume-current-date', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Date handling'));
      await user.click(screen.getByTestId('event-sync-assume-current-date'));
      await user.click(screen.getByTestId('event-sync-demote-stale-dateless'));
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const saved = onSave.mock.calls[0][0].event_sync_config;
      expect(saved.assume_current_date).toBe(true);
      expect(saved.demote_stale_dateless).toBe(false);
    });

    it('initializes from a stored demote_stale_dateless: false and round-trips it', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          assume_current_date: true,
          demote_stale_dateless: false,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Date handling'));
      expect(
        screen.getByTestId('event-sync-demote-stale-dateless')
      ).not.toBeChecked();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.demote_stale_dateless
      ).toBe(false);
    });

    it('re-checking a stored false preserves the explicit key as true', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          assume_current_date: true,
          demote_stale_dateless: false,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Date handling'));
      await user.click(screen.getByTestId('event-sync-demote-stale-dateless'));
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.demote_stale_dateless
      ).toBe(true);
    });
  });

  describe('auto-run opt-in (ti939.3.1)', () => {
    it('defaults OFF and omits auto_run for a rule that never had the key', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Automation'));
      expect(screen.getByTestId('event-sync-auto-run')).not.toBeChecked();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config).not.toHaveProperty('auto_run');
    });

    it('emits auto_run: true when the operator checks the box', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Automation'));
      await user.click(screen.getByTestId('event-sync-auto-run'));
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config.auto_run).toBe(true);
    });

    it('initializes checked from a stored auto_run: true and round-trips it untouched', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          auto_run: true,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Automation'));
      expect(screen.getByTestId('event-sync-auto-run')).toBeChecked();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config.auto_run).toBe(true);
    });

    it('preserves a stored explicit auto_run: false on an untouched save (z4y4a round-trip)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          auto_run: false,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config.auto_run).toBe(false);
    });

    it('turning a stored auto_run: true OFF saves an explicit false', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          auto_run: true,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Automation'));
      await user.click(screen.getByTestId('event-sync-auto-run'));
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config.auto_run).toBe(false);
    });

    it('explains the unattended behavior honestly (default off, notifications, breaker)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Automation'));
      expect(
        screen.getByText(/enable it only after you trust/i)
      ).toBeInTheDocument();
      expect(screen.getByText(/warning notifications/i)).toBeInTheDocument();
      expect(screen.getByText(/circuit breaker/i)).toBeInTheDocument();
      expect(screen.getByText(/attaches on the next run/i)).toBeInTheDocument();
    });
  });

  describe('refresh_providers_before_run toggle (bead y8yby)', () => {
    it('defaults OFF and omits the key for a rule that never had it', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Automation'));
      expect(
        screen.getByTestId('event-sync-refresh-providers-before-run')
      ).not.toBeChecked();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config).not.toHaveProperty(
        'refresh_providers_before_run'
      );
    });

    it('emits refresh_providers_before_run: true when checked, and surfaces the Test-writes warning', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Automation'));
      await user.click(
        screen.getByTestId('event-sync-refresh-providers-before-run')
      );
      // The "Test is no longer zero-write" consequence is surfaced inline.
      expect(
        screen.getByTestId('event-sync-refresh-test-writes-note')
      ).toBeInTheDocument();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.refresh_providers_before_run
      ).toBe(true);
    });

    it('initializes checked from a stored true and round-trips it', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          refresh_providers_before_run: true,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Automation'));
      expect(
        screen.getByTestId('event-sync-refresh-providers-before-run')
      ).toBeChecked();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.refresh_providers_before_run
      ).toBe(true);
    });
  });

  describe('inline auto-sync status (picker; toggles ONLY via the confirmed fix)', () => {
    it('shows an inline mismatch + Fix when the master scope has auto-sync OFF', async () => {
      seedGroups();
      stubGroupSettings({ 1: false, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      // The migrated master scope (whole group 1) renders checked with an OFF
      // junction — a master needs it ON, so the picker shows the inline fix.
      const fix = await screen.findByRole('button', {
        name: /turn auto-sync ON for Provider A/i,
      });
      expect(fix).toBeInTheDocument();
      expect(screen.getByText(/a master needs it ON/i)).toBeInTheDocument();
    });

    it('shows an OK sync badge (no Fix) when the master scope has auto-sync ON', async () => {
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      const master = await screen.findByTestId('psg-master');
      expect(await within(master).findByText(/Auto-sync ON ✓/)).toBeInTheDocument();
      expect(
        within(master).queryByRole('button', { name: /turn auto-sync/i })
      ).toBeNull();
    });

    it('shows an inline mismatch + Fix when a secondary scope has auto-sync ON', async () => {
      seedGroups();
      stubGroupSettings({ 1: true, 2: true });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      const secondary = await screen.findByTestId('psg-secondary');
      expect(
        await within(secondary).findByRole('button', {
          name: /turn auto-sync OFF for Provider A/i,
        })
      ).toBeInTheDocument();
      expect(within(secondary).getByText(/a secondary needs it OFF/i)).toBeInTheDocument();
    });
  });

  describe('guided auto-sync fix (ti939.3.4 — confirmed, never a side effect)', () => {
    /** Stub the toggle endpoint, recording every request body. */
    function stubToggleEndpoint(calls: unknown[]) {
      server.use(
        http.post('/api/m3u/accounts/1/group-auto-sync-toggle', async ({ request }) => {
          const body = await request.json();
          calls.push(body);
          return HttpResponse.json({
            changed: true,
            channel_group_id: (body as { channel_group_id: number }).channel_group_id,
            group_name: 'Secondary Events',
            account_id: 1,
            account_name: 'Provider A',
            auto_channel_sync: (body as { auto_channel_sync: boolean }).auto_channel_sync,
          });
        })
      );
    }

    it('the picker Fix affordance only OPENS the confirmation dialog — nothing is written yet', async () => {
      const user = userEvent.setup();
      const calls: unknown[] = [];
      seedGroups();
      stubGroupSettings({ 1: true, 2: true });
      stubToggleEndpoint(calls);
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      await user.click(
        await screen.findByRole('button', { name: /turn auto-sync OFF for Provider A/i })
      );

      // Dialog states exactly what will change and why, including the
      // consequence and the snapshot-restore recovery note.
      const dialog = screen.getByRole('alertdialog');
      expect(dialog).toHaveTextContent('Secondary Events');
      expect(dialog).toHaveTextContent('Provider A');
      expect(dialog).toHaveTextContent(/stop creating duplicate channels/i);
      expect(dialog).toHaveTextContent(/may be removed by Dispatcharr/i);
      expect(dialog).toHaveTextContent(/snapshot restore does .*not.* revert/i);
      expect(dialog).toHaveTextContent(/journal entry is the recovery breadcrumb/i);
      expect(calls).toHaveLength(0);
    });

    it('Cancel closes the dialog without calling the toggle API', async () => {
      const user = userEvent.setup();
      const calls: unknown[] = [];
      seedGroups();
      stubGroupSettings({ 1: true, 2: true });
      stubToggleEndpoint(calls);
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      await user.click(
        await screen.findByRole('button', { name: /turn auto-sync OFF for Provider A/i })
      );
      const dialog = screen.getByRole('alertdialog');
      await user.click(within(dialog).getByRole('button', { name: 'Cancel' }));

      expect(screen.queryByRole('alertdialog')).toBeNull();
      expect(calls).toHaveLength(0);
    });

    it('Confirm sends confirm:true for the OFF direction and the inline fix clears after refetch', async () => {
      const user = userEvent.setup();
      const calls: unknown[] = [];
      seedGroups();
      stubGroupSettings({ 1: true, 2: true });
      stubToggleEndpoint(calls);
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      await user.click(
        await screen.findByRole('button', { name: /turn auto-sync OFF for Provider A/i })
      );
      // The refetch after the confirmed toggle sees the FIXED junction.
      stubGroupSettings({ 1: true, 2: false });
      await user.click(screen.getByTestId('autosync-fix-confirm'));

      await waitFor(() => expect(calls).toHaveLength(1));
      expect(calls[0]).toEqual({
        channel_group_id: 2,
        auto_channel_sync: false,
        confirm: true,
      });
      // The inline mismatch fix clears — the editor refetched the junctions.
      await waitFor(() =>
        expect(
          screen.queryByRole('button', { name: /turn auto-sync OFF for Provider A/i })
        ).toBeNull()
      );
      expect(screen.queryByRole('alertdialog')).toBeNull();
    });

    it('Confirm sends confirm:true for the ON direction (master fix)', async () => {
      const user = userEvent.setup();
      const calls: unknown[] = [];
      seedGroups();
      stubGroupSettings({ 1: false, 2: false });
      stubToggleEndpoint(calls);
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      await user.click(
        await screen.findByRole('button', { name: /turn auto-sync ON for Provider A/i })
      );
      const dialog = screen.getByRole('alertdialog');
      expect(dialog).toHaveTextContent(/begin creating and managing channels/i);
      stubGroupSettings({ 1: true, 2: false });
      await user.click(screen.getByTestId('autosync-fix-confirm'));

      await waitFor(() => expect(calls).toHaveLength(1));
      expect(calls[0]).toEqual({
        channel_group_id: 1,
        auto_channel_sync: true,
        confirm: true,
      });
      await waitFor(() =>
        expect(
          screen.queryByRole('button', { name: /turn auto-sync ON for Provider A/i })
        ).toBeNull()
      );
    });

    it('saving a rule NEVER calls the toggle endpoint, even with a mismatch showing', async () => {
      const user = userEvent.setup();
      const calls: unknown[] = [];
      seedGroups();
      stubGroupSettings({ 1: false, 2: true });
      stubToggleEndpoint(calls);
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await screen.findByRole('button', { name: /turn auto-sync ON for Provider A/i });
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(calls).toHaveLength(0);
    });
  });

  describe('saving', () => {
    it('saves placeholder conditions/actions and omits patterns when the built-in defaults are selected', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const saved = onSave.mock.calls[0][0];
      expect(saved.name).toBe('PPV Events');
      expect(saved.conditions).toEqual([{ type: 'always' }]);
      expect(saved.actions).toEqual([{ type: 'skip' }]);
      // bead 38dzi: the editor emits the nested provider-scoped shape; the
      // legacy flat master_group_id/secondary_group_ids migrate to
      // whole-group scopes (m3u_account_id null) and the flat keys are gone.
      expect(saved.event_sync_config).toEqual({
        master: { group_id: 1, m3u_account_id: null },
        secondary: [{ group_id: 2, m3u_account_id: null }],
        time_window_minutes: 30,
        attach_threshold: 0.8,
        enabled: true,
      });
      // Built-in default selection → no patterns key (backend defaults apply)
      expect(saved.event_sync_config).not.toHaveProperty('patterns');
    });

    it('sends the selected patterns explicitly when the selection differs from the defaults', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      // Parse patterns live on the Matching step.
      await goToStep(user, 2);
      // Deselect one of the built-ins; the remaining built-ins are emitted.
      await user.click(screen.getByRole('checkbox', { name: /month-first date \(built-in\)/i }));
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const config = onSave.mock.calls[0][0].event_sync_config;
      const names = config.patterns.map((p: { name: string }) => p.name);
      expect(names).toContain('slot-title-day-first-date');
      expect(names).not.toContain('slot-title-month-first-date');
      expect(config.patterns[0].title_pattern).toContain('(?P<title>');
    });

    it('blocks saving without a name', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(
        <EventSyncRuleEditor
          rule={{ ...EXISTING_RULE, name: '' }}
          onSave={onSave}
          onCancel={vi.fn()}
        />
      );

      await user.click(screen.getByRole('button', { name: 'Save' }));

      expect(onSave).not.toHaveBeenCalled();
      expect(screen.getByRole('alert')).toHaveTextContent('Name is required');
    });
  });

  describe('API-authored multi-pattern round-trip (z4y4a)', () => {
    /** A hand-/API-authored config the UI cannot fully express: two shared
     * custom patterns (the editor edits only the first) plus a two-pattern
     * per-group override list (the editor edits only patterns[0]). */
    const API_CONFIG = {
      master_group_id: 1,
      secondary_group_ids: [2],
      patterns: [
        {
          name: 'api-primary',
          title_pattern: '^(?P<title>.+?)\\s*@',
          time_pattern: '(?P<hour>\\d{1,2}):(?P<minute>\\d{2})',
          date_pattern: '(?P<day>\\d{1,2})\\s+(?P<month>[A-Za-z]{3,9})',
        },
        // Nameless second shared pattern — no editor control can express it.
        { title_pattern: '^(?P<title>.+)$' },
      ],
      group_patterns: {
        '2': [
          { name: 'g2-first', title_pattern: 'x(?P<title>.+)' },
          { name: 'g2-extra', title_pattern: 'y(?P<title>.+)' },
        ],
      },
      time_window_minutes: 45,
      attach_threshold: 0.9,
      enabled: true,
    };

    const apiRule: Partial<ChannelPipelineRule> = {
      ...EXISTING_RULE,
      event_sync_config: API_CONFIG,
    };

    it('survives open → save byte-identically when nothing was edited', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={apiRule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const saved = onSave.mock.calls[0][0].event_sync_config;
      // The group scopes migrate to the nested provider-scoped shape (the
      // flat keys are gone), but everything else round-trips content-identically...
      expect(saved.master).toEqual({ group_id: 1, m3u_account_id: null });
      expect(saved.secondary).toEqual([{ group_id: 2, m3u_account_id: null }]);
      expect(saved).not.toHaveProperty('master_group_id');
      expect(saved).not.toHaveProperty('secondary_group_ids');
      expect(saved.time_window_minutes).toBe(45);
      expect(saved.attach_threshold).toBe(0.9);
      // ...and the arrays the UI cannot express are passed through VERBATIM
      // (same objects — not a re-built lossy approximation).
      expect(saved.patterns).toBe(API_CONFIG.patterns);
      expect(saved.group_patterns['2']).toBe(API_CONFIG.group_patterns['2']);
      expect(JSON.stringify(saved.patterns)).toBe(JSON.stringify(API_CONFIG.patterns));
      expect(JSON.stringify(saved.group_patterns)).toBe(
        JSON.stringify(API_CONFIG.group_patterns)
      );
    });

    it('does NOT silently re-add built-ins to an all-custom config', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={apiRule} onSave={onSave} onCancel={vi.fn()} />);

      await goToStep(user, 2);
      // The built-in checkboxes reflect the saved selection: none selected.
      expect(screen.getByRole('checkbox', { name: /day-first date \(built-in\)/i }))
        .not.toBeChecked();
      expect(screen.getByRole('checkbox', { name: /month-first date \(built-in\)/i }))
        .not.toBeChecked();

      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const saved = onSave.mock.calls[0][0].event_sync_config;
      const savedNames = saved.patterns.map((p: { name?: string }) => p.name);
      expect(savedNames).not.toContain('slot-title-day-first-date');
      expect(savedNames).not.toContain('slot-title-month-first-date');
      expect(saved.patterns).toHaveLength(2);
    });

    it('preserves the inexpressible extras (and their names) when the editable first custom IS edited', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={apiRule} onSave={onSave} onCancel={vi.fn()} />);

      // Edit the first custom shared pattern's title regex.
      await user.click(screen.getByText('Custom shared pattern (regex fallback)'));
      const titleInput = screen.getAllByLabelText('Title pattern')
        .find(el => el.id.includes('-custom-title'))!;
      await user.clear(titleInput);
      await user.type(titleInput, '^EDITED (?P<title>.+)$');
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const saved = onSave.mock.calls[0][0].event_sync_config;
      // Edited first custom keeps its API-authored name; the trailing extra
      // survives untouched; nothing re-ordered around it, no built-ins added.
      expect(saved.patterns).toEqual([
        {
          name: 'api-primary',
          title_pattern: '^EDITED (?P<title>.+)$',
          time_pattern: '(?P<hour>\\d{1,2}):(?P<minute>\\d{2})',
          date_pattern: '(?P<day>\\d{1,2})\\s+(?P<month>[A-Za-z]{3,9})',
        },
        { title_pattern: '^(?P<title>.+)$' },
      ]);
      // Untouched group_patterns still round-trip verbatim.
      expect(saved.group_patterns).toEqual(API_CONFIG.group_patterns);
    });

    it('preserves per-group extras when the editable override is edited', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={apiRule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Per-group pattern overrides'));
      // Open the secondary group's override editor and change its title.
      await user.click(screen.getByText(/Secondary Events/, { selector: 'summary' }));
      const overrideTitle = screen.getAllByLabelText('Title pattern')
        .find(el => el.id.includes('-ov-2-'))!;
      await user.clear(overrideTitle);
      await user.type(overrideTitle, 'z(?P<title>.+)');
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const saved = onSave.mock.calls[0][0].event_sync_config;
      expect(saved.group_patterns['2']).toEqual([
        { name: 'g2-first', title_pattern: 'z(?P<title>.+)' },
        { name: 'g2-extra', title_pattern: 'y(?P<title>.+)' },
      ]);
      // Untouched shared patterns still round-trip verbatim.
      expect(saved.patterns).toEqual(API_CONFIG.patterns);
    });

    it('surfaces the preserved inexpressible patterns with a read-only indicator', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={apiRule} onSave={vi.fn()} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Custom shared pattern (regex fallback)'));
      const sharedIndicator = screen.getByTestId('custom-shared-extras');
      expect(sharedIndicator).toHaveTextContent(/preserved as saved/i);

      await user.click(screen.getByText('Per-group pattern overrides'));
      await user.click(screen.getByText(/Secondary Events/, { selector: 'summary' }));
      const groupIndicator = screen.getByTestId('group-override-extras-2');
      expect(groupIndicator).toHaveTextContent(/preserved as saved/i);
      expect(groupIndicator).toHaveTextContent('g2-extra');
    });

    it('a UI-authored single-custom config still saves exactly as before (no extras machinery)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByText('Custom shared pattern (regex fallback)'));
      const customTitle = screen.getAllByLabelText('Title pattern')
        .find(el => el.id.includes('-custom-title'))!;
      await user.type(customTitle, '^(?P<title>.+?)\\s*@');
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const saved = onSave.mock.calls[0][0].event_sync_config;
      // Built-in defaults still selected + one new custom → shipped verbatim
      // then the UI-named custom, exactly the pre-z4y4a shape.
      const names = saved.patterns.map((p: { name?: string }) => p.name);
      expect(names).toEqual([
        'slot-title-day-first-date',
        'slot-title-month-first-date',
        'slot-title-numeric-date',
        'custom-shared',
      ]);
      expect(saved.patterns[3].title_pattern).toBe('^(?P<title>.+?)\\s*@');
    });
  });

  describe('dummy EPG profile reference (ti939.3.3)', () => {
    function stubDummyProfiles() {
      server.use(
        http.get('/api/dummy-epg/profiles', () =>
          HttpResponse.json([
            { id: 7, name: 'Events EPG', enabled: true },
            { id: 8, name: 'Old EPG', enabled: false },
          ])
        )
      );
    }

    it('preserves an API-set dummy_epg_profile_id on an untouched save', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(
        <EventSyncRuleEditor
          rule={{
            ...EXISTING_RULE,
            event_sync_config: {
              ...EXISTING_RULE.event_sync_config!,
              dummy_epg_profile_id: 7,
            },
          }}
          onSave={onSave}
          onCancel={vi.fn()}
        />
      );

      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const saved = onSave.mock.calls[0][0].event_sync_config;
      expect(saved.dummy_epg_profile_id).toBe(7);
    });

    it('selecting a profile under Advanced emits it; the default omits the key', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      stubDummyProfiles();
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await goToStep(user, 3);
      await user.click(screen.getByText('Guide data'));
      // Open the profile picker (shows the None placeholder) and pick one.
      await user.click(
        screen.getByRole('button', { name: /none — no automatic guide data/i })
      );
      await user.click(await screen.findByRole('option', { name: 'Events EPG' }));
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const saved = onSave.mock.calls[0][0].event_sync_config;
      expect(saved.dummy_epg_profile_id).toBe(7);
    });

    it('omits the key entirely when no profile is selected (absent means off)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const saved = onSave.mock.calls[0][0].event_sync_config;
      expect(saved).not.toHaveProperty('dummy_epg_profile_id');
    });
  });

  describe('stream order (bead io0tv)', () => {
    it('defaults to no sorting: save emits empty stream_sort_field (no behavior change)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].stream_sort_field).toBe('');
    });

    it('selecting Provider Order emits stream_sort_field with the desc default', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await goToStep(user, 3);
      await user.click(screen.getByText('Stream order', { selector: 'summary' }));
      await user.click(
        screen.getByRole('button', { name: /no sorting — attach order/i })
      );
      await user.click(
        await screen.findByRole('option', { name: 'Provider Order (M3U)' })
      );
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const saved = onSave.mock.calls[0][0];
      expect(saved.stream_sort_field).toBe('provider_order');
      expect(saved.stream_sort_order).toBe('desc');
    });

    it('round-trips an existing rule with stream sort set on an untouched save', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(
        <EventSyncRuleEditor
          rule={{
            ...EXISTING_RULE,
            stream_sort_field: 'provider_order',
            stream_sort_order: 'asc',
          }}
          onSave={onSave}
          onCancel={vi.fn()}
        />
      );

      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const saved = onSave.mock.calls[0][0];
      expect(saved.stream_sort_field).toBe('provider_order');
      expect(saved.stream_sort_order).toBe('asc');
    });

    it('mentions the stream order in the rule-intent sentence once a sort is picked', async () => {
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(
        <EventSyncRuleEditor
          rule={{ ...EXISTING_RULE, stream_sort_field: 'provider_order' }}
          onSave={vi.fn()}
          onCancel={vi.fn()}
        />
      );

      const intent = await screen.findByTestId('event-sync-intent');
      await waitFor(() => {
        expect(intent).toHaveTextContent(/Attach streams from 1 secondary group to master Master Events/i);
      });
      expect(intent).toHaveTextContent(
        /orders each master channel's streams by provider order/i
      );
    });
  });

  it('has NO apply or attach control anywhere (Phase 1A hard constraint)', async () => {
    seedGroups();
    stubGroupSettings({ 1: true, 2: false });
    render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

    const master = await screen.findByTestId('psg-master');
    await within(master).findByText(/Auto-sync ON ✓/);
    expect(screen.queryByRole('button', { name: /apply|attach/i })).toBeNull();
  });

  describe('enabled-groups filter (bead x82s3)', () => {
    /** Group 3 ('Disabled Group') has `enabled: false` — hidden by default. */
    function stubThreeGroups() {
      stubGroupSettingsFull({
        1: { enabled: true, auto_channel_sync: true },
        2: { enabled: true, auto_channel_sync: false },
        3: { enabled: false, auto_channel_sync: false },
      });
    }

    it('defaults to hiding disabled junctions from both the master and secondary pickers', async () => {
      seedGroupsWithDisabled();
      stubThreeGroups();
      render(<EventSyncRuleEditor onSave={vi.fn()} onCancel={vi.fn()} />);

      // Secondary picker: disabled group's junction absent, enabled present.
      expect(await screen.findByTestId('psg-secondary-1-any')).toBeInTheDocument();
      expect(screen.getByTestId('psg-secondary-2-any')).toBeInTheDocument();
      expect(screen.queryByTestId('psg-secondary-3-any')).toBeNull();

      // Master picker: same filtering.
      expect(screen.getByTestId('psg-master-1-any')).toBeInTheDocument();
      expect(screen.getByTestId('psg-master-2-any')).toBeInTheDocument();
      expect(screen.queryByTestId('psg-master-3-any')).toBeNull();
    });

    it('reveals disabled junctions in both pickers when "Show all groups" is checked', async () => {
      const user = userEvent.setup();
      seedGroupsWithDisabled();
      stubThreeGroups();
      render(<EventSyncRuleEditor onSave={vi.fn()} onCancel={vi.fn()} />);

      await screen.findByTestId('psg-secondary-1-any');
      expect(screen.queryByTestId('psg-secondary-3-any')).toBeNull();

      await user.click(
        screen.getByRole('checkbox', { name: /show all groups/i })
      );

      expect(await screen.findByTestId('psg-secondary-3-any')).toBeInTheDocument();
      expect(screen.getByTestId('psg-master-3-any')).toBeInTheDocument();
    });

    it('keeps an already-selected but disabled master group visible, selected, and round-tripped on save', async () => {
      const user = userEvent.setup();
      seedGroupsWithDisabled();
      // The rule's master group (1) is itself disabled.
      stubGroupSettingsFull({
        1: { enabled: false, auto_channel_sync: true },
        2: { enabled: true, auto_channel_sync: false },
        3: { enabled: false, auto_channel_sync: false },
      });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      // The migrated whole-group master scope stays visible (round-trip guard)
      // with a disabled hint, and checked.
      const masterRow = await screen.findByTestId('psg-master-1-any');
      expect(masterRow).toBeChecked();
      expect(masterRow.closest('label')).toHaveTextContent('(disabled)');

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config.master).toEqual({
        group_id: 1,
        m3u_account_id: null,
      });
    });

    it('keeps an already-checked but disabled secondary group visible, checked, and round-tripped on save', async () => {
      const user = userEvent.setup();
      seedGroupsWithDisabled();
      stubThreeGroups();
      const rule: Partial<ChannelPipelineRule> = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          secondary_group_ids: [2, 3],
        },
      };
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      const disabledRow = await screen.findByTestId('psg-secondary-3-any');
      expect(disabledRow).toBeChecked();
      expect(disabledRow.closest('label')).toHaveTextContent('(disabled)');

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config.secondary).toEqual([
        { group_id: 2, m3u_account_id: null },
        { group_id: 3, m3u_account_id: null },
      ]);
    });

    it('round-trips a selected group with no provider junction (shown as a chip, kept on save)', async () => {
      const user = userEvent.setup();
      seedGroupsWithDisabled();
      // Group 3 has NO junction row at all, so it is not a selectable picker
      // option — but the secondary selection chip still surfaces it and the
      // scope survives on save (no data loss).
      stubGroupSettingsFull({
        1: { enabled: true, auto_channel_sync: true },
        2: { enabled: true, auto_channel_sync: false },
      });
      const rule: Partial<ChannelPipelineRule> = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          secondary_group_ids: [2, 3],
        },
      };
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      const secondary = await screen.findByTestId('psg-secondary');
      expect(within(secondary).getByText(/Group 3 · Any provider/)).toBeInTheDocument();

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config.secondary).toEqual([
        { group_id: 2, m3u_account_id: null },
        { group_id: 3, m3u_account_id: null },
      ]);
    });

    it('composes the enabled filter with the picker search on the secondary list', async () => {
      const user = userEvent.setup();
      seedGroupsWithDisabled();
      stubThreeGroups();
      render(<EventSyncRuleEditor onSave={vi.fn()} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('checkbox', { name: /show all groups/i }));
      await screen.findByTestId('psg-secondary-3-any');

      await user.type(screen.getByLabelText('Filter secondary groups'), 'Secondary');

      expect(screen.getByTestId('psg-secondary-2-any')).toBeInTheDocument();
      expect(screen.queryByTestId('psg-secondary-3-any')).toBeNull();
      expect(screen.queryByTestId('psg-secondary-1-any')).toBeNull();
    });
  });

  describe('UX redesign (bead dvzrf): 3-phase spine, intent, subgroups', () => {
    it('renders the three phase headings and the Advanced subgroups by purpose', async () => {
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      // Phase spine headings.
      expect(await screen.findByText('Scope', { selector: 'h2' })).toBeInTheDocument();
      expect(screen.getByText('Matching', { selector: 'h2' })).toBeInTheDocument();
      expect(screen.getByText('Behavior', { selector: 'h2' })).toBeInTheDocument();

      // The flat Advanced wall is gone; the flags are grouped into labeled
      // subgroups by purpose.
      expect(screen.queryByText('Advanced', { selector: 'summary' })).toBeNull();
      for (const label of [
        'Time tuning',
        'Score threshold',
        'Date handling',
        'Per-group pattern overrides',
        'Automation',
        'Scope extension',
        'Guide data',
      ]) {
        expect(screen.getByText(label, { selector: 'summary' })).toBeInTheDocument();
      }
    });

    it('shows a plain-language rule-intent sentence derived from the current config', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      const intent = await screen.findByTestId('event-sync-intent');
      await waitFor(() => {
        expect(intent).toHaveTextContent(/Attach streams from 1 secondary group to master Master Events/i);
      });
      // Default (enforced time window, manual runs, default threshold).
      expect(intent).toHaveTextContent(/Attach streams from 1 secondary group to master Master Events/i);
      expect(intent).toHaveTextContent(/title \+ start time within ±30 min/i);
      expect(intent).toHaveTextContent(/Runs only when you run it manually\./i);

      // Turning on Ignore-time-window flips the matching clause to the risky
      // (bold) phrasing, and auto-run flips the run clause.
      await user.click(screen.getByText('Time tuning'));
      await user.click(screen.getByTestId('event-sync-ignore-time-window'));
      expect(intent).toHaveTextContent(/title only \(time ignored\)/i);

      await user.click(screen.getByText('Automation'));
      await user.click(screen.getByTestId('event-sync-auto-run'));
      expect(intent).toHaveTextContent(/Runs automatically after each M3U refresh\./i);
    });

    it('keeps the rule intent neutral while channel groups are pending', async () => {
      let resolveGroupsResponse: ((response: Response) => void) | undefined;
      const groupsResponse = new Promise<Response>((resolve) => {
        resolveGroupsResponse = resolve;
      });
      server.use(
        http.get('/api/channel-groups', async () => groupsResponse),
      );
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      const intent = await screen.findByTestId('event-sync-intent');
      expect(intent).toHaveTextContent('Loading channel groups…');
      expect(intent).not.toHaveTextContent(/master Group 1/i);
      expect(intent).toHaveAttribute('aria-busy', 'true');

      await act(async () => {
        resolveGroupsResponse!(HttpResponse.json([
          createMockChannelGroup({ id: 1, name: 'Master Events' }),
          createMockChannelGroup({ id: 2, name: 'Secondary Events' }),
        ]));
      });

      await waitFor(() => {
        expect(intent).toHaveTextContent(/Attach streams from 1 secondary group to master Master Events/i);
      });
      expect(intent).toHaveAttribute('aria-busy', 'false');
    });

    it('resolves group names before a slower junction request completes', async () => {
      let resolveJunctionResponse: ((response: Response) => void) | undefined;
      const junctionResponse = new Promise<Response>((resolve) => {
        resolveJunctionResponse = resolve;
      });
      seedGroups();
      server.use(
        http.get('/api/providers/group-settings/by-provider', async () => junctionResponse),
      );
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      try {
        const intent = await screen.findByTestId('event-sync-intent');
        await waitFor(() => {
          expect(intent).toHaveTextContent(/Attach streams from 1 secondary group to master Master Events/i);
        });
        expect(intent).toHaveAttribute('aria-busy', 'false');
      } finally {
        await act(async () => {
          resolveJunctionResponse!(HttpResponse.json([]));
        });
      }
    });

    it('uses a numeric group identity only after a successful group list omits it', async () => {
      server.use(
        http.get('/api/channel-groups', () => HttpResponse.json([])),
      );
      stubJunctions([]);
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      const intent = await screen.findByTestId('event-sync-intent');
      await waitFor(() => {
        expect(intent).toHaveTextContent(/Attach streams from 1 secondary group to master Group 1/i);
      });
      expect(intent).not.toHaveTextContent(/could not be loaded/i);
    });

    it('shows an unavailable state when the channel-group request fails', async () => {
      server.use(
        http.get('/api/channel-groups', () => HttpResponse.error()),
      );
      stubJunctions([]);
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      const intent = await screen.findByTestId('event-sync-intent');
      await waitFor(() => {
        expect(intent).toHaveTextContent(
          'Channel groups could not be loaded. Close and reopen this editor to try again.',
        );
      });
      expect(intent).toHaveAttribute('aria-busy', 'false');
      expect(screen.getAllByText('Group name unavailable').length).toBeGreaterThan(0);
      expect(intent).not.toHaveTextContent(/master Group 1/i);
    });

    it('keeps the resolved group name when the junction request fails', async () => {
      seedGroups();
      server.use(
        http.get('/api/providers/group-settings/by-provider', () => HttpResponse.error()),
      );
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      const intent = await screen.findByTestId('event-sync-intent');
      await waitFor(() => {
        expect(intent).toHaveTextContent(/Attach streams from 1 secondary group to master Master Events/i);
      });
      expect(intent).not.toHaveTextContent(/could not be loaded/i);
    });

    it('starts a fresh channel-group state when the editor reopens after failure', async () => {
      server.use(
        http.get('/api/channel-groups', () => HttpResponse.error()),
      );
      stubJunctions([]);
      const firstEditor = render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />,
      );

      await waitFor(() => {
        expect(screen.getByTestId('event-sync-intent')).toHaveTextContent(/could not be loaded/i);
      });
      firstEditor.unmount();

      server.use(
        http.get('/api/channel-groups', () => HttpResponse.json([
          createMockChannelGroup({ id: 1, name: 'Master Events' }),
          createMockChannelGroup({ id: 2, name: 'Secondary Events' }),
        ])),
      );
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      const intent = await screen.findByTestId('event-sync-intent');
      await waitFor(() => {
        expect(intent).toHaveTextContent(/Attach streams from 1 secondary group to master Master Events/i);
      });
      expect(intent).not.toHaveTextContent(/could not be loaded/i);
    });

    it('ignores a pending request released after unmount when a fresh editor is ready', async () => {
      let resolveGroupsResponse: ((response: Response) => void) | undefined;
      const groupsResponse = new Promise<Response>((resolve) => {
        resolveGroupsResponse = resolve;
      });
      server.use(
        http.get('/api/channel-groups', async () => groupsResponse),
      );
      stubJunctions([]);
      const firstEditor = render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />,
      );

      expect(await screen.findByTestId('event-sync-intent')).toHaveTextContent('Loading channel groups…');
      firstEditor.unmount();

      server.use(
        http.get('/api/channel-groups', () => HttpResponse.json([
          createMockChannelGroup({ id: 1, name: 'Fresh Master Events' }),
          createMockChannelGroup({ id: 2, name: 'Fresh Secondary Events' }),
        ])),
      );
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      const intent = await screen.findByTestId('event-sync-intent');
      await waitFor(() => {
        expect(intent).toHaveTextContent(/Attach streams from 1 secondary group to master Fresh Master Events/i);
      });
      await act(async () => {
        resolveGroupsResponse!(HttpResponse.json([
          createMockChannelGroup({ id: 1, name: 'Old Master Events' }),
          createMockChannelGroup({ id: 2, name: 'Old Secondary Events' }),
        ]));
      });
      expect(intent).toHaveTextContent('Fresh Master Events');
      expect(intent).not.toHaveTextContent('Old Master Events');
    });

    it('badges a subgroup with the count of non-default flags it holds', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      // Automation starts at its default — no badge.
      const automation = screen.getByText('Automation', { selector: 'summary' });
      expect(within(automation).queryByText(/changed/i)).toBeNull();

      await user.click(automation);
      await user.click(screen.getByTestId('event-sync-auto-run'));
      expect(within(automation).getByText('1 changed')).toBeInTheDocument();
    });
  });

  describe('Test Patterns collapse (bead dvzrf / F4)', () => {
    /** The batch preview endpoint the Test Patterns panel calls; return one
     * non-matching row so the run registers a parse failure. */
    function stubBatchParseFailure() {
      server.use(
        http.post('/api/dummy-epg/preview/batch', () =>
          HttpResponse.json([{ matched: false, groups: {}, event_sync_start_valid: false }])
        )
      );
    }

    it('is collapsed by default so it does not compete with the Preview rail', async () => {
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      const details = await screen.findByTestId('event-sync-test-patterns-details');
      expect(details).not.toHaveAttribute('open');
    });

    it('auto-expands when a test run turns up parse failures', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      stubBatchParseFailure();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);

      const details = await screen.findByTestId('event-sync-test-patterns-details');
      expect(details).not.toHaveAttribute('open');

      // The Test Patterns panel lives on the Matching step.
      await goToStep(user, 2);
      await user.type(screen.getByLabelText('Sample stream names'), 'Totally Unparseable Name');
      await user.click(screen.getByRole('button', { name: /test patterns/i }));

      await waitFor(() => expect(details).toHaveAttribute('open'));
    });
  });

  describe('dirty-state discard guard (bead dvzrf / S4a)', () => {
    it('closes immediately via Cancel when nothing changed (no confirm)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onCancel = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={onCancel} />);
      await screen.findByTestId('psg-master');

      await user.click(screen.getByRole('button', { name: 'Cancel' }));

      expect(onCancel).toHaveBeenCalledTimes(1);
      expect(screen.queryByTestId('event-sync-discard-dialog')).toBeNull();
    });

    it('confirms before discarding via Cancel when the form is dirty', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onCancel = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={onCancel} />);
      await screen.findByTestId('psg-master');

      // Any divergence from the loaded rule marks it dirty.
      await user.type(screen.getByLabelText(/rule name/i), '!');
      await user.click(screen.getByRole('button', { name: 'Cancel' }));

      expect(screen.getByTestId('event-sync-discard-dialog')).toBeInTheDocument();
      expect(screen.getByRole('alertdialog', { name: 'Discard this rule?' })).toBeInTheDocument();
      expect(onCancel).not.toHaveBeenCalled();

      // "Keep editing" dismisses the prompt without discarding.
      await user.click(screen.getByTestId('event-sync-discard-keep'));
      expect(screen.queryByTestId('event-sync-discard-dialog')).toBeNull();
      expect(onCancel).not.toHaveBeenCalled();
    });

    it('discards only after the confirm button is pressed', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onCancel = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={onCancel} />);
      await screen.findByTestId('psg-master');

      await user.type(screen.getByLabelText(/rule name/i), '!');
      await user.click(screen.getByRole('button', { name: 'Cancel' }));
      await user.click(screen.getByTestId('event-sync-discard-confirm'));

      expect(onCancel).toHaveBeenCalledTimes(1);
    });

    it('routes the parent Escape/× dismissors through the same guard (onRegisterClose)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onCancel = vi.fn();
      let registeredClose: (() => void) | undefined;
      render(
        <EventSyncRuleEditor
          rule={EXISTING_RULE}
          onSave={vi.fn()}
          onCancel={onCancel}
          onRegisterClose={fn => {
            if (fn) registeredClose = fn;
          }}
        />
      );
      await screen.findByTestId('psg-master');
      expect(typeof registeredClose).toBe('function');

      // Clean → the registered close (what Escape/× invoke) closes at once.
      await act(async () => registeredClose!());
      expect(onCancel).toHaveBeenCalledTimes(1);

      // Dirty → the registered close shows the confirm instead of discarding.
      await user.type(screen.getByLabelText(/rule name/i), '!');
      await act(async () => registeredClose!());
      expect(screen.getByTestId('event-sync-discard-dialog')).toBeInTheDocument();
      expect(onCancel).toHaveBeenCalledTimes(1);
    });
  });

  describe('secondary-empty inline anchor (bead dvzrf / S4b)', () => {
    it('expands the Scope-extension subgroup and focuses the master-self-attach checkbox', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);
      await screen.findByTestId('psg-secondary');

      const checkbox = screen.getByTestId('event-sync-include-master-group-streams');
      expect(checkbox).not.toHaveFocus();

      await user.click(screen.getByTestId('event-sync-use-master-streams'));

      await waitFor(() => expect(checkbox).toHaveFocus());
      expect(checkbox.closest('details')).toHaveAttribute('open');
    });
  });

  describe('4-step wizard (bead m1s38.1)', () => {
    const PREVIEW_FIXTURE: EventSyncPreviewResponse = {
      preflight: { ok: true, failures: [] },
      summary: {
        secondary_streams: 5,
        would_attach: 3,
        ambiguous_skipped: 1,
        unmatched: 1,
        parse_failed: 0,
        master_channels: 4,
        master_channels_unparsed: 0,
        would_attach_via_review: 0,
        candidates_pending_review: 0,
      },
      streams: [
        {
          stream_id: 1,
          stream_name: 'X vs Y @ 11 Jul 06:00 PM ET',
          group_id: 2,
          provider: 'Provider B',
          parsed_title: 'X vs Y',
          parsed_start: '2026-07-11T18:00:00-04:00',
          matched_pattern: 'slot-title-day-first-date',
          disposition: 'would_attach',
          unmatchable_reason: null,
          attach_source: 'threshold',
          would_attach_master: { channel_id: 7, name: 'Master X vs Y' },
          candidates: [],
        },
      ],
      unmatched_streams: [],
      parse_failures: [],
      unparsed_master_channels: [],
      truncated: false,
    };

    function stubPreview() {
      server.use(
        http.post('/api/channel-pipeline/event-sync-preview', () =>
          HttpResponse.json(PREVIEW_FIXTURE)
        )
      );
    }

    it.each([1, 101])('previews and saves the same confirmed idle settings without changing zero or false', async (count) => {
      const user = userEvent.setup();
      seedGroups();
      mockDataStore.channelGroups.push(
        createMockChannelGroup({ id: 40, name: 'Promoted Events' })
      );
      stubGroupSettings({ 1: true, 2: false });
      const previewRequest = vi.fn();
      server.use(
        http.post('/api/channel-pipeline/event-sync-preview', async ({ request }) => {
          previewRequest(await request.json());
          return HttpResponse.json({
            ...PREVIEW_FIXTURE,
            summary: { ...PREVIEW_FIXTURE.summary, secondary_streams: 3, unmatched: 2 },
            unmatched_streams: [
              { group_id: 2, stream_id: 101, stream_name: 'Old event', provider: 'Example',
                parsed: { title: 'Old event', start: null }, would_promote: false, promote_skipped_past: true },
              { group_id: 2, stream_id: 102, stream_name: 'Future event', provider: 'Example',
                parsed: { title: 'Future event', start: null }, would_promote: false, promote_skipped_early: true },
            ],
            promotion: {
              retire_finished_events: true,
              enabled: true, target_group_id: 40, would_promote: 0,
              would_promote_streams: 0, would_create: 0, would_attach_existing: 0,
              cap: 25, capped: false, cap_overage: 0, skipped_past: 1, skipped_early: 1,
              skipped_past_adopted: 0, units: [],
              event_states: Array.from({ length: count }, (_, index) => ({ channel_id: 900 + index, status: 'idle' })),
              retirements: Array.from({ length: count }, (_, index) => ({
                channel_id: 900 + index,
                action: index === 0 ? "Would delete orphaned channel 'Fury vs. Usyk'" : `Preserve channel ${900 + index}`,
              })),
            },
          });
        })
      );
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          dummy_epg_profile_id: 7,
          retire_finished_events: true,
          promote_lead_hours: 0,
          skip_past_events: false,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);
      await screen.findByTestId('psg-master');

      await user.click(screen.getByRole('button', { name: /preview matches/i }));
      await screen.findByTestId('event-sync-summary');
      await goToStep(user, 4);
      expect(await screen.findByText(/Would delete orphaned channel 'Fury vs. Usyk'/)).toBeInTheDocument();
      expect(screen.getByText('Skipped — start is before the current 24-hour event window')).toBeInTheDocument();
      expect(screen.getByText('Deferred until this event starts')).toBeInTheDocument();
      expect(screen.getByText(/This does not establish that a broadcast has ended/)).toBeInTheDocument();
      expect(screen.getByText(`Event states (${count})`)).toBeInTheDocument();
      expect(screen.getByText(`Retirement decisions (${count})`)).toBeInTheDocument();
      if (count > 100) {
        expect(screen.getByText('Showing 100 of 101 event states.')).toBeInTheDocument();
        expect(screen.getByText('Showing 100 of 101 retirement decisions.')).toBeInTheDocument();
        expect(screen.queryByText('Channel 1000: idle')).not.toBeInTheDocument();
        expect(screen.queryByText(/Preserve channel 1000/)).not.toBeInTheDocument();
      }
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(previewRequest.mock.calls[0][0].event_sync_config).toEqual(
        onSave.mock.calls[0][0].event_sync_config
      );
      expect(previewRequest.mock.calls[0][0].rule_id).toBe(rule.id);
      expect(onSave.mock.calls[0][0].event_sync_config).toMatchObject({
        retire_finished_events: true,
        promote_lead_hours: 0,
        skip_past_events: false,
      });
    });

    it('step pills switch the visible left-column section and mark the active pill', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);
      await screen.findByTestId('psg-master');

      // Step 1 active: Scope visible, Matching hidden.
      expect(screen.getByTestId('event-sync-step-1')).toHaveAttribute('aria-current', 'step');
      expect(screen.getByText('Scope', { selector: 'h2' })).toBeVisible();
      expect(screen.getByText('Matching', { selector: 'h2' })).not.toBeVisible();

      await goToStep(user, 2);

      expect(screen.getByTestId('event-sync-step-2')).toHaveAttribute('aria-current', 'step');
      expect(screen.getByTestId('event-sync-step-1')).not.toHaveAttribute('aria-current');
      expect(screen.getByText('Matching', { selector: 'h2' })).toBeVisible();
      expect(screen.getByText('Scope', { selector: 'h2' })).not.toBeVisible();
    });

    it('reactively updates the rail impact block per step', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);
      await screen.findByTestId('psg-master');

      const impact = screen.getByTestId('event-sync-impact');
      expect(within(impact).getByText('Scope impact')).toBeInTheDocument();

      await goToStep(user, 2);
      expect(within(impact).getByText('Matching impact')).toBeInTheDocument();

      await goToStep(user, 3);
      expect(within(impact).getByText('Behavior impact')).toBeInTheDocument();
    });

    it('renders ONE preview: compact on config steps, expanded on Review, stale on config change', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      stubPreview();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);
      await screen.findByTestId('psg-master');

      // Run the single preview from the rail while on a config step (compact):
      // the one-line summary shows, the detailed match-result list does not.
      await user.click(screen.getByRole('button', { name: /preview matches/i }));
      await screen.findByTestId('event-sync-summary');
      expect(screen.queryByRole('list', { name: /match results/i })).toBeNull();

      // The Review step expands the SAME preview instance → detail appears.
      await goToStep(user, 4);
      expect(screen.getByRole('list', { name: /match results/i })).toBeInTheDocument();
      expect(screen.queryByTestId('event-sync-preview-stale')).toBeNull();

      // Changing a config field marks the results stale (not cleared).
      await goToStep(user, 2);
      await user.click(screen.getByTestId('event-sync-ignore-time-window'));
      expect(screen.getByTestId('event-sync-preview-stale')).toBeInTheDocument();
      expect(screen.getByTestId('event-sync-summary')).toBeInTheDocument();
    });

    it('Back and Next never trigger the discard confirm when dirty; Cancel does', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);
      await screen.findByTestId('psg-master');

      // Make the form dirty.
      await user.type(screen.getByLabelText(/rule name/i), '!');

      // Next advances without a discard prompt.
      await user.click(screen.getByRole('button', { name: 'Next' }));
      expect(screen.queryByTestId('event-sync-discard-dialog')).toBeNull();
      expect(screen.getByTestId('event-sync-step-2')).toHaveAttribute('aria-current', 'step');

      // Back returns to the previous step (N-1), not nav history, no prompt.
      await user.click(screen.getByRole('button', { name: 'Back' }));
      expect(screen.queryByTestId('event-sync-discard-dialog')).toBeNull();
      expect(screen.getByTestId('event-sync-step-1')).toHaveAttribute('aria-current', 'step');

      // Cancel (dirty) still routes through the discard guard.
      await user.click(screen.getByRole('button', { name: 'Cancel' }));
      expect(screen.getByTestId('event-sync-discard-dialog')).toBeInTheDocument();
    });

    it('Save-from-Review routes to the offending field step and focuses it (name)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(
        <EventSyncRuleEditor
          rule={{ ...EXISTING_RULE, name: '' }}
          onSave={vi.fn()}
          onCancel={vi.fn()}
        />
      );
      await screen.findByTestId('psg-master');

      await goToStep(user, 4);
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() =>
        expect(screen.getByTestId('event-sync-step-1')).toHaveAttribute('aria-current', 'step')
      );
      expect(screen.getByLabelText(/rule name/i)).toHaveFocus();
      expect(screen.getByRole('alert')).toHaveTextContent('Name is required');
    });

    it('Save-from-Review routes to the Matching step when the pattern selection is empty', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);
      await screen.findByTestId('psg-master');

      // Deselect every built-in pattern on the Matching step (no custom either).
      await goToStep(user, 2);
      for (const box of screen.getAllByRole('checkbox').filter(cb => (cb as HTMLInputElement).checked)) {
        await user.click(box);
      }

      await goToStep(user, 4);
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() =>
        expect(screen.getByTestId('event-sync-step-2')).toHaveAttribute('aria-current', 'step')
      );
      expect(screen.getByText('Matching', { selector: 'h2' })).toHaveFocus();
      expect(screen.getByRole('alert')).toHaveTextContent(/parse pattern/i);
    });

    it('a NEW rule exposes Save only on the Review step (Next on config steps)', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor onSave={vi.fn()} onCancel={vi.fn()} />);
      await screen.findByTestId('psg-master');

      expect(screen.queryByRole('button', { name: 'Save' })).toBeNull();
      expect(screen.getByRole('button', { name: 'Next' })).toBeInTheDocument();

      await goToStep(user, 4);
      expect(screen.getByRole('button', { name: 'Save' })).toBeInTheDocument();
      expect(screen.queryByRole('button', { name: 'Next' })).toBeNull();
    });

    it('editing an existing rule exposes Save on every step', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);
      await screen.findByTestId('psg-master');

      expect(screen.getByRole('button', { name: 'Save' })).toBeInTheDocument();
      await goToStep(user, 2);
      expect(screen.getByRole('button', { name: 'Save' })).toBeInTheDocument();
      await goToStep(user, 4);
      expect(screen.getByRole('button', { name: 'Save' })).toBeInTheDocument();
    });

    it('consumes the shared .modal-* two-pane layout classes and drops the old event-sync layout variants', async () => {
      const { container } = render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />
      );
      await screen.findByTestId('psg-master');

      for (const cls of [
        '.modal-twopane',
        '.modal-main',
        '.modal-rail',
        '.modal-stepper',
        '.modal-stepper-item',
        '.modal-intent',
        '.modal-subgroup',
        '.modal-why',
      ]) {
        expect(container.querySelector(cls)).not.toBeNull();
      }

      // The migrated-away layout variants must not survive (grep guard).
      for (const retired of [
        '.event-sync-editor-grid',
        '.event-sync-main',
        '.event-sync-rail',
        '.event-sync-scrollspy',
        '.event-sync-scrollspy-item',
        '.event-sync-intent',
        '.event-sync-subgroup',
      ]) {
        expect(container.querySelector(retired)).toBeNull();
      }
    });
  });
  describe('unmatched-event promotion (bead ti939.4.1)', () => {
    function seedPromoGroup() {
      seedGroups();
      mockDataStore.channelGroups.push(
        createMockChannelGroup({ id: 40, name: 'Promoted Events' })
      );
    }

    it('defaults OFF and omits all promotion keys when never set', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      expect(screen.getByTestId('event-sync-promote-unmatched')).not.toBeChecked();
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const config = onSave.mock.calls[0][0].event_sync_config;
      expect(config).not.toHaveProperty('promote_unmatched');
      expect(config).not.toHaveProperty('promote_target_group_id');
      expect(config).not.toHaveProperty('max_promote_per_run');
      expect(config).not.toHaveProperty('retire_finished_events');
    });

    it('blocks save when enabled without a target group, with a teaching error', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByTestId('event-sync-promote-unmatched'));
      await user.click(screen.getByRole('button', { name: 'Save' }));

      expect(onSave).not.toHaveBeenCalled();
      expect(
        screen.getAllByText(/Pick a target group for promoted channels/i).length
      ).toBeGreaterThan(0);
    });

    it('emits the promotion keys when enabled with a target group, and shows the create-AND-delete warning', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />
      );

      await user.click(screen.getByTestId('event-sync-promote-unmatched'));
      // The honest ownership copy renders as soon as the toggle is on.
      expect(screen.getByTestId('event-sync-promote-warning').textContent)
        .toMatch(/ECM will CREATE and DELETE channels/i);

      // Pick the dedicated group through the CustomSelect (master and
      // secondary groups are filtered out of the option list).
      await waitFor(() =>
        expect(screen.getByText('Target group for promoted channels')).toBeInTheDocument()
      );
      const promoteGroup = screen
        .getByText('Target group for promoted channels')
        .closest('.form-group')!;
      await user.click(promoteGroup.querySelector('.custom-select-trigger')!);
      const option = await screen.findByText('Promoted Events');
      const menu = option.closest('.custom-select-menu') as HTMLElement;
      // Ownership rails in the option list: master (1) and secondary (2)
      // groups are filtered out — only dedicated groups are offered.
      expect(within(menu).queryByText('Master Events')).toBeNull();
      expect(within(menu).queryByText('Secondary Events')).toBeNull();
      await user.click(option);

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const config = onSave.mock.calls[0][0].event_sync_config;
      expect(config.promote_unmatched).toBe(true);
      expect(config.promote_target_group_id).toBe(40);
      // No editor control for the cap — the key stays absent so the
      // backend default applies.
      expect(config).not.toHaveProperty('max_promote_per_run');
      // The past-event filter is its own opt-in: turning promotion on does
      // not turn it on, so the keys stay absent.
      expect(config).not.toHaveProperty('skip_past_events');
      expect(config).not.toHaveProperty('past_event_grace_hours');
    });

    it('says on the past-event toggle that channels are removed too', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />
      );

      await user.click(screen.getByTestId('event-sync-promote-unmatched'));
      const toggle = screen.getByTestId('event-sync-skip-past-events');
      const toggleGroup = toggle.closest('.form-group')!;
      // The label alone has to carry the destructive half, because that is
      // the only text an operator reads before ticking the box.
      expect(toggleGroup.textContent).toMatch(/remove their channels/i);
      expect(toggleGroup.textContent).toMatch(/orphan cleanup/i);
      expect(toggleGroup.textContent).not.toMatch(/never deleted/i);

      await user.click(toggle);
      const grace = await screen.findByTestId('event-sync-past-event-grace');
      expect(grace.closest('.form-group')!.textContent).toMatch(
        /its channel removed/i
      );
    });

    it('emits the past-event filter with its grace once the box is ticked', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />
      );

      await user.click(screen.getByTestId('event-sync-promote-unmatched'));
      // The grace box only appears once the filter itself is on.
      expect(screen.queryByTestId('event-sync-past-event-grace')).toBeNull();
      await user.click(screen.getByTestId('event-sync-skip-past-events'));
      const grace = await screen.findByTestId('event-sync-past-event-grace');
      await user.clear(grace);
      await user.type(grace, '6');

      await waitFor(() =>
        expect(screen.getByText('Target group for promoted channels')).toBeInTheDocument()
      );
      const promoteGroup = screen
        .getByText('Target group for promoted channels')
        .closest('.form-group')!;
      await user.click(promoteGroup.querySelector('.custom-select-trigger')!);
      await user.click(await screen.findByText('Promoted Events'));

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const config = onSave.mock.calls[0][0].event_sync_config;
      expect(config.skip_past_events).toBe(true);
      expect(config.past_event_grace_hours).toBe(6);
    });

    it('leaves promote_lead_hours absent until the operator asks for a limit', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />
      );

      await user.click(screen.getByTestId('event-sync-promote-unmatched'));
      // Turning promotion on does not turn the lead window on, and the hours
      // box only appears once the box is ticked.
      expect(screen.getByTestId('event-sync-limit-promote-lead')).not.toBeChecked();
      expect(screen.queryByTestId('event-sync-promote-lead-hours')).toBeNull();

      await waitFor(() =>
        expect(screen.getByText('Target group for promoted channels')).toBeInTheDocument()
      );
      const promoteGroup = screen
        .getByText('Target group for promoted channels')
        .closest('.form-group')!;
      await user.click(promoteGroup.querySelector('.custom-select-trigger')!);
      await user.click(await screen.findByText('Promoted Events'));

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const config = onSave.mock.calls[0][0].event_sync_config;
      expect(config).not.toHaveProperty('promote_lead_hours');
    });

    it('leaves promote_channel_number absent until the operator asks for a range', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />
      );

      await user.click(screen.getByTestId('event-sync-promote-unmatched'));
      // Absent means 'auto', which is what every rule saved before this
      // field existed already does — turning promotion on must not change
      // their numbering.
      expect(screen.getByTestId('event-sync-number-promoted')).not.toBeChecked();
      expect(screen.queryByTestId('event-sync-promote-channel-number')).toBeNull();

      await waitFor(() =>
        expect(screen.getByText('Target group for promoted channels')).toBeInTheDocument()
      );
      const promoteGroup = screen
        .getByText('Target group for promoted channels')
        .closest('.form-group')!;
      await user.click(promoteGroup.querySelector('.custom-select-trigger')!);
      await user.click(await screen.findByText('Promoted Events'));

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const config = onSave.mock.calls[0][0].event_sync_config;
      expect(config).not.toHaveProperty('promote_channel_number');
    });

    it('emits the promoted channel range once the box is ticked', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />
      );

      await user.click(screen.getByTestId('event-sync-promote-unmatched'));
      await user.click(screen.getByTestId('event-sync-number-promoted'));
      const box = await screen.findByTestId('event-sync-promote-channel-number');
      expect(box).toHaveValue('900-999');
      await user.clear(box);
      await user.type(box, '500-599');

      await waitFor(() =>
        expect(screen.getByText('Target group for promoted channels')).toBeInTheDocument()
      );
      const promoteGroup = screen
        .getByText('Target group for promoted channels')
        .closest('.form-group')!;
      await user.click(promoteGroup.querySelector('.custom-select-trigger')!);
      await user.click(await screen.findByText('Promoted Events'));

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.promote_channel_number
      ).toBe('500-599');
    });

    it('falls back to the default when the range is not a shape the backend takes', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />
      );

      await user.click(screen.getByTestId('event-sync-promote-unmatched'));
      await user.click(screen.getByTestId('event-sync-number-promoted'));
      const box = await screen.findByTestId('event-sync-promote-channel-number');
      await user.clear(box);
      await user.type(box, 'bottom please');

      await waitFor(() =>
        expect(screen.getByText('Target group for promoted channels')).toBeInTheDocument()
      );
      const promoteGroup = screen
        .getByText('Target group for promoted channels')
        .closest('.form-group')!;
      await user.click(promoteGroup.querySelector('.custom-select-trigger')!);
      await user.click(await screen.findByText('Promoted Events'));

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      // Saving the typed text would fail backend validation and lose the
      // whole edit, so the box degrades to the offered default.
      expect(
        onSave.mock.calls[0][0].event_sync_config.promote_channel_number
      ).toBe('900-999');
    });

    it('emits the lead hours once the box is ticked', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={onSave} onCancel={vi.fn()} />
      );

      await user.click(screen.getByTestId('event-sync-promote-unmatched'));
      await user.click(screen.getByTestId('event-sync-limit-promote-lead'));
      const lead = await screen.findByTestId('event-sync-promote-lead-hours');
      // The default the editor offers is the one an operator sees first.
      expect(lead).toHaveValue(24);
      await user.clear(lead);
      await user.type(lead, '48');

      await waitFor(() =>
        expect(screen.getByText('Target group for promoted channels')).toBeInTheDocument()
      );
      const promoteGroup = screen
        .getByText('Target group for promoted channels')
        .closest('.form-group')!;
      await user.click(promoteGroup.querySelector('.custom-select-trigger')!);
      await user.click(await screen.findByText('Promoted Events'));

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.promote_lead_hours
      ).toBe(48);
    });

    it('clamps the lead hours to the schema ceiling', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          promote_lead_hours: 24,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      const lead = await screen.findByTestId('event-sync-promote-lead-hours');
      await user.clear(lead);
      await user.type(lead, '9999');
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.promote_lead_hours
      ).toBe(720);
    });

    it('falls back to the default when the lead box is left empty', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          promote_lead_hours: 6,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      const lead = await screen.findByTestId('event-sync-promote-lead-hours');
      await user.clear(lead);
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      // An empty box means the operator has not chosen, not "zero hours".
      // Zero would block every promotion.
      expect(
        onSave.mock.calls[0][0].event_sync_config.promote_lead_hours
      ).toBe(24);
    });

    it('starts checked from a stored lead window and round-trips it', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          promote_lead_hours: 12,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      expect(screen.getByTestId('event-sync-limit-promote-lead')).toBeChecked();
      expect(
        await screen.findByTestId('event-sync-promote-lead-hours')
      ).toHaveValue(12);

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.promote_lead_hours
      ).toBe(12);
    });

    it('unticking the lead window drops the key so no limit is left behind', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          promote_lead_hours: 12,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByTestId('event-sync-limit-promote-lead'));
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const config = onSave.mock.calls[0][0].event_sync_config;
      // An absent key is the only way to say "no limit", so the stored value
      // must not be written back the way an explicit false would be.
      expect(config).not.toHaveProperty('promote_lead_hours');
      expect(config.promote_unmatched).toBe(true);
    });

    it('says on the lead window which channels it gates', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />
      );

      await user.click(screen.getByTestId('event-sync-promote-unmatched'));
      const toggle = screen.getByTestId('event-sync-limit-promote-lead');
      const toggleGroup = toggle.closest('.form-group')!;
      expect(toggleGroup.textContent).toMatch(/only gates new channels/i);

      await user.click(toggle);
      const lead = await screen.findByTestId('event-sync-promote-lead-hours');
      // The hours hint has to say the held-back events come back, or the
      // operator reads the window as a filter that loses them.
      expect(lead.closest('.form-group')!.textContent).toMatch(
        /picked up on a later run/i
      );
    });

    it('hides the existing-channel box until there is a lead window to extend', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      render(
        <EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />
      );

      await user.click(screen.getByTestId('event-sync-promote-unmatched'));
      expect(
        screen.queryByTestId('event-sync-apply-lead-to-existing')
      ).toBeNull();

      await user.click(screen.getByTestId('event-sync-limit-promote-lead'));
      expect(
        screen.getByTestId('event-sync-apply-lead-to-existing')
      ).not.toBeChecked();
    });

    it('extends the lead window to existing channels once the box is ticked', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          promote_lead_hours: 12,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(await screen.findByTestId('event-sync-apply-lead-to-existing'));
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.apply_lead_to_existing
      ).toBe(true);
    });

    it('leaves the key out of a config that never carried it', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          promote_lead_hours: 12,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await screen.findByTestId('event-sync-apply-lead-to-existing');
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config
      ).not.toHaveProperty('apply_lead_to_existing');
    });

    it('starts ticked from a stored true and writes false back when unticked', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          promote_lead_hours: 12,
          apply_lead_to_existing: true,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      const box = await screen.findByTestId('event-sync-apply-lead-to-existing');
      expect(box).toBeChecked();

      await user.click(box);
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      // Unlike the lead window itself, false is expressible here, so the
      // written false is what turns a stored true back off.
      expect(
        onSave.mock.calls[0][0].event_sync_config.apply_lead_to_existing
      ).toBe(false);
    });

    it('marks the form dirty when the existing-channel box changes', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onCancel = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          promote_lead_hours: 12,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={vi.fn()} onCancel={onCancel} />);

      await user.click(await screen.findByTestId('event-sync-apply-lead-to-existing'));
      await user.click(screen.getByRole('button', { name: 'Cancel' }));

      expect(screen.getByTestId('event-sync-discard-dialog')).toBeInTheDocument();
      expect(onCancel).not.toHaveBeenCalled();
    });

    it('emits the stream health check once the box is ticked, and says what it costs', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      const toggle = await screen.findByTestId('event-sync-skip-dead-streams');
      expect(toggle).not.toBeChecked();
      // Both consequences have to be readable before the box is ticked: the
      // run gets slower, and some events stop being promoted.
      const toggleGroup = toggle.closest('.form-group')!;
      expect(toggleGroup.textContent).toMatch(/adds time to every run/i);
      expect(toggleGroup.textContent).toMatch(/streams all fail is not promoted/i);

      await user.click(toggle);
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(
        onSave.mock.calls[0][0].event_sync_config.skip_dead_streams
      ).toBe(true);
    });

    it('unticking the stream health check writes an explicit false', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          skip_dead_streams: true,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      const toggle = await screen.findByTestId('event-sync-skip-dead-streams');
      expect(toggle).toBeChecked();
      await user.click(toggle);
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      // Unlike promote_lead_hours, false is expressible here, so the written
      // false is what turns the stored true off. Dropping the key would leave
      // the check running.
      expect(
        onSave.mock.calls[0][0].event_sync_config.skip_dead_streams
      ).toBe(false);
    });

    it('leaves the stream health key absent on a config that never had it', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      expect(
        await screen.findByTestId('event-sync-skip-dead-streams')
      ).not.toBeChecked();
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const config = onSave.mock.calls[0][0].event_sync_config;
      // Opening and saving a rule authored before this control existed must
      // not start writing the key.
      expect(config).not.toHaveProperty('skip_dead_streams');
      expect(config.promote_unmatched).toBe(true);
    });

    it.each([true, false])('preserves confirmed idle retirement %s on save', async retireFinishedEvents => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          dummy_epg_profile_id: 7,
          retire_finished_events: retireFinishedEvents,
          promote_lead_hours: 0,
          skip_past_events: false,
          past_event_grace_hours: 0,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config).toMatchObject({
        retire_finished_events: retireFinishedEvents,
        promote_lead_hours: 0,
        skip_past_events: false,
        past_event_grace_hours: 0,
      });
    });

    it('preserves a zero-hour promotion lead on save', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          promote_lead_hours: 0,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config.promote_lead_hours).toBe(0);
    });

    it.each([true, false])('marks a retirement toggle from %s dirty and saves the new value', async retireFinishedEvents => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const onCancel = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          dummy_epg_profile_id: 7,
          retire_finished_events: retireFinishedEvents,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={onCancel} />);

      const toggle = screen.getByLabelText('Remove confirmed idle event channels');
      expect(toggle).toHaveProperty('checked', retireFinishedEvents);
      await user.click(toggle);
      await user.click(screen.getByRole('button', { name: 'Cancel' }));
      expect(screen.getByTestId('event-sync-discard-dialog')).toBeInTheDocument();
      expect(onCancel).not.toHaveBeenCalled();
      await user.click(screen.getByRole('button', { name: 'Keep editing' }));
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config.retire_finished_events).toBe(!retireFinishedEvents);
    });

    it.each([
      { promote_unmatched: true, dummy_epg_profile_id: undefined },
      { promote_unmatched: false, dummy_epg_profile_id: 7 },
    ])('requires promotion and a guide for retirement but allows turning it off: %s', async prerequisites => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_target_group_id: 40,
          retire_finished_events: true,
          ...prerequisites,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByRole('button', { name: 'Save' }));
      expect(onSave).not.toHaveBeenCalled();
      expect(screen.getAllByText(/Confirmed idle event removal requires promotion and a dummy EPG profile/).length).toBeGreaterThan(0);
      await user.click(screen.getByLabelText('Remove confirmed idle event channels'));
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0].event_sync_config.retire_finished_events).toBe(false);
    });

    it('keeps untouched confirmed idle settings pristine when closing', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onCancel = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          dummy_epg_profile_id: 7,
          retire_finished_events: true,
          promote_lead_hours: 0,
          skip_past_events: false,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={vi.fn()} onCancel={onCancel} />);

      await user.click(screen.getByRole('button', { name: 'Cancel' }));
      expect(onCancel).toHaveBeenCalledTimes(1);
      expect(screen.queryByTestId('event-sync-discard-dialog')).not.toBeInTheDocument();
    });

    it('round-trips a stored promotion config and preserves an API-set cap', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          max_promote_per_run: 10,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      expect(screen.getByTestId('event-sync-promote-unmatched')).toBeChecked();
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const config = onSave.mock.calls[0][0].event_sync_config;
      expect(config.promote_unmatched).toBe(true);
      expect(config.promote_target_group_id).toBe(40);
      expect(config.max_promote_per_run).toBe(10);
    });

    it('turning a stored promotion OFF keeps an explicit false and the group choice', async () => {
      const user = userEvent.setup();
      seedPromoGroup();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        event_sync_config: {
          ...EXISTING_RULE.event_sync_config!,
          promote_unmatched: true,
          promote_target_group_id: 40,
          max_promote_per_run: 10,
        },
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      await user.click(screen.getByTestId('event-sync-promote-unmatched'));
      await user.click(screen.getByRole('button', { name: 'Save' }));
      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      const config = onSave.mock.calls[0][0].event_sync_config;
      expect(config.promote_unmatched).toBe(false);
      // The group choice survives the off-toggle so re-enabling later does
      // not lose it (inert while off; backend validates shape only).
      expect(config.promote_target_group_id).toBe(40);
    });

    it('round-trips the rule active date window', async () => {
      const user = userEvent.setup();
      seedGroups();
      stubGroupSettings({ 1: true, 2: false });
      const onSave = vi.fn();
      const rule = {
        ...EXISTING_RULE,
        active_from: '2026-09-01',
        active_until: '2027-02-15',
      };
      render(<EventSyncRuleEditor rule={rule} onSave={onSave} onCancel={vi.fn()} />);

      expect(screen.getByText(/Dates are inclusive UTC calendar days/)).toBeInTheDocument();
      expect(screen.getByText(/does not undo prior changes/)).toBeInTheDocument();
      expect(screen.getByLabelText('Start date')).toHaveValue('2026-09-01');
      expect(screen.getByLabelText('End date')).toHaveValue('2027-02-15');
      await user.click(screen.getByRole('button', { name: 'Save' }));

      await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
      expect(onSave.mock.calls[0][0]).toEqual(expect.objectContaining({
        active_from: '2026-09-01',
        active_until: '2027-02-15',
      }));
    });
  });
});

const DEDICATED_PROFILE = {
  id: 7,
  name: 'Event Guide',
  enabled: false,
  channel_group_ids: [40],
  hide_empty_group_ids: [40],
  epg_source_ids: [],
  channel_mappings: [],
  event_sync_config: {
    secondary: [{ group_id: 2, m3u_account_id: 1 }],
    time_window_minutes: 30,
    enforce_time_window: true,
    attach_threshold: 0.8,
    assume_current_date: false,
    demote_stale_dateless: true,
    use_default_patterns: false,
    slot_patterns: [],
  },
};

const DEDICATED_RULE: Partial<ChannelPipelineRule> = {
  ...EXISTING_RULE,
  enabled: false,
  run_on_refresh: false,
  event_sync_config: {
    mode: 'dedicated',
    master: null,
    master_group_id: null,
    secondary: [{ group_id: 2, m3u_account_id: 1 }],
    time_window_minutes: 30,
    attach_threshold: 0.8,
    enabled: false,
    auto_run: false,
    include_master_group_streams: false,
    parse_master_from_stream: false,
    assume_current_date: false,
    promote_unmatched: true,
    promote_target_group_id: 40,
    dummy_epg_profile_id: 7,
    skip_dead_streams: true,
    skip_past_events: true,
    retire_finished_events: true,
  },
};

const DEDICATED_PREVIEW: EventSyncPreviewResponse = {
  preflight: { ok: true, failures: [] },
  summary: {
    secondary_streams: 1,
    would_attach: 0,
    ambiguous_skipped: 0,
    unmatched: 1,
    parse_failed: 0,
    master_channels: 0,
    master_channels_unparsed: 0,
    would_attach_via_review: 0,
    candidates_pending_review: 0,
  },
  streams: [],
  unmatched_streams: [],
  parse_failures: [],
  unparsed_master_channels: [],
  truncated: false,
};

function seedDedicated() {
  seedGroups();
  mockDataStore.channelGroups.push(createMockChannelGroup({ id: 40, name: 'Dedicated Events' }));
  stubGroupSettings({ 1: true, 2: false });
  server.use(http.get('/api/dummy-epg/profiles', () => HttpResponse.json([DEDICATED_PROFILE])));
}

async function fillDedicatedDraft(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText('Rule Name *'), 'Dated Events');
  await user.click(screen.getByRole('radio', { name: 'Dedicated event group' }));
  await user.click(await screen.findByTestId('psg-secondary-2-1'));
  await goToStep(user, 3);
  await user.click(screen.getByRole('button', { name: 'Target group for promoted channels' }));
  await user.click(await screen.findByRole('option', { name: 'Dedicated Events' }));
  await user.click(screen.getByRole('button', { name: 'Source-free dummy EPG profile (required)' }));
  await user.click(await screen.findByRole('option', { name: 'Event Guide (disabled)' }));
  await goToStep(user, 4);
}

describe('dedicated event group ownership', () => {
  it('selects the labeled ownership radio from the keyboard', async () => {
    const user = userEvent.setup();
    seedDedicated();
    render(<EventSyncRuleEditor onSave={vi.fn()} onCancel={vi.fn()} />);
    const group = screen.getByRole('radiogroup', { name: 'Channel ownership' });
    const radio = within(group).getByRole('radio', { name: 'Dedicated event group' });
    radio.focus();
    await user.keyboard(' ');
    expect(radio).toBeChecked();
    expect(screen.queryByTestId('psg-master')).not.toBeInTheDocument();
    expect(screen.getByText(/execution gates|stay disabled when you save/)).toBeInTheDocument();
  });

  it('defaults a new draft to legacy ownership and locks a saved legacy rule', async () => {
    seedDedicated();
    const { unmount } = render(<EventSyncRuleEditor onSave={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByRole('radio', { name: 'Existing master group' })).toBeChecked();
    expect(screen.getByRole('radio', { name: 'Dedicated event group' })).not.toBeChecked();
    await screen.findByTestId('psg-master-1-any');
    unmount();
    render(<EventSyncRuleEditor rule={EXISTING_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByRole('radio', { name: 'Existing master group' })).toBeChecked();
    expect(screen.getByRole('radio', { name: 'Existing master group' })).toBeDisabled();
    expect(screen.getByRole('radio', { name: 'Dedicated event group' })).toBeDisabled();
  });

  it('saves a new dedicated draft with null masters and all execution gates off', async () => {
    const user = userEvent.setup();
    seedDedicated();
    const onSave = vi.fn();
    let previewConfig: unknown;
    server.use(http.post('/api/channel-pipeline/event-sync-preview', async ({ request }) => {
      const body = await request.json() as { event_sync_config: unknown };
      previewConfig = body.event_sync_config;
      return HttpResponse.json(DEDICATED_PREVIEW);
    }));
    render(<EventSyncRuleEditor onSave={onSave} onCancel={vi.fn()} />);
    await user.click(await screen.findByTestId('psg-master-1-any'));
    await fillDedicatedDraft(user);
    await user.click(screen.getByRole('button', { name: /Preview matches/ }));
    await screen.findByTestId('event-sync-summary');
    await user.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    const saved = onSave.mock.calls[0][0];
    expect(saved).toEqual(expect.objectContaining({ enabled: false, run_on_refresh: false }));
    expect(saved.event_sync_config).toEqual(expect.objectContaining({
      mode: 'dedicated', master: null, master_group_id: null,
      secondary: [{ group_id: 2, m3u_account_id: 1 }],
      enabled: false, auto_run: false, include_master_group_streams: false,
      parse_master_from_stream: false, assume_current_date: false,
      promote_unmatched: true, promote_target_group_id: 40, dummy_epg_profile_id: 7,
      skip_dead_streams: true, skip_past_events: true, retire_finished_events: true,
    }));
    expect(previewConfig).toEqual(saved.event_sync_config);
    expect(screen.getByTestId('event-sync-review')).toHaveTextContent('Dedicated event group');
    expect(screen.getByTestId('event-sync-review')).not.toHaveTextContent('Master Events');
  });

  it.each([false, true])('preserves saved dedicated gates set to %s', async gate => {
    const user = userEvent.setup();
    seedDedicated();
    const onSave = vi.fn();
    render(<EventSyncRuleEditor rule={{
      ...DEDICATED_RULE, enabled: gate, run_on_refresh: gate,
      event_sync_config: { ...DEDICATED_RULE.event_sync_config!, enabled: gate, auto_run: gate },
    }} onSave={onSave} onCancel={vi.fn()} />);
    await screen.findByTestId('psg-secondary-2-1');
    expect(screen.getByRole('radio', { name: 'Dedicated event group' })).toBeChecked();
    expect(screen.getByRole('radio', { name: 'Dedicated event group' })).toBeDisabled();
    expect(screen.getByLabelText('Enabled')).not.toBeDisabled();
    await waitFor(() => expect(screen.getByRole('button', { name: /Preview matches/ })).toBeEnabled());
    await user.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    expect(onSave.mock.calls[0][0]).toEqual(expect.objectContaining({ enabled: gate, run_on_refresh: gate }));
    expect(onSave.mock.calls[0][0].event_sync_config).toEqual(expect.objectContaining({
      mode: 'dedicated', enabled: gate, auto_run: gate, master: null, master_group_id: null,
    }));
  });

  it('hides master controls, locks required safety values and offers no input Fix action', async () => {
    const user = userEvent.setup();
    seedDedicated();
    stubGroupSettings({ 1: true, 2: true });
    render(<EventSyncRuleEditor rule={DEDICATED_RULE} onSave={vi.fn()} onCancel={vi.fn()} />);
    await screen.findByTestId('psg-secondary-2-1');
    expect(screen.queryByTestId('psg-master')).not.toBeInTheDocument();
    expect(screen.queryByTestId('event-sync-use-master-streams')).not.toBeInTheDocument();
    expect(screen.getByText(/Resolve this setting in M3U Manager/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Fix/ })).not.toBeInTheDocument();
    await goToStep(user, 2);
    expect(screen.getByTestId('event-sync-assume-current-date')).toBeDisabled();
    expect(screen.getByTestId('event-sync-assume-current-date')).not.toBeChecked();
    await goToStep(user, 3);
    expect(screen.queryByTestId('event-sync-include-master-group-streams')).not.toBeInTheDocument();
    expect(screen.queryByTestId('event-sync-parse-master-from-stream')).not.toBeInTheDocument();
    for (const key of ['promote-unmatched', 'skip-dead-streams', 'skip-past-events', 'retire-finished-events']) {
      expect(screen.getByTestId('event-sync-' + key)).toBeChecked();
      expect(screen.getByTestId('event-sync-' + key)).toBeDisabled();
    }
  });

  it.each([
    { group_id: 2, m3u_account_id: null },
    { group_id: 2, m3u_account_id: 999 },
    { group_id: 999, m3u_account_id: 1 },
    { group_id: 2, m3u_account_id: 0 },
  ])('blocks invalid saved input %j without rewriting it', async scope => {
    const user = userEvent.setup();
    seedDedicated();
    const onSave = vi.fn();
    render(<EventSyncRuleEditor rule={{ ...DEDICATED_RULE,
      event_sync_config: { ...DEDICATED_RULE.event_sync_config!, secondary: [scope] },
    }} onSave={onSave} onCancel={vi.fn()} />);
    await screen.findByTestId('psg-secondary-2-1');
    const label = scope.group_id === 999 ? 'Remove 999' : 'Remove Secondary Events';
    expect(screen.getByRole('button', { name: label })).toBeInTheDocument();
    await goToStep(user, 4);
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSave).not.toHaveBeenCalled();
    expect(screen.getByRole('heading', { name: '1 Scope' })).toHaveFocus();
    expect(screen.getByRole('button', { name: /Preview matches/ })).toBeDisabled();
    await user.click(screen.getByRole('button', { name: label }));
    expect(screen.queryByRole('button', { name: label })).not.toBeInTheDocument();
  });

  it('requires a dedicated target and focuses Behavior from Review', async () => {
    const user = userEvent.setup();
    seedDedicated();
    const onSave = vi.fn();
    render(<EventSyncRuleEditor rule={{ ...DEDICATED_RULE,
      event_sync_config: { ...DEDICATED_RULE.event_sync_config!, promote_target_group_id: undefined },
    }} onSave={onSave} onCancel={vi.fn()} />);
    await screen.findByTestId('psg-secondary-2-1');
    await goToStep(user, 4);
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSave).not.toHaveBeenCalled();
    expect(screen.getByRole('heading', { name: '3 Behavior' })).toHaveFocus();
    expect(screen.getByRole('alert')).toHaveTextContent('Pick a target group');
  });
});

describe('dedicated draft validation and request ownership', () => {
  it.each<{ name: string; groupPatterns: EventSyncConfig['group_patterns'] }>([
    { name: 'absent', groupPatterns: undefined },
    { name: 'empty', groupPatterns: {} },
    { name: 'populated', groupPatterns: { '2': [
      { name: 'first', title_pattern: 'x(?P<title>.+)' },
      { name: 'extra', title_pattern: 'y(?P<title>.+)' },
    ] } },
  ])('preserves $name group overrides in dedicated Preview and Save', async ({ groupPatterns }) => {
    const user = userEvent.setup();
    seedDedicated();
    const onSave = vi.fn();
    let previewConfig: EventSyncConfig | undefined;
    server.use(http.post('/api/channel-pipeline/event-sync-preview', async ({ request }) => {
      const body = await request.json() as { event_sync_config: EventSyncConfig };
      previewConfig = body.event_sync_config;
      return HttpResponse.json(DEDICATED_PREVIEW);
    }));
    render(<EventSyncRuleEditor rule={{ ...DEDICATED_RULE,
      event_sync_config: {
        ...DEDICATED_RULE.event_sync_config!,
        ...(groupPatterns === undefined ? {} : { group_patterns: groupPatterns }),
      },
    }} onSave={onSave} onCancel={vi.fn()} />);
    await waitFor(() => expect(screen.getByRole('button', { name: /Preview matches/ })).toBeEnabled());
    await user.click(screen.getByRole('button', { name: /Preview matches/ }));
    await screen.findByTestId('event-sync-summary');
    await user.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    const saved = onSave.mock.calls[0][0].event_sync_config;
    expect(previewConfig).toEqual(saved);
    if (groupPatterns === undefined) {
      expect(previewConfig).not.toHaveProperty('group_patterns');
      expect(saved).not.toHaveProperty('group_patterns');
    } else {
      expect(previewConfig).toHaveProperty('group_patterns', groupPatterns);
      expect(saved).toHaveProperty('group_patterns', groupPatterns);
      for (const [key, patterns] of Object.entries(groupPatterns)) {
        expect(saved.group_patterns[key]).toBe(patterns);
      }
    }
    expect(screen.queryByTestId('event-sync-preview-stale')).not.toBeInTheDocument();
    await goToStep(user, 2);
    await user.click(screen.getByTestId('event-sync-ignore-time-window'));
    expect(screen.getByTestId('event-sync-preview-stale')).toBeInTheDocument();
  });

  it('emits an empty map after removing the last dedicated group override', async () => {
    const user = userEvent.setup();
    seedDedicated();
    const onSave = vi.fn();
    let previewConfig: EventSyncConfig | undefined;
    server.use(http.post('/api/channel-pipeline/event-sync-preview', async ({ request }) => {
      const body = await request.json() as { event_sync_config: EventSyncConfig };
      previewConfig = body.event_sync_config;
      return HttpResponse.json(DEDICATED_PREVIEW);
    }));
    render(<EventSyncRuleEditor rule={{ ...DEDICATED_RULE,
      event_sync_config: { ...DEDICATED_RULE.event_sync_config!, group_patterns: {
        '2': [{ title_pattern: 'x(?P<title>.+)' }],
      } },
    }} onSave={onSave} onCancel={vi.fn()} />);
    await waitFor(() => expect(screen.getByRole('button', { name: /Preview matches/ })).toBeEnabled());
    await goToStep(user, 2);
    await user.click(screen.getByText('Per-group pattern overrides'));
    await user.click(screen.getByText(/Secondary Events/, { selector: 'summary' }));
    const overrideTitle = screen.getAllByLabelText('Title pattern')
      .find(el => el.id.includes('-ov-2-'))!;
    await user.clear(overrideTitle);
    await user.click(screen.getByRole('button', { name: /Preview matches/ }));
    await screen.findByTestId('event-sync-summary');
    await user.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    expect(previewConfig).toHaveProperty('group_patterns', {});
    expect(onSave.mock.calls[0][0].event_sync_config).toHaveProperty('group_patterns', {});
  });

  it('preserves the exact order of two account scopes on save', async () => {
    const user = userEvent.setup();
    seedDedicated();
    const secondary = [{ group_id: 2, m3u_account_id: 1 }, { group_id: 1, m3u_account_id: 1 }];
    server.use(http.get('/api/dummy-epg/profiles', () => HttpResponse.json([
      { ...DEDICATED_PROFILE, event_sync_config: { ...DEDICATED_PROFILE.event_sync_config, secondary } },
    ])));
    const onSave = vi.fn();
    render(<EventSyncRuleEditor rule={{ ...DEDICATED_RULE,
      event_sync_config: { ...DEDICATED_RULE.event_sync_config!, secondary },
    }} onSave={onSave} onCancel={vi.fn()} />);
    await waitFor(() => expect(screen.getByRole('button', { name: /Preview matches/ })).toBeEnabled());
    await user.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    expect(onSave.mock.calls[0][0].event_sync_config.secondary).toEqual(secondary);
  });

  it('rejects duplicate saved scopes and focuses Scope', async () => {
    const user = userEvent.setup();
    seedDedicated();
    const onSave = vi.fn();
    render(<EventSyncRuleEditor rule={{ ...DEDICATED_RULE,
      event_sync_config: { ...DEDICATED_RULE.event_sync_config!, secondary: [
        { group_id: 2, m3u_account_id: 1 }, { group_id: 2, m3u_account_id: 1 },
      ] },
    }} onSave={onSave} onCancel={vi.fn()} />);
    await screen.findByTestId('psg-secondary-2-1');
    await goToStep(user, 4);
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSave).not.toHaveBeenCalled();
    expect(screen.getByRole('heading', { name: '1 Scope' })).toHaveFocus();
    expect(screen.getByRole('alert')).toHaveTextContent('Dedicated input scopes must be unique');
  });

  it('refuses a target that overlaps an input group', async () => {
    const user = userEvent.setup();
    seedDedicated();
    const onSave = vi.fn();
    render(<EventSyncRuleEditor rule={{ ...DEDICATED_RULE,
      event_sync_config: { ...DEDICATED_RULE.event_sync_config!, promote_target_group_id: 2 },
    }} onSave={onSave} onCancel={vi.fn()} />);
    await screen.findByTestId('psg-secondary-2-1');
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSave).not.toHaveBeenCalled();
    expect(screen.getByRole('heading', { name: '3 Behavior' })).toHaveFocus();
    expect(screen.getByRole('alert')).toHaveTextContent('must not be a secondary group');
  });

  it('refuses empty loaded inventories without creating a scope, target or profile', async () => {
    const user = userEvent.setup();
    server.use(
      http.get('/api/channel-groups', () => HttpResponse.json([])),
      http.get('/api/providers/group-settings/by-provider', () => HttpResponse.json([])),
      http.get('/api/dummy-epg/profiles', () => HttpResponse.json([])),
    );
    const onSave = vi.fn();
    render(<EventSyncRuleEditor onSave={onSave} onCancel={vi.fn()} />);
    await user.type(screen.getByLabelText('Rule Name *'), 'Empty Events');
    await user.click(screen.getByRole('radio', { name: 'Dedicated event group' }));
    await screen.findByText('No matching groups');
    await waitFor(() => expect(screen.getByRole('button', { name: /Preview matches/ }))
      .toHaveAttribute('title', 'Pick at least one account-scoped secondary group'));
    await goToStep(user, 4);
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSave).not.toHaveBeenCalled();
    expect(screen.getByRole('heading', { name: '1 Scope' })).toHaveFocus();
    expect(screen.getByRole('alert')).toHaveTextContent('Pick at least one account-scoped secondary group');
    expect(screen.queryByRole('button', { name: /Remove/ })).not.toBeInTheDocument();
  });

  it.each([
    { label: 'different target', profile: { ...DEDICATED_PROFILE, channel_group_ids: [2] } },
    { label: 'different hidden group', profile: { ...DEDICATED_PROFILE, hide_empty_group_ids: [] } },
    { label: 'source input', profile: { ...DEDICATED_PROFILE, epg_source_ids: [88] } },
    { label: 'channel mapping', profile: { ...DEDICATED_PROFILE, channel_mappings: [{ channel_id: 10, tvg_id: 'external' }] } },
    { label: 'slot', profile: { ...DEDICATED_PROFILE, event_sync_config: { ...DEDICATED_PROFILE.event_sync_config, slot_patterns: [{}] } } },
    { label: 'date assumption', profile: { ...DEDICATED_PROFILE, event_sync_config: { ...DEDICATED_PROFILE.event_sync_config, assume_current_date: true } } },
    { label: 'different account', profile: { ...DEDICATED_PROFILE, event_sync_config: { ...DEDICATED_PROFILE.event_sync_config,
      secondary: [{ group_id: 2, m3u_account_id: 9 }] } } },
  ])('rejects a profile with $label and focuses Behavior', async ({ profile }) => {
    const user = userEvent.setup();
    seedDedicated();
    server.use(http.get('/api/dummy-epg/profiles', () => HttpResponse.json([profile])));
    const onSave = vi.fn();
    render(<EventSyncRuleEditor rule={DEDICATED_RULE} onSave={onSave} onCancel={vi.fn()} />);
    await screen.findByTestId('psg-secondary-2-1');
    await waitFor(() => expect(screen.getByRole('button', { name: /Preview matches/ }))
      .toHaveAttribute('title', expect.stringContaining('same ordered account scopes')));
    await goToStep(user, 4);
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSave).not.toHaveBeenCalled();
    expect(screen.getByRole('heading', { name: '3 Behavior' })).toHaveFocus();
    expect(screen.getByRole('alert')).toHaveTextContent('no source inputs, mappings, slots or date assumption');
  });

  it.each(['skip_dead_streams', 'skip_past_events', 'retire_finished_events', 'promote_unmatched'] as const)(
    'refuses a saved dedicated rule with %s disabled', async flag => {
      const user = userEvent.setup();
      seedDedicated();
      const onSave = vi.fn();
      render(<EventSyncRuleEditor rule={{ ...DEDICATED_RULE,
        event_sync_config: { ...DEDICATED_RULE.event_sync_config!, [flag]: false },
      }} onSave={onSave} onCancel={vi.fn()} />);
      await screen.findByTestId('psg-secondary-2-1');
      await goToStep(user, 4);
      await user.click(screen.getByRole('button', { name: 'Save' }));
      expect(onSave).not.toHaveBeenCalled();
      expect(screen.getByRole('heading', { name: '3 Behavior' })).toHaveFocus();
      expect(screen.getByRole('button', { name: /Preview matches/ })).toBeDisabled();
    },
  );

  it('treats reordered dedicated scopes as dirty and invalidates a differently ordered profile', async () => {
    const user = userEvent.setup();
    seedDedicated();
    const secondary = [{ group_id: 2, m3u_account_id: 1 }, { group_id: 1, m3u_account_id: 1 }];
    server.use(http.get('/api/dummy-epg/profiles', () => HttpResponse.json([
      { ...DEDICATED_PROFILE, event_sync_config: { ...DEDICATED_PROFILE.event_sync_config, secondary } },
    ])));
    const onCancel = vi.fn();
    render(<EventSyncRuleEditor rule={{ ...DEDICATED_RULE,
      event_sync_config: { ...DEDICATED_RULE.event_sync_config!, secondary },
    }} onSave={vi.fn()} onCancel={onCancel} />);
    await waitFor(() => expect(screen.getByRole('button', { name: /Preview matches/ })).toBeEnabled());
    await user.click(screen.getByRole('button', { name: 'Remove Secondary Events' }));
    await user.click(screen.getByTestId('psg-secondary-2-1'));
    expect(screen.getByRole('button', { name: /Preview matches/ })).toBeDisabled();
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(screen.getByTestId('event-sync-discard-dialog')).toBeInTheDocument();
    expect(onCancel).not.toHaveBeenCalled();
  });

  it('waits for profiles without guessing a profile or enabling save', async () => {
    const user = userEvent.setup();
    seedDedicated();
    let release!: () => void;
    const pending = new Promise<void>(resolve => { release = resolve; });
    server.use(http.get('/api/dummy-epg/profiles', async () => {
      await pending;
      return HttpResponse.json([DEDICATED_PROFILE]);
    }));
    const onSave = vi.fn();
    render(<EventSyncRuleEditor rule={DEDICATED_RULE} onSave={onSave} onCancel={vi.fn()} />);
    try {
      await screen.findByTestId('psg-secondary-2-1');
      expect(screen.getByRole('button', { name: /Preview matches/ })).toHaveAttribute('title', 'Loading dummy EPG profiles');
      await user.click(screen.getByRole('button', { name: 'Save' }));
      expect(onSave).not.toHaveBeenCalled();
      expect(screen.getByRole('heading', { name: '3 Behavior' })).toHaveFocus();
    } finally {
      await act(async () => { release(); });
    }
    await waitFor(() => expect(screen.getByRole('button', { name: /Preview matches/ })).toBeEnabled());
    expect(onSave).not.toHaveBeenCalled();
    await user.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(1));
    expect(onSave.mock.calls[0][0].event_sync_config.dummy_epg_profile_id).toBe(7);
    expect(onSave.mock.calls[0][0].event_sync_config.enabled).toBe(false);
  });

  it.each([
    ['/api/channel-groups', 'Channel groups could not be loaded'],
    ['/api/providers/group-settings/by-provider', 'Account-scoped input groups could not be loaded'],
    ['/api/dummy-epg/profiles', 'Dummy EPG profiles could not be loaded'],
  ])('refuses a failed inventory at %s without saving', async (path, message) => {
    const user = userEvent.setup();
    seedDedicated();
    server.use(http.get(path, () => HttpResponse.json({ detail: message }, { status: 500 })));
    const onSave = vi.fn();
    render(<EventSyncRuleEditor rule={DEDICATED_RULE} onSave={onSave} onCancel={vi.fn()} />);
    await waitFor(() => expect(screen.getByRole('button', { name: /Preview matches/ }))
      .toHaveAttribute('title', expect.stringContaining(message)));
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSave).not.toHaveBeenCalled();
    expect(screen.getByRole('alert')).toHaveTextContent(message);
    expect(screen.getByRole('heading', { name: path === '/api/dummy-epg/profiles' ? '3 Behavior' : '1 Scope' })).toHaveFocus();
  });

  it('keeps an unavailable saved profile visible and refuses the stale selection', async () => {
    const user = userEvent.setup();
    seedDedicated();
    const onSave = vi.fn();
    render(<EventSyncRuleEditor rule={{ ...DEDICATED_RULE,
      event_sync_config: { ...DEDICATED_RULE.event_sync_config!, dummy_epg_profile_id: 99 },
    }} onSave={onSave} onCancel={vi.fn()} />);
    await screen.findByTestId('psg-secondary-2-1');
    await goToStep(user, 3);
    await waitFor(() => expect(screen.getByRole('button', { name: 'Source-free dummy EPG profile (required)' }))
      .toHaveTextContent('Unavailable profile 99'));
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSave).not.toHaveBeenCalled();
    expect(screen.getByRole('alert')).toHaveTextContent('Choose an available source-free dummy EPG profile');
  });

  it('retains staged gates after a rejected save and sends the same draft on retry', async () => {
    const user = userEvent.setup();
    seedDedicated();
    const onSave = vi.fn().mockRejectedValueOnce(new Error('Target changed; reload its settings')).mockResolvedValueOnce(undefined);
    render(<EventSyncRuleEditor onSave={onSave} onCancel={vi.fn()} />);
    await fillDedicatedDraft(user);
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Target changed; reload its settings');
    expect(screen.getByRole('button', { name: 'Save' })).toBeEnabled();
    await goToStep(user, 1);
    expect(screen.getByLabelText('Enabled')).not.toBeChecked();
    expect(screen.getByLabelText('Enabled')).toBeDisabled();
    expect(screen.getByRole('radio', { name: 'Dedicated event group' })).toBeChecked();
    await goToStep(user, 3);
    expect(screen.getByTestId('event-sync-auto-run')).not.toBeChecked();
    expect(screen.getByTestId('event-sync-auto-run')).toBeDisabled();
    await goToStep(user, 4);
    await user.click(screen.getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(onSave).toHaveBeenCalledTimes(2));
    expect(onSave.mock.calls[1][0]).toEqual(onSave.mock.calls[0][0]);
    await waitFor(() => expect(screen.queryByRole('alert')).not.toBeInTheDocument());
  });

  it('marks an edited dedicated preview stale and rejects it as authority for a missing target', async () => {
    const user = userEvent.setup();
    seedDedicated();
    server.use(http.post('/api/channel-pipeline/event-sync-preview', () => HttpResponse.json(DEDICATED_PREVIEW)));
    const onSave = vi.fn();
    render(<EventSyncRuleEditor rule={DEDICATED_RULE} onSave={onSave} onCancel={vi.fn()} />);
    await waitFor(() => expect(screen.getByRole('button', { name: /Preview matches/ })).toBeEnabled());
    await user.click(screen.getByRole('button', { name: /Preview matches/ }));
    await screen.findByTestId('event-sync-summary');
    await goToStep(user, 3);
    await user.click(screen.getByRole('button', { name: 'Target group for promoted channels' }));
    await user.click(screen.getByRole('option', { name: 'Pick a dedicated group…' }));
    expect(screen.getByTestId('event-sync-preview-stale')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(onSave).not.toHaveBeenCalled();
    expect(screen.getByRole('alert')).toHaveTextContent('Pick a target group');
  });

  it('rejects a late legacy preview after switching mode and completing a newer dedicated preview', async () => {
    const user = userEvent.setup();
    seedDedicated();
    let release!: () => void;
    const pending = new Promise<void>(resolve => { release = resolve; });
    let legacyReturned = false;
    const requests: { event_sync_config: { mode?: string; master: unknown } }[] = [];
    server.use(http.post('/api/channel-pipeline/event-sync-preview', async ({ request }) => {
      const body = await request.json() as typeof requests[number];
      requests.push(body);
      if (body.event_sync_config.mode !== 'dedicated') {
        await pending;
        legacyReturned = true;
        return HttpResponse.json({ ...DEDICATED_PREVIEW, summary: { ...DEDICATED_PREVIEW.summary, unmatched: 9 } });
      }
      return HttpResponse.json({ ...DEDICATED_PREVIEW, summary: { ...DEDICATED_PREVIEW.summary, unmatched: 2 } });
    }));
    render(<EventSyncRuleEditor onSave={vi.fn()} onCancel={vi.fn()} />);
    try {
      await user.click(await screen.findByTestId('psg-master-1-any'));
      await user.click(screen.getByTestId('psg-secondary-2-any'));
      await user.click(screen.getByRole('button', { name: /Preview matches/ }));
      await waitFor(() => expect(requests).toHaveLength(1));
      await user.click(screen.getByRole('radio', { name: 'Dedicated event group' }));
      await user.click(screen.getByRole('button', { name: 'Remove Secondary Events' }));
      await user.click(screen.getByRole('radio', { name: 'Existing master group' }));
      expect(screen.getByTestId('psg-master-1-any')).toBeChecked();
      await fillDedicatedDraft(user);
      await user.click(screen.getByRole('button', { name: /Preview matches/ }));
      expect(await screen.findByTestId('event-sync-summary')).toHaveTextContent('2 unmatched');
      expect(requests[1].event_sync_config).toEqual(expect.objectContaining({ mode: 'dedicated', master: null }));
      await act(async () => { release(); });
      await waitFor(() => expect(legacyReturned).toBe(true));
      await goToStep(user, 2);
      expect(screen.getByTestId('event-sync-summary')).toHaveTextContent('2 unmatched');
      expect(screen.getByTestId('event-sync-summary')).not.toHaveTextContent('9 unmatched');
    } finally {
      await act(async () => { release(); });
    }
  });

  it('discards a pending preview and reopens with a fresh legacy draft', async () => {
    const user = userEvent.setup();
    seedDedicated();
    let release!: () => void;
    const pending = new Promise<void>(resolve => { release = resolve; });
    let returned = false;
    server.use(http.post('/api/channel-pipeline/event-sync-preview', async () => {
      await pending;
      returned = true;
      return HttpResponse.json(DEDICATED_PREVIEW);
    }));
    const onCancel = vi.fn();
    const first = render(<EventSyncRuleEditor onSave={vi.fn()} onCancel={onCancel} />);
    try {
      await fillDedicatedDraft(user);
      await user.click(screen.getByRole('button', { name: /Preview matches/ }));
      await user.click(screen.getByRole('button', { name: 'Cancel' }));
      await user.click(screen.getByTestId('event-sync-discard-confirm'));
      expect(onCancel).toHaveBeenCalledTimes(1);
      first.unmount();
      render(<EventSyncRuleEditor onSave={vi.fn()} onCancel={vi.fn()} />);
      await screen.findByTestId('psg-master-1-any');
      expect(screen.getByRole('radio', { name: 'Existing master group' })).toBeChecked();
      expect(screen.getByLabelText('Rule Name *')).toHaveValue('');
      expect(screen.getByLabelText('Enabled')).toBeChecked();
      await act(async () => { release(); });
      await waitFor(() => expect(returned).toBe(true));
      await goToStep(user, 4);
      expect(screen.queryByTestId('event-sync-summary')).not.toBeInTheDocument();
      expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    } finally {
      await act(async () => { release(); });
    }
  });
});
