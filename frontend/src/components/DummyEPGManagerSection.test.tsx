/**
 * Unit tests for DummyEPGManagerSection — section retitle (bead
 * enhancedchannelmanager-09x38.4).
 *
 * PO DECISION #2 (Option B): this is now THE dummy EPG feature, so the section
 * drops the "ECM" qualifier and is titled simply "Dummy EPG Profiles".
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, within, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { DummyEPGManagerSection } from './DummyEPGManagerSection';
import * as api from '../services/api';
import type { TaskExecution } from '../services/api';
import type { DummyEPGGenerationOutcome, DummyEPGProfile } from '../types';

const notifications = vi.hoisted(() => ({
  success: vi.fn(), warning: vi.fn(), error: vi.fn(), info: vi.fn(),
}));

vi.mock('../services/api', () => ({
  getDummyEPGProfiles: vi.fn().mockResolvedValue([]),
  exportDummyEPGProfilesYAML: vi.fn().mockResolvedValue('profiles: []'),
  regenerateDummyEPG: vi.fn(),
  getTaskHistory: vi.fn(),
}));

vi.mock('./DummyEPGProfileModal', () => ({
  DummyEPGProfileModal: () => null,
}));
vi.mock('./ImportDummyEPGModal', () => ({
  ImportDummyEPGModal: () => null,
}));

vi.mock('../contexts/NotificationContext', () => ({
  useNotifications: () => notifications,
}));

function generationOutcome(overrides: Partial<DummyEPGGenerationOutcome> = {}): DummyEPGGenerationOutcome {
  return {
    configured_profile_count: 1,
    published_profile_ids: [1],
    retained_profile_ids: [],
    unavailable_profile_ids: [],
    publication_times: { '1': '2026-09-20T20:00:00Z' },
    source_reason_codes: {},
    idle_channel_count: 0,
    active_channel_count: 1,
    unknown_channel_count: 0,
    stream_updated_channel_ids: [],
    epg_linked_channel_ids: [],
    revealed_channel_ids: [],
    hidden_channel_ids: [],
    pending_source_hashes: {},
    emby_request_outcome: 'accepted',
    pending_emby: false,
    delivery_pending: false,
    reason_codes: [],
    ...overrides,
  };
}

function taskExecution(
  status: TaskExecution['status'],
  details: DummyEPGGenerationOutcome | null,
): TaskExecution {
  return {
    id: 42,
    task_id: 'dummy_epg_refresh',
    started_at: '2026-09-20T20:00:00Z',
    completed_at: status === 'running' ? null : '2026-09-20T20:00:01Z',
    duration_seconds: status === 'running' ? null : 1,
    status,
    success: status === 'completed',
    message: null,
    error: null,
    total_items: 1,
    success_count: status === 'completed' ? 1 : 0,
    failed_count: status === 'failed' ? 1 : 0,
    skipped_count: 0,
    details: details as unknown as Record<string, unknown> | null,
    triggered_by: 'manual',
  };
}

describe('DummyEPGManagerSection — title', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.getDummyEPGProfiles).mockResolvedValue([]);
  });

  it('exposes all three owned dialogs by their visible headings', async () => {
    const user = userEvent.setup();
    vi.mocked(api.getDummyEPGProfiles).mockResolvedValue([{ id: 1, name: 'Sports', enabled: true } as DummyEPGProfile]);
    render(<DummyEPGManagerSection />);
    await screen.findByText('Sports');

    await user.click(screen.getByRole('button', { name: 'Delete profile' }));
    expect(screen.getByRole('dialog', { name: 'Delete Profile' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Cancel' }));

    await user.click(screen.getByRole('button', { name: /Export$/ }));
    const exportDialog = await screen.findByRole('dialog', { name: 'Export Profiles (YAML)' });
    await user.click(within(exportDialog).getAllByRole('button', { name: 'Close' })[1]);

    await user.click(screen.getByRole('button', { name: /Import YAML$/ }));
    expect(screen.getByRole('dialog', { name: 'Import Profiles (YAML)' })).toBeInTheDocument();
  });

  it('titles the section "Dummy EPG Profiles" without the ECM qualifier', async () => {
    render(<DummyEPGManagerSection />);

    expect(
      await screen.findByRole('heading', { name: 'Dummy EPG Profiles' })
    ).toBeInTheDocument();
    expect(screen.queryByText('ECM Dummy EPG Profiles')).not.toBeInTheDocument();
  });

  it('uses non-ECM copy in the empty state', async () => {
    render(<DummyEPGManagerSection />);

    expect(
      await screen.findByText(/No Dummy EPG profiles\./i)
    ).toBeInTheDocument();
    expect(screen.queryByText(/No ECM Dummy EPG profiles/i)).not.toBeInTheDocument();
  });
});


it('identifies profiles that combine source schedules with gap coverage', async () => {
  vi.mocked(api.getDummyEPGProfiles).mockResolvedValue([
    { id: 1, name: 'Universal', enabled: true, epg_source_ids: [51] } as DummyEPGProfile,
  ]);
  render(<DummyEPGManagerSection />);
  expect(await screen.findByText('Source schedules + neutral gaps')).toBeInTheDocument();
});

describe('DummyEPGManagerSection regeneration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.getDummyEPGProfiles).mockResolvedValue([
      { id: 1, name: 'Sports', enabled: true } as DummyEPGProfile,
    ]);
    vi.mocked(api.regenerateDummyEPG).mockResolvedValue({
      status: 'accepted', task_id: 'dummy_epg_refresh', execution_id: 42,
      started_at: '2026-09-20T20:00:00Z',
    });
  });

  it.each([
    ['completed', generationOutcome(), 'success', 'Guide generation completed.'],
    ['completed_with_warnings', generationOutcome({ published_profile_ids: [], retained_profile_ids: [1], reason_codes: ['GUIDE_SOURCES_PENDING'] }), 'warning', 'Guide generation completed with retained or degraded output. Check saved guide coverage for details.'],
    ['failed', generationOutcome({ published_profile_ids: [], unavailable_profile_ids: [1], reason_codes: ['GUIDE_UNAVAILABLE'] }), 'error', 'Guide generation failed because no usable publication was available for one or more profiles.'],
    ['completed_with_warnings', generationOutcome({ emby_request_outcome: 'pending', pending_emby: true, delivery_pending: true, reason_codes: ['GUIDE_EMBY_PENDING'] }), 'warning', 'Guide generation completed, but delivery is still pending.'],
    ['cancelled', generationOutcome({ published_profile_ids: [], reason_codes: ['CANCELLED'] }), 'warning', 'Guide generation was cancelled. Committed guide work remains available.'],
  ] as const)('reports %s durable generation accurately', async (status, details, method, message) => {
    vi.mocked(api.getTaskHistory).mockResolvedValue({ history: [taskExecution(status, details)] });
    const user = userEvent.setup();
    render(<DummyEPGManagerSection />);
    await screen.findByText('Sports');
    await user.click(screen.getByRole('button', { name: /Regenerate$/ }));
    await waitFor(() => expect(notifications.info).toHaveBeenCalledWith(
      'Guide generation started. ECM will report the durable task result when it finishes.',
      'Dummy EPG',
    ));
    await waitFor(() => expect(notifications[method]).toHaveBeenCalledWith(message, 'Dummy EPG'));
    for (const other of ['success', 'warning', 'error'] as const) {
      if (other !== method) expect(notifications[other]).not.toHaveBeenCalled();
    }
    expect(screen.getByRole('button', { name: /Regenerate$/ })).toBeEnabled();
    expect(api.regenerateDummyEPG).toHaveBeenCalledOnce();
  });

  it('does not report completion while the durable execution is still running', async () => {
    vi.mocked(api.getTaskHistory)
      .mockResolvedValueOnce({ history: [taskExecution('running', null)] })
      .mockResolvedValueOnce({ history: [taskExecution('completed', null)] });
    const user = userEvent.setup();
    render(<DummyEPGManagerSection />);
    await screen.findByText('Sports');
    await user.click(screen.getByRole('button', { name: /Regenerate$/ }));
    await waitFor(() => expect(notifications.info).toHaveBeenCalled());
    expect(notifications.success).not.toHaveBeenCalled();
    await waitFor(() => expect(notifications.success).toHaveBeenCalledWith('Guide generation completed.', 'Dummy EPG'), { timeout: 2000 });
  });
});
