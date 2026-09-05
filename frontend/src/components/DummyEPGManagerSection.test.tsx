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
import type { DummyEPGProfile } from '../types';

const notifications = vi.hoisted(() => ({
  success: vi.fn(), warning: vi.fn(), error: vi.fn(),
}));

vi.mock('../services/api', () => ({
  getDummyEPGProfiles: vi.fn().mockResolvedValue([]),
  exportDummyEPGProfilesYAML: vi.fn().mockResolvedValue('profiles: []'),
  regenerateDummyEPG: vi.fn(),
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
  });

  it.each([
    ['ok', 'success', 'XMLTV regenerated successfully'],
    ['pending', 'warning', 'Guide sources or artwork are still loading. Check saved guide coverage again shortly.'],
    ['error', 'error', 'XMLTV generation is incomplete. Check saved guide coverage for source errors.'],
  ] as const)('reports %s generation accurately', async (status, method, message) => {
    vi.mocked(api.regenerateDummyEPG).mockResolvedValue({ status, profiles_generated: 1 });
    const user = userEvent.setup();
    render(<DummyEPGManagerSection />);
    await screen.findByText('Sports');
    await user.click(screen.getByRole('button', { name: /Regenerate$/ }));
    await waitFor(() => expect(notifications[method]).toHaveBeenCalledWith(message, 'Dummy EPG'));
    for (const other of ['success', 'warning', 'error'] as const) {
      if (other !== method) expect(notifications[other]).not.toHaveBeenCalled();
    }
    expect(screen.getByRole('button', { name: /Regenerate$/ })).toBeEnabled();
    expect(api.regenerateDummyEPG).toHaveBeenCalledOnce();
  });
});
