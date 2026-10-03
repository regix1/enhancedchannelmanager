import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const { mockError, mockSuccess, mockWarning } = vi.hoisted(() => ({
  mockError: vi.fn(),
  mockSuccess: vi.fn(),
  mockWarning: vi.fn(),
}));

vi.mock('../services/api', () => ({
  getSettings: vi.fn(),
  restoreBackupInitial: vi.fn(),
  saveSettings: vi.fn(),
  testConnection: vi.fn(),
}));

vi.mock('../contexts/NotificationContext', () => ({
  useNotifications: () => ({
    success: mockSuccess,
    error: mockError,
    warning: mockWarning,
    info: vi.fn(),
  }),
}));

vi.mock('../hooks/useServerDataInvalidation', () => ({
  invalidateServerData: vi.fn(),
}));

import * as api from '../services/api';
import { SettingsModal } from './SettingsModal';

const settings = {
  configured: false,
  url: '',
  username: '',
  auth_method: 'password',
  dispatcharr_api_key_configured: false,
  theme: 'dark',
  include_channel_number_in_name: false,
  channel_number_separator: '-',
  remove_country_prefix: false,
  include_country_in_name: false,
  country_separator: '|',
  timezone_preference: 'both',
  show_stream_urls: true,
  hide_auto_sync_groups: false,
} as api.SettingsResponse;

const accountNotice =
  'This instance has no ECM user account. Create your admin account through first-run setup.';
const guideNotice =
  'Guide-promotion rules paused: 2. This backup has no event recovery records. ' +
  'Inspect the external channels and recovery state before you re-enable these rules.';

describe('SettingsModal initial restore notices', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.restoreBackupInitial).mockReset();
    vi.mocked(api.getSettings).mockResolvedValue(settings);
    Element.prototype.scrollTo = vi.fn();
    Element.prototype.scrollIntoView = vi.fn();
  });

  it('delivers every restore follow-up before saving and closing', async () => {
    let resolveRestore!: (value: api.RestoreResult) => void;
    const restoreRequest = new Promise<api.RestoreResult>((resolve) => {
      resolveRestore = resolve;
    });
    vi.mocked(api.restoreBackupInitial).mockReturnValueOnce(restoreRequest);
    const onSaved = vi.fn();
    const onClose = vi.fn();

    render(<SettingsModal isOpen onClose={onClose} onSaved={onSaved} />);
    await waitFor(() => expect(api.getSettings).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: /restore from backup/i }));

    const file = new File(['zip-content'], 'initial-backup.zip', { type: 'application/zip' });
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    Object.defineProperty(input, 'files', { value: [file] });
    fireEvent.click(screen.getByRole('button', { name: 'Restore' }));

    await waitFor(() => expect(api.restoreBackupInitial).toHaveBeenCalledWith(file));
    expect(mockWarning).not.toHaveBeenCalled();
    expect(onSaved).not.toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();

    await act(async () => {
      resolveRestore({
        status: 'ok',
        backup_version: '0.18.1',
        backup_date: '2026-10-03T00:00:00Z',
        restored_files: ['settings.json', 'journal.db'],
        notices: [accountNotice, guideNotice],
      });
      await restoreRequest;
    });

    expect(mockSuccess).toHaveBeenCalledWith('Restored 2 files from backup');
    expect(mockWarning).toHaveBeenNthCalledWith(1, accountNotice, 'Restore Follow-up Required');
    expect(mockWarning).toHaveBeenNthCalledWith(2, guideNotice, 'Restore Follow-up Required');
    expect(mockWarning).toHaveBeenCalledTimes(2);
    expect(onSaved).toHaveBeenCalledTimes(1);
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(mockWarning.mock.invocationCallOrder[1]).toBeLessThan(onSaved.mock.invocationCallOrder[0]);
    expect(mockWarning.mock.invocationCallOrder[1]).toBeLessThan(onClose.mock.invocationCallOrder[0]);
  });

  it.each([
    ['omitted', undefined],
    ['empty', [] as string[]],
  ])('accepts an %s notice list without warning', async (_label, notices) => {
    const result: api.RestoreResult = {
      status: 'ok',
      backup_version: '0.18.1',
      backup_date: '2026-10-03T00:00:00Z',
      restored_files: ['settings.json'],
      ...(notices === undefined ? {} : { notices }),
    };
    vi.mocked(api.restoreBackupInitial).mockResolvedValueOnce(result);
    const onSaved = vi.fn();
    const onClose = vi.fn();

    render(<SettingsModal isOpen onClose={onClose} onSaved={onSaved} />);
    await waitFor(() => expect(api.getSettings).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: /restore from backup/i }));

    const file = new File(['zip-content'], 'initial-backup.zip', { type: 'application/zip' });
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    Object.defineProperty(input, 'files', { value: [file] });
    fireEvent.click(screen.getByRole('button', { name: 'Restore' }));

    await waitFor(() => expect(onClose).toHaveBeenCalledTimes(1));
    expect(api.restoreBackupInitial).toHaveBeenCalledWith(file);
    expect(mockSuccess).toHaveBeenCalledWith('Restored 1 files from backup');
    expect(mockWarning).not.toHaveBeenCalled();
    expect(onSaved).toHaveBeenCalledTimes(1);
  });

  it('keeps the modal open and emits no success notices when restore fails', async () => {
    vi.mocked(api.restoreBackupInitial).mockRejectedValueOnce(new Error('Restore refused'));
    const onSaved = vi.fn();
    const onClose = vi.fn();

    render(<SettingsModal isOpen onClose={onClose} onSaved={onSaved} />);
    await waitFor(() => expect(api.getSettings).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: /restore from backup/i }));

    const file = new File(['zip-content'], 'initial-backup.zip', { type: 'application/zip' });
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    Object.defineProperty(input, 'files', { value: [file] });
    fireEvent.click(screen.getByRole('button', { name: 'Restore' }));

    await waitFor(() => {
      expect(mockError).toHaveBeenCalledWith('Restore refused', 'Restore Failed');
    });
    expect(mockSuccess).not.toHaveBeenCalled();
    expect(mockWarning).not.toHaveBeenCalled();
    expect(onSaved).not.toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();
    expect(screen.getByRole('button', { name: 'Restore' })).toBeEnabled();
  });
});
