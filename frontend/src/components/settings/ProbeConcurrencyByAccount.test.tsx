/**
 * Tests for the two probe settings on the Maintenance page: the minimum
 * stream bitrate and the per-account probe limit.
 *
 * Both are plumbing, and plumbing breaks quietly. A field missing from the
 * save literal reads back fine and is only noticed later, when the operator
 * saves something unrelated and their value has gone back to the default.
 * These pin the round trip for both:
 *   - the control renders and shows what GET /api/settings returned
 *   - an edit reaches the saveSettings payload
 *   - for the account list: a row can be added and removed, and a row the
 *     operator half filled in is dropped rather than stored
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';

let mockUser: { is_admin: boolean; username: string } = { is_admin: true, username: 'admin' };

vi.mock('../../services/api', () => ({
  getSettings: vi.fn(),
  saveSettings: vi.fn(),
  getChannelProfiles: vi.fn(),
  generateMCPApiKey: vi.fn(),
  revokeMCPApiKey: vi.fn(),
  getMCPStatus: vi.fn(),
  listAlertMethods: vi.fn(),
  getM3UAccounts: vi.fn(),
  getExportSections: vi.fn(),
  listSavedBackups: vi.fn(),
  testEmbyConnection: vi.fn(),
  testPlexConnection: vi.fn(),
  testJellyfinConnection: vi.fn(),
  getStreams: vi.fn().mockResolvedValue({ streams: [], total: 0 }),
  getProbeHistory: vi.fn().mockResolvedValue([]),
  getProbeProgress: vi.fn().mockResolvedValue(null),
  getStreamGroups: vi.fn().mockResolvedValue([]),
}));

vi.mock('../../services/channelPipelineApi', () => ({
  getChannelPipelineRules: vi.fn(),
  getChannelPipelineGroups: vi.fn(),
  generateAndFetchDebugBundle: vi.fn(),
}));

vi.mock('../../contexts/NotificationContext', () => ({
  useNotifications: () => ({
    success: vi.fn(),
    error: vi.fn(),
    warning: vi.fn(),
    info: vi.fn(),
    notify: vi.fn().mockReturnValue('toast-id'),
    dismiss: vi.fn(),
  }),
}));

vi.mock('../../hooks/useAuth', () => ({
  useAuth: () => ({ user: mockUser }),
}));

vi.mock('../settings/NormalizationEngineSection', () => ({
  NormalizationEngineSection: () => <div data-testid="stub-normalization" />,
}));
vi.mock('../settings/TagEngineSection', () => ({
  TagEngineSection: () => <div data-testid="stub-tag-engine" />,
}));
vi.mock('../settings/AuthSettingsSection', () => ({
  AuthSettingsSection: () => <div data-testid="stub-auth" />,
}));
vi.mock('../settings/UserManagementSection', () => ({
  UserManagementSection: () => <div data-testid="stub-users" />,
}));
vi.mock('../settings/LinkedAccountsSection', () => ({
  LinkedAccountsSection: () => <div data-testid="stub-linked-accounts" />,
}));
vi.mock('../settings/TLSSettingsSection', () => ({
  TLSSettingsSection: () => <div data-testid="stub-tls" />,
}));
vi.mock('../settings/BackupRestoreSection', () => ({
  BackupRestoreSection: () => <div data-testid="stub-backup" />,
}));
vi.mock('../settings/MCPSettingsSection', () => ({
  MCPSettingsSection: () => <div data-testid="stub-mcp" />,
}));
vi.mock('../settings/LookupTableSection', () => ({
  LookupTableSection: () => <div data-testid="stub-lookup" />,
}));
vi.mock('../ScheduledTasksSection', () => ({
  ScheduledTasksSection: () => <div data-testid="stub-scheduled-tasks" />,
}));
vi.mock('../SettingsModal', () => ({
  SettingsModal: () => <div data-testid="stub-settings-modal" />,
}));
vi.mock('../DeleteOrphanedGroupsModal', () => ({
  DeleteOrphanedGroupsModal: () => <div data-testid="stub-delete-orphaned" />,
}));
vi.mock('../CustomSelect', () => ({
  CustomSelect: ({ value, onChange, options }: {
    value: string;
    onChange: (v: string) => void;
    options: { value: string; label: string }[];
  }) => (
    <select value={value} onChange={(e) => onChange(e.target.value)}>
      {options.map((o) => (
        <option key={o.value} value={o.value}>{o.label}</option>
      ))}
    </select>
  ),
}));

import * as api from '../../services/api';
import { SettingsTab } from '../tabs/SettingsTab';

function makeSettings(overrides: Partial<typeof settingsBase> = {}): Awaited<ReturnType<typeof api.getSettings>> {
  return { ...settingsBase, ...overrides } as Awaited<ReturnType<typeof api.getSettings>>;
}

const settingsBase = {
  configured: true,
  url: 'http://dispatcharr.test',
  auth_method: 'password' as const,
  username: 'admin',
  dispatcharr_api_key_configured: false,
  api_key_configured: false,
  theme: 'dark' as const,
  date_format: 'auto',
  auto_rename_channel_number: false,
  include_channel_number_in_name: false,
  channel_number_separator: '-',
  remove_country_prefix: false,
  include_country_in_name: false,
  country_separator: '|',
  timezone_preference: 'both',
  show_stream_urls: true,
  hide_auto_sync_groups: false,
  hide_ungrouped_streams: true,
  hide_epg_urls: false,
  hide_m3u_urls: false,
  gracenote_conflict_mode: 'ask' as const,
  default_channel_profile_ids: [],
  linked_m3u_accounts: [],
  allow_multi_provider_auto_sync: false,
  epg_auto_match_threshold: 80,
  epg_auto_link_after_pipeline: true,
  sports_banner_base_url: '',
  sports_banner_leagues: [] as api.SportsBannerLeagueRule[],
  custom_network_prefixes: [],
  custom_network_suffixes: [],
  stats_poll_interval: 10,
  user_timezone: '',
  backend_log_level: 'INFO',
  frontend_log_level: 'INFO',
  vlc_open_behavior: 'm3u_fallback' as const,
  stream_preview_mode: 'passthrough' as const,
  auto_creation_excluded_terms: [],
  auto_creation_excluded_groups: [],
  auto_creation_exclude_auto_sync_groups: false,
  max_auto_created_channels_per_run: 500,
  max_auto_creation_log_entries: 500,
  stream_probe_timeout: 30,
  stream_probe_schedule_time: '03:00',
  bitrate_sample_duration: 10,
  min_stream_bitrate_kbps: 2000,
  parallel_probing_enabled: true,
  max_concurrent_probes: 8,
  probe_concurrency_by_account: {} as Record<string, number>,
  profile_distribution_strategy: 'fill_first',
  skip_recently_probed_hours: 0,
  refresh_m3us_before_probe: true,
  auto_reorder_after_probe: false,
  push_stream_stats_to_dispatcharr: false,
  probe_retry_count: 1,
  probe_retry_delay: 2,
  stream_fetch_page_limit: 200,
  stream_sort_priority: ['resolution', 'bitrate', 'framerate'] as api.SortCriterion[],
  stream_sort_enabled: { resolution: true, bitrate: true, framerate: true, video_codec: false, m3u_priority: false, audio_channels: false, custom_streams: false } as api.SortEnabledMap,
  m3u_account_priorities: {},
  black_screen_detection_enabled: false,
  black_screen_sample_duration: 5,
  low_fps_threshold: 20,
  deprioritize_failed_streams: true,
  deprioritize_black_screen: true,
  deprioritize_low_fps: true,
  failed_stream_sort_order: ['failed', 'black_screen', 'low_fps'] as api.FailedStreamCategory[],
  strike_threshold: 3,
  normalize_on_channel_create: false,
  smtp_configured: false,
  smtp_host: '',
  smtp_port: 587,
  smtp_user: '',
  smtp_from_email: '',
  smtp_from_name: 'ECM Alerts',
  smtp_use_tls: true,
  smtp_use_ssl: false,
  discord_configured: false,
  discord_webhook_url: '',
  telegram_configured: false,
  telegram_bot_token: '',
  telegram_chat_id: '',
  mcp_api_key_configured: false,
  telemetry_client_errors_enabled: true,
  dedup_threshold: 0.80,
  dedup_m3u_toast_suppressed: false,
  emby_enabled: false,
  emby_refresh_guide_after_pipeline: true,
  emby_base_url: '',
  emby_api_key_configured: false,
  plex_enabled: false,
  plex_base_url: '',
  plex_token_configured: false,
  jellyfin_enabled: false,
  jellyfin_base_url: '',
  jellyfin_api_key_configured: false,
  trusted_media_networks: [],
  ssrf_outbound_mode: 'lan_friendly' as const,
};

function renderOnMaintenance() {
  return render(
    <SettingsTab
      onSaved={vi.fn()}
      initialSettingsPage="maintenance"
    />
  );
}

async function save(): Promise<Parameters<typeof api.saveSettings>[0]> {
  fireEvent.click(screen.getByRole('button', { name: /Save Settings/i }));
  await waitFor(() => expect(api.saveSettings).toHaveBeenCalled());
  return vi.mocked(api.saveSettings).mock.calls[0][0];
}

describe('Minimum stream bitrate', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUser = { is_admin: true, username: 'admin' };
    vi.mocked(api.getSettings).mockResolvedValue(makeSettings());
    vi.mocked(api.saveSettings).mockResolvedValue({ status: 'ok', configured: true, server_changed: false });
    vi.mocked(api.getChannelProfiles).mockResolvedValue([]);
    vi.mocked(api.listAlertMethods).mockResolvedValue([]);
    vi.mocked(api.getM3UAccounts).mockResolvedValue([]);
  });

  it('shows the floor the settings were loaded with', async () => {
    vi.mocked(api.getSettings).mockResolvedValue(makeSettings({ min_stream_bitrate_kbps: 3500 }));
    renderOnMaintenance();

    // The input renders from its useState default, so it exists before the
    // mocked getSettings() promise resolves. Read the value inside waitFor.
    const input = await screen.findByLabelText(/Minimum stream bitrate/i) as HTMLInputElement;
    await waitFor(() => expect(input.value).toBe('3500'));
  });

  it('sends an edited floor on save', async () => {
    renderOnMaintenance();

    const input = await screen.findByLabelText(/Minimum stream bitrate/i) as HTMLInputElement;
    await waitFor(() => expect(input.value).toBe('2000'));
    fireEvent.change(input, { target: { value: '1500' } });

    expect((await save()).min_stream_bitrate_kbps).toBe(1500);
  });

  it('sends a floor of 0, which is how the check is turned off', async () => {
    renderOnMaintenance();

    const input = await screen.findByLabelText(/Minimum stream bitrate/i) as HTMLInputElement;
    await waitFor(() => expect(input.value).toBe('2000'));
    fireEvent.change(input, { target: { value: '0' } });

    expect((await save()).min_stream_bitrate_kbps).toBe(0);
  });
});

describe('Per-account probe limit', () => {
  const CAPPED = { probe_concurrency_by_account: { '4': 1, '7': 3 } };

  beforeEach(() => {
    vi.clearAllMocks();
    mockUser = { is_admin: true, username: 'admin' };
    vi.mocked(api.getSettings).mockResolvedValue(makeSettings(CAPPED));
    vi.mocked(api.saveSettings).mockResolvedValue({ status: 'ok', configured: true, server_changed: false });
    vi.mocked(api.getChannelProfiles).mockResolvedValue([]);
    vi.mocked(api.listAlertMethods).mockResolvedValue([]);
    vi.mocked(api.getM3UAccounts).mockResolvedValue([]);
  });

  it('says what an empty list means instead of showing a bare heading', async () => {
    vi.mocked(api.getSettings).mockResolvedValue(makeSettings());
    renderOnMaintenance();

    expect(await screen.findByText(/No accounts are capped/i)).toBeInTheDocument();
    expect(screen.queryByLabelText(/Account id 1/i)).not.toBeInTheDocument();
  });

  it('shows the accounts the settings were loaded with', async () => {
    renderOnMaintenance();

    const first = await screen.findByLabelText(/Account id 1/i) as HTMLInputElement;
    await waitFor(() => expect(first.value).toBe('4'));
    expect((screen.getByLabelText(/Streams at a time 1/i) as HTMLInputElement).value).toBe('1');
    expect((screen.getByLabelText(/Account id 2/i) as HTMLInputElement).value).toBe('7');
    expect((screen.getByLabelText(/Streams at a time 2/i) as HTMLInputElement).value).toBe('3');
  });

  it('sends an edited limit on save', async () => {
    renderOnMaintenance();

    const limit = await screen.findByLabelText(/Streams at a time 2/i) as HTMLInputElement;
    await waitFor(() => expect(limit.value).toBe('3'));
    fireEvent.change(limit, { target: { value: '2' } });

    expect((await save()).probe_concurrency_by_account).toEqual({ '4': 1, '7': 2 });
  });

  it('adds an account and sends it', async () => {
    renderOnMaintenance();

    await screen.findByLabelText(/Account id 1/i);
    fireEvent.click(screen.getByRole('button', { name: /Add account/i }));
    fireEvent.change(screen.getByLabelText(/Account id 3/i), { target: { value: '9' } });
    fireEvent.change(screen.getByLabelText(/Streams at a time 3/i), { target: { value: '4' } });

    expect((await save()).probe_concurrency_by_account).toEqual({ '4': 1, '7': 3, '9': 4 });
  });

  it('removes an account and leaves it out of the save', async () => {
    renderOnMaintenance();

    await screen.findByLabelText(/Account id 1/i);
    fireEvent.click(screen.getByRole('button', { name: /Remove account limit 1/i }));

    expect(screen.queryByLabelText(/Account id 2/i)).not.toBeInTheDocument();
    expect((await save()).probe_concurrency_by_account).toEqual({ '7': 3 });
  });

  it('drops a row whose account id was never filled in', async () => {
    renderOnMaintenance();

    await screen.findByLabelText(/Account id 1/i);
    fireEvent.click(screen.getByRole('button', { name: /Add account/i }));
    fireEvent.change(screen.getByLabelText(/Streams at a time 3/i), { target: { value: '2' } });

    expect((await save()).probe_concurrency_by_account).toEqual({ '4': 1, '7': 3 });
  });

  it('drops a row whose limit is not a number', async () => {
    renderOnMaintenance();

    await screen.findByLabelText(/Account id 1/i);
    fireEvent.click(screen.getByRole('button', { name: /Add account/i }));
    fireEvent.change(screen.getByLabelText(/Account id 3/i), { target: { value: '9' } });
    fireEvent.change(screen.getByLabelText(/Streams at a time 3/i), { target: { value: 'lots' } });

    expect((await save()).probe_concurrency_by_account).toEqual({ '4': 1, '7': 3 });
  });
});
