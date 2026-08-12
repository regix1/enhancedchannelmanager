/**
 * Tests for the "Sports matchup banners" game-thumbs base URL.
 *
 * Guide providers publish no artwork for an individual game: measured
 * against the live feed, 39% of College Football airings carried no icon at
 * all and the rest all shared ONE series image. `sports_banner_base_url`
 * points the EPG artwork proxy at a game-thumbs server so each matchup gets
 * a banner built from its two teams.
 *
 * The field is pure wiring, which is exactly the kind that breaks silently
 * — an empty value is also the off switch, so a load or save that drops it
 * turns the feature off rather than erroring. These pin the round trip:
 *   - It renders on the Channel Defaults page and populates from settings.
 *   - An edit reaches the save payload.
 *   - A blank stays blank in the payload instead of going undefined, which
 *     is what keeps "off" distinguishable from "field not sent".
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
  probe_concurrency_by_account: {},
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

function renderOnChannelDefaults() {
  return render(
    <SettingsTab
      onSaved={vi.fn()}
      initialSettingsPage="channel-defaults"
    />
  );
}

describe('Sports matchup banner URL', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUser = { is_admin: true, username: 'admin' };
    vi.mocked(api.getSettings).mockResolvedValue(makeSettings());
    vi.mocked(api.saveSettings).mockResolvedValue({ status: 'ok', configured: true, server_changed: false });
    vi.mocked(api.getChannelProfiles).mockResolvedValue([]);
    vi.mocked(api.listAlertMethods).mockResolvedValue([]);
    vi.mocked(api.getM3UAccounts).mockResolvedValue([]);
  });

  it('shows the URL the settings were loaded with', async () => {
    vi.mocked(api.getSettings).mockResolvedValue(
      makeSettings({ sports_banner_base_url: 'http://thumbs.example:3100' }));
    renderOnChannelDefaults();

    // The input renders from the component's `useState('')` default, so it
    // exists before the mocked getSettings() promise has resolved. Read the
    // value inside waitFor so it re-polls until the loaded one lands.
    const input = await screen.findByLabelText(/Sports matchup banners/i) as HTMLInputElement;
    await waitFor(() => expect(input.value).toBe('http://thumbs.example:3100'));
  });

  it('is empty when no server is configured, which is the off state', async () => {
    renderOnChannelDefaults();

    const input = await screen.findByLabelText(/Sports matchup banners/i) as HTMLInputElement;
    expect(input.value).toBe('');
  });

  it('says what an empty value does, since blank is a real choice', async () => {
    renderOnChannelDefaults();

    await screen.findByLabelText(/Sports matchup banners/i);
    expect(screen.getByText(/Leave it empty/i)).toBeInTheDocument();
  });

  it('sends an edited URL on save', async () => {
    renderOnChannelDefaults();

    const input = await screen.findByLabelText(/Sports matchup banners/i) as HTMLInputElement;
    fireEvent.change(input, { target: { value: 'http://thumbs.example:3100' } });

    fireEvent.click(screen.getByRole('button', { name: /Save Settings/i }));

    await waitFor(() => expect(api.saveSettings).toHaveBeenCalled());
    const payload = vi.mocked(api.saveSettings).mock.calls[0][0];
    expect(payload.sports_banner_base_url).toBe('http://thumbs.example:3100');
  });

  it('trims a pasted URL so stray whitespace cannot break every banner', async () => {
    renderOnChannelDefaults();

    const input = await screen.findByLabelText(/Sports matchup banners/i) as HTMLInputElement;
    fireEvent.change(input, { target: { value: '  http://thumbs.example:3100  ' } });
    fireEvent.click(screen.getByRole('button', { name: /Save Settings/i }));

    await waitFor(() => expect(api.saveSettings).toHaveBeenCalled());
    const payload = vi.mocked(api.saveSettings).mock.calls[0][0];
    expect(payload.sports_banner_base_url).toBe('http://thumbs.example:3100');
  });

  it('hides the league rules until a server is configured', async () => {
    renderOnChannelDefaults();

    await screen.findByLabelText(/Sports matchup banners/i);
    expect(screen.queryByLabelText(/Title pattern 1/i)).not.toBeInTheDocument();
  });

  it('sends a blank as an empty string, not undefined', async () => {
    vi.mocked(api.getSettings).mockResolvedValue(
      makeSettings({ sports_banner_base_url: 'http://thumbs.example:3100' }));
    renderOnChannelDefaults();

    const input = await screen.findByLabelText(/Sports matchup banners/i) as HTMLInputElement;
    await waitFor(() => expect(input.value).toBe('http://thumbs.example:3100'));
    fireEvent.change(input, { target: { value: '' } });

    fireEvent.click(screen.getByRole('button', { name: /Save Settings/i }));

    await waitFor(() => expect(api.saveSettings).toHaveBeenCalled());
    const payload = vi.mocked(api.saveSettings).mock.calls[0][0];
    expect(payload.sports_banner_base_url).toBe('');
  });
});

describe('Sports matchup banner league rules', () => {
  const CONFIGURED = {
    sports_banner_base_url: 'http://thumbs.example:3100',
    sports_banner_leagues: [
      { match: 'College Football|CFP', league: 'ncaaf' },
      { match: '\\bNHL\\b', league: 'nhl' },
    ],
  };

  beforeEach(() => {
    vi.clearAllMocks();
    mockUser = { is_admin: true, username: 'admin' };
    vi.mocked(api.getSettings).mockResolvedValue(makeSettings(CONFIGURED));
    vi.mocked(api.saveSettings).mockResolvedValue({ status: 'ok', configured: true, server_changed: false });
    vi.mocked(api.getChannelProfiles).mockResolvedValue([]);
    vi.mocked(api.listAlertMethods).mockResolvedValue([]);
    vi.mocked(api.getM3UAccounts).mockResolvedValue([]);
  });

  it('shows the rules the settings were loaded with, in order', async () => {
    renderOnChannelDefaults();

    const first = await screen.findByLabelText(/Title pattern 1/i) as HTMLInputElement;
    await waitFor(() => expect(first.value).toBe('College Football|CFP'));
    expect((screen.getByLabelText(/League 1/i) as HTMLInputElement).value).toBe('ncaaf');
    expect((screen.getByLabelText(/Title pattern 2/i) as HTMLInputElement).value).toBe('\\bNHL\\b');
  });

  it('sends an edited rule on save', async () => {
    renderOnChannelDefaults();

    const league = await screen.findByLabelText(/League 2/i) as HTMLInputElement;
    await waitFor(() => expect(league.value).toBe('nhl'));
    fireEvent.change(league, { target: { value: 'nhl-alt' } });
    fireEvent.click(screen.getByRole('button', { name: /Save Settings/i }));

    await waitFor(() => expect(api.saveSettings).toHaveBeenCalled());
    const payload = vi.mocked(api.saveSettings).mock.calls[0][0];
    expect(payload.sports_banner_leagues).toEqual([
      { match: 'College Football|CFP', league: 'ncaaf' },
      { match: '\\bNHL\\b', league: 'nhl-alt' },
    ]);
  });

  it('adds a rule and sends it', async () => {
    renderOnChannelDefaults();

    await screen.findByLabelText(/Title pattern 1/i);
    fireEvent.click(screen.getByRole('button', { name: /Add rule/i }));
    fireEvent.change(screen.getByLabelText(/Title pattern 3/i), { target: { value: 'Leagues Cup' } });
    fireEvent.change(screen.getByLabelText(/League 3/i), { target: { value: 'epl' } });
    fireEvent.click(screen.getByRole('button', { name: /Save Settings/i }));

    await waitFor(() => expect(api.saveSettings).toHaveBeenCalled());
    const payload = vi.mocked(api.saveSettings).mock.calls[0][0];
    expect(payload.sports_banner_leagues).toContainEqual({ match: 'Leagues Cup', league: 'epl' });
  });

  it('removes a rule and sends the shorter list', async () => {
    renderOnChannelDefaults();

    await screen.findByLabelText(/Title pattern 1/i);
    fireEvent.click(screen.getByRole('button', { name: /Remove rule 1/i }));
    fireEvent.click(screen.getByRole('button', { name: /Save Settings/i }));

    await waitFor(() => expect(api.saveSettings).toHaveBeenCalled());
    const payload = vi.mocked(api.saveSettings).mock.calls[0][0];
    expect(payload.sports_banner_leagues).toEqual([{ match: '\\bNHL\\b', league: 'nhl' }]);
  });

  it('drops a half-finished row instead of storing it', async () => {
    // An unfilled "Add rule" must not reach the stored list.
    renderOnChannelDefaults();

    await screen.findByLabelText(/Title pattern 1/i);
    fireEvent.click(screen.getByRole('button', { name: /Add rule/i }));
    fireEvent.change(screen.getByLabelText(/Title pattern 3/i), { target: { value: 'Half typed' } });
    fireEvent.click(screen.getByRole('button', { name: /Save Settings/i }));

    await waitFor(() => expect(api.saveSettings).toHaveBeenCalled());
    const payload = vi.mocked(api.saveSettings).mock.calls[0][0];
    expect(payload.sports_banner_leagues).toHaveLength(2);
  });

  it('sends an empty list when every rule is removed, not the defaults back', async () => {
    renderOnChannelDefaults();

    await screen.findByLabelText(/Title pattern 1/i);
    fireEvent.click(screen.getByRole('button', { name: /Remove rule 2/i }));
    fireEvent.click(screen.getByRole('button', { name: /Remove rule 1/i }));
    fireEvent.click(screen.getByRole('button', { name: /Save Settings/i }));

    await waitFor(() => expect(api.saveSettings).toHaveBeenCalled());
    const payload = vi.mocked(api.saveSettings).mock.calls[0][0];
    expect(payload.sports_banner_leagues).toEqual([]);
  });
});
