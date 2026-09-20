import { useState } from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type {
  DummyEPGPreviewResult,
  DummyEPGProfile,
  DummyEPGProfileCreateRequest,
  PatternVariant,
} from '../types';
import type { ProfileEventSyncConfig } from '../types/eventSync';
import { DummyEPGProfileModal } from './DummyEPGProfileModal';

const mocks = vi.hoisted(() => ({
  getChannelGroups: vi.fn(),
  getProviderGroupSettingsByProvider: vi.fn(),
  getEPGSources: vi.fn(),
  getDummyEPGCoverage: vi.fn(),
  previewDummyEPGBatch: vi.fn(),
  updateDummyEPGProfile: vi.fn(),
  createDummyEPGProfile: vi.fn(),
}));

vi.mock('../services/api', () => mocks);

vi.mock('./patternBuilder', () => ({
  PatternBuilder: ({
    titlePattern,
    timePattern,
    datePattern,
    onTitlePatternChange,
    onTimePatternChange,
    onDatePatternChange,
  }: {
    titlePattern: string;
    timePattern: string;
    datePattern: string;
    onTitlePatternChange: (value: string) => void;
    onTimePatternChange: (value: string) => void;
    onDatePatternChange: (value: string) => void;
  }) => (
    <div>
      <label>Title Pattern<input value={titlePattern} onChange={event => onTitlePatternChange(event.target.value)} /></label>
      <label>Time Pattern<input value={timePattern} onChange={event => onTimePatternChange(event.target.value)} /></label>
      <label>Date Pattern<input value={datePattern} onChange={event => onDatePatternChange(event.target.value)} /></label>
    </div>
  ),
}));

vi.mock('./patternBuilder/VariantTabs', () => ({
  VariantTabs: () => null,
}));

const variant: PatternVariant = {
  name: 'Default',
  title_pattern: '(?P<title>.+?) \\| (?<league>.+)',
  time_pattern: '(?P<hour>\\d{1,2}):(?P<minute>\\d{2})',
  date_pattern: null,
  title_template: '{title}',
  description_template: null,
  channel_logo_url_template: null,
  program_poster_url_template: null,
  pattern_builder_examples: null,
  upcoming_title_template: null,
  upcoming_description_template: null,
  ended_title_template: null,
  ended_description_template: null,
  fallback_title_template: null,
  fallback_description_template: null,
  program_duration: null,
};

const eventConfig: ProfileEventSyncConfig = {
  secondary: [
    { group_id: 10, m3u_account_id: 7 },
    { group_id: 20, m3u_account_id: null },
  ],
  time_window_minutes: 30,
  enforce_time_window: false,
  attach_threshold: 0,
  assume_current_date: true,
  demote_stale_dateless: false,
  use_default_patterns: false,
  slot_patterns: [{
    name: 'Arena',
    channel_pattern: '^Arena\\s+(?P<slot>\\d+)$',
    fallback_pattern: '^Arena Fallback (?<slot>\\d+)$',
    event_patterns: ['^Arena (?<slot>\\d+) \\| .+$'],
    bootstrap: true,
  }],
};

function makeProfile(overrides: Partial<DummyEPGProfile> = {}): DummyEPGProfile {
  return {
    id: 1,
    name: 'Arena guide',
    enabled: true,
    name_source: 'channel',
    stream_index: 1,
    title_pattern: variant.title_pattern,
    time_pattern: variant.time_pattern,
    date_pattern: null,
    substitution_pairs: [],
    title_template: '{title}',
    description_template: null,
    upcoming_title_template: null,
    upcoming_description_template: null,
    ended_title_template: null,
    ended_description_template: null,
    fallback_title_template: null,
    fallback_description_template: null,
    event_timezone: 'US/Eastern',
    output_timezone: null,
    program_duration: 180,
    categories: null,
    channel_logo_url_template: null,
    program_poster_url_template: null,
    tvg_id_template: 'ecm-{channel_id}',
    include_date_tag: false,
    include_live_tag: false,
    include_new_tag: false,
    pattern_builder_examples: null,
    pattern_variants: [variant],
    channel_group_ids: [10],
    hide_empty_group_ids: [10],
    stream_match_group_ids: [10, 20],
    event_sync_config: eventConfig,
    epg_source_ids: [],
    last_generated_at: null,
    created_at: null,
    updated_at: null,
    ...overrides,
  };
}

async function saveRequest(): Promise<DummyEPGProfileCreateRequest> {
  fireEvent.click(screen.getByRole('button', { name: 'Save Changes' }));
  await waitFor(() => expect(mocks.updateDummyEPGProfile).toHaveBeenCalled());
  const calls = mocks.updateDummyEPGProfile.mock.calls;
  return calls[calls.length - 1][1] as DummyEPGProfileCreateRequest;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}

function preview(name: string): DummyEPGPreviewResult {
  return {
    original_name: name,
    substituted_name: name,
    substitution_steps: [],
    matched: true,
    matched_variant: 'Default',
    groups: { title: name },
    time_variables: null,
    rendered: {
      title: name,
      description: '',
      upcoming_title: '',
      upcoming_description: '',
      ended_title: '',
      ended_description: '',
      fallback_title: '',
      fallback_description: '',
      channel_logo_url: '',
      program_poster_url: '',
    },
    event: {
      family: 'Arena', slot: '7', role: 'event', start: '2026-09-20T20:00:00Z',
      stop: '2026-09-20T23:00:00Z', matched_pattern: 'Default', validation_issues: [],
    },
  };
}

describe('DummyEPGProfileModal event matching', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.getChannelGroups.mockResolvedValue([
      { id: 10, name: 'Arena Slots', channel_count: 20 },
      { id: 20, name: 'Arena Events', channel_count: 30 },
    ]);
    mocks.getProviderGroupSettingsByProvider.mockResolvedValue([
      { m3u_account_id: 3, m3u_account_name: 'North', channel_group_id: 10, auto_channel_sync: false, enabled: true, stream_count: 20 },
      { m3u_account_id: 7, m3u_account_name: 'South', channel_group_id: 10, auto_channel_sync: false, enabled: true, stream_count: 18 },
      { m3u_account_id: 7, m3u_account_name: 'South', channel_group_id: 20, auto_channel_sync: false, enabled: true, stream_count: 30 },
    ]);
    mocks.getEPGSources.mockResolvedValue([]);
    mocks.updateDummyEPGProfile.mockResolvedValue({});
    mocks.createDummyEPGProfile.mockResolvedValue({});
    mocks.previewDummyEPGBatch.mockResolvedValue([]);
  });

  it('round-trips Python and JavaScript named captures, zero, false, scope order, and slot expressions exactly', async () => {
    render(<DummyEPGProfileModal isOpen profile={makeProfile()} onClose={vi.fn()} onSave={vi.fn()} />);
    expect((await screen.findAllByText('Arena Slots · South')).length).toBeGreaterThan(0);

    const saved = await saveRequest();

    expect(saved.title_pattern).toBe(variant.title_pattern);
    expect(saved.time_pattern).toBe(variant.time_pattern);
    expect(saved.pattern_variants).toEqual([variant]);
    expect(saved.event_sync_config).toEqual(eventConfig);
  });

  it('keeps edits and stored scopes through a catalogue failure, then clears only that error after retry', async () => {
    mocks.getProviderGroupSettingsByProvider
      .mockRejectedValueOnce(new Error('offline'))
      .mockResolvedValueOnce([
        { m3u_account_id: 7, m3u_account_name: 'South', channel_group_id: 10, auto_channel_sync: false, enabled: true, stream_count: 18 },
      ]);
    render(<DummyEPGProfileModal isOpen profile={makeProfile()} onClose={vi.fn()} onSave={vi.fn()} />);

    expect(await screen.findByText(/Group accounts could not be loaded/)).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText(/Name \*/i), { target: { value: 'Edited while offline' } });
    expect(screen.getByText(/Group 10 · Account 7/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Retry groups' }));

    await waitFor(() => expect(screen.queryByText(/Group accounts could not be loaded/)).not.toBeInTheDocument());
    expect(screen.getByLabelText(/Name \*/i)).toHaveValue('Edited while offline');
    expect((await saveRequest()).event_sync_config?.secondary).toEqual(eventConfig.secondary);
  });

  it('sets, clears, and reorders account-scoped groups without widening them', async () => {
    const user = userEvent.setup();
    render(<DummyEPGProfileModal isOpen profile={makeProfile({
      event_sync_config: { ...eventConfig, secondary: [] },
      stream_match_group_ids: [],
    })} onClose={vi.fn()} onSave={vi.fn()} />);

    await user.click(await screen.findByRole('button', { name: /Arena Slots/i }));
    await user.click(screen.getByTestId('psg-secondary-10-7'));
    await user.click(screen.getByTestId('psg-secondary-20-any'));
    await user.click(screen.getByRole('button', { name: 'Move scope 2 up' }));
    await user.click(screen.getByRole('button', { name: 'Remove scope 2' }));
    await user.click(screen.getByTestId('psg-secondary-10-7'));

    expect((await saveRequest()).event_sync_config?.secondary).toEqual([
      { group_id: 20, m3u_account_id: null },
      { group_id: 10, m3u_account_id: 7 },
    ]);
  });

  it('does not replace a dirty draft when the same profile object refreshes and restores it after cancel and reopen', async () => {
    const first = makeProfile();
    const onClose = vi.fn();
    const { rerender } = render(<DummyEPGProfileModal isOpen profile={first} onClose={onClose} onSave={vi.fn()} />);
    const name = await screen.findByLabelText(/Name \*/i);
    fireEvent.change(name, { target: { value: 'Unsaved edit' } });

    rerender(<DummyEPGProfileModal isOpen profile={{ ...first, name: 'Server refresh' }} onClose={onClose} onSave={vi.fn()} />);
    expect(screen.getByLabelText(/Name \*/i)).toHaveValue('Unsaved edit');

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    rerender(<DummyEPGProfileModal isOpen={false} profile={first} onClose={onClose} onSave={vi.fn()} />);
    rerender(<DummyEPGProfileModal isOpen profile={first} onClose={onClose} onSave={vi.fn()} />);
    expect(await screen.findByLabelText(/Name \*/i)).toHaveValue('Arena guide');
  });

  it('keeps only the newest A to B to A preview response and shows event diagnostics', async () => {
    const firstA = deferred<DummyEPGPreviewResult[]>();
    const b = deferred<DummyEPGPreviewResult[]>();
    const lastA = deferred<DummyEPGPreviewResult[]>();
    mocks.previewDummyEPGBatch
      .mockReturnValueOnce(firstA.promise)
      .mockReturnValueOnce(b.promise)
      .mockReturnValueOnce(lastA.promise);
    const user = userEvent.setup();
    render(<DummyEPGProfileModal isOpen profile={makeProfile()} onClose={vi.fn()} onSave={vi.fn()} />);
    const samples = await screen.findByLabelText('Sample Names');

    await user.type(samples, 'A');
    await user.click(screen.getByRole('button', { name: /Test/ }));
    await user.clear(samples);
    await user.type(samples, 'B');
    await user.click(screen.getByRole('button', { name: /Test/ }));
    await user.clear(samples);
    await user.type(samples, 'A');
    await user.click(screen.getByRole('button', { name: /Test/ }));

    lastA.resolve([preview('latest A')]);
    expect((await screen.findAllByText('latest A')).length).toBeGreaterThan(0);
    b.resolve([preview('stale B')]);
    firstA.resolve([preview('stale A')]);
    await Promise.resolve();
    expect(screen.queryByText('stale B')).not.toBeInTheDocument();
    expect(screen.queryByText('stale A')).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: /latest A/i }));
    expect(screen.getByText('Arena')).toBeInTheDocument();
    expect(screen.getByText('7')).toBeInTheDocument();
    expect(screen.getByText('event')).toBeInTheDocument();
    expect(mocks.previewDummyEPGBatch.mock.calls[2][0]).toMatchObject({
      sample_names: ['A'],
      event_sync_config: eventConfig,
    });
  });

  it('keeps the modal, draft, and invalid field open after a failed save and succeeds on retry', async () => {
    mocks.updateDummyEPGProfile
      .mockRejectedValueOnce(new Error('event_sync_config.slot_patterns[0].channel_pattern: invalid expression'))
      .mockResolvedValueOnce({});
    const onClose = vi.fn();
    render(<DummyEPGProfileModal isOpen profile={makeProfile()} onClose={onClose} onSave={vi.fn()} />);

    fireEvent.click(await screen.findByRole('button', { name: 'Save Changes' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('event_sync_config.slot_patterns[0].channel_pattern');
    expect(screen.getByLabelText('Channel expression')).toHaveAttribute('aria-invalid', 'true');
    expect(onClose).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: 'Save Changes' }));
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it('creates a new profile with the canonical generic matching defaults', async () => {
    render(<DummyEPGProfileModal isOpen profile={null} onClose={vi.fn()} onSave={vi.fn()} />);
    fireEvent.change(await screen.findByLabelText(/Name \*/i), { target: { value: 'New guide' } });
    fireEvent.change(screen.getByLabelText('Title Pattern'), { target: { value: '(?P<title>.+)' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create Profile' }));

    await waitFor(() => expect(mocks.createDummyEPGProfile).toHaveBeenCalled());
    const saved = mocks.createDummyEPGProfile.mock.calls[0][0] as DummyEPGProfileCreateRequest;
    expect(saved.event_sync_config).toEqual({
      secondary: [],
      time_window_minutes: 30,
      enforce_time_window: true,
      attach_threshold: 0.8,
      assume_current_date: false,
      demote_stale_dateless: true,
      use_default_patterns: false,
      slot_patterns: [],
    });
  });

  it('creates an imported legacy profile and a new generic family without a client regex gate', async () => {
    function Host() {
      const [open, setOpen] = useState(true);
      return <DummyEPGProfileModal isOpen={open} profile={null} importData={{
        ...makeProfile({ id: 0, event_sync_config: undefined }),
        name: 'Imported Arena',
        stream_match_group_ids: [20],
      }} onClose={() => setOpen(false)} onSave={vi.fn()} />;
    }
    render(<Host />);
    expect(await screen.findByTestId('psg-secondary-20-any')).toBeChecked();
    fireEvent.click(screen.getByRole('button', { name: 'Add family' }));
    fireEvent.change(screen.getByLabelText('Family key'), { target: { value: 'Arena' } });
    fireEvent.change(screen.getByLabelText('Channel expression'), { target: { value: '^Arena (?P<slot>\\d+)$' } });
    fireEvent.click(screen.getByRole('button', { name: 'Add event expression' }));
    fireEvent.change(screen.getByLabelText('Family 1 event expression 1'), { target: { value: '(?<slot>[invalid' } });
    fireEvent.click(screen.getByRole('button', { name: 'Create Profile' }));

    await waitFor(() => expect(mocks.createDummyEPGProfile).toHaveBeenCalled());
    const saved = mocks.createDummyEPGProfile.mock.calls[0][0] as DummyEPGProfileCreateRequest;
    expect(saved.event_sync_config?.secondary).toEqual([{ group_id: 20, m3u_account_id: null }]);
    expect(saved.event_sync_config).toMatchObject({
      assume_current_date: true,
      use_default_patterns: true,
    });
    expect(saved.event_sync_config?.slot_patterns[0]).toMatchObject({
      name: 'Arena',
      channel_pattern: '^Arena (?P<slot>\\d+)$',
      event_patterns: ['(?<slot>[invalid'],
    });
  });

  it('contains keyboard focus and restores the opener after Escape', async () => {
    function Host() {
      const [open, setOpen] = useState(false);
      return <>
        <button type="button" onClick={() => setOpen(true)}>Open profile</button>
        <DummyEPGProfileModal
          isOpen={open}
          profile={null}
          onClose={() => setOpen(false)}
          onSave={vi.fn()}
        />
      </>;
    }

    const user = userEvent.setup();
    render(<Host />);
    const opener = screen.getByRole('button', { name: 'Open profile' });
    await user.click(opener);

    const close = await screen.findByRole('button', { name: 'Close' });
    await waitFor(() => expect(close).toHaveFocus());
    fireEvent.keyDown(document, { key: 'Tab', shiftKey: true });
    expect(screen.getByRole('button', { name: 'Create Profile' })).toHaveFocus();

    fireEvent.keyDown(document, { key: 'Escape' });
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    expect(opener).toHaveFocus();
  });
});
