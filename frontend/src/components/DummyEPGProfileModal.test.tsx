/**
 * Unit tests for the per-variant program duration in DummyEPGProfileModal.
 *
 * The duration is optional and absent means "use the profile's own
 * program_duration", so the field must never write a number into a variant
 * the operator did not touch.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import type {
  DummyEPGCoverage,
  DummyEPGPublication,
  DummyEPGProfile,
  DummyEPGProfileCreateRequest,
  PatternVariant,
} from '../types';
import { DummyEPGProfileModal } from './DummyEPGProfileModal';

const mocks = vi.hoisted(() => ({
  getChannelGroups: vi.fn().mockResolvedValue([]),
  getProviderGroupSettingsByProvider: vi.fn().mockResolvedValue([]),
  updateDummyEPGProfile: vi.fn(),
  createDummyEPGProfile: vi.fn(),
  getEPGSources: vi.fn().mockResolvedValue([]),
  getDummyEPGCoverage: vi.fn(),
}));

vi.mock('../services/api', () => ({
  getChannelGroups: mocks.getChannelGroups,
  getProviderGroupSettingsByProvider: mocks.getProviderGroupSettingsByProvider,
  getEPGSources: mocks.getEPGSources,
  getDummyEPGCoverage: mocks.getDummyEPGCoverage,
  previewDummyEPGBatch: vi.fn().mockResolvedValue([]),
  updateDummyEPGProfile: mocks.updateDummyEPGProfile,
  createDummyEPGProfile: mocks.createDummyEPGProfile,
}));

vi.mock('./patternBuilder', () => ({
  PatternBuilder: () => null,
}));
vi.mock('./patternBuilder/VariantTabs', () => ({
  VariantTabs: () => null,
}));

const DURATION_LABEL = 'Program Duration (minutes, Optional)';

/** A variant stored before the per-variant duration existed: the key is
 * simply not there. */
type StoredVariantWithoutDuration = Omit<PatternVariant, 'program_duration'>;

const legacyVariant: StoredVariantWithoutDuration = {
  name: 'Default',
  title_pattern: '(?<team1>.+?) vs (?<team2>.+)',
  time_pattern: null,
  date_pattern: null,
  title_template: null,
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
};

function makeProfile(variants: PatternVariant[]): DummyEPGProfile {
  return {
    id: 1,
    name: 'Sports',
    enabled: true,
    name_source: 'channel',
    stream_index: 1,
    title_pattern: null,
    time_pattern: null,
    date_pattern: null,
    substitution_pairs: [],
    title_template: null,
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
    tvg_id_template: 'ecm-{channel_number}',
    include_date_tag: false,
    include_live_tag: false,
    include_new_tag: false,
    pattern_builder_examples: null,
    pattern_variants: variants,
    channel_group_ids: [],
    last_generated_at: null,
    created_at: null,
    updated_at: null,
  };
}

function makeCoverage(
  overrides: Partial<Omit<DummyEPGCoverage, 'publication'>> & {
    publication?: Partial<DummyEPGPublication>;
  } = {},
): DummyEPGCoverage {
  const publication: DummyEPGPublication = {
    status: 'published',
    published_at: '2026-09-05T00:30:00Z',
    revision: 7,
    window_start: '2026-09-05T00:00:00Z',
    window_stop: '2026-09-07T00:00:00Z',
    config_matches: true,
    reason_codes: [],
    delivery: { dispatcharr_status: 'confirmed', pending_emby: false },
    channels: [{
      channel_id: 10,
      xmltv_id: 'ecm-10',
      visibility_evidence: 'published',
      events: [{
        start: '2026-09-05T01:00:00Z',
        stop: '2026-09-05T05:00:00Z',
        title: 'ONE Fight Night 47',
      }],
    }],
    ...overrides.publication,
  };
  return {
    generated_at: '2026-09-05T01:00:00Z',
    window_start: '2026-09-05T00:00:00Z',
    window_stop: '2026-09-07T00:00:00Z',
    sources: [{ source_id: 51, status: 'ready', last_success: '2026-09-05T00:55:00Z', error: null }],
    channels: [],
    profiles: {
      '1': {
        profile_id: 1,
        source_ids: [51],
        sources: [{ source_id: 51, status: 'ready', last_success: '2026-09-05T00:55:00Z', error: null }],
        owned_channel_ids: [10],
        can_publish: true,
        reason_codes: [],
      },
    },
    artwork_pending: false,
    ...overrides,
    publication,
  };
}

/** Renders the modal and waits for the channel-group load to settle, so the
 * duration assertions never race the effect. Returns the duration input. */
async function renderModal(
  variants: PatternVariant[]
): Promise<HTMLInputElement> {
  render(
    <DummyEPGProfileModal
      isOpen
      profile={makeProfile(variants)}
      onClose={vi.fn()}
      onSave={vi.fn()}
    />
  );
  return (await screen.findByLabelText(DURATION_LABEL)) as HTMLInputElement;
}

async function saveAndReadRequest(): Promise<DummyEPGProfileCreateRequest> {
  fireEvent.click(screen.getByRole('button', { name: 'Save Changes' }));
  await waitFor(() => expect(mocks.updateDummyEPGProfile).toHaveBeenCalled());
  const call = mocks.updateDummyEPGProfile.mock.calls[0] as [
    number,
    DummyEPGProfileCreateRequest,
  ];
  return call[1];
}

describe('DummyEPGProfileModal per-variant program duration', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.getChannelGroups.mockResolvedValue([]);
    mocks.getProviderGroupSettingsByProvider.mockResolvedValue([]);
    mocks.updateDummyEPGProfile.mockResolvedValue({});
    mocks.createDummyEPGProfile.mockResolvedValue({});
  });

  it('leaves the field blank for a variant that sets no duration', async () => {
    const input = await renderModal([legacyVariant as PatternVariant]);

    expect(input.value).toBe('');
  });

  it('sends a variant with no duration back exactly as it arrived', async () => {
    await renderModal([legacyVariant as PatternVariant]);

    const saved = await saveAndReadRequest();

    expect(JSON.stringify(saved.pattern_variants)).toBe(
      JSON.stringify([legacyVariant])
    );
    expect(JSON.stringify(saved.pattern_variants)).not.toContain(
      'program_duration'
    );
  });

  it('puts a typed duration on the variant', async () => {
    const input = await renderModal([legacyVariant as PatternVariant]);

    fireEvent.change(input, { target: { value: '240' } });
    const saved = await saveAndReadRequest();

    expect(saved.pattern_variants?.[0].program_duration).toBe(240);
  });

  it('refuses a duration out of range on a variant that is not on screen', async () => {
    // Only the active variant is rendered, so the input's own min and max
    // never see this one, and the API rejects the whole profile without
    // saying which variant caused it.
    await renderModal([
      legacyVariant as PatternVariant,
      {
        ...legacyVariant,
        name: 'Boxing',
        program_duration: 5000,
      } as PatternVariant,
    ]);

    fireEvent.click(screen.getByRole('button', { name: 'Save Changes' }));

    expect(
      await screen.findByText(
        'Variant "Boxing" needs a Program Duration between 0 and 1440 minutes'
      )
    ).toBeTruthy();
    expect(mocks.updateDummyEPGProfile).not.toHaveBeenCalled();
  });

  it('shows a duration the variant already carries', async () => {
    const input = await renderModal([
      { ...legacyVariant, program_duration: 240 },
    ]);

    expect(input.value).toBe('240');
  });

  it('keeps a duration of zero rather than reading it as unset', async () => {
    const input = await renderModal([{ ...legacyVariant, program_duration: 0 }]);

    expect(input.value).toBe('0');

    const saved = await saveAndReadRequest();
    expect(saved.pattern_variants?.[0].program_duration).toBe(0);
  });

  it('clears the duration to null instead of the profile number', async () => {
    const input = await renderModal([
      { ...legacyVariant, program_duration: 240 },
    ]);

    fireEvent.change(input, { target: { value: '' } });
    const saved = await saveAndReadRequest();

    expect(saved.pattern_variants?.[0].program_duration).toBeNull();
  });

  it('explains that legacy ended templates use the inferred schedule', async () => {
    render(
      <DummyEPGProfileModal
        isOpen
        profile={{
          ...makeProfile([legacyVariant as PatternVariant]),
          ended_title_template: 'Ended: {title}',
        }}
        onClose={vi.fn()}
        onSave={vi.fn()}
      />
    );

    const hints = await screen.findAllByText(/ended templates use the inferred event duration/i);
    expect(hints.length).toBeGreaterThan(0);
  });
});


describe('Dummy EPG programme sources and coverage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.getChannelGroups.mockResolvedValue([]);
    mocks.getProviderGroupSettingsByProvider.mockResolvedValue([]);
    mocks.getEPGSources.mockResolvedValue([
      { id: 51, name: 'USA 3-day', source_type: 'xmltv', is_active: true, url: 'https://guide.example/private-key' },
      { id: 49, name: 'Portrait sports', source_type: 'xmltv', is_active: true, url: '/api/epg/artwork-proxy/42' },
      { id: 46, name: 'Generated guide', source_type: 'xmltv', is_active: true, url: '/api/dummy-epg/xmltv/1' },
    ]);
    mocks.updateDummyEPGProfile.mockResolvedValue({});
    mocks.createDummyEPGProfile.mockResolvedValue({});
  });

  it('saves automatic idle hiding only for selected event groups', async () => {
    mocks.getChannelGroups.mockResolvedValue([
      { id: 65, name: 'Live Events', channel_count: 7 },
      { id: 2479, name: 'ESPN+ Slots', channel_count: 100 },
    ]);
    render(<DummyEPGProfileModal isOpen profile={{
      ...makeProfile([legacyVariant]),
      channel_group_ids: [65, 2479],
      hide_empty_group_ids: [2479],
    }} onClose={vi.fn()} onSave={vi.fn()} />);

    expect(await screen.findByRole('checkbox', {
      name: 'Hide idle channels in ESPN+ Slots',
    })).toBeChecked();
    const liveEvents = screen.getByRole('checkbox', {
      name: 'Hide idle channels in Live Events',
    });
    expect(liveEvents).not.toBeChecked();
    fireEvent.click(liveEvents);

    const saved = await saveAndReadRequest();
    expect(saved.hide_empty_group_ids).toEqual([2479, 65]);
  });

  it('preserves saved sources and leaves remembered mappings to the sparse update', async () => {
    render(<DummyEPGProfileModal isOpen profile={{
      ...makeProfile([legacyVariant]), epg_source_ids: [49],
      channel_mappings: [{ channel_id: 2950, source_id: 42, tvg_id: '32645' }],
    }} onClose={vi.fn()} onSave={vi.fn()} />);
    expect(await screen.findByRole('checkbox', { name: 'Portrait sports' })).toBeChecked();
    expect(screen.queryByRole('checkbox', { name: 'Generated guide' })).not.toBeInTheDocument();
    expect(screen.queryByText(/private-key/)).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('checkbox', { name: 'USA 3-day' }));
    const saved = await saveAndReadRequest();
    expect(saved.epg_source_ids).toEqual([49, 51]);
    expect(saved.channel_mappings).toBeUndefined();
    expect(saved.pattern_variants).toEqual([legacyVariant]);
  });

  it('retains imported source identities when creating a copy', async () => {
    const mapping = { channel_id: 2950, source_id: 42, tvg_id: '32645' };
    render(<DummyEPGProfileModal isOpen profile={null} importData={{
      ...makeProfile([legacyVariant]), name: 'Copy', epg_source_ids: [49], channel_mappings: [mapping],
    }} onClose={vi.fn()} onSave={vi.fn()} />);
    await screen.findByRole('checkbox', { name: 'Portrait sports' });
    fireEvent.click(screen.getByRole('button', { name: 'Create Profile' }));
    await waitFor(() => expect(mocks.createDummyEPGProfile).toHaveBeenCalled());
    expect(mocks.createDummyEPGProfile.mock.calls[0][0]).toMatchObject({ epg_source_ids: [49], channel_mappings: [mapping] });
  });

  it('creates a source profile without a name regex and keeps imported flags', async () => {
    render(<DummyEPGProfileModal isOpen profile={null} importData={{
      name: 'Linear', epg_source_ids: [51], enabled: false, program_duration: 0,
    }} onClose={vi.fn()} onSave={vi.fn()} />);
    await screen.findByRole('checkbox', { name: 'USA 3-day' });
    fireEvent.click(screen.getByRole('button', { name: 'Create Profile' }));
    await waitFor(() => expect(mocks.createDummyEPGProfile).toHaveBeenCalled());
    expect(mocks.createDummyEPGProfile.mock.calls[0][0]).toMatchObject({
      epg_source_ids: [51], enabled: false, program_duration: 0,
    });
  });

  it('shows pending coverage and allows a second check to show current programmes', async () => {
    const retained = makeCoverage({
      sources: [{ source_id: 51, status: 'pending', last_success: '2026-09-04T20:00:00Z', error: 'Provider refresh failed' }],
      profiles: {
        '1': {
          profile_id: 1,
          source_ids: [51],
          sources: [{ source_id: 51, status: 'pending', last_success: '2026-09-04T20:00:00Z', error: 'Provider refresh failed' }],
          owned_channel_ids: [10],
          can_publish: false,
          reason_codes: ['GUIDE_SOURCES_PENDING'],
        },
      },
      publication: {
        status: 'retained',
        delivery: { dispatcharr_status: 'pending', pending_emby: true },
        channels: [{
          channel_id: 10,
          xmltv_id: 'ecm-10',
          visibility_evidence: 'retained',
          events: [{
            start: '2026-09-05T01:00:00Z',
            stop: '2026-09-05T05:00:00Z',
            title: 'ONE Fight Night 47 retained after a failed provider refresh',
          }],
        }],
      },
    });
    mocks.getDummyEPGCoverage.mockResolvedValueOnce(retained).mockResolvedValueOnce(makeCoverage({
      channels: [{ channel_id: 10, xmltv_id: 'ecm-10', source_id: 51, source_tvg_id: 'PPV10.art', match: 'event',
        current: { start: '2026-09-05T01:00:00Z', stop: '2026-09-05T05:00:00Z', title: 'ONE Fight Night 47' },
        next: null, real_minutes: 240, gap_minutes: 2640, warnings: [] }],
    }));
    render(<DummyEPGProfileModal isOpen profile={{ ...makeProfile([legacyVariant]), epg_source_ids: [51] }} onClose={vi.fn()} onSave={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: 'Check saved guide coverage' }));
    expect(await screen.findByText(/Sources are still loading/)).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Retained' })).toBeInTheDocument();
    expect(screen.getByText('Retained event evidence')).toBeInTheDocument();
    expect(screen.getByText('Guide import pending')).toBeInTheDocument();
    expect(screen.getByText('Emby refresh pending')).toBeInTheDocument();
    expect(screen.getByText(/Provider refresh failed/)).toBeInTheDocument();
    expect(screen.getByLabelText('Stored guide publication')).not.toHaveTextContent('Original publication time: Unavailable');
    fireEvent.click(screen.getByRole('button', { name: 'Check saved guide coverage' }));
    expect(await screen.findByRole('heading', { name: 'Published' })).toBeInTheDocument();
    expect((await screen.findAllByText(/ONE Fight Night 47/)).length).toBeGreaterThan(0);
    expect(screen.getByText('240 / 2640')).toBeInTheDocument();
    expect(mocks.getDummyEPGCoverage).toHaveBeenCalledTimes(2);
    expect(mocks.updateDummyEPGProfile).not.toHaveBeenCalled();
  });

  it('keeps fresh readiness separate from an unavailable publication', async () => {
    mocks.getDummyEPGCoverage.mockResolvedValueOnce(makeCoverage({
      publication: {
        status: 'unavailable',
        published_at: null,
        revision: null,
        window_start: null,
        window_stop: null,
        config_matches: null,
        reason_codes: ['GUIDE_UNAVAILABLE'],
        delivery: null,
        channels: [{
          channel_id: 10,
          xmltv_id: null,
          visibility_evidence: 'unknown',
          events: [],
        }],
      },
    }));
    render(<DummyEPGProfileModal isOpen profile={{ ...makeProfile([legacyVariant]), epg_source_ids: [51] }} onClose={vi.fn()} onSave={vi.fn()} />);

    fireEvent.click(screen.getByRole('button', { name: 'Check saved guide coverage' }));
    expect(await screen.findByRole('heading', { name: 'Unavailable' })).toBeInTheDocument();
    expect(screen.getByLabelText('Stored guide publication')).toHaveTextContent('Original publication time: Unavailable');
    expect(screen.getByText('No durable guide publication is available.')).toBeInTheDocument();
    expect(screen.getByText('Unknown visibility evidence')).toBeInTheDocument();
    expect(screen.getByText('Ready to publish')).toBeInTheDocument();
  });

  it('shows historical events without claiming visibility after a config change', async () => {
    mocks.getDummyEPGCoverage.mockResolvedValueOnce(makeCoverage({
      publication: {
        status: 'retained',
        config_matches: false,
        reason_codes: ['GUIDE_CONFIG_CHANGED'],
        delivery: { dispatcharr_status: 'unknown', pending_emby: false },
        channels: [{
          channel_id: 10,
          xmltv_id: 'ecm-10',
          visibility_evidence: 'unknown',
          events: [{
            start: '2026-09-05T01:00:00Z',
            stop: '2026-09-05T05:00:00Z',
            title: 'A historical programme title that stays readable even when current visibility cannot be proven',
          }],
        }],
      },
    }));
    render(<DummyEPGProfileModal isOpen profile={{ ...makeProfile([legacyVariant]), epg_source_ids: [51] }} onClose={vi.fn()} onSave={vi.fn()} />);

    fireEvent.click(screen.getByRole('button', { name: 'Check saved guide coverage' }));
    expect(await screen.findByText('The stored publication belongs to an earlier profile configuration.')).toBeInTheDocument();
    expect(screen.getByText('Guide import not confirmed')).toBeInTheDocument();
    expect(screen.getByText('Unknown visibility evidence')).toBeInTheDocument();
    expect(screen.getByText(/A historical programme title/)).toBeInTheDocument();
    expect(screen.getByText(/Unknown evidence does not mean/)).toBeInTheDocument();
  });

  it('ignores a late coverage response after switching profiles', async () => {
    let resolveFirst!: (coverage: DummyEPGCoverage) => void;
    let resolveSecond!: (coverage: DummyEPGCoverage) => void;
    mocks.getDummyEPGCoverage
      .mockReturnValueOnce(new Promise(resolve => { resolveFirst = resolve; }))
      .mockReturnValueOnce(new Promise(resolve => { resolveSecond = resolve; }));
    const first = { ...makeProfile([legacyVariant]), epg_source_ids: [51] };
    const second = { ...first, id: 2, name: 'Second profile' };
    const { rerender } = render(<DummyEPGProfileModal isOpen profile={first} onClose={vi.fn()} onSave={vi.fn()} />);

    fireEvent.click(screen.getByRole('button', { name: 'Check saved guide coverage' }));
    rerender(<DummyEPGProfileModal isOpen profile={second} onClose={vi.fn()} onSave={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: 'Check saved guide coverage' }));
    resolveSecond(makeCoverage());
    expect(await screen.findByRole('heading', { name: 'Published' })).toBeInTheDocument();
    resolveFirst(makeCoverage({
      publication: {
        status: 'unavailable',
        published_at: null,
        revision: null,
        window_start: null,
        window_stop: null,
        config_matches: null,
        reason_codes: ['GUIDE_UNAVAILABLE'],
        delivery: null,
        channels: [],
      },
    }));
    await Promise.resolve();
    expect(screen.queryByRole('heading', { name: 'Unavailable' })).not.toBeInTheDocument();
  });

  it('shows source-load and coverage errors without clearing saved selection', async () => {
    mocks.getEPGSources.mockRejectedValueOnce(new Error('secret-url'));
    mocks.getDummyEPGCoverage.mockRejectedValueOnce(new Error('secret-url'));
    render(<DummyEPGProfileModal isOpen profile={{ ...makeProfile([legacyVariant]), epg_source_ids: [51] }} onClose={vi.fn()} onSave={vi.fn()} />);
    expect(await screen.findByText(/Programme sources could not be loaded/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Check saved guide coverage' }));
    expect(await screen.findByText(/Guide coverage could not be loaded/)).toBeInTheDocument();
    expect(screen.queryByText(/secret-url/)).not.toBeInTheDocument();
    expect((await saveAndReadRequest()).epg_source_ids).toEqual([51]);
  });
});
