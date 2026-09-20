import { useState, useEffect, useCallback, memo, useMemo, useRef } from 'react';
import type {
  DummyEPGProfile,
  DummyEPGProfileCreateRequest,
  DummyEPGPreviewResult,
  DummyEPGCoverage,
  EPGSource,
  SubstitutionPair,
  PatternVariant,
  ChannelGroup,
  GuideReasonCode,
  GuidePublicationReasonCode,
} from '../types';
import type {
  EventSlotPattern,
  ProfileEventSyncConfig,
} from '../types/eventSync';
import * as api from '../services/api';
import { useAsyncOperation } from '../hooks/useAsyncOperation';
import { ModalOverlay } from './ModalOverlay';
import { useOwnedDialog } from '../hooks/useOwnedDialog';
import { SubstitutionPairsEditor } from './SubstitutionPairsEditor';
import { PatternBuilder } from './patternBuilder';
import { VariantTabs } from './patternBuilder/VariantTabs';
import { ProviderScopedGroupPicker } from './channelPipeline/ProviderScopedGroupPicker';
import {
  joinProviderRows,
  type GroupProviderRow,
} from './channelPipeline/providerScopedGroups';
import './ModalBase.css';
import './DummyEPGProfileModal.css';

const ENDED_TEMPLATE_HINT =
  'Without programme sources, ended templates use the inferred event duration. This is a scheduled end, not confirmation that playback has finished.';

const GUIDE_REASON_TEXT: Record<GuideReasonCode, string> = {
  GUIDE_SOURCES_PENDING: 'One or more programme sources are not ready.',
  GUIDE_CHANNEL_UNAVAILABLE: 'A configured channel is not available.',
  GUIDE_CONFIG_INVALID: 'The saved profile configuration is not valid.',
  GUIDE_MAPPING_UNAVAILABLE: 'A saved channel mapping is not available.',
  GUIDE_QUERY_PENDING: 'A programme lookup is still pending.',
  GUIDE_SOURCE_NOT_SELECTED: 'A mapped programme source is not selected.',
  GUIDE_SOURCE_STALE: 'A programme source is stale.',
  GUIDE_OWNERSHIP_CONFLICT: 'More than one enabled profile owns a channel.',
  GUIDE_XMLTV_ID_COLLISION: 'More than one channel resolves to the same XMLTV ID.',
  PROFILE_DISABLED: 'This profile is disabled, so fresh guide sources were not inspected.',
};

const PUBLICATION_REASON_TEXT: Record<GuidePublicationReasonCode, string> = {
  GUIDE_UNAVAILABLE: 'No durable guide publication is available.',
  GUIDE_CONFIG_CHANGED: 'The stored publication belongs to an earlier profile configuration.',
  GUIDE_WINDOW_EXPIRED: 'The stored guide window has ended.',
  GUIDE_WINDOW_PENDING: 'The stored guide window has not started.',
  PROFILE_DISABLED: 'This publication is retained as history for a disabled profile.',
};

const PUBLICATION_STATUS_TEXT = {
  published: 'Published',
  retained: 'Retained',
  unavailable: 'Unavailable',
} as const;

const VISIBILITY_EVIDENCE_TEXT = {
  published: 'Published evidence',
  retained: 'Retained event evidence',
  unknown: 'Unknown visibility evidence',
} as const;

const DISPATCHARR_STATUS_TEXT = {
  pending: 'Guide import pending',
  confirmed: 'Guide import confirmed',
  unknown: 'Guide import not confirmed',
} as const;

const TIMEZONES = [
  { value: '', label: '-- None --' },
  { value: 'US/Eastern', label: 'US/Eastern (ET)' },
  { value: 'US/Central', label: 'US/Central (CT)' },
  { value: 'US/Mountain', label: 'US/Mountain (MT)' },
  { value: 'US/Pacific', label: 'US/Pacific (PT)' },
  { value: 'US/Alaska', label: 'US/Alaska' },
  { value: 'US/Hawaii', label: 'US/Hawaii' },
  { value: 'America/New_York', label: 'America/New_York' },
  { value: 'America/Chicago', label: 'America/Chicago' },
  { value: 'America/Denver', label: 'America/Denver' },
  { value: 'America/Los_Angeles', label: 'America/Los_Angeles' },
  { value: 'America/Toronto', label: 'America/Toronto' },
  { value: 'America/Vancouver', label: 'America/Vancouver' },
  { value: 'Europe/London', label: 'Europe/London (GMT/BST)' },
  { value: 'Europe/Paris', label: 'Europe/Paris (CET)' },
  { value: 'Europe/Berlin', label: 'Europe/Berlin (CET)' },
  { value: 'Europe/Amsterdam', label: 'Europe/Amsterdam (CET)' },
  { value: 'Australia/Sydney', label: 'Australia/Sydney (AEST)' },
  { value: 'Australia/Melbourne', label: 'Australia/Melbourne (AEST)' },
  { value: 'UTC', label: 'UTC' },
];

function makeEmptyVariant(name: string = 'Default'): PatternVariant {
  return {
    name,
    title_pattern: null,
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
    program_duration: null,
  };
}

/** Migrate flat profile fields into a single "Default" variant. */
function migrateToVariant(profile: DummyEPGProfile): PatternVariant {
  return {
    name: 'Default',
    title_pattern: profile.title_pattern,
    time_pattern: profile.time_pattern,
    date_pattern: profile.date_pattern,
    title_template: profile.title_template,
    description_template: profile.description_template,
    channel_logo_url_template: profile.channel_logo_url_template,
    program_poster_url_template: profile.program_poster_url_template,
    pattern_builder_examples: profile.pattern_builder_examples,
    upcoming_title_template: null,
    upcoming_description_template: null,
    ended_title_template: null,
    ended_description_template: null,
    fallback_title_template: null,
    fallback_description_template: null,
    program_duration: null,
  };
}

function extractGroupNames(pattern: string | null): string[] {
  if (!pattern) return [];
  const names: string[] = [];
  const re = /\(\?<(\w+)>/g;
  let m;
  while ((m = re.exec(pattern)) !== null) {
    if (!names.includes(m[1])) names.push(m[1]);
  }
  return names;
}

function makeEventSyncConfig(
  config?: ProfileEventSyncConfig | null,
  compatibilityIds: number[] = [],
): ProfileEventSyncConfig {
  if (config) {
    return {
      secondary: config.secondary.map(scope => ({ ...scope })),
      time_window_minutes: config.time_window_minutes,
      enforce_time_window: config.enforce_time_window,
      attach_threshold: config.attach_threshold,
      assume_current_date: config.assume_current_date,
      demote_stale_dateless: config.demote_stale_dateless,
      use_default_patterns: config.use_default_patterns,
      slot_patterns: config.slot_patterns.map(slot => ({
        ...slot,
        event_patterns: [...slot.event_patterns],
      })),
    };
  }
  const legacyCompatibility = compatibilityIds.length > 0;
  return {
    secondary: compatibilityIds.map(groupId => ({ group_id: groupId, m3u_account_id: null })),
    time_window_minutes: 30,
    enforce_time_window: true,
    attach_threshold: 0.8,
    assume_current_date: legacyCompatibility,
    demote_stale_dateless: true,
    use_default_patterns: legacyCompatibility,
    slot_patterns: [],
  };
}

function makeEventSlot(index: number): EventSlotPattern {
  return {
    name: `Family ${index + 1}`,
    channel_pattern: '',
    fallback_pattern: null,
    event_patterns: [],
    bootstrap: false,
  };
}

function moveItem<T>(items: T[], index: number, direction: -1 | 1): T[] {
  const target = index + direction;
  if (target < 0 || target >= items.length) return items;
  const next = [...items];
  [next[index], next[target]] = [next[target], next[index]];
  return next;
}

interface CollapsibleSectionProps {
  title: string;
  isOpen: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}

const CollapsibleSection = memo(function CollapsibleSection({ title, isOpen, onToggle, children }: CollapsibleSectionProps) {
  return (
    <div className="modal-collapsible">
      <button type="button" className="modal-collapsible-header" onClick={onToggle} aria-expanded={isOpen}>
        <span className="material-icons" aria-hidden="true">{isOpen ? 'expand_less' : 'expand_more'}</span>
        <span>{title}</span>
      </button>
      {isOpen && <div className="modal-collapsible-content">{children}</div>}
    </div>
  );
});

interface DummyEPGProfileModalProps {
  isOpen: boolean;
  profile: DummyEPGProfile | null;
  onClose: () => void;
  onSave: () => void;
  importData?: Partial<DummyEPGProfile> | null;
}

export const DummyEPGProfileModal = memo(function DummyEPGProfileModal({
  isOpen,
  profile,
  onClose,
  onSave,
  importData,
}: DummyEPGProfileModalProps) {
  const { titleId, containerRef } = useOwnedDialog(isOpen);
  // Basic Info
  const [name, setName] = useState('');
  const [enabled, setEnabled] = useState(true);

  // Channel Groups
  const [channelGroups, setChannelGroups] = useState<ChannelGroup[]>([]);
  const [groupRows, setGroupRows] = useState<GroupProviderRow[]>([]);
  const [channelGroupIds, setChannelGroupIds] = useState<number[]>([]);
  const [hideEmptyGroupIds, setHideEmptyGroupIds] = useState<number[]>([]);
  const [groupsLoading, setGroupsLoading] = useState(false);
  const [groupsError, setGroupsError] = useState(false);
  const [groupSearchTerm, setGroupSearchTerm] = useState('');
  const [epgSources, setEpgSources] = useState<EPGSource[]>([]);
  const [epgSourceIds, setEpgSourceIds] = useState<number[]>([]);
  const [sourcesLoading, setSourcesLoading] = useState(false);
  const [sourcesError, setSourcesError] = useState(false);
  const [coverage, setCoverage] = useState<DummyEPGCoverage | null>(null);
  const [coverageLoading, setCoverageLoading] = useState(false);
  const [coverageError, setCoverageError] = useState(false);
  const coverageRequest = useRef(0);
  const catalogueRequest = useRef(0);

  // Stable event-slot matching
  const [eventSyncConfig, setEventSyncConfig] = useState<ProfileEventSyncConfig>(makeEventSyncConfig());

  // Substitution Pairs (profile-level)
  const [substitutionPairs, setSubstitutionPairs] = useState<SubstitutionPair[]>([]);

  // Name source
  const [nameSource, setNameSource] = useState<'channel' | 'stream'>('channel');
  const [streamIndex, setStreamIndex] = useState(1);

  // Pattern variants
  const [variants, setVariants] = useState<PatternVariant[]>([makeEmptyVariant()]);
  const [activeVariantIndex, setActiveVariantIndex] = useState(0);

  // Profile-level templates (defaults for upcoming/ended/fallback)
  const [upcomingTitleTemplate, setUpcomingTitleTemplate] = useState('');
  const [upcomingDescriptionTemplate, setUpcomingDescriptionTemplate] = useState('');
  const [endedTitleTemplate, setEndedTitleTemplate] = useState('');
  const [endedDescriptionTemplate, setEndedDescriptionTemplate] = useState('');
  const [fallbackTitleTemplate, setFallbackTitleTemplate] = useState('');
  const [fallbackDescriptionTemplate, setFallbackDescriptionTemplate] = useState('');

  // EPG Settings
  const [eventTimezone, setEventTimezone] = useState('US/Eastern');
  const [outputTimezone, setOutputTimezone] = useState('');
  const [programDuration, setProgramDuration] = useState(180);
  const [categories, setCategories] = useState('');
  const [tvgIdTemplate, setTvgIdTemplate] = useState('ecm-{channel_id}');
  const [includeDateTag, setIncludeDateTag] = useState(false);
  const [includeLiveTag, setIncludeLiveTag] = useState(false);
  const [includeNewTag, setIncludeNewTag] = useState(false);

  // Batch test
  const [batchInput, setBatchInput] = useState('');
  const [sampleChannelName, setSampleChannelName] = useState('');
  const [batchResults, setBatchResults] = useState<DummyEPGPreviewResult[]>([]);
  const [batchLoading, setBatchLoading] = useState(false);
  const [batchError, setBatchError] = useState(false);
  const [expandedBatchRows, setExpandedBatchRows] = useState<Set<number>>(new Set());
  const batchRequest = useRef(0);
  const draftIdentity = useRef<string | null>(null);

  // UI State
  const { loading: saving, error, execute, setError, clearError } = useAsyncOperation();

  // Collapsible sections
  const [subsOpen, setSubsOpen] = useState(false);
  const [upcomingEndedOpen, setUpcomingEndedOpen] = useState(false);
  const [fallbackOpen, setFallbackOpen] = useState(false);
  const [epgTagsOpen, setEpgTagsOpen] = useState(false);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [variantOverridesOpen, setVariantOverridesOpen] = useState(false);
  const [eventMatchingOpen, setEventMatchingOpen] = useState(false);

  // Timezone dropdowns
  const [eventTimezoneDropdownOpen, setEventTimezoneDropdownOpen] = useState(false);
  const [outputTimezoneDropdownOpen, setOutputTimezoneDropdownOpen] = useState(false);
  const [eventTimezoneSearch, setEventTimezoneSearch] = useState('');
  const [outputTimezoneSearch, setOutputTimezoneSearch] = useState('');
  const eventTimezoneDropdownRef = useRef<HTMLDivElement>(null);
  const outputTimezoneDropdownRef = useRef<HTMLDivElement>(null);

  // Name source dropdown
  const [nameSourceDropdownOpen, setNameSourceDropdownOpen] = useState(false);
  const nameSourceDropdownRef = useRef<HTMLDivElement>(null);

  // Close dropdowns on outside click
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (nameSourceDropdownRef.current && !nameSourceDropdownRef.current.contains(event.target as Node)) {
        setNameSourceDropdownOpen(false);
      }
      if (eventTimezoneDropdownRef.current && !eventTimezoneDropdownRef.current.contains(event.target as Node)) {
        setEventTimezoneDropdownOpen(false);
      }
      if (outputTimezoneDropdownRef.current && !outputTimezoneDropdownRef.current.contains(event.target as Node)) {
        setOutputTimezoneDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const loadGroupCatalogue = useCallback(async () => {
    const request = ++catalogueRequest.current;
    setGroupsLoading(true);
    try {
      const [groups, scopes] = await Promise.all([
        api.getChannelGroups(),
        api.getProviderGroupSettingsByProvider(),
      ]);
      if (catalogueRequest.current !== request) return;
      setChannelGroups(groups);
      setGroupRows(joinProviderRows(
        scopes,
        groupId => groups.find(group => group.id === groupId)?.name,
      ));
      setGroupsError(false);
    } catch {
      if (catalogueRequest.current === request) setGroupsError(true);
    } finally {
      if (catalogueRequest.current === request) setGroupsLoading(false);
    }
  }, []);

  // Load both catalogues without allowing an old open/profile response to
  // replace the current modal state.
  useEffect(() => {
    if (isOpen) {
      void loadGroupCatalogue();
      return () => { catalogueRequest.current += 1; };
    }
    catalogueRequest.current += 1;
  }, [isOpen, profile?.id, loadGroupCatalogue]);

  useEffect(() => {
    if (!isOpen) return;
    let active = true;
    setSourcesLoading(true);
    setSourcesError(false);
    coverageRequest.current += 1;
    setCoverage(null);
    setCoverageError(false);
    setCoverageLoading(false);
    api.getEPGSources()
      .then(sources => { if (active) setEpgSources(sources); })
      .catch(() => { if (active) setSourcesError(true); })
      .finally(() => { if (active) setSourcesLoading(false); });
    return () => { active = false; coverageRequest.current += 1; };
  }, [isOpen, profile?.id]);

  // Load profile data when modal opens
  useEffect(() => {
    if (!isOpen) {
      draftIdentity.current = null;
      batchRequest.current += 1;
      return;
    }
    const identity = profile ? `profile:${profile.id}` : importData ? 'import' : 'new';
    if (draftIdentity.current === identity) return;
    draftIdentity.current = identity;

    if (profile) {
        setName(profile.name);
        setEnabled(profile.enabled);
        setChannelGroupIds(profile.channel_group_ids || []);
        setHideEmptyGroupIds(
          (profile.hide_empty_group_ids || []).filter(id =>
            (profile.channel_group_ids || []).includes(id)
          )
        );
        setEpgSourceIds(profile.epg_source_ids || []);
        setSubstitutionPairs(profile.substitution_pairs || []);
        setNameSource(profile.name_source);
        setStreamIndex(profile.stream_index);

        // Load variants
        if (profile.pattern_variants && profile.pattern_variants.length > 0) {
          setVariants(profile.pattern_variants);
        } else {
          // Migrate flat fields to single variant
          setVariants([migrateToVariant(profile)]);
        }
        setActiveVariantIndex(0);

        // Profile-level templates
        setUpcomingTitleTemplate(profile.upcoming_title_template || '');
        setUpcomingDescriptionTemplate(profile.upcoming_description_template || '');
        setEndedTitleTemplate(profile.ended_title_template || '');
        setEndedDescriptionTemplate(profile.ended_description_template || '');
        setFallbackTitleTemplate(profile.fallback_title_template || '');
        setFallbackDescriptionTemplate(profile.fallback_description_template || '');

        setEventTimezone(profile.event_timezone || 'US/Eastern');
        setOutputTimezone(profile.output_timezone || '');
        setProgramDuration(profile.program_duration ?? 180);
        setCategories(profile.categories || '');
        setTvgIdTemplate(profile.tvg_id_template || 'ecm-{channel_id}');
        setIncludeDateTag(profile.include_date_tag || false);
        setIncludeLiveTag(profile.include_live_tag || false);
        setIncludeNewTag(profile.include_new_tag || false);
        setEventSyncConfig(makeEventSyncConfig(
          profile.event_sync_config,
          profile.stream_match_group_ids,
        ));

        setSubsOpen((profile.substitution_pairs || []).length > 0);
        setUpcomingEndedOpen(Boolean(profile.upcoming_title_template || profile.upcoming_description_template || profile.ended_title_template || profile.ended_description_template));
        setFallbackOpen(Boolean(profile.fallback_title_template || profile.fallback_description_template));
        setEpgTagsOpen(Boolean(profile.include_date_tag || profile.include_live_tag || profile.include_new_tag));
        setAdvancedOpen(Boolean(profile.tvg_id_template && profile.tvg_id_template !== 'ecm-{channel_id}'));
        setEventMatchingOpen(Boolean(
          profile.event_sync_config
          || profile.stream_match_group_ids?.length,
        ));
    } else if (importData) {
        // Import mode: pre-fill from Dispatcharr source data
        const d = importData;
        setName(d.name || '');
        setEnabled(d.enabled ?? true);
        setChannelGroupIds(d.channel_group_ids || []);
        setHideEmptyGroupIds(
          (d.hide_empty_group_ids || []).filter(id =>
            (d.channel_group_ids || []).includes(id)
          )
        );
        setEpgSourceIds(d.epg_source_ids || []);
        setSubstitutionPairs(d.substitution_pairs || []);
        setNameSource(d.name_source || 'channel');
        setStreamIndex(d.stream_index || 1);

        // Build a Default variant from the imported flat fields
        const importedVariant = {
          ...makeEmptyVariant(),
          title_pattern: d.title_pattern ?? null,
          time_pattern: d.time_pattern ?? null,
          date_pattern: d.date_pattern ?? null,
          title_template: d.title_template ?? null,
          description_template: d.description_template ?? null,
          channel_logo_url_template: d.channel_logo_url_template ?? null,
          program_poster_url_template: d.program_poster_url_template ?? null,
        };
        setVariants(d.pattern_variants?.length ? d.pattern_variants : [importedVariant]);
        setActiveVariantIndex(0);

        setUpcomingTitleTemplate(d.upcoming_title_template || '');
        setUpcomingDescriptionTemplate(d.upcoming_description_template || '');
        setEndedTitleTemplate(d.ended_title_template || '');
        setEndedDescriptionTemplate(d.ended_description_template || '');
        setFallbackTitleTemplate(d.fallback_title_template || '');
        setFallbackDescriptionTemplate(d.fallback_description_template || '');
        setEventTimezone(d.event_timezone || 'US/Eastern');
        setOutputTimezone(d.output_timezone || '');
        setProgramDuration(d.program_duration ?? 180);
        setCategories(d.categories || '');
        setTvgIdTemplate(d.tvg_id_template || 'ecm-{channel_id}');
        setIncludeDateTag(d.include_date_tag || false);
        setIncludeLiveTag(d.include_live_tag || false);
        setIncludeNewTag(d.include_new_tag || false);
        setEventSyncConfig(makeEventSyncConfig(
          d.event_sync_config,
          d.stream_match_group_ids,
        ));

        // Open sections that have data
        setSubsOpen((d.substitution_pairs || []).length > 0);
        setUpcomingEndedOpen(Boolean(d.upcoming_title_template || d.upcoming_description_template || d.ended_title_template || d.ended_description_template));
        setFallbackOpen(Boolean(d.fallback_title_template || d.fallback_description_template));
        setEpgTagsOpen(Boolean(d.include_date_tag || d.include_live_tag || d.include_new_tag));
        setAdvancedOpen(false);
        setEventMatchingOpen(Boolean(
          d.event_sync_config
          || d.stream_match_group_ids?.length,
        ));
    } else {
        setName('');
        setEnabled(true);
        setChannelGroupIds([]);
        setHideEmptyGroupIds([]);
        setEpgSourceIds([]);
        setSubstitutionPairs([]);
        setNameSource('channel');
        setStreamIndex(1);
        setVariants([makeEmptyVariant()]);
        setActiveVariantIndex(0);
        setUpcomingTitleTemplate('');
        setUpcomingDescriptionTemplate('');
        setEndedTitleTemplate('');
        setEndedDescriptionTemplate('');
        setFallbackTitleTemplate('');
        setFallbackDescriptionTemplate('');
        setEventTimezone('US/Eastern');
        setOutputTimezone('');
        setProgramDuration(180);
        setCategories('');
        setTvgIdTemplate('ecm-{channel_id}');
        setIncludeDateTag(false);
        setIncludeLiveTag(false);
        setIncludeNewTag(false);
        setEventSyncConfig(makeEventSyncConfig());
        setSubsOpen(false);
        setUpcomingEndedOpen(false);
        setFallbackOpen(false);
        setEpgTagsOpen(false);
        setAdvancedOpen(false);
        setEventMatchingOpen(false);
    }
    setBatchInput('');
    setSampleChannelName('');
    setBatchResults([]);
    setBatchError(false);
    batchRequest.current += 1;
    setExpandedBatchRows(new Set());
    setVariantOverridesOpen(false);
    setGroupSearchTerm('');
    clearError();
  }, [isOpen, profile, importData, clearError]);

  // Active variant helpers
  const activeVariant = variants[activeVariantIndex] || makeEmptyVariant();

  const updateActiveVariant = useCallback((updates: Partial<PatternVariant>) => {
    setVariants(prev => prev.map((v, i) => i === activeVariantIndex ? { ...v, ...updates } : v));
  }, [activeVariantIndex]);

  const updateEventSlot = useCallback((index: number, updates: Partial<EventSlotPattern>) => {
    setEventSyncConfig(current => ({
      ...current,
      slot_patterns: current.slot_patterns.map((slot, slotIndex) =>
        slotIndex === index ? { ...slot, ...updates } : slot
      ),
    }));
  }, []);

  const readiness = profile && coverage?.profiles
    ? coverage.profiles[String(profile.id)]
    : undefined;
  const invalidField = useMemo(() => {
    if (!error) return null;
    return error.match(/event_sync_config\.([^:]+)/)?.[1] ?? null;
  }, [error]);

  useEffect(() => {
    if (invalidField) setEventMatchingOpen(true);
  }, [invalidField]);

  // Variant tab handlers
  const handleAddVariant = useCallback(() => {
    const newVariant = makeEmptyVariant(`Variant ${variants.length + 1}`);
    setVariants(prev => [...prev, newVariant]);
    setActiveVariantIndex(variants.length);
  }, [variants.length]);

  const handleRenameVariant = useCallback((index: number, newName: string) => {
    setVariants(prev => prev.map((v, i) => i === index ? { ...v, name: newName } : v));
  }, []);

  const handleDeleteVariant = useCallback((index: number) => {
    if (variants.length <= 1) return;
    setVariants(prev => prev.filter((_, i) => i !== index));
    if (activeVariantIndex >= index && activeVariantIndex > 0) {
      setActiveVariantIndex(activeVariantIndex - 1);
    }
  }, [variants.length, activeVariantIndex]);

  // Batch test
  const handleBatchTest = useCallback(async () => {
    const names = batchInput.split('\n').map(s => s.trim()).filter(Boolean);
    if (!names.length) return;
    const request = ++batchRequest.current;
    setBatchLoading(true);
    setBatchError(false);
    try {
      const v = variants[0]; // Use first variant's flat fields for backward compat
      const results = await api.previewDummyEPGBatch({
        sample_names: names,
        sample_channel_name: sampleChannelName || undefined,
        event_sync_config: eventSyncConfig,
        substitution_pairs: substitutionPairs,
        title_pattern: v?.title_pattern ?? undefined,
        time_pattern: v?.time_pattern ?? undefined,
        date_pattern: v?.date_pattern ?? undefined,
        title_template: v?.title_template || undefined,
        description_template: v?.description_template || undefined,
        upcoming_title_template: upcomingTitleTemplate || undefined,
        upcoming_description_template: upcomingDescriptionTemplate || undefined,
        ended_title_template: endedTitleTemplate || undefined,
        ended_description_template: endedDescriptionTemplate || undefined,
        fallback_title_template: fallbackTitleTemplate || undefined,
        fallback_description_template: fallbackDescriptionTemplate || undefined,
        event_timezone: eventTimezone,
        output_timezone: outputTimezone || undefined,
        program_duration: programDuration,
        channel_logo_url_template: v?.channel_logo_url_template || undefined,
        program_poster_url_template: v?.program_poster_url_template || undefined,
        pattern_variants: variants.length > 1 || variants[0]?.title_pattern
          ? variants
          : undefined,
      });
      if (batchRequest.current === request) {
        setBatchResults(results);
        setExpandedBatchRows(new Set());
      }
    } catch {
      if (batchRequest.current === request) setBatchError(true);
    } finally {
      if (batchRequest.current === request) setBatchLoading(false);
    }
  }, [batchInput, variants, substitutionPairs, upcomingTitleTemplate, upcomingDescriptionTemplate,
      endedTitleTemplate, endedDescriptionTemplate, fallbackTitleTemplate, fallbackDescriptionTemplate,
      eventTimezone, outputTimezone, programDuration, sampleChannelName, eventSyncConfig]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    clearError();

    if (!name.trim()) {
      setError('Name is required');
      return;
    }

    // Validate all variant patterns
    for (const v of variants) {
      if (!v.title_pattern?.trim() && epgSourceIds.length === 0) {
        setError(`Variant "${v.name}" needs a Title Pattern`);
        return;
      }
      // The min and max on the input are checked by the browser only for
      // the variant currently on screen, and the save sends them all, so a
      // value typed into a variant the operator then switched away from
      // reaches the API and 422s the whole profile with nothing pointing at
      // the field that caused it. [67]
      if (
        v.program_duration != null
        && (v.program_duration < 0 || v.program_duration > 1440)
      ) {
        setError(
          `Variant "${v.name}" needs a Program Duration between 0 and 1440 minutes`
        );
        return;
      }
    }

    await execute(async () => {
      // Build data with backward compat: flat fields from variant[0]
      const v0 = variants[0];
      const data: DummyEPGProfileCreateRequest = {
        name: name.trim(),
        enabled,
        name_source: nameSource,
        stream_index: streamIndex,
        title_pattern: v0.title_pattern ?? undefined,
        time_pattern: v0.time_pattern ?? undefined,
        date_pattern: v0.date_pattern ?? undefined,
        substitution_pairs: substitutionPairs,
        title_template: v0.title_template?.trim() || undefined,
        description_template: v0.description_template?.trim() || undefined,
        upcoming_title_template: upcomingTitleTemplate.trim() || undefined,
        upcoming_description_template: upcomingDescriptionTemplate.trim() || undefined,
        ended_title_template: endedTitleTemplate.trim() || undefined,
        ended_description_template: endedDescriptionTemplate.trim() || undefined,
        fallback_title_template: fallbackTitleTemplate.trim() || undefined,
        fallback_description_template: fallbackDescriptionTemplate.trim() || undefined,
        event_timezone: eventTimezone,
        output_timezone: outputTimezone || undefined,
        program_duration: programDuration,
        categories: categories.trim() || undefined,
        channel_logo_url_template: v0.channel_logo_url_template?.trim() || undefined,
        program_poster_url_template: v0.program_poster_url_template?.trim() || undefined,
        tvg_id_template: tvgIdTemplate.trim() || 'ecm-{channel_id}',
        include_date_tag: includeDateTag,
        include_live_tag: includeLiveTag,
        include_new_tag: includeNewTag,
        pattern_builder_examples: v0.pattern_builder_examples || undefined,
        pattern_variants: variants,
        channel_group_ids: channelGroupIds,
        hide_empty_group_ids: hideEmptyGroupIds,
        event_sync_config: eventSyncConfig,
        epg_source_ids: epgSourceIds,
        ...(!profile && importData?.channel_mappings ? { channel_mappings: importData.channel_mappings } : {}),
      };

      if (profile) {
        await api.updateDummyEPGProfile(profile.id, data);
      } else {
        await api.createDummyEPGProfile(data);
      }
      onSave();
      onClose();
    });
  };

  if (!isOpen) return null;

  return (
    <ModalOverlay onClose={onClose} role="dialog" aria-modal="true" aria-labelledby={titleId}>
      <div className="modal-container modal-xl dummy-epg-profile-modal" ref={containerRef}>
        <div className="modal-header">
          <h2 id={titleId}>{profile ? 'Edit Profile' : importData ? 'Import Dummy EPG Profile' : 'New Dummy EPG Profile'}</h2>
          <button className="modal-close-btn" onClick={onClose} aria-label="Close" title="Close">
            <span className="material-icons" aria-hidden="true">close</span>
          </button>
        </div>

        <form onSubmit={handleSubmit}>
          <div className="modal-body">
            {/* Basic Info */}
            <div className="modal-form-group">
              <label htmlFor="depName">Name <span className="modal-required">*</span></label>
              <input
                id="depName"
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="My Sports EPG"
              />
            </div>

            <div className="modal-form-group">
              <label className="modal-checkbox-label">
                <input
                  type="checkbox"
                  checked={enabled}
                  onChange={(e) => setEnabled(e.target.checked)}
                />
                <span>Enabled</span>
              </label>
            </div>

            {/* Channel Groups */}
            <div className="modal-section-divider">
              <span>Channel Groups</span>
            </div>
            <p className="modal-section-description">
              Select the channel groups to generate EPG data for. All channels in the selected groups will be included.
            </p>

            {groupsLoading ? (
              <div className="dep-groups-loading">Loading groups...</div>
            ) : channelGroups.length === 0 ? (
              <div className="dep-groups-empty">No channel groups found in Dispatcharr</div>
            ) : (
              <div className="dep-group-selector">
                <div className="dep-group-search">
                  <span className="material-icons">search</span>
                  <input
                    type="text"
                    placeholder="Filter groups..."
                    value={groupSearchTerm}
                    onChange={(e) => setGroupSearchTerm(e.target.value)}
                  />
                  {groupSearchTerm && (
                    <button type="button" className="clear-search" onClick={() => setGroupSearchTerm('')} aria-label="Clear search" title="Clear search">
                      <span className="material-icons" aria-hidden="true">close</span>
                    </button>
                  )}
                </div>
                <div className="dep-group-actions-row">
                  <span className="dep-group-count">{channelGroupIds.length} group{channelGroupIds.length !== 1 ? 's' : ''} selected</span>
                  {channelGroupIds.length > 0 && (
                    <button type="button" className="dep-group-clear-btn" onClick={() => {
                      setChannelGroupIds([]);
                      setHideEmptyGroupIds([]);
                    }}>
                      Clear all
                    </button>
                  )}
                </div>
                <div className="dep-group-list">
                  {channelGroups
                    .filter(g => g.channel_count > 0)
                    .filter(g => !groupSearchTerm || g.name.toLowerCase().includes(groupSearchTerm.toLowerCase()))
                    .map(group => {
                      const isSelected = channelGroupIds.includes(group.id);
                      return (
                        <label
                          key={group.id}
                          className={`dep-group-item ${isSelected ? 'selected' : ''}`}
                        >
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={() => {
                              if (isSelected) {
                                setHideEmptyGroupIds(ids => ids.filter(id => id !== group.id));
                              }
                              setChannelGroupIds(prev =>
                                isSelected
                                  ? prev.filter(id => id !== group.id)
                                  : [...prev, group.id]
                              );
                            }}
                          />
                          <span className="dep-group-name">{group.name}</span>
                          <span className="dep-group-ch-count">{group.channel_count} ch</span>
                        </label>
                      );
                    })}
                </div>
              </div>
            )}

            {channelGroupIds.length > 0 && (
              <fieldset className="dep-idle-visibility" aria-describedby="depIdleVisibilityHelp">
                <legend>Automatic visibility</legend>
                <p id="depIdleVisibilityHelp" className="modal-section-description">
                  For event-slot groups only. Recent measured stream flow decides visibility when available;
                  the current programme is the fallback. Channels, streams, and guide links stay intact and
                  reappear automatically when an event starts.
                </p>
                <div className="dep-group-selector">
                  <div className="dep-group-list" aria-label="Groups with automatic idle hiding">
                    {channelGroupIds.map(groupId => {
                      const group = channelGroups.find(item => item.id === groupId);
                      if (!group) return null;
                      const hidesIdle = hideEmptyGroupIds.includes(groupId);
                      return (
                        <label
                          key={groupId}
                          className={`dep-group-item ${hidesIdle ? 'selected' : ''}`}
                        >
                          <input
                            type="checkbox"
                            checked={hidesIdle}
                            onChange={() => setHideEmptyGroupIds(ids =>
                              hidesIdle
                                ? ids.filter(id => id !== groupId)
                                : [...ids, groupId]
                            )}
                          />
                          <span className="dep-group-name">Hide idle channels in {group.name}</span>
                        </label>
                      );
                    })}
                  </div>
                </div>
              </fieldset>
            )}

            <fieldset className="dep-event-matching" aria-describedby="depEventMatchingHelp">
              <legend>Event matching</legend>
              <p id="depEventMatchingHelp" className="modal-section-description">
                Match provider event names to stable numbered channels. The existing <a href="#depPatternVariants">Pattern Variants</a> parse each event title and start time.
              </p>

              {groupsError && (
                <div className="dep-catalogue-error" role="alert">
                  <span>Group accounts could not be loaded. Saved scopes remain selected, and you can keep editing other fields.</span>
                  <button type="button" className="modal-btn modal-btn-secondary" onClick={() => void loadGroupCatalogue()} disabled={groupsLoading}>
                    {groupsLoading ? 'Retrying…' : 'Retry groups'}
                  </button>
                </div>
              )}
              {groupsLoading && <p role="status">Loading group accounts…</p>}

              <ProviderScopedGroupPicker
                role="secondary"
                rows={groupRows}
                value={eventSyncConfig.secondary}
                onChange={next => setEventSyncConfig(current => ({
                  ...current,
                  secondary: Array.isArray(next) ? next : [],
                }))}
                showAll={false}
                disabled={groupsLoading}
              />

              {eventSyncConfig.secondary.length > 0 && (
                <ol className="dep-scope-order" aria-label="Event matching scope order">
                  {eventSyncConfig.secondary.map((scope, index) => {
                    const group = channelGroups.find(item => item.id === scope.group_id);
                    const account = groupRows.find(row =>
                      row.groupId === scope.group_id
                      && row.m3uAccountId === scope.m3u_account_id
                    );
                    return (
                      <li key={`${scope.group_id}:${scope.m3u_account_id ?? 'all'}`}>
                        <span>{group?.name ?? `Group ${scope.group_id}`}{scope.m3u_account_id === null ? ' · Any provider' : account ? ` · ${account.m3uAccountName}` : ` · Account ${scope.m3u_account_id}`}</span>
                        <div className="dep-order-actions">
                          <button type="button" aria-label={`Move scope ${index + 1} up`} disabled={index === 0} onClick={() => setEventSyncConfig(current => ({ ...current, secondary: moveItem(current.secondary, index, -1) }))}>
                            <span className="material-icons" aria-hidden="true">arrow_upward</span>
                          </button>
                          <button type="button" aria-label={`Move scope ${index + 1} down`} disabled={index === eventSyncConfig.secondary.length - 1} onClick={() => setEventSyncConfig(current => ({ ...current, secondary: moveItem(current.secondary, index, 1) }))}>
                            <span className="material-icons" aria-hidden="true">arrow_downward</span>
                          </button>
                          <button type="button" aria-label={`Remove scope ${index + 1}`} onClick={() => setEventSyncConfig(current => ({ ...current, secondary: current.secondary.filter((_, scopeIndex) => scopeIndex !== index) }))}>
                            <span className="material-icons" aria-hidden="true">delete</span>
                          </button>
                        </div>
                      </li>
                    );
                  })}
                </ol>
              )}

              <CollapsibleSection
                title="Matching details"
                isOpen={eventMatchingOpen}
                onToggle={() => setEventMatchingOpen(!eventMatchingOpen)}
              >
                <div className="dep-collapsible-inner dep-event-details">
                  <div className="modal-form-row">
                    <div className="modal-form-group">
                      <label htmlFor="depMatchWindow">Time window (minutes)</label>
                      <input
                        id="depMatchWindow"
                        type="number"
                        min="1"
                        max="1440"
                        value={eventSyncConfig.time_window_minutes}
                        aria-invalid={invalidField === 'time_window_minutes' || undefined}
                        aria-describedby={invalidField === 'time_window_minutes' ? 'depSaveError' : undefined}
                        onChange={event => setEventSyncConfig(current => ({ ...current, time_window_minutes: Number(event.target.value) }))}
                      />
                    </div>
                    <div className="modal-form-group">
                      <label htmlFor="depAttachThreshold">Attach threshold</label>
                      <input
                        id="depAttachThreshold"
                        type="number"
                        min="0"
                        max="1"
                        step="0.01"
                        value={eventSyncConfig.attach_threshold}
                        aria-invalid={invalidField === 'attach_threshold' || undefined}
                        aria-describedby={invalidField === 'attach_threshold' ? 'depSaveError' : undefined}
                        onChange={event => setEventSyncConfig(current => ({ ...current, attach_threshold: Number(event.target.value) }))}
                      />
                    </div>
                  </div>
                  <label className="modal-checkbox-label">
                    <input type="checkbox" checked={eventSyncConfig.enforce_time_window} onChange={event => setEventSyncConfig(current => ({ ...current, enforce_time_window: event.target.checked }))} />
                    <span>Enforce the time window</span>
                  </label>
                  <label className="modal-checkbox-label">
                    <input type="checkbox" checked={eventSyncConfig.use_default_patterns} onChange={event => setEventSyncConfig(current => ({ ...current, use_default_patterns: event.target.checked }))} />
                    <span>Also use built-in event patterns</span>
                  </label>
                  <label className="modal-checkbox-label">
                    <input type="checkbox" checked={eventSyncConfig.assume_current_date} onChange={event => setEventSyncConfig(current => ({ ...current, assume_current_date: event.target.checked }))} />
                    <span>Place dateless events on the current date</span>
                  </label>
                  <label className="modal-checkbox-label">
                    <input type="checkbox" checked={eventSyncConfig.demote_stale_dateless} onChange={event => setEventSyncConfig(current => ({ ...current, demote_stale_dateless: event.target.checked }))} />
                    <span>Keep stale dateless events out of automatic matches</span>
                  </label>

                  <div className="dep-slot-heading">
                    <div>
                      <h3>Stable slot families</h3>
                      <p className="form-hint">Expressions use a named <code>slot</code> capture. Python <code>(?P&lt;slot&gt;…)</code> and JavaScript <code>(?&lt;slot&gt;…)</code> forms are stored exactly as entered.</p>
                    </div>
                    <button type="button" className="modal-btn modal-btn-secondary" disabled={eventSyncConfig.slot_patterns.length >= 32} onClick={() => setEventSyncConfig(current => ({ ...current, slot_patterns: [...current.slot_patterns, makeEventSlot(current.slot_patterns.length)] }))}>
                      Add family
                    </button>
                  </div>

                  {eventSyncConfig.slot_patterns.length === 0 ? (
                    <p className="dep-groups-empty">No stable slot families configured.</p>
                  ) : eventSyncConfig.slot_patterns.map((slot, slotIndex) => (
                    <fieldset className="dep-slot-card" key={slotIndex}>
                      <legend>Family {slotIndex + 1}</legend>
                      <div className="dep-slot-actions">
                        <button type="button" aria-label={`Move family ${slotIndex + 1} up`} disabled={slotIndex === 0} onClick={() => setEventSyncConfig(current => ({ ...current, slot_patterns: moveItem(current.slot_patterns, slotIndex, -1) }))}><span className="material-icons" aria-hidden="true">arrow_upward</span></button>
                        <button type="button" aria-label={`Move family ${slotIndex + 1} down`} disabled={slotIndex === eventSyncConfig.slot_patterns.length - 1} onClick={() => setEventSyncConfig(current => ({ ...current, slot_patterns: moveItem(current.slot_patterns, slotIndex, 1) }))}><span className="material-icons" aria-hidden="true">arrow_downward</span></button>
                        <button type="button" aria-label={`Remove family ${slotIndex + 1}`} onClick={() => setEventSyncConfig(current => ({ ...current, slot_patterns: current.slot_patterns.filter((_, index) => index !== slotIndex) }))}><span className="material-icons" aria-hidden="true">delete</span></button>
                      </div>
                      <div className="modal-form-group">
                        <label htmlFor={`depSlotName-${slotIndex}`}>Family key</label>
                        <input id={`depSlotName-${slotIndex}`} type="text" value={slot.name} aria-invalid={invalidField === `slot_patterns[${slotIndex}].name` || undefined} aria-describedby={invalidField === `slot_patterns[${slotIndex}].name` ? 'depSaveError' : undefined} onChange={event => updateEventSlot(slotIndex, { name: event.target.value })} />
                      </div>
                      <div className="modal-form-group">
                        <label htmlFor={`depChannelPattern-${slotIndex}`}>Channel expression</label>
                        <input id={`depChannelPattern-${slotIndex}`} type="text" className="dep-expression" value={slot.channel_pattern} aria-invalid={invalidField === `slot_patterns[${slotIndex}].channel_pattern` || undefined} aria-describedby={invalidField === `slot_patterns[${slotIndex}].channel_pattern` ? 'depSaveError' : undefined} onChange={event => updateEventSlot(slotIndex, { channel_pattern: event.target.value })} />
                      </div>
                      <div className="modal-form-group">
                        <label htmlFor={`depFallbackPattern-${slotIndex}`}>Fallback expression (optional)</label>
                        <input id={`depFallbackPattern-${slotIndex}`} type="text" className="dep-expression" value={slot.fallback_pattern ?? ''} aria-invalid={invalidField === `slot_patterns[${slotIndex}].fallback_pattern` || undefined} aria-describedby={invalidField === `slot_patterns[${slotIndex}].fallback_pattern` ? 'depSaveError' : undefined} onChange={event => updateEventSlot(slotIndex, { fallback_pattern: event.target.value === '' ? null : event.target.value })} />
                      </div>
                      <div className="dep-event-expressions">
                        <span className="dep-field-label">Event expressions</span>
                        {slot.event_patterns.map((expression, expressionIndex) => (
                          <div className="dep-expression-row" key={expressionIndex}>
                            <label className="sr-only" htmlFor={`depEventPattern-${slotIndex}-${expressionIndex}`}>Family {slotIndex + 1} event expression {expressionIndex + 1}</label>
                            <input id={`depEventPattern-${slotIndex}-${expressionIndex}`} type="text" className="dep-expression" value={expression} aria-invalid={invalidField === `slot_patterns[${slotIndex}].event_patterns[${expressionIndex}]` || undefined} aria-describedby={invalidField === `slot_patterns[${slotIndex}].event_patterns[${expressionIndex}]` ? 'depSaveError' : undefined} onChange={event => updateEventSlot(slotIndex, { event_patterns: slot.event_patterns.map((item, index) => index === expressionIndex ? event.target.value : item) })} />
                            <button type="button" aria-label={`Move event expression ${expressionIndex + 1} up`} disabled={expressionIndex === 0} onClick={() => updateEventSlot(slotIndex, { event_patterns: moveItem(slot.event_patterns, expressionIndex, -1) })}><span className="material-icons" aria-hidden="true">arrow_upward</span></button>
                            <button type="button" aria-label={`Move event expression ${expressionIndex + 1} down`} disabled={expressionIndex === slot.event_patterns.length - 1} onClick={() => updateEventSlot(slotIndex, { event_patterns: moveItem(slot.event_patterns, expressionIndex, 1) })}><span className="material-icons" aria-hidden="true">arrow_downward</span></button>
                            <button type="button" aria-label={`Remove event expression ${expressionIndex + 1}`} onClick={() => updateEventSlot(slotIndex, { event_patterns: slot.event_patterns.filter((_, index) => index !== expressionIndex) })}><span className="material-icons" aria-hidden="true">delete</span></button>
                          </div>
                        ))}
                        <button type="button" className="modal-btn modal-btn-secondary" disabled={slot.event_patterns.length >= 16} onClick={() => updateEventSlot(slotIndex, { event_patterns: [...slot.event_patterns, ''] })}>Add event expression</button>
                      </div>
                      <label className="modal-checkbox-label">
                        <input type="checkbox" checked={slot.bootstrap} onChange={event => updateEventSlot(slotIndex, { bootstrap: event.target.checked })} />
                        <span>Allow this family to bootstrap a new stable slot</span>
                      </label>
                    </fieldset>
                  ))}
                </div>
              </CollapsibleSection>
            </fieldset>

            <div className="modal-section-divider"><span>Programme Sources</span></div>
            <p className="modal-section-description">
              Select existing XMLTV sources for real schedules. Their configured priority breaks ties between equivalent mappings.
              Uncovered time stays neutral. With no sources selected, the existing name templates generate the guide.
            </p>
            {sourcesLoading ? <p role="status">Loading programme sources…</p> : sourcesError ? (
              <p role="alert">Programme sources could not be loaded. Your saved selection is preserved; reopen this profile to retry.</p>
            ) : (
              <div className="dep-group-list" aria-label="Programme sources">
                {epgSources.filter(source => source.source_type === 'xmltv' && !source.url?.includes('/dummy-epg/xmltv')).map(source => (
                  <label key={source.id} className={`dep-group-item ${epgSourceIds.includes(source.id) ? 'selected' : ''}`}>
                    <input
                      type="checkbox"
                      checked={epgSourceIds.includes(source.id)}
                      disabled={!source.is_active && !epgSourceIds.includes(source.id)}
                      onChange={() => setEpgSourceIds(ids => ids.includes(source.id) ? ids.filter(id => id !== source.id) : [...ids, source.id])}
                    />
                    <span className="dep-group-name">{source.name}{!source.is_active ? ' (disabled)' : ''}</span>
                  </label>
                ))}
                {!epgSources.some(source => source.source_type === 'xmltv' && !source.url?.includes('/dummy-epg/xmltv')) && (
                  <p>No XMLTV programme sources are available.</p>
                )}
              </div>
            )}
            {epgSourceIds.filter(id => !epgSources.some(source => source.id === id)).length > 0 && !sourcesLoading && !sourcesError && (
              <div>
                <p role="status">Some selected sources are unavailable. Remove them to save a new selection.</p>
                {epgSourceIds.filter(id => !epgSources.some(source => source.id === id)).map(id => (
                  <label key={id} className="dep-group-item selected">
                    <input type="checkbox" checked onChange={() => setEpgSourceIds(ids => ids.filter(item => item !== id))} />
                    <span className="dep-group-name">Unavailable source {id}</span>
                  </label>
                ))}
              </div>
            )}
            {(profile?.channel_mappings?.length ?? importData?.channel_mappings?.length ?? 0) > 0 && (
              <p className="form-hint">Original source mappings are remembered automatically when you save. Editing templates keeps these mappings.</p>
            )}
            {profile && (
              <div className="dep-coverage">
                <button
                  type="button"
                  className="btn-secondary"
                  disabled={coverageLoading}
                  onClick={async () => {
                    const request = ++coverageRequest.current;
                    setCoverageLoading(true);
                    setCoverageError(false);
                    try {
                      const result = await api.getDummyEPGCoverage(profile.id);
                      if (coverageRequest.current === request) setCoverage(result);
                    } catch {
                      if (coverageRequest.current === request) setCoverageError(true);
                    } finally {
                      if (coverageRequest.current === request) setCoverageLoading(false);
                    }
                  }}
                >{coverageLoading ? 'Checking coverage…' : 'Check saved guide coverage'}</button>
                <p className="form-hint">Coverage uses the saved profile. Save source or group changes before checking.</p>
                {coverageLoading && <p role="status">Loading saved guide coverage…</p>}
                {coverageError && <p role="alert">Guide coverage could not be loaded. Try checking again.</p>}
                {coverage && !coverageError && (
                  <div aria-live="polite">
                    <section className={`dep-publication ${coverage.publication.status}`} aria-label="Stored guide publication">
                      <div className="dep-publication-heading">
                        <h3>{PUBLICATION_STATUS_TEXT[coverage.publication.status]}</h3>
                        <span>Coverage checked {new Date(coverage.generated_at).toLocaleString()}</span>
                      </div>
                      <p>
                        <strong>Original publication time:</strong>{' '}
                        {coverage.publication.published_at
                          ? new Date(coverage.publication.published_at).toLocaleString()
                          : 'Unavailable'}
                      </p>
                      {coverage.publication.window_start && (
                        <p><strong>Stored window starts:</strong> {new Date(coverage.publication.window_start).toLocaleString()}</p>
                      )}
                      {coverage.publication.window_stop && (
                        <p><strong>Stored window ends:</strong> {new Date(coverage.publication.window_stop).toLocaleString()}</p>
                      )}
                      {coverage.publication.reason_codes.map(reason => (
                        <p key={reason}>{PUBLICATION_REASON_TEXT[reason]}</p>
                      ))}
                      {coverage.publication.delivery && (
                        <div className="dep-publication-delivery">
                          <strong>{DISPATCHARR_STATUS_TEXT[coverage.publication.delivery.dispatcharr_status]}</strong>
                          {coverage.publication.delivery.pending_emby && <span>Emby refresh pending</span>}
                        </div>
                      )}
                      {coverage.publication.channels.length > 0 && (
                        <ul className="dep-publication-channels">
                          {coverage.publication.channels.map(channel => (
                            <li key={channel.channel_id}>
                              <div className="dep-publication-channel-heading">
                                <span>Channel {channel.channel_id}{channel.xmltv_id && <> · {channel.xmltv_id}</>}</span>
                                <strong className={`dep-evidence ${channel.visibility_evidence}`}>
                                  {VISIBILITY_EVIDENCE_TEXT[channel.visibility_evidence]}
                                </strong>
                              </div>
                              {channel.events.map(event => (
                                <p key={`${event.start}:${event.stop}:${event.title}`}>
                                  {event.title}<br />
                                  <span>{new Date(event.start).toLocaleString()} – {new Date(event.stop).toLocaleString()}</span>
                                </p>
                              ))}
                            </li>
                          ))}
                        </ul>
                      )}
                      {coverage.publication.channels.some(channel => channel.visibility_evidence === 'unknown') && (
                        <p className="form-hint">Unknown evidence does not mean a channel is hidden, idle, or unavailable.</p>
                      )}
                    </section>
                    {readiness && (
                      <div className={`dep-readiness ${readiness.can_publish ? 'ready' : 'blocked'}`}>
                        <strong>{readiness.can_publish ? 'Ready to publish' : 'Publication is waiting'}</strong>
                        {readiness.reason_codes.map(reason => (
                          <p key={reason}>{GUIDE_REASON_TEXT[reason]}</p>
                        ))}
                      </div>
                    )}
                    {coverage.sources.map(source => (
                      <p key={source.source_id}>
                        {epgSources.find(item => item.id === source.source_id)?.name ?? `Source ${source.source_id}`}: {source.status}
                        {source.error ? ` — ${source.error}` : ''}
                        {source.last_success ? ` · last complete ${new Date(source.last_success).toLocaleString()}` : ''}
                      </p>
                    ))}
                    {coverage.sources.some(source => source.status === 'pending') && <p>Sources are still loading. Check again shortly; gaps remain neutral.</p>}
                    {coverage.channels.length === 0 ? <p>No channels are assigned to this saved profile.</p> : (
                      <div className="dep-coverage-table">
                        <table>
                          <thead><tr><th>Channel</th><th>Mapping</th><th>Current / Next</th><th>Real / Gap minutes</th></tr></thead>
                          <tbody>{coverage.channels.map(channel => (
                            <tr key={channel.channel_id}>
                              <td>{channel.xmltv_id}</td>
                              <td>{channel.match}<br />{channel.source_id === null ? 'No source match' : epgSources.find(item => item.id === channel.source_id)?.name ?? `Source ${channel.source_id}`}
                                {channel.warnings.map(warning => <p key={warning}>{warning}</p>)}
                              </td>
                              <td>{channel.current?.title ?? 'No real programme now'}<br />{channel.current && <span>{new Date(channel.current.start).toLocaleString()} – {new Date(channel.current.stop).toLocaleString()}<br /></span>}{channel.next ? `Next: ${channel.next.title} (${new Date(channel.next.start).toLocaleString()})` : 'No next programme'}</td>
                              <td>{channel.real_minutes} / {channel.gap_minutes}</td>
                            </tr>
                          ))}</tbody>
                        </table>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}

            {/* Substitution Pairs */}
            <CollapsibleSection
              title="Substitution Pairs"
              isOpen={subsOpen}
              onToggle={() => setSubsOpen(!subsOpen)}
            >
              <div className="dep-collapsible-inner">
                <p className="modal-section-description">
                  Ordered find/replace rules applied to the name before pattern matching. Pairs are applied top-to-bottom.
                </p>
                <SubstitutionPairsEditor pairs={substitutionPairs} onChange={setSubstitutionPairs} />
              </div>
            </CollapsibleSection>

            {/* Name Source */}
            <div className="modal-section-divider">
              <span>Name Source</span>
            </div>

            <div className="modal-form-group">
              <label>Name Source <span className="modal-required">*</span></label>
              <div className="searchable-select-dropdown" ref={nameSourceDropdownRef}>
                <button
                  type="button"
                  className="dropdown-trigger"
                  onClick={() => setNameSourceDropdownOpen(!nameSourceDropdownOpen)}
                >
                  <span className="dropdown-value">
                    {nameSource === 'channel' ? 'Channel Name' : 'Stream Name'}
                  </span>
                  <span className="material-icons">expand_more</span>
                </button>
                {nameSourceDropdownOpen && (
                  <div className="dropdown-menu">
                    <div className="dropdown-options">
                      <div
                        className={`dropdown-option-item${nameSource === 'channel' ? ' selected' : ''}`}
                        onClick={() => { setNameSource('channel'); setNameSourceDropdownOpen(false); }}
                      >
                        Channel Name
                      </div>
                      <div
                        className={`dropdown-option-item${nameSource === 'stream' ? ' selected' : ''}`}
                        onClick={() => { setNameSource('stream'); setNameSourceDropdownOpen(false); }}
                      >
                        Stream Name
                      </div>
                    </div>
                  </div>
                )}
              </div>
              <p className="form-hint">Choose whether to parse the channel name or a stream name assigned to the channel</p>
            </div>

            {nameSource === 'stream' && (
              <div className="modal-form-group">
                <label htmlFor="depStreamIndex">Stream Index</label>
                <input
                  id="depStreamIndex"
                  type="number"
                  min="1"
                  value={streamIndex}
                  onChange={(e) => setStreamIndex(parseInt(e.target.value) || 1)}
                />
                <p className="form-hint">Which stream&apos;s name to use (1 = first stream)</p>
              </div>
            )}

            {/* Variant Tabs */}
            <div className="modal-section-divider" id="depPatternVariants">
              <span>Pattern Variants</span>
            </div>
            <p className="modal-section-description">
              Define multiple pattern variants. The engine tries each variant in order and uses the first match.
            </p>

            <VariantTabs
              variants={variants}
              activeIndex={activeVariantIndex}
              onSelect={setActiveVariantIndex}
              onAdd={handleAddVariant}
              onRename={handleRenameVariant}
              onDelete={handleDeleteVariant}
            />

            {/* Per-Variant Pattern Builder */}
            <PatternBuilder
              key={`pb-${activeVariantIndex}`}
              titlePattern={activeVariant.title_pattern || ''}
              timePattern={activeVariant.time_pattern || ''}
              datePattern={activeVariant.date_pattern || ''}
              onTitlePatternChange={(p) => updateActiveVariant({ title_pattern: p || null })}
              onTimePatternChange={(p) => updateActiveVariant({ time_pattern: p || null })}
              onDatePatternChange={(p) => updateActiveVariant({ date_pattern: p || null })}
              builderExamples={activeVariant.pattern_builder_examples}
              onBuilderExamplesChange={(json) => updateActiveVariant({ pattern_builder_examples: json })}
            />

            {/* Available Variables */}
            {(() => {
              const userGroups = extractGroupNames(activeVariant.title_pattern);
              const hasTime = Boolean(activeVariant.time_pattern);
              const hasDate = Boolean(activeVariant.date_pattern);
              const showPanel = userGroups.length > 0 || hasTime || hasDate;
              return showPanel ? (
                <div className="dep-available-vars">
                  <div className="dep-available-vars-title">Available Variables — {activeVariant.name}</div>
                  {userGroups.length > 0 && (
                    <div className="dep-var-section">
                      <span className="dep-var-section-label">Pattern</span>
                      {userGroups.map(g => <code key={g} className="dep-var-chip">{`{${g}}`}</code>)}
                    </div>
                  )}
                  {hasTime && (
                    <div className="dep-var-section">
                      <span className="dep-var-section-label">Time</span>
                      {['starttime', 'starttime24', 'endtime', 'endtime24'].map(v => <code key={v} className="dep-var-chip">{`{${v}}`}</code>)}
                    </div>
                  )}
                  {hasDate && (
                    <div className="dep-var-section">
                      <span className="dep-var-section-label">Date</span>
                      {['date', 'month', 'day', 'year'].map(v => <code key={v} className="dep-var-chip">{`{${v}}`}</code>)}
                    </div>
                  )}
                  <div className="dep-var-section">
                    <span className="dep-var-section-label">Built-in</span>
                    <code className="dep-var-chip">{'{original_name}'}</code>
                    <code className="dep-var-chip">{'{substituted_name}'}</code>
                  </div>
                  {userGroups.length > 0 && (
                    <div className="dep-var-hint">Tip: use <code className="dep-var-chip">{'{groupname_normalize}'}</code> variants for clean URLs</div>
                  )}
                </div>
              ) : null;
            })()}

            {/* Per-Variant Output Templates */}
            <div className="modal-section-divider">
              <span>Output Templates — {activeVariant.name}</span>
            </div>

            <p className="modal-section-description">
              Use extracted groups to format EPG titles and descriptions. Reference groups using &#123;groupname&#125; syntax. For clean URLs, use &#123;groupname_normalize&#125;.
            </p>

            <div className="modal-form-group">
              <label htmlFor="depTitleTemplate">Title Template</label>
              <input
                id="depTitleTemplate"
                type="text"
                value={activeVariant.title_template || ''}
                onChange={(e) => updateActiveVariant({ title_template: e.target.value || null })}
                placeholder="{league} - {team1} vs {team2}"
              />
              <p className="form-hint">Use &#123;starttime&#125;, &#123;starttime24&#125;, &#123;endtime&#125;, &#123;date&#125;, &#123;month&#125;, &#123;day&#125;, or &#123;year&#125;</p>
            </div>

            <div className="modal-form-group">
              <label htmlFor="depDescriptionTemplate">Description Template</label>
              <textarea
                id="depDescriptionTemplate"
                value={activeVariant.description_template || ''}
                onChange={(e) => updateActiveVariant({ description_template: e.target.value || null })}
                placeholder="Watch {team1} take on {team2} in this exciting {league} matchup!"
                rows={3}
              />
            </div>

            {/* Per-Variant Logo/Poster URLs */}
            <div className="modal-form-group">
              <label htmlFor="depChannelLogoUrl">Channel Logo URL</label>
              <input
                id="depChannelLogoUrl"
                type="text"
                value={activeVariant.channel_logo_url_template || ''}
                onChange={(e) => updateActiveVariant({ channel_logo_url_template: e.target.value || null })}
                placeholder="https://example.com/logos/{league_normalize}/{team1_normalize}.png"
              />
              <p className="form-hint">Use &#123;groupname_normalize&#125; for clean URLs</p>
            </div>
            <div className="modal-form-group">
              <label htmlFor="depProgramPosterUrl">Program Poster URL (Optional)</label>
              <input
                id="depProgramPosterUrl"
                type="text"
                value={activeVariant.program_poster_url_template || ''}
                onChange={(e) => updateActiveVariant({ program_poster_url_template: e.target.value || null })}
                placeholder="https://example.com/posters/{team1_normalize}-vs-{team2_normalize}.jpg"
              />
            </div>
            <div className="modal-form-group">
              <label htmlFor="depVariantProgramDuration">Program Duration (minutes, Optional)</label>
              <input
                id="depVariantProgramDuration"
                type="number"
                min="0"
                max="1440"
                value={activeVariant.program_duration ?? ''}
                onChange={(e) => updateActiveVariant({
                  program_duration: e.target.value === '' ? null : parseInt(e.target.value, 10),
                })}
                placeholder="Use profile default"
              />
              <p className="form-hint">How long an event on this variant runs. Blank uses the profile duration.</p>
            </div>

            {/* Per-Variant Template Overrides (optional — collapse by default) */}
            <CollapsibleSection
              title={`Template Overrides — ${activeVariant.name} (Optional)`}
              isOpen={variantOverridesOpen}
              onToggle={() => setVariantOverridesOpen(!variantOverridesOpen)}
            >
              <div className="dep-collapsible-inner">
                <p className="modal-section-description">
                  Override the profile-level upcoming/ended/fallback templates for this variant. Leave blank to use profile defaults.
                </p>
                <div className="modal-form-group">
                  <label>Upcoming Title Override</label>
                  <input
                    type="text"
                    value={activeVariant.upcoming_title_template || ''}
                    onChange={(e) => updateActiveVariant({ upcoming_title_template: e.target.value || null })}
                    placeholder="Use profile default"
                  />
                </div>
                <div className="modal-form-group">
                  <label>Upcoming Description Override</label>
                  <textarea
                    value={activeVariant.upcoming_description_template || ''}
                    onChange={(e) => updateActiveVariant({ upcoming_description_template: e.target.value || null })}
                    placeholder="Use profile default"
                    rows={2}
                  />
                </div>
                <div className="modal-form-group">
                  <label>Ended Title Override</label>
                  <input
                    type="text"
                    value={activeVariant.ended_title_template || ''}
                    onChange={(e) => updateActiveVariant({ ended_title_template: e.target.value || null })}
                    placeholder="Use profile default"
                  />
                </div>
                <div className="modal-form-group">
                  <label>Ended Description Override</label>
                  <textarea
                    value={activeVariant.ended_description_template || ''}
                    onChange={(e) => updateActiveVariant({ ended_description_template: e.target.value || null })}
                    placeholder="Use profile default"
                    rows={2}
                  />
                  <p className="form-hint">{epgSourceIds.length ? "Ended templates apply only when no programme sources are selected. Real schedules use their original times, and uncovered time stays neutral." : ENDED_TEMPLATE_HINT}</p>
                </div>
                <div className="modal-form-group">
                  <label>Fallback Title Override</label>
                  <input
                    type="text"
                    value={activeVariant.fallback_title_template || ''}
                    onChange={(e) => updateActiveVariant({ fallback_title_template: e.target.value || null })}
                    placeholder="Use profile default"
                  />
                </div>
                <div className="modal-form-group">
                  <label>Fallback Description Override</label>
                  <textarea
                    value={activeVariant.fallback_description_template || ''}
                    onChange={(e) => updateActiveVariant({ fallback_description_template: e.target.value || null })}
                    placeholder="Use profile default"
                    rows={2}
                  />
                </div>
              </div>
            </CollapsibleSection>

            {/* Profile-level: Upcoming/Ended Templates */}
            <CollapsibleSection
              title="Upcoming/Ended Templates — Profile Defaults (Optional)"
              isOpen={upcomingEndedOpen}
              onToggle={() => setUpcomingEndedOpen(!upcomingEndedOpen)}
            >
              <div className="dep-collapsible-inner">
                <p className="modal-section-description">
                  Customize how programs appear before and after the event. Each variant uses these unless it has its own override.
                </p>
                <div className="modal-form-group">
                  <label htmlFor="depUpcomingTitle">Upcoming Title Template</label>
                  <input
                    id="depUpcomingTitle"
                    type="text"
                    value={upcomingTitleTemplate}
                    onChange={(e) => setUpcomingTitleTemplate(e.target.value)}
                    placeholder="{team1} vs {team2} starting at {starttime}"
                  />
                </div>
                <div className="modal-form-group">
                  <label htmlFor="depUpcomingDesc">Upcoming Description Template</label>
                  <textarea
                    id="depUpcomingDesc"
                    value={upcomingDescriptionTemplate}
                    onChange={(e) => setUpcomingDescriptionTemplate(e.target.value)}
                    placeholder="Upcoming: {team1} take on {team2} from {starttime} to {endtime}!"
                    rows={2}
                  />
                </div>
                <div className="modal-form-group">
                  <label htmlFor="depEndedTitle">Ended Title Template</label>
                  <input
                    id="depEndedTitle"
                    type="text"
                    value={endedTitleTemplate}
                    onChange={(e) => setEndedTitleTemplate(e.target.value)}
                    placeholder="{team1} vs {team2} started at {starttime}"
                  />
                </div>
                <div className="modal-form-group">
                  <label htmlFor="depEndedDesc">Ended Description Template</label>
                  <textarea
                    id="depEndedDesc"
                    value={endedDescriptionTemplate}
                    onChange={(e) => setEndedDescriptionTemplate(e.target.value)}
                    placeholder="The {league} match between {team1} and {team2} ran from {starttime} to {endtime}."
                    rows={2}
                  />
                  <p className="form-hint">{epgSourceIds.length ? "Ended templates apply only when no programme sources are selected. Real schedules use their original times, and uncovered time stays neutral." : ENDED_TEMPLATE_HINT}</p>
                </div>
              </div>
            </CollapsibleSection>

            {/* Profile-level: Fallback Templates */}
            <CollapsibleSection
              title="Fallback Templates — Profile Defaults (Optional)"
              isOpen={fallbackOpen}
              onToggle={() => setFallbackOpen(!fallbackOpen)}
            >
              <div className="dep-collapsible-inner">
                <p className="modal-section-description">
                  Used when no variant matches the channel/stream name.
                </p>
                <div className="modal-form-group">
                  <label htmlFor="depFallbackTitle">Fallback Title Template</label>
                  <input
                    id="depFallbackTitle"
                    type="text"
                    value={fallbackTitleTemplate}
                    onChange={(e) => setFallbackTitleTemplate(e.target.value)}
                    placeholder="No EPG data available"
                  />
                </div>
                <div className="modal-form-group">
                  <label htmlFor="depFallbackDesc">Fallback Description Template</label>
                  <textarea
                    id="depFallbackDesc"
                    value={fallbackDescriptionTemplate}
                    onChange={(e) => setFallbackDescriptionTemplate(e.target.value)}
                    placeholder="EPG information is currently unavailable for this channel."
                    rows={2}
                  />
                </div>
              </div>
            </CollapsibleSection>

            {/* EPG Settings */}
            <div className="modal-section-divider">
              <span>EPG Settings</span>
            </div>

            <div className="modal-form-row">
              <div className="modal-form-group">
                <label>Event Timezone</label>
                <div className="searchable-select-dropdown" ref={eventTimezoneDropdownRef}>
                  <button
                    type="button"
                    className="dropdown-trigger"
                    onClick={() => { setEventTimezoneDropdownOpen(!eventTimezoneDropdownOpen); setEventTimezoneSearch(''); }}
                  >
                    <span className="dropdown-value">
                      {TIMEZONES.find(tz => tz.value === eventTimezone)?.label || eventTimezone}
                    </span>
                    <span className="material-icons">expand_more</span>
                  </button>
                  {eventTimezoneDropdownOpen && (
                    <div className="dropdown-menu">
                      <div className="dropdown-search">
                        <span className="material-icons">search</span>
                        <input
                          type="text"
                          placeholder="Search timezones..."
                          value={eventTimezoneSearch}
                          onChange={(e) => setEventTimezoneSearch(e.target.value)}
                          autoFocus
                        />
                        {eventTimezoneSearch && (
                          <button type="button" className="clear-search" onClick={() => setEventTimezoneSearch('')} aria-label="Clear search" title="Clear search">
                            <span className="material-icons" aria-hidden="true">close</span>
                          </button>
                        )}
                      </div>
                      <div className="dropdown-options">
                        {TIMEZONES.filter(tz => tz.value !== '' && tz.label.toLowerCase().includes(eventTimezoneSearch.toLowerCase())).map(tz => (
                          <div
                            key={tz.value}
                            className={`dropdown-option-item${eventTimezone === tz.value ? ' selected' : ''}`}
                            onClick={() => { setEventTimezone(tz.value); setEventTimezoneDropdownOpen(false); }}
                          >
                            {tz.label}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
                <p className="form-hint">Timezone of event times in channel titles</p>
              </div>

              <div className="modal-form-group">
                <label>Output Timezone (Optional)</label>
                <div className="searchable-select-dropdown" ref={outputTimezoneDropdownRef}>
                  <button
                    type="button"
                    className="dropdown-trigger"
                    onClick={() => { setOutputTimezoneDropdownOpen(!outputTimezoneDropdownOpen); setOutputTimezoneSearch(''); }}
                  >
                    <span className="dropdown-value">
                      {outputTimezone ? (TIMEZONES.find(tz => tz.value === outputTimezone)?.label || outputTimezone) : 'Same as event timezone'}
                    </span>
                    <span className="material-icons">expand_more</span>
                  </button>
                  {outputTimezoneDropdownOpen && (
                    <div className="dropdown-menu">
                      <div className="dropdown-search">
                        <span className="material-icons">search</span>
                        <input
                          type="text"
                          placeholder="Search timezones..."
                          value={outputTimezoneSearch}
                          onChange={(e) => setOutputTimezoneSearch(e.target.value)}
                          autoFocus
                        />
                        {outputTimezoneSearch && (
                          <button type="button" className="clear-search" onClick={() => setOutputTimezoneSearch('')} aria-label="Clear search" title="Clear search">
                            <span className="material-icons" aria-hidden="true">close</span>
                          </button>
                        )}
                      </div>
                      <div className="dropdown-options">
                        {(!outputTimezoneSearch || 'same as event timezone'.includes(outputTimezoneSearch.toLowerCase())) && (
                          <div
                            className={`dropdown-option-item${outputTimezone === '' ? ' selected' : ''}`}
                            onClick={() => { setOutputTimezone(''); setOutputTimezoneDropdownOpen(false); }}
                          >
                            Same as event timezone
                          </div>
                        )}
                        {TIMEZONES.filter(tz => tz.value !== '' && tz.label.toLowerCase().includes(outputTimezoneSearch.toLowerCase())).map(tz => (
                          <div
                            key={tz.value}
                            className={`dropdown-option-item${outputTimezone === tz.value ? ' selected' : ''}`}
                            onClick={() => { setOutputTimezone(tz.value); setOutputTimezoneDropdownOpen(false); }}
                          >
                            {tz.label}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
                <p className="form-hint">Display times in a different timezone</p>
              </div>
            </div>

            <div className="modal-form-row">
              <div className="modal-form-group">
                <label htmlFor="depProgramDuration">Program Duration (minutes)</label>
                <input
                  id="depProgramDuration"
                  type="number"
                  min="0"
                  max="1440"
                  value={programDuration}
                  onChange={(e) => setProgramDuration(e.target.value === '' ? 180 : Number(e.target.value))}
                />
                <p className="form-hint">Default duration for each program</p>
              </div>

              <div className="modal-form-group">
                <label htmlFor="depCategories">Categories (Optional)</label>
                <input
                  id="depCategories"
                  type="text"
                  value={categories}
                  onChange={(e) => setCategories(e.target.value)}
                  placeholder="Sports, Live, HD"
                />
                <p className="form-hint">Comma-separated EPG categories</p>
              </div>
            </div>

            {/* EPG Tags */}
            <CollapsibleSection
              title="EPG Tags"
              isOpen={epgTagsOpen}
              onToggle={() => setEpgTagsOpen(!epgTagsOpen)}
            >
              <div className="dep-collapsible-inner">
                <div className="modal-form-group">
                  <label className="modal-checkbox-label">
                    <input type="checkbox" checked={includeDateTag} onChange={(e) => setIncludeDateTag(e.target.checked)} />
                    <span>Include Date Tag</span>
                  </label>
                  <p className="form-hint">Add &lt;date&gt; tag with program start date</p>
                </div>
                <div className="modal-form-group">
                  <label className="modal-checkbox-label">
                    <input type="checkbox" checked={includeLiveTag} onChange={(e) => setIncludeLiveTag(e.target.checked)} />
                    <span>Include Live Tag</span>
                  </label>
                  <p className="form-hint">Mark programs as live content (main event only)</p>
                </div>
                <div className="modal-form-group">
                  <label className="modal-checkbox-label">
                    <input type="checkbox" checked={includeNewTag} onChange={(e) => setIncludeNewTag(e.target.checked)} />
                    <span>Include New Tag</span>
                  </label>
                  <p className="form-hint">Mark programs as new content (main event only)</p>
                </div>
              </div>
            </CollapsibleSection>

            {/* Advanced Settings */}
            <CollapsibleSection
              title="Advanced Settings"
              isOpen={advancedOpen}
              onToggle={() => setAdvancedOpen(!advancedOpen)}
            >
              <div className="dep-collapsible-inner">
                <div className="modal-form-group">
                  <label htmlFor="depTvgIdTemplate">TVG ID Template</label>
                  <input
                    id="depTvgIdTemplate"
                    type="text"
                    value={tvgIdTemplate}
                    onChange={(e) => setTvgIdTemplate(e.target.value)}
                    placeholder="ecm-{channel_id}"
                  />
                  <p className="form-hint">Template for tvg-id in XMLTV output. Keep &#123;channel_id&#125; in it, and make sure it matches the tvg-id used in Dispatcharr for channel matching. Channel ids are never reused, but channel numbers start over at 1 whenever channels are rebuilt, so a template built on the number hands the previous channel&apos;s programmes to whatever event now holds that number.</p>
                </div>
              </div>
            </CollapsibleSection>

            {/* Batch Test */}
            <div className="modal-section-divider">
              <span>Batch Test</span>
            </div>

            <p className="modal-section-description">
              Paste multiple channel/stream names (one per line) to test which variant matches each.
            </p>

            <div className="modal-form-group">
              <label htmlFor="depSampleChannelName">Sample channel name (optional)</label>
              <input
                id="depSampleChannelName"
                type="text"
                value={sampleChannelName}
                onChange={event => setSampleChannelName(event.target.value)}
                placeholder="Arena 07"
              />
              <p className="form-hint">The preview stays local to ECM and does not contact Dispatcharr.</p>
            </div>

            <div className="modal-form-group">
              <label htmlFor="depBatchInput">Sample Names</label>
              <textarea
                id="depBatchInput"
                value={batchInput}
                onChange={(e) => setBatchInput(e.target.value)}
                placeholder={"ESPN+ 17 : Ohio vs Notre Dame @ Feb 20 8:00PM ET\nPPV: UFC 300 Main Card\nNFL 12 : Cowboys VS Eagles @ Oct 17 1:00PM"}
                rows={4}
                style={{ fontFamily: "'JetBrains Mono', 'Fira Code', monospace", fontSize: 'var(--type-body-size)' }}
              />
            </div>

            <button
              type="button"
              className="modal-btn modal-btn-secondary"
              onClick={handleBatchTest}
              disabled={!batchInput.trim()}
              style={{ marginBottom: '0.75rem' }}
            >
              {batchLoading ? 'Test again' : 'Test All'}
            </button>

            {batchLoading && <span role="status" className="dep-preview-status">Testing the latest samples…</span>}

            {batchError && <p role="alert">The preview could not be generated. Your samples and matching settings are unchanged.</p>}

            {batchResults.length > 0 && (
              <div className="dep-batch-results">
                <div className="dep-batch-header">
                  <span>Name</span>
                  <span>Variant</span>
                  <span>Title Output</span>
                  <span>Status</span>
                </div>
                {batchResults.map((r, i) => {
                  const isExpanded = expandedBatchRows.has(i);
                  const toggleRow = () => {
                    setExpandedBatchRows(prev => {
                      const next = new Set(prev);
                      if (next.has(i)) next.delete(i); else next.add(i);
                      return next;
                    });
                  };
                  // Collect detail rows to show (skip empty values)
                  const detailFields: Array<{ label: string; value: string }> = [];
                  if (r.matched && r.rendered) {
                    if (r.rendered.title) detailFields.push({ label: 'Title', value: r.rendered.title });
                    if (r.rendered.description) detailFields.push({ label: 'Description', value: r.rendered.description });
                    if (r.rendered.channel_logo_url) detailFields.push({ label: 'Channel Logo URL', value: r.rendered.channel_logo_url });
                    if (r.rendered.program_poster_url) detailFields.push({ label: 'Program Poster URL', value: r.rendered.program_poster_url });
                    if (r.rendered.upcoming_title) detailFields.push({ label: 'Upcoming Title', value: r.rendered.upcoming_title });
                    if (r.rendered.upcoming_description) detailFields.push({ label: 'Upcoming Desc', value: r.rendered.upcoming_description });
                    if (r.rendered.ended_title) detailFields.push({ label: 'Ended Title', value: r.rendered.ended_title });
                    if (r.rendered.ended_description) detailFields.push({ label: 'Ended Desc', value: r.rendered.ended_description });
                  } else if (r.rendered) {
                    if (r.rendered.fallback_title) detailFields.push({ label: 'Fallback Title', value: r.rendered.fallback_title });
                    if (r.rendered.fallback_description) detailFields.push({ label: 'Fallback Desc', value: r.rendered.fallback_description });
                  }
                  const hasGroups = r.groups && Object.keys(r.groups).length > 0;
                  const hasTimeVars = r.time_variables && Object.keys(r.time_variables).length > 0;

                  return (
                    <div key={i} className={`dep-batch-row-wrap ${r.matched ? 'dep-batch-match' : 'dep-batch-no-match'}`}>
                      <button type="button" className="dep-batch-summary" onClick={toggleRow} aria-expanded={isExpanded}>
                        <span className="dep-batch-name" title={r.original_name}>
                          {r.original_name.length > 40 ? r.original_name.slice(0, 40) + '...' : r.original_name}
                        </span>
                        <span className="dep-batch-variant">
                          {r.matched_variant || '—'}
                        </span>
                        <span className="dep-batch-title">
                          {r.matched ? (r.rendered?.title || '—') : (r.rendered?.fallback_title || '—')}
                        </span>
                        <span className="dep-batch-status">
                          <span className={`material-icons ${r.matched ? 'dep-batch-icon-match' : 'dep-batch-icon-fail'}`} aria-hidden="true">
                            {r.matched ? 'check_circle' : 'cancel'}
                          </span>
                        </span>
                      </button>
                      {isExpanded && (
                        <div className="dep-batch-detail">
                          {(hasGroups || hasTimeVars) && (
                            <div className="dep-batch-detail-row">
                              <span className="dep-batch-detail-label">Variables</span>
                              <div className="dep-batch-groups">
                                {hasGroups && Object.entries(r.groups!).map(([k, v]) => (
                                  <span key={k} className="dep-batch-group-chip"><strong>{`{${k}}`}</strong> = &quot;{v}&quot;</span>
                                ))}
                                {hasTimeVars && Object.entries(r.time_variables!).map(([k, v]) => (
                                  <span key={k} className="dep-batch-group-chip"><strong>{`{${k}}`}</strong> = &quot;{v}&quot;</span>
                                ))}
                              </div>
                            </div>
                          )}
                          {detailFields.map(({ label, value }) => (
                            <div key={label} className="dep-batch-detail-row">
                              <span className="dep-batch-detail-label">{label}</span>
                              <span className="dep-batch-detail-value">{value}</span>
                            </div>
                          ))}
                          {r.event && (
                            <>
                              {r.event.family && <div className="dep-batch-detail-row"><span className="dep-batch-detail-label">Event family</span><span className="dep-batch-detail-value">{r.event.family}</span></div>}
                              {r.event.slot && <div className="dep-batch-detail-row"><span className="dep-batch-detail-label">Slot</span><span className="dep-batch-detail-value">{r.event.slot}</span></div>}
                              {r.event.role && <div className="dep-batch-detail-row"><span className="dep-batch-detail-label">Role</span><span className="dep-batch-detail-value">{r.event.role}</span></div>}
                              {r.event.start && <div className="dep-batch-detail-row"><span className="dep-batch-detail-label">Starts</span><span className="dep-batch-detail-value">{r.event.start}</span></div>}
                              {r.event.stop && <div className="dep-batch-detail-row"><span className="dep-batch-detail-label">Stops</span><span className="dep-batch-detail-value">{r.event.stop}</span></div>}
                              {r.event.matched_pattern && <div className="dep-batch-detail-row"><span className="dep-batch-detail-label">Matched expression</span><span className="dep-batch-detail-value">{r.event.matched_pattern}</span></div>}
                              {r.event.validation_issues.map(issue => (
                                <div className="dep-batch-detail-row" key={issue}><span className="dep-batch-detail-label">Issue</span><span className="dep-batch-detail-value">{issue}</span></div>
                              ))}
                            </>
                          )}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}

            {error && <div className="modal-error-banner" id="depSaveError" role="alert">{error}</div>}
          </div>

          <div className="modal-footer modal-footer-spread">
            <button type="button" className="modal-btn modal-btn-secondary" onClick={onClose}>
              Cancel
            </button>
            <button type="submit" className="modal-btn modal-btn-primary" disabled={saving}>
              {saving ? 'Saving...' : profile ? 'Save Changes' : 'Create Profile'}
            </button>
          </div>
        </form>
      </div>
    </ModalOverlay>
  );
});
