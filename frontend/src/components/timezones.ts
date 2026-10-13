import type { SelectOption } from './CustomSelect';

// Common time zones for the time-zone dropdowns.
export const TIMEZONES: SelectOption[] = [
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

// A stored zone missing from the list stays visible and selectable instead
// of showing as an empty picker that hides what the rule reads.
export function timezoneOptions(stored: string): SelectOption[] {
  return TIMEZONES.some(zone => zone.value === stored)
    ? TIMEZONES
    : [...TIMEZONES, { value: stored, label: stored }];
}
