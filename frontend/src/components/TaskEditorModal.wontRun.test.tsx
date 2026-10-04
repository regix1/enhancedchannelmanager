/**
 * TaskEditorModal — "enabled but won't run" UX tests (bead vkktd.4).
 *
 * Locks:
 *   1. The rewritten enable hint copy ("The task and at least one schedule
 *      below must both be enabled...") — the old copy implied the task toggle
 *      alone was the whole story.
 *   2. The live inline warning when the task is enabled but no child schedule
 *      is (and its absence for MANUAL-only tasks / when a schedule is on).
 *   3. The auto-reconcile toast: saving an enabled task whose schedules are
 *      all disabled triggers the backend reconcile; the modal re-reads the
 *      schedules and announces "Also enabled ... schedule" — a silent
 *      reconcile is a trust problem.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { TaskStatus, TaskSchedule } from '../services/api';

vi.mock('../services/api', () => ({
  getTaskParameterSchema: vi.fn().mockResolvedValue({ parameters: [] }),
  getTaskSchedules: vi.fn().mockResolvedValue({ schedules: [] }),
  getChannelGroups: vi.fn().mockResolvedValue([]),
  getEPGSources: vi.fn().mockResolvedValue([]),
  getM3UAccounts: vi.fn().mockResolvedValue([]),
  getExportSections: vi.fn().mockResolvedValue([]),
  getSettings: vi.fn().mockResolvedValue({}),
  updateTask: vi.fn().mockResolvedValue(undefined),
  createTaskSchedule: vi.fn().mockResolvedValue({ id: 1 }),
  updateTaskSchedule: vi.fn().mockResolvedValue({ id: 1 }),
}));

vi.mock('../services/channelPipelineApi', () => ({
  getChannelPipelineRules: vi.fn().mockResolvedValue([]),
}));

const notify = { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() };
vi.mock('../contexts/NotificationContext', () => ({
  useNotifications: () => notify,
}));

vi.mock('../contexts/BackupDestinationPromptContext', () => ({
  useBackupDestinationPrompt: () => ({ promptBackupDestination: vi.fn() }),
}));

import * as api from '../services/api';
import { TaskEditorModal } from './TaskEditorModal';

function makeSchedule(overrides: Partial<TaskSchedule> = {}): TaskSchedule {
  return {
    id: 11,
    task_id: 'auto_creation',
    name: 'Hourly',
    enabled: false,
    schedule_type: 'interval',
    interval_seconds: 3600,
    schedule_time: null,
    timezone: null,
    days_of_week: null,
    day_of_month: null,
    week_parity: null,
    parameters: {},
    next_run_at: null,
    last_run_at: null,
    description: 'Every hour',
    created_at: '2026-07-01T00:00:00Z',
    updated_at: null,
    ...overrides,
  } as TaskSchedule;
}

function makeTask(overrides: Partial<TaskStatus> = {}): TaskStatus {
  return {
    task_id: 'auto_creation',
    task_name: 'Channel Pipeline',
    task_description: 'Runs pipeline rules',
    status: 'idle',
    enabled: true,
    progress: {
      total: 0, current: 0, status: 'idle', current_item: null,
      success_count: 0, failed_count: 0, skipped_count: 0,
    } as unknown as TaskStatus['progress'],
    schedule: { schedule_type: 'interval' } as unknown as TaskStatus['schedule'],
    schedules: [],
    last_run: null,
    next_run: null,
    config: {},
    ...overrides,
  };
}

function renderEditor(task: TaskStatus) {
  return render(<TaskEditorModal task={task} onClose={() => {}} onSaved={() => {}} />);
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

describe('TaskEditorModal — vkktd.4 wontRun UX', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(api.getTaskSchedules).mockReset().mockResolvedValue({ schedules: [] });
  });

  it('shows the rewritten enable hint copy', async () => {
    vi.mocked(api.getTaskSchedules).mockResolvedValue({ schedules: [] });
    renderEditor(makeTask());

    expect(
      await screen.findByText(/the task and at least one schedule below must both be enabled/i)
    ).toBeInTheDocument();
    expect(
      screen.queryByText(/when disabled, no schedules will run/i)
    ).not.toBeInTheDocument();
  });

  it('keeps an unresolved empty schedule list neutral until the request completes', async () => {
    let resolveSchedules: ((value: Awaited<ReturnType<typeof api.getTaskSchedules>>) => void) | undefined;
    vi.mocked(api.getTaskSchedules).mockReturnValueOnce(
      new Promise<Awaited<ReturnType<typeof api.getTaskSchedules>>>((resolve) => {
        resolveSchedules = resolve;
      }),
    );
    renderEditor(makeTask());

    const schedulesSection = screen.getByText('Schedules').closest('.schedules-section');
    expect(screen.queryByText(/no schedules configured/i)).not.toBeInTheDocument();
    expect(screen.queryByTestId('schedule-wont-run-warning')).not.toBeInTheDocument();
    expect(screen.getByRole('status')).toHaveTextContent('Loading schedules…');
    expect(schedulesSection).toHaveAttribute('aria-busy', 'true');

    await act(async () => {
      resolveSchedules!({ schedules: [] });
    });

    expect(await screen.findByText(/no schedules configured/i)).toBeInTheDocument();
    expect(await screen.findByTestId('schedule-wont-run-warning')).toHaveTextContent(
      /has no schedules/i,
    );
    expect(schedulesSection).toHaveAttribute('aria-busy', 'false');
  });

  it('blocks pointer and keyboard Save until the authoritative schedule read completes', async () => {
    const user = userEvent.setup();
    const schedules = deferred<Awaited<ReturnType<typeof api.getTaskSchedules>>>();
    const onClose = vi.fn();
    const onSaved = vi.fn();
    vi.mocked(api.getTaskSchedules).mockReturnValueOnce(schedules.promise);
    render(<TaskEditorModal task={makeTask({ schedules: [] })} onClose={onClose} onSaved={onSaved} />);

    const save = screen.getByRole('button', { name: /save changes/i });
    expect(save).toBeDisabled();
    await user.click(save);

    save.removeAttribute('disabled');
    save.focus();
    await user.keyboard('{Enter}');

    expect(api.updateTask).not.toHaveBeenCalled();
    expect(onSaved).not.toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();
    expect(notify.success).not.toHaveBeenCalled();
    expect(notify.info).not.toHaveBeenCalled();
    expect(api.getTaskSchedules).toHaveBeenCalledTimes(1);

    await act(async () => {
      schedules.resolve({ schedules: [makeSchedule({ enabled: false })] });
    });

    await waitFor(() => expect(save).toBeEnabled());
    expect(await screen.findByTestId('schedule-wont-run-warning')).toBeInTheDocument();
    expect(api.updateTask).not.toHaveBeenCalled();
  });

  it('keeps initial schedule rows mounted while their refresh is pending', async () => {
    let resolveSchedules: ((value: Awaited<ReturnType<typeof api.getTaskSchedules>>) => void) | undefined;
    const schedule = makeSchedule({ enabled: false });
    vi.mocked(api.getTaskSchedules).mockReturnValueOnce(
      new Promise<Awaited<ReturnType<typeof api.getTaskSchedules>>>((resolve) => {
        resolveSchedules = resolve;
      }),
    );
    renderEditor(makeTask({ schedules: [schedule] }));

    expect(screen.getByText('Hourly')).toBeInTheDocument();
    expect(screen.getByText('Schedules').closest('.schedules-section')).toHaveAttribute('aria-busy', 'true');
    expect(screen.queryByTestId('schedule-wont-run-warning')).not.toBeInTheDocument();

    await act(async () => {
      resolveSchedules!({ schedules: [schedule] });
    });

    expect(await screen.findByTestId('schedule-wont-run-warning')).toBeInTheDocument();
    expect(screen.getByText('Hourly')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /save changes/i })).toBeEnabled();
  });

  it('shows a load error without concluding that an empty task has no schedules', async () => {
    let rejectSchedules: ((reason?: unknown) => void) | undefined;
    vi.mocked(api.getTaskSchedules).mockReturnValueOnce(
      new Promise<Awaited<ReturnType<typeof api.getTaskSchedules>>>((_resolve, reject) => {
        rejectSchedules = reject;
      }),
    );
    renderEditor(makeTask());

    await act(async () => {
      rejectSchedules!(new Error('Schedule request failed'));
    });

    const loadError = await screen.findByText(
      'Could not load schedules. Close and reopen this dialog to try again.',
    );
    expect(loadError.closest('[role="alert"]')).toBeInTheDocument();
    expect(screen.queryByText(/no schedules configured/i)).not.toBeInTheDocument();
    expect(screen.queryByTestId('schedule-wont-run-warning')).not.toBeInTheDocument();
    const save = screen.getByRole('button', { name: /save changes/i });
    expect(save).toBeDisabled();
    fireEvent.click(save);
    expect(api.updateTask).not.toHaveBeenCalled();
    expect(notify.success).not.toHaveBeenCalled();
  });

  it('retains populated rows beside a schedule load error', async () => {
    let rejectSchedules: ((reason?: unknown) => void) | undefined;
    vi.mocked(api.getTaskSchedules).mockReturnValueOnce(
      new Promise<Awaited<ReturnType<typeof api.getTaskSchedules>>>((_resolve, reject) => {
        rejectSchedules = reject;
      }),
    );
    renderEditor(makeTask({ schedules: [makeSchedule({ enabled: false })] }));

    await act(async () => {
      rejectSchedules!(new Error('Schedule request failed'));
    });

    const loadError = await screen.findByText(
      'Could not load schedules. Close and reopen this dialog to try again.',
    );
    expect(loadError.closest('[role="alert"]')).toBeInTheDocument();
    expect(screen.getByText('Hourly')).toBeInTheDocument();
    expect(screen.queryByTestId('schedule-wont-run-warning')).not.toBeInTheDocument();
    const save = screen.getByRole('button', { name: /save changes/i });
    expect(save).toBeDisabled();
    fireEvent.click(save);
    expect(api.updateTask).not.toHaveBeenCalled();
    expect(notify.success).not.toHaveBeenCalled();
  });

  it('clears a prior load error after a later schedule refresh succeeds', async () => {
    let resolveSchedules: ((value: Awaited<ReturnType<typeof api.getTaskSchedules>>) => void) | undefined;
    const disabledSchedule = makeSchedule({ enabled: false });
    const enabledSchedule = makeSchedule({ enabled: true });
    vi.mocked(api.getTaskSchedules)
      .mockRejectedValueOnce(new Error('Schedule request failed'))
      .mockReturnValueOnce(
        new Promise<Awaited<ReturnType<typeof api.getTaskSchedules>>>((resolve) => {
          resolveSchedules = resolve;
        }),
      );
    renderEditor(makeTask({ schedules: [disabledSchedule] }));

    expect(await screen.findByText(
      'Could not load schedules. Close and reopen this dialog to try again.',
    )).toBeInTheDocument();
    const toggle = screen.getByRole('checkbox', { name: /Hourly/i });
    fireEvent.click(toggle);

    await waitFor(() => expect(api.getTaskSchedules).toHaveBeenCalledTimes(2));
    expect(screen.getByText('Hourly')).toBeInTheDocument();
    expect(screen.getByText('Schedules').closest('.schedules-section')).toHaveAttribute('aria-busy', 'true');
    expect(screen.queryByText(
      'Could not load schedules. Close and reopen this dialog to try again.',
    )).not.toBeInTheDocument();

    await act(async () => {
      resolveSchedules!({ schedules: [enabledSchedule] });
    });

    await waitFor(() => {
      expect(screen.getByText('Schedules').closest('.schedules-section')).toHaveAttribute('aria-busy', 'false');
    });
    expect(screen.getByText('Hourly')).toBeInTheDocument();
    expect(screen.queryByTestId('schedule-wont-run-warning')).not.toBeInTheDocument();
  });

  it('lets only the latest schedule refresh set rows and Save readiness', async () => {
    const initial = deferred<Awaited<ReturnType<typeof api.getTaskSchedules>>>();
    const failedRefresh = deferred<Awaited<ReturnType<typeof api.getTaskSchedules>>>();
    const latestRefresh = deferred<Awaited<ReturnType<typeof api.getTaskSchedules>>>();
    const snapshot = makeSchedule({ name: 'Snapshot schedule', enabled: false });
    vi.mocked(api.getTaskSchedules)
      .mockReturnValueOnce(initial.promise)
      .mockReturnValueOnce(failedRefresh.promise)
      .mockReturnValueOnce(latestRefresh.promise);
    renderEditor(makeTask({ schedules: [snapshot] }));

    const save = screen.getByRole('button', { name: /save changes/i });
    expect(save).toBeDisabled();
    fireEvent.click(screen.getByRole('checkbox', { name: /Snapshot schedule/i }));
    await waitFor(() => expect(api.getTaskSchedules).toHaveBeenCalledTimes(2));

    await act(async () => {
      initial.resolve({ schedules: [makeSchedule({ name: 'Older response', enabled: true })] });
    });

    expect(screen.queryByText('Older response')).not.toBeInTheDocument();
    expect(screen.getByText('Snapshot schedule')).toBeInTheDocument();
    expect(screen.getByText('Schedules').closest('.schedules-section')).toHaveAttribute('aria-busy', 'true');
    expect(save).toBeDisabled();

    await act(async () => {
      failedRefresh.reject(new Error('Latest schedule request failed'));
    });

    expect(await screen.findByText(
      'Could not load schedules. Close and reopen this dialog to try again.',
    )).toBeInTheDocument();
    expect(screen.getByText('Snapshot schedule')).toBeInTheDocument();
    expect(save).toBeDisabled();

    fireEvent.click(screen.getByRole('checkbox', { name: /Snapshot schedule/i }));
    await waitFor(() => expect(api.getTaskSchedules).toHaveBeenCalledTimes(3));
    await act(async () => {
      latestRefresh.resolve({ schedules: [makeSchedule({ name: 'Latest response', enabled: true })] });
    });

    await waitFor(() => expect(save).toBeEnabled());
    expect(screen.getByText('Latest response')).toBeInTheDocument();
    expect(screen.queryByText('Snapshot schedule')).not.toBeInTheDocument();
    expect(screen.queryByText(
      'Could not load schedules. Close and reopen this dialog to try again.',
    )).not.toBeInTheDocument();
  });

  it('keeps a reopened Task Editor owned by its new schedule request', async () => {
    const changedRequest = deferred<Awaited<ReturnType<typeof api.getTaskSchedules>>>();
    const closedRequest = deferred<Awaited<ReturnType<typeof api.getTaskSchedules>>>();
    const reopenedRequest = deferred<Awaited<ReturnType<typeof api.getTaskSchedules>>>();
    vi.mocked(api.getTaskSchedules)
      .mockReturnValueOnce(changedRequest.promise)
      .mockReturnValueOnce(closedRequest.promise)
      .mockReturnValueOnce(reopenedRequest.promise);

    const first = renderEditor(makeTask({ schedules: [makeSchedule({ name: 'Original snapshot' })] }));
    first.rerender(
      <TaskEditorModal
        task={makeTask({ task_id: 'cleanup', schedules: [makeSchedule({ name: 'Changed snapshot' })] })}
        onClose={() => {}}
        onSaved={() => {}}
      />,
    );

    const changedSave = screen.getByRole('button', { name: /save changes/i });
    expect(changedSave).toBeDisabled();
    await waitFor(() => expect(api.getTaskSchedules).toHaveBeenCalledTimes(2));
    await act(async () => {
      changedRequest.resolve({ schedules: [makeSchedule({ name: 'Old task response', enabled: true })] });
    });

    expect(screen.getByText('Original snapshot')).toBeInTheDocument();
    expect(screen.queryByText('Old task response')).not.toBeInTheDocument();
    expect(changedSave).toBeDisabled();

    first.unmount();
    renderEditor(makeTask({ task_id: 'stream_probe', schedules: [makeSchedule({ name: 'Reopened snapshot' })] }));

    const save = screen.getByRole('button', { name: /save changes/i });
    expect(save).toBeDisabled();
    await act(async () => {
      closedRequest.resolve({ schedules: [makeSchedule({ name: 'Closed response', enabled: true })] });
    });

    expect(screen.getByText('Reopened snapshot')).toBeInTheDocument();
    expect(screen.queryByText('Closed response')).not.toBeInTheDocument();
    expect(save).toBeDisabled();

    await act(async () => {
      reopenedRequest.resolve({ schedules: [makeSchedule({ name: 'Reopened response', enabled: true })] });
    });

    await waitFor(() => expect(save).toBeEnabled());
    expect(screen.getByText('Reopened response')).toBeInTheDocument();
    expect(screen.queryByText('Reopened snapshot')).not.toBeInTheDocument();
  });

  it('shows the inline warning when the task is enabled and all schedules are disabled', async () => {
    vi.mocked(api.getTaskSchedules).mockResolvedValue({ schedules: [makeSchedule({ enabled: false })] });
    renderEditor(makeTask());

    const warning = await screen.findByTestId('schedule-wont-run-warning');
    expect(warning).toHaveTextContent(/will not run automatically/i);
    // Non-manual task with an existing schedule → promises the save reconcile.
    expect(warning).toHaveTextContent(/save and the most recent schedule will be enabled/i);
    expect(screen.getByRole('button', { name: /save changes/i })).toBeEnabled();
  });

  it('hides the warning when at least one schedule is enabled', async () => {
    vi.mocked(api.getTaskSchedules).mockResolvedValue({ schedules: [makeSchedule({ enabled: true })] });
    renderEditor(makeTask());

    // Wait for schedules to load, then assert no warning.
    await screen.findByText('Hourly');
    expect(screen.queryByTestId('schedule-wont-run-warning')).not.toBeInTheDocument();
  });

  it('hides the warning for MANUAL-only tasks with no schedules', async () => {
    vi.mocked(api.getTaskSchedules).mockResolvedValue({ schedules: [] });
    renderEditor(makeTask({
      task_id: 'cleanup',
      schedule: { schedule_type: 'manual' } as unknown as TaskStatus['schedule'],
    }));

    await screen.findByText(/no schedules configured/i);
    expect(screen.queryByTestId('schedule-wont-run-warning')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /save changes/i })).toBeEnabled();
  });

  it('shows the warning when unchecking is reverted (live with the checkbox)', async () => {
    vi.mocked(api.getTaskSchedules).mockResolvedValue({ schedules: [makeSchedule({ enabled: false })] });
    renderEditor(makeTask());

    await screen.findByTestId('schedule-wont-run-warning');

    // Disable the task → warning goes away (a disabled task is honest).
    fireEvent.click(screen.getByLabelText(/enable task/i));
    expect(screen.queryByTestId('schedule-wont-run-warning')).not.toBeInTheDocument();

    // Re-enable → warning returns.
    fireEvent.click(screen.getByLabelText(/enable task/i));
    expect(screen.getByTestId('schedule-wont-run-warning')).toBeInTheDocument();
  });

  it('toasts when saving auto-reconciled the existing schedule', async () => {
    // Before save: one disabled schedule. After save: backend reconciled it on.
    vi.mocked(api.getTaskSchedules)
      .mockResolvedValueOnce({ schedules: [makeSchedule({ enabled: false })] })
      .mockResolvedValue({ schedules: [makeSchedule({ enabled: true })] });
    renderEditor(makeTask());

    await screen.findByTestId('schedule-wont-run-warning');
    const save = screen.getByRole('button', { name: /save changes/i });
    await waitFor(() => expect(save).toBeEnabled());
    fireEvent.click(save);

    await waitFor(() => {
      expect(notify.info).toHaveBeenCalledWith(
        expect.stringMatching(/also enabled the "Hourly" schedule, so this task will actually run/i),
        'Channel Pipeline'
      );
    });
    expect(api.updateTask).toHaveBeenCalledWith('auto_creation', expect.objectContaining({ enabled: true }));
  });

  it('does not toast a reconcile when a schedule was already enabled', async () => {
    vi.mocked(api.getTaskSchedules).mockResolvedValue({ schedules: [makeSchedule({ enabled: true })] });
    renderEditor(makeTask());

    await screen.findByText('Hourly');
    const save = screen.getByRole('button', { name: /save changes/i });
    await waitFor(() => expect(save).toBeEnabled());
    fireEvent.click(save);

    await waitFor(() => expect(api.updateTask).toHaveBeenCalled());
    expect(notify.info).not.toHaveBeenCalled();
  });

  it('prevents a second Save while the first authoritative update is pending', async () => {
    const user = userEvent.setup();
    const update = deferred<Awaited<ReturnType<typeof api.updateTask>>>();
    const onClose = vi.fn();
    const onSaved = vi.fn();
    vi.mocked(api.getTaskSchedules).mockResolvedValue({ schedules: [makeSchedule({ enabled: true })] });
    vi.mocked(api.updateTask).mockReturnValueOnce(update.promise);
    render(<TaskEditorModal task={makeTask()} onClose={onClose} onSaved={onSaved} />);

    await screen.findByText('Hourly');
    const save = screen.getByRole('button', { name: /save changes/i });
    await waitFor(() => expect(save).toBeEnabled());
    await user.click(save);

    const saving = screen.getByRole('button', { name: /saving/i });
    expect(saving).toBeDisabled();
    await user.click(saving);
    expect(api.updateTask).toHaveBeenCalledTimes(1);
    expect(onSaved).not.toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();

    await act(async () => {
      update.resolve(makeTask());
    });

    await waitFor(() => expect(onSaved).toHaveBeenCalledTimes(1));
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(api.updateTask).toHaveBeenCalledTimes(1);
  });

  it('opens straight at Add Schedule when openAddSchedule is set (Fix-it path)', async () => {
    vi.mocked(api.getTaskSchedules).mockResolvedValue({ schedules: [] });
    render(
      <TaskEditorModal task={makeTask()} onClose={() => {}} onSaved={() => {}} openAddSchedule />
    );

    expect(await screen.findByRole('heading', { name: /add schedule/i })).toBeInTheDocument();
  });

  it('stacks one named schedule dialog, closes it first, and restores focus to its parent opener', async () => {
    const user = userEvent.setup();
    vi.mocked(api.getTaskSchedules).mockResolvedValue({ schedules: [] });
    const onClose = vi.fn();
    render(<TaskEditorModal task={makeTask()} onClose={onClose} onSaved={() => {}} />);

    const parent = await screen.findByRole('dialog', { name: 'Configure Task' });
    const opener = within(parent).getByRole('button', { name: /Add Schedule$/ });
    await user.click(opener);

    const child = screen.getByRole('dialog', { name: 'Add Schedule' });
    expect(screen.getAllByRole('dialog')).toEqual([parent, child]);
    expect(parent.getAttribute('aria-labelledby')).not.toBe(child.getAttribute('aria-labelledby'));
    await waitFor(() => expect(child).toContainElement(document.activeElement as HTMLElement));

    await user.keyboard('{Escape}');
    expect(screen.queryByRole('dialog', { name: 'Add Schedule' })).not.toBeInTheDocument();
    expect(screen.getByRole('dialog', { name: 'Configure Task' })).toBe(parent);
    expect(opener).toHaveFocus();
    expect(onClose).not.toHaveBeenCalled();

    await user.keyboard('{Escape}');
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
