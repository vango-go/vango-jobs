package jobscmd

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	jobs "github.com/vango-go/vango-jobs"
)

var writeStatusTextFn = writeStatusText

// Config configures the scaffold-friendly worker/admin command surface.
type Config struct {
	Runtime   *jobs.Runtime
	Worker    *jobs.Worker
	Scheduler *jobs.Scheduler
	Stdout    io.Writer
	Stderr    io.Writer
}

// Run executes a jobs worker/admin subcommand.
func Run(ctx context.Context, cfg Config, args []string) error {
	if cfg.Runtime == nil {
		return fmt.Errorf("jobscmd: runtime is required")
	}
	if cfg.Worker == nil {
		cfg.Worker = cfg.Runtime.NewWorker()
	}
	if cfg.Scheduler == nil {
		cfg.Scheduler = cfg.Runtime.NewScheduler()
	}
	if cfg.Stdout == nil {
		cfg.Stdout = os.Stdout
	}
	if cfg.Stderr == nil {
		cfg.Stderr = os.Stderr
	}
	if len(args) == 0 {
		return usageError("jobs subcommand required (run, status, inspect, retry, cancel, pause, resume, prune)")
	}
	switch args[0] {
	case "run":
		return runWorkerProcess(ctx, cfg, args[1:])
	case "status":
		return runStatus(ctx, cfg, args[1:])
	case "inspect":
		return runInspect(ctx, cfg, args[1:])
	case "retry":
		return runRetry(ctx, cfg, args[1:])
	case "cancel":
		return runCancel(ctx, cfg, args[1:])
	case "pause":
		return runPause(ctx, cfg, args[1:])
	case "resume":
		return runResume(ctx, cfg, args[1:])
	case "prune":
		return runPrune(ctx, cfg, args[1:])
	default:
		return usageError("unknown jobs subcommand: " + args[0])
	}
}

func runWorkerProcess(ctx context.Context, cfg Config, args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(cfg.Stderr)
	var (
		noScheduler    bool
		schedulerOnly  bool
		statusInterval time.Duration
		statusFailures int
	)
	fs.BoolVar(&noScheduler, "no-scheduler", false, "run workers without the scheduler loop")
	fs.BoolVar(&schedulerOnly, "scheduler-only", false, "run only the scheduler loop")
	fs.DurationVar(&statusInterval, "status-interval", 0, "emit periodic queue/failure status snapshots")
	fs.IntVar(&statusFailures, "status-failures", 5, "recent failed jobs to include in status snapshots")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if noScheduler && schedulerOnly {
		return usageError("cannot combine --no-scheduler with --scheduler-only")
	}

	runWorker := !schedulerOnly
	runScheduler := !noScheduler
	if !runWorker && !runScheduler {
		return usageError("nothing to run")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg    sync.WaitGroup
		errCh = make(chan error, 3)
	)
	if runWorker {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- cfg.Worker.Run(ctx)
		}()
	}
	if runScheduler {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- cfg.Scheduler.Run(ctx)
		}()
	}
	if statusInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runWorkerStatusLoop(ctx, cfg, statusInterval, statusFailures); err != nil && ctx.Err() == nil {
				_, _ = fmt.Fprintf(statusErrorWriter(cfg), "jobs status reporter stopped: %v\n", err)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		<-done
		return nil
	case err := <-errCh:
		cancel()
		<-done
		if err == nil || ctx.Err() != nil {
			return nil
		}
		return err
	}
}

func runStatus(ctx context.Context, cfg Config, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(cfg.Stderr)
	var (
		watch    bool
		interval time.Duration
		failures int
	)
	fs.BoolVar(&watch, "watch", false, "poll and print a human-readable jobs status stream")
	fs.DurationVar(&interval, "interval", 10*time.Second, "poll interval for --watch")
	fs.IntVar(&failures, "failures", 5, "recent failed jobs to include")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return usageError("status does not accept positional arguments")
	}
	if watch {
		return runStatusLoop(ctx, cfg, interval, failures)
	}
	snapshot, err := readStatusSnapshot(ctx, cfg, failures)
	if err != nil {
		return err
	}
	return writeJSON(cfg.Stdout, snapshot)
}

func runInspect(ctx context.Context, cfg Config, args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	fs.SetOutput(cfg.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return usageError("inspect requires exactly one job id")
	}
	id := fs.Arg(0)
	snapshot, err := cfg.Runtime.Inspect(ctx, id, jobs.InspectAdmin())
	if err != nil {
		return err
	}
	attempts, err := cfg.Runtime.Attempts(ctx, id, jobs.InspectAdmin())
	if err != nil {
		return err
	}
	events, err := cfg.Runtime.Events(ctx, id, jobs.InspectAdmin())
	if err != nil {
		return err
	}
	return writeJSON(cfg.Stdout, map[string]any{
		"snapshot": snapshot,
		"attempts": attempts,
		"events":   events,
	})
}

func runRetry(ctx context.Context, cfg Config, args []string) error {
	fs := flag.NewFlagSet("retry", flag.ContinueOnError)
	fs.SetOutput(cfg.Stderr)
	var (
		queue          string
		jobName        string
		tenantID       string
		statuses       string
		since          string
		until          string
		limit          int
		preserveUnique bool
		overrideQueue  string
		delay          time.Duration
		runAt          string
	)
	fs.StringVar(&queue, "queue", "", "filter or override queue")
	fs.StringVar(&jobName, "job", "", "filter by durable job name")
	fs.StringVar(&tenantID, "tenant", "", "filter by tenant id")
	fs.StringVar(&statuses, "status", "failed", "comma-separated historical statuses to replay")
	fs.StringVar(&since, "since", "", "relative duration or RFC3339 lower bound")
	fs.StringVar(&until, "until", "", "relative duration or RFC3339 upper bound")
	fs.IntVar(&limit, "limit", 100, "maximum jobs to replay or prune in one pass")
	fs.BoolVar(&preserveUnique, "preserve-unique", false, "preserve the historical unique key on replayed jobs")
	fs.StringVar(&overrideQueue, "override-queue", "", "override the queue for cloned retries")
	fs.DurationVar(&delay, "delay", 0, "override cloned retries to run after a delay")
	fs.StringVar(&runAt, "run-at", "", "override cloned retries to run at an RFC3339 timestamp")
	if err := fs.Parse(args); err != nil {
		return err
	}

	retryOpts := []jobs.RetryOption{jobs.RetryAdmin(), jobs.RetryPreserveUnique(preserveUnique)}
	if overrideQueue != "" {
		retryOpts = append(retryOpts, jobs.RetryQueue(overrideQueue))
	}
	if delay > 0 {
		retryOpts = append(retryOpts, jobs.RetryDelay(delay))
	}
	if runAt != "" {
		parsed, err := time.Parse(time.RFC3339, runAt)
		if err != nil {
			return fmt.Errorf("retry --run-at: %w", err)
		}
		retryOpts = append(retryOpts, jobs.RetryRunAt(parsed))
	}

	switch fs.NArg() {
	case 1:
		ref, err := cfg.Runtime.Retry(ctx, fs.Arg(0), retryOpts...)
		if err != nil {
			return err
		}
		return writeJSON(cfg.Stdout, ref)
	case 0:
		filter := jobs.ReplayFilter{
			Queue:    queue,
			JobName:  jobName,
			TenantID: tenantID,
			Statuses: parseStatuses(statuses),
			Limit:    limit,
		}
		var err error
		if filter.Since, err = parseTimeBound(since); err != nil {
			return fmt.Errorf("retry --since: %w", err)
		}
		if filter.Until, err = parseTimeBound(until); err != nil {
			return fmt.Errorf("retry --until: %w", err)
		}
		summary, err := cfg.Runtime.Replay(ctx, filter, retryOpts...)
		if err != nil {
			return err
		}
		return writeJSON(cfg.Stdout, summary)
	default:
		return usageError("retry accepts either one job id or only filter flags")
	}
}

func runCancel(ctx context.Context, cfg Config, args []string) error {
	fs := flag.NewFlagSet("cancel", flag.ContinueOnError)
	fs.SetOutput(cfg.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return usageError("cancel requires exactly one job id")
	}
	if err := cfg.Runtime.Cancel(ctx, fs.Arg(0), jobs.ControlAdmin()); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(cfg.Stdout, fs.Arg(0))
	return nil
}

func runPause(ctx context.Context, cfg Config, args []string) error {
	fs := flag.NewFlagSet("pause", flag.ContinueOnError)
	fs.SetOutput(cfg.Stderr)
	var reason string
	fs.StringVar(&reason, "reason", "", "pause reason")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return usageError("pause requires exactly one queue name")
	}
	if err := cfg.Runtime.PauseQueue(ctx, fs.Arg(0), reason, jobs.ControlAdmin()); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(cfg.Stdout, fs.Arg(0))
	return nil
}

func runResume(ctx context.Context, cfg Config, args []string) error {
	fs := flag.NewFlagSet("resume", flag.ContinueOnError)
	fs.SetOutput(cfg.Stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return usageError("resume requires exactly one queue name")
	}
	if err := cfg.Runtime.ResumeQueue(ctx, fs.Arg(0), jobs.ControlAdmin()); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(cfg.Stdout, fs.Arg(0))
	return nil
}

func runPrune(ctx context.Context, cfg Config, args []string) error {
	fs := flag.NewFlagSet("prune", flag.ContinueOnError)
	fs.SetOutput(cfg.Stderr)
	var limit int
	fs.IntVar(&limit, "limit", 100, "maximum rows to prune in one pass")
	if err := fs.Parse(args); err != nil {
		return err
	}
	summary, err := cfg.Runtime.Prune(ctx, jobs.PruneAdmin(), jobs.PruneLimit(limit))
	if err != nil {
		return err
	}
	return writeJSON(cfg.Stdout, summary)
}

type statusSnapshot struct {
	Queues         []jobs.QueueSnapshot   `json:"queues"`
	RecentFailures []statusFailureSummary `json:"recent_failures"`
	GeneratedAt    time.Time              `json:"generated_at"`
}

// statusFailureSummary is the safe, summary-oriented status view for failed jobs.
// Deep payload/output/metadata inspection belongs to the explicit inspect command.
type statusFailureSummary struct {
	Ref         jobs.Ref    `json:"ref"`
	Queue       string      `json:"queue"`
	Status      jobs.Status `json:"status"`
	Attempts    int         `json:"attempts"`
	MaxAttempts int         `json:"max_attempts"`
	CreatedAt   time.Time   `json:"created_at"`
	FinishedAt  *time.Time  `json:"finished_at,omitempty"`
	SafeError   string      `json:"safe_error,omitempty"`
	ErrorCode   string      `json:"error_code,omitempty"`
}

func readStatusSnapshot(ctx context.Context, cfg Config, failures int) (*statusSnapshot, error) {
	queues, err := cfg.Runtime.Queues(ctx, jobs.ControlAdmin())
	if err != nil {
		return nil, err
	}
	if failures < 0 {
		failures = 0
	}
	recentFailures := make([]statusFailureSummary, 0)
	if failures > 0 {
		rows, err := cfg.Runtime.List(ctx, jobs.ListFilter{
			Statuses: []jobs.Status{jobs.StatusFailed},
			Limit:    failures,
			Order:    jobs.ListOrderTerminalTimeDesc,
		}, jobs.InspectAdmin())
		if err != nil {
			return nil, err
		}
		recentFailures = summarizeStatusFailures(rows)
	}
	return &statusSnapshot{
		Queues:         queues,
		RecentFailures: recentFailures,
		GeneratedAt:    time.Now().UTC(),
	}, nil
}

func runStatusLoop(ctx context.Context, cfg Config, interval time.Duration, failures int) error {
	if interval <= 0 {
		return usageError("status interval must be greater than zero")
	}
	if err := runStatusTick(ctx, cfg, failures, false); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := runStatusTick(ctx, cfg, failures, false); err != nil {
				return err
			}
		}
	}
}

func runWorkerStatusLoop(ctx context.Context, cfg Config, interval time.Duration, failures int) error {
	if interval <= 0 {
		return usageError("status interval must be greater than zero")
	}
	if err := runStatusTick(ctx, cfg, failures, true); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := runStatusTick(ctx, cfg, failures, true); err != nil {
				return err
			}
		}
	}
}

func runStatusTick(ctx context.Context, cfg Config, failures int, bestEffort bool) error {
	if err := writeStatusTextFn(ctx, cfg, failures); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if !bestEffort {
			return err
		}
		_, _ = fmt.Fprintf(statusErrorWriter(cfg), "jobs status snapshot failed; continuing: %v\n", err)
	}
	return nil
}

func statusErrorWriter(cfg Config) io.Writer {
	if cfg.Stderr != nil {
		return cfg.Stderr
	}
	return os.Stderr
}

func writeStatusText(ctx context.Context, cfg Config, failures int) error {
	snapshot, err := readStatusSnapshot(ctx, cfg, failures)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(cfg.Stdout, "jobs status @ %s\n", snapshot.GeneratedAt.Format(time.RFC3339))
	if len(snapshot.Queues) == 0 {
		_, _ = fmt.Fprintln(cfg.Stdout, "  queues: none")
	} else {
		for _, queue := range snapshot.Queues {
			line := fmt.Sprintf("  queue=%s queued=%d running=%d", queue.Queue, queue.Queued, queue.Running)
			if queue.Paused {
				line += " paused=true"
				if queue.PauseReason != "" {
					line += " reason=" + queue.PauseReason
				}
			}
			_, _ = fmt.Fprintln(cfg.Stdout, line)
		}
	}
	if len(snapshot.RecentFailures) == 0 {
		_, _ = fmt.Fprintln(cfg.Stdout, "  recent_failures: none")
	} else {
		_, _ = fmt.Fprintln(cfg.Stdout, "  recent_failures:")
		for _, item := range snapshot.RecentFailures {
			_, _ = fmt.Fprintf(cfg.Stdout, "    %s %s queue=%s attempts=%d", item.Ref.ID, item.Ref.Name, item.Queue, item.Attempts)
			if item.SafeError != "" {
				_, _ = fmt.Fprintf(cfg.Stdout, " error=%q", item.SafeError)
			}
			_, _ = fmt.Fprintln(cfg.Stdout)
		}
	}
	_, _ = fmt.Fprintln(cfg.Stdout)
	return nil
}

func summarizeStatusFailures(rows []*jobs.RawSnapshot) []statusFailureSummary {
	out := make([]statusFailureSummary, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		out = append(out, statusFailureSummary{
			Ref:         row.Ref,
			Queue:       row.Queue,
			Status:      row.Status,
			Attempts:    row.Attempts,
			MaxAttempts: row.MaxAttempts,
			CreatedAt:   row.CreatedAt,
			FinishedAt:  row.FinishedAt,
			SafeError:   row.SafeError,
			ErrorCode:   row.ErrorCode,
		})
	}
	return out
}

func parseStatuses(raw string) []jobs.Status {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []jobs.Status{jobs.StatusFailed}
	}
	parts := strings.Split(raw, ",")
	out := make([]jobs.Status, 0, len(parts))
	for _, part := range parts {
		status := jobs.Status(strings.TrimSpace(part))
		switch status {
		case jobs.StatusQueued, jobs.StatusRunning, jobs.StatusSucceeded, jobs.StatusFailed, jobs.StatusCanceled:
			out = append(out, status)
		}
	}
	if len(out) == 0 {
		return []jobs.Status{jobs.StatusFailed}
	}
	return out
}

func parseTimeBound(raw string) (*time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if d, err := time.ParseDuration(raw); err == nil {
		t := time.Now().UTC().Add(-d)
		return &t, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, err
	}
	t = t.UTC()
	return &t, nil
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

type usageError string

func (e usageError) Error() string { return string(e) }
