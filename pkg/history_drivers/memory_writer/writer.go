package memory_writer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/history"
	"github.com/inngest/inngest/pkg/history_drivers/memory_store"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/oklog/ulid/v2"
)

const (
	// MaxRuns limits the number of runs stored in memory to prevent unbounded growth
	MaxRuns = 1000
	// MaxHistoryPerRun limits the number of history items per run
	MaxHistoryPerRun = 1000
)

type WriterOptions struct {
	DumpToFile bool
}

func NewWriter(ctx context.Context, opts WriterOptions) history.Driver {
	l := logger.StdlibLogger(ctx).With("caller", "memory writer")

	w := &writer{
		store:   memory_store.Singleton,
		options: opts,
		log:     l,
	}

	if !opts.DumpToFile || len(memory_store.Singleton.Data) > 0 {
		return w
	}

	// read data from file and populate memory_store.Singleton
	file, err := os.ReadFile(fmt.Sprintf("%s/%s", consts.DefaultInngestConfigDir, consts.DevServerHistoryFile))
	if err != nil {
		if os.IsNotExist(err) {
			return w
		}
		l.Error("failed to read history file", "error", err)
	}

	err = json.Unmarshal(file, &memory_store.Singleton.Data)
	if err != nil {
		l.Error("failed to unmarshal history file", "error", err)
	}

	humanSize := fmt.Sprintf("%.2fKB", float64(len(file))/1024)
	l.Info("imported history snapshot", "size", humanSize)

	return w
}

type writer struct {
	store   *memory_store.RunStore
	options WriterOptions
	log     logger.Logger
}

func (w *writer) Close(ctx context.Context) error {
	if !w.options.DumpToFile {
		return nil
	}

	w.store.Mu.Lock()
	// never unlock

	b, err := json.Marshal(w.store.Data)
	if err != nil {
		w.log.Error("error marshalling history data for export", "error", err)
		return err
	}

	err = os.WriteFile(fmt.Sprintf("%s/%s", consts.DefaultInngestConfigDir, consts.DevServerHistoryFile), b, 0600)
	if err != nil {
		w.log.Error("error writing history data to file", "error", err)
		return err
	}

	humanSize := fmt.Sprintf("%.2fKB", float64(len(b))/1024)
	w.log.Error("exported history snapshot", "size", humanSize)

	return nil
}

func (w *writer) Write(
	ctx context.Context,
	item history.History,
) error {
	w.store.Mu.Lock()
	defer w.store.Mu.Unlock()

	if item.Type == enums.HistoryTypeFunctionScheduled.String() ||
		item.Type == enums.HistoryTypeFunctionStarted.String() {
		w.writeWorkflowStart(ctx, item)
	} else if item.Type == enums.HistoryTypeFunctionCancelled.String() ||
		item.Type == enums.HistoryTypeFunctionCompleted.String() ||
		item.Type == enums.HistoryTypeFunctionFailed.String() {
		w.writeWorkflowEnd(ctx, item)
	}

	w.writeHistory(ctx, item)
	return nil
}

func (w *writer) writeHistory(
	ctx context.Context,
	item history.History,
) {
	run := w.store.Data[item.RunID]
	run.History = append(run.History, item)

	// Limit history items per run to prevent unbounded growth
	if len(run.History) > MaxHistoryPerRun {
		// Keep the most recent items
		run.History = run.History[len(run.History)-MaxHistoryPerRun:]
	}

	w.store.Data[item.RunID] = run

	// Clean up old runs if we exceed the limit
	w.cleanupOldRuns(ctx)
}

func (w *writer) cleanupOldRuns(ctx context.Context) {
	if len(w.store.Data) <= MaxRuns {
		return
	}

	// Find the oldest runs by RunID (ULID contains timestamp)
	type runEntry struct {
		runID ulid.ULID
		time  time.Time
	}

	runs := make([]runEntry, 0, len(w.store.Data))
	for runID := range w.store.Data {
		runs = append(runs, runEntry{
			runID: runID,
			time:  ulid.Time(runID.Time()),
		})
	}

	// Sort by time (oldest first)
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].time.Before(runs[j].time)
	})

	// Remove oldest runs until we're under the limit
	toRemove := len(w.store.Data) - MaxRuns
	for i := 0; i < toRemove; i++ {
		delete(w.store.Data, runs[i].runID)
	}

	if toRemove > 0 {
		w.log.Debug("cleaned up old runs", "removed", toRemove, "remaining", len(w.store.Data))
	}
}

func (w *writer) writeWorkflowEnd(
	ctx context.Context,
	item history.History,
) {
	var status enums.RunStatus
	switch item.Type {
	case enums.HistoryTypeFunctionCancelled.String():
		status = enums.RunStatusCancelled
	case enums.HistoryTypeFunctionCompleted.String():
		status = enums.RunStatusCompleted
	case enums.HistoryTypeFunctionFailed.String():
		status = enums.RunStatusFailed
	default:
		w.log.Error("unknown history type", "type", item.Type)
	}

	run := w.store.Data[item.RunID]
	run.Run.EndedAt = timePtr(time.Now())

	if item.Result != nil {
		run.Run.Output = &item.Result.Output
	}

	run.Run.Status = status
	w.store.Data[item.RunID] = run
}

func (w *writer) writeWorkflowStart(
	ctx context.Context,
	item history.History,
) {
	run := w.store.Data[item.RunID]
	run.Run.AccountID = item.AccountID
	run.Run.BatchID = item.BatchID
	run.Run.Cron = item.Cron
	run.Run.EventID = item.EventID
	run.Run.ID = item.RunID
	run.Run.OriginalRunID = item.OriginalRunID

	if item.Result != nil {
		run.Run.Output = &item.Result.Output
	}

	var status enums.RunStatus
	switch item.Type {
	case enums.HistoryTypeFunctionScheduled.String():
		status = enums.RunStatusScheduled
	case enums.HistoryTypeFunctionStarted.String():
		status = enums.RunStatusRunning
	default:
		w.log.Error("unknown history type", "type", item.Type)
	}

	run.Run.StartedAt = time.UnixMilli(int64(item.RunID.Time()))
	run.Run.Status = status
	run.Run.WorkflowID = item.FunctionID
	run.Run.WorkspaceID = item.WorkspaceID
	run.Run.WorkflowVersion = int(item.FunctionVersion)
	w.store.Data[item.RunID] = run
}

func timePtr(t time.Time) *time.Time {
	return &t
}
