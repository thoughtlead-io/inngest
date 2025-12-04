package metrics

import (
	"context"
	"encoding/json"
	"time"

	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	statev1 "github.com/inngest/inngest/pkg/execution/state"
	statev2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
)

// promLifecycle implements execution.LifecycleListener and updates Prometheus metrics.
type promLifecycle struct {
	execution.NoopLifecyceListener
	api *MetricsAPI
}

// NewPrometheusLifecycleListener returns a lifecycle listener that updates metrics.
func NewPrometheusLifecycleListener(api *MetricsAPI) execution.LifecycleListener {
	return &promLifecycle{
		api: api,
	}
}

func fnLabel(md statev2.Metadata) string {
	if slug := md.Config.FunctionSlug(); slug != "" {
		return slug
	}
	return md.ID.FunctionID.String()
}

func dateLabel(t time.Time) string {
	return t.UTC().Format("2006-01-02")
}

// func parseConcurrencyKeyLabel(label string) string {
// 	if parts := strings.SplitN(label, ":", 3); len(parts) == 3 {
// 		return parts[2]
// 	}
// 	return label
// }

// func parseFunctionConcurrency(concurrencyKeys []statev1.CustomConcurrency) (string, string) {
// 	for _, cc := range concurrencyKeys {
// 		scope, _, _, err := cc.ParseKey()
// 		if err != nil {
// 			continue
// 		}
// 		if scope != enums.ConcurrencyScopeFn {
// 			continue
// 		}
// 		return cc.UnhashedEvaluatedKeyValue, strconv.Itoa(cc.Limit)
// 	}
// 	return "", ""
// }

// ---- function run metrics ----

func (l *promLifecycle) OnFunctionScheduled(
	_ context.Context,
	md statev2.Metadata,
	_ queue.Item,
	_ []event.TrackedEvent,
) {
	fn := fnLabel(md)
	date := dateLabel(time.Now())
	l.api.fnRunScheduled.WithLabelValues(fn, date).Inc()
}

func (l *promLifecycle) OnFunctionStarted(
	_ context.Context,
	md statev2.Metadata,
	_ queue.Item,
	_ []json.RawMessage,
) {
	fn := fnLabel(md)
	date := dateLabel(time.Now())
	l.api.fnRunStarted.WithLabelValues(fn, date).Inc()
}

func (l *promLifecycle) OnFunctionFinished(
	_ context.Context,
	md statev2.Metadata,
	_ queue.Item,
	_ []json.RawMessage,
	resp statev1.DriverResponse,
) {
	fn := fnLabel(md)
	date := dateLabel(time.Now())

	status := "succeeded"
	if resp.Err != nil {
		status = "failed"
	}

	l.api.fnRunEnded.WithLabelValues(fn, date, status).Inc()
}

// Treat cancellations as a separate status.
func (l *promLifecycle) OnFunctionCancelled(
	_ context.Context,
	md statev2.Metadata,
	_ execution.CancelRequest,
	_ []json.RawMessage,
) {
	fn := fnLabel(md)
	date := dateLabel(time.Now())
	l.api.fnRunEnded.WithLabelValues(fn, date, "cancelled").Inc()
}

// ---- step metrics ----

func (l *promLifecycle) OnStepScheduled(
	_ context.Context,
	md statev2.Metadata,
	_ queue.Item,
	_ *string,
) {
	// No-op
}

func (l *promLifecycle) OnStepStarted(
	_ context.Context,
	md statev2.Metadata,
	_ queue.Item,
	_ inngest.Edge,
	_ string,
) {
	// No-op.
}

func (l *promLifecycle) OnStepFinished(
	_ context.Context,
	md statev2.Metadata,
	_ queue.Item,
	_ inngest.Edge,
	resp *statev1.DriverResponse,
	_ error,
) {
	fn := fnLabel(md)

	if resp != nil && resp.OutputSize > 0 {
		date := dateLabel(time.Now())
		l.api.stepOutputBytes.WithLabelValues(fn, date).Add(float64(resp.OutputSize))
	}
}
