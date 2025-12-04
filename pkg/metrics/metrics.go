package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

const (
	// MaxReasonableQueueDepth defines the maximum reasonable queue depth value.
	// This helps detect potential data corruption or calculation errors.
	// Set to 1 billion items, which should handle very large production systems.
	MaxReasonableQueueDepth int64 = 1_000_000_000

	// MaxSafeInt64 is the maximum safe integer value for int64 to avoid overflow
	// when converting to float64 for Prometheus metrics.
	// This is 2^53 - 1, the largest integer that can be exactly represented as float64.
	MaxSafeInt64 int64 = 1<<53 - 1 // 9,007,199,254,740,991
)

// QueueDepthValidationError represents a queue depth validation error
type QueueDepthValidationError struct {
	Value   int64
	Message string
}

func (e *QueueDepthValidationError) Error() string {
	return fmt.Sprintf("invalid queue depth %d: %s", e.Value, e.Message)
}

// ValidateQueueDepth validates a queue depth value for reasonableness and safety
func ValidateQueueDepth(depth int64) error {
	if depth < 0 {
		return &QueueDepthValidationError{
			Value:   depth,
			Message: "queue depth cannot be negative",
		}
	}

	// Check safe integer limit first (more restrictive for very large numbers)
	if depth > MaxSafeInt64 {
		return &QueueDepthValidationError{
			Value:   depth,
			Message: fmt.Sprintf("queue depth %d exceeds safe integer limit for metrics (max: %d)", depth, MaxSafeInt64),
		}
	}

	// Check reasonable limit (for operational sanity)
	if depth > MaxReasonableQueueDepth {
		return &QueueDepthValidationError{
			Value:   depth,
			Message: fmt.Sprintf("queue depth %d exceeds maximum reasonable limit of %d", depth, MaxReasonableQueueDepth),
		}
	}

	return nil
}

// SanitizeQueueDepth validates and sanitizes a queue depth value, returning a safe value
func SanitizeQueueDepth(depth int64) (int64, error) {
	if err := ValidateQueueDepth(depth); err != nil {
		// For negative values, return 0 as a safe fallback
		if depth < 0 {
			return 0, fmt.Errorf("sanitized negative queue depth to 0: %w", err)
		}
		// For values that exceed safe integer limit, clamp to max safe
		if depth > MaxSafeInt64 {
			return MaxSafeInt64, fmt.Errorf("clamped queue depth to maximum safe value: %w", err)
		}
		// For values that exceed reasonable limit but are within safe integer range,
		// clamp to reasonable limit
		if depth > MaxReasonableQueueDepth {
			return MaxReasonableQueueDepth, fmt.Errorf("clamped queue depth to maximum reasonable value: %w", err)
		}
	}
	return depth, nil
}

// QueueManager defines the interface for accessing queue metrics
type QueueManager interface {
	TotalSystemQueueDepth(ctx context.Context) (int64, error)

	ScanConcurrencyKeys(ctx context.Context, prefixPattern string) (map[string]int64, error)
}

// Opts holds the configuration options for the metrics API
type Opts struct {
	AuthMiddleware func(http.Handler) http.Handler
	QueueManager   QueueManager
	FunctionReader cqrs.FunctionReader // Add this - optional, for function name lookups
}

// MetricsAPI provides Prometheus-compatible metrics endpoints
type MetricsAPI struct {
	opts   Opts
	Router chi.Router

	queueGauge       prometheus.Gauge
	concurrencyGauge *prometheus.GaugeVec

	fnRunScheduled *prometheus.CounterVec
	fnRunStarted   *prometheus.CounterVec
	fnRunEnded     *prometheus.CounterVec

	stepOutputBytes *prometheus.CounterVec

	registry *prometheus.Registry

	functionNameCache map[uuid.UUID]string
}

// NewMetricsAPI creates a new metrics API instance with Prometheus integration
func NewMetricsAPI(opts Opts) (*MetricsAPI, error) {
	// Validate required options
	if opts.QueueManager == nil {
		return nil, fmt.Errorf("QueueManager is required")
	}

	registry := prometheus.NewRegistry()

	queueGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "inngest_queue_depth",
		Help: "Total depth of all system queues including backlog and ready state items",
	})

	concurrencyGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "inngest_concurrency_in_progress",
			Help: "Number of items currently in progress for a concurrency key",
		},
		[]string{"type", "scope", "entity", "key"},
	)

	fnRunScheduled := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "inngest_function_run_scheduled_total",
			Help: "Total number of function runs scheduled",
		},
		[]string{"fn", "date"},
	)
	fnRunStarted := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "inngest_function_run_started_total",
			Help: "Total number of function runs started",
		},
		[]string{"fn", "date"},
	)
	fnRunEnded := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "inngest_function_run_ended_total",
			Help: "Total number of function runs ended",
		},
		[]string{"fn", "date", "status"},
	)

	stepOutputBytes := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "inngest_step_output_bytes_total",
			Help: "Total bytes of step output data",
		},
		[]string{"fn", "date"},
	)

	registry.MustRegister(
		queueGauge,
		concurrencyGauge,
		fnRunScheduled,
		fnRunStarted,
		fnRunEnded,
		stepOutputBytes,
	)

	api := &MetricsAPI{
		opts:              opts,
		Router:            chi.NewRouter(),
		queueGauge:        queueGauge,
		concurrencyGauge:  concurrencyGauge,
		fnRunScheduled:    fnRunScheduled,
		fnRunStarted:      fnRunStarted,
		fnRunEnded:        fnRunEnded,
		stepOutputBytes:   stepOutputBytes,
		registry:          registry,
		functionNameCache: make(map[uuid.UUID]string),
	}

	api.setupRoutes()
	return api, nil
}

// setupRoutes configures the HTTP routes for the metrics API
func (api *MetricsAPI) setupRoutes() {
	handler := http.HandlerFunc(api.handleMetrics)

	if api.opts.AuthMiddleware != nil {
		handler = api.opts.AuthMiddleware(handler).ServeHTTP
	}

	api.Router.Get("/", handler)
}

// handleMetrics serves Prometheus-formatted metrics
func (api *MetricsAPI) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Update queue depth metric
	depth, err := api.opts.QueueManager.TotalSystemQueueDepth(r.Context())
	if err != nil {
		http.Error(w, "Failed to get queue depth", http.StatusInternalServerError)
		return
	}

	// Validate and sanitize the queue depth value
	sanitizedDepth, validationErr := SanitizeQueueDepth(depth)
	if validationErr != nil {
		// Log the validation warning but continue with sanitized value
		// In a production system, you might want to use a proper logger here
		fmt.Printf("Warning: Queue depth validation issue: %v\n", validationErr)
	}

	api.queueGauge.Set(float64(sanitizedDepth))

	api.updateConcurrencyMetrics(r.Context())

	// Gather metrics from registry
	metricFamilies, err := api.registry.Gather()
	if err != nil {
		http.Error(w, "Failed to gather metrics", http.StatusInternalServerError)
		return
	}

	// Set content type
	w.Header().Set("Content-Type", string(expfmt.FmtText))

	// Encode metrics in Prometheus text format
	encoder := expfmt.NewEncoder(w, expfmt.FmtText)
	for _, mf := range metricFamilies {
		if err := encoder.Encode(mf); err != nil {
			http.Error(w, "Failed to encode metrics", http.StatusInternalServerError)
			return
		}
	}
}

func (api *MetricsAPI) getFunctionSlug(ctx context.Context, fnID uuid.UUID) string {
	if name, ok := api.functionNameCache[fnID]; ok {
		return name
	}

	// If FunctionReader is available, look up the function name
	if api.opts.FunctionReader != nil {
		if fn, err := api.opts.FunctionReader.GetFunctionByInternalUUID(ctx, fnID); err == nil {
			// Prefer name, fallback to slug if name is empty
			name := fn.Slug
			if name == "" {
				name = fn.Name
			}
			api.functionNameCache[fnID] = name
			return name
		}
	}

	// Fallback to UUID string if lookup fails or FunctionReader not available
	return fnID.String()
}

// updateConcurrencyMetrics scans and updates all concurrency metrics
func (api *MetricsAPI) updateConcurrencyMetrics(ctx context.Context) {
	// Track function-level concurrency (prefix "p" for partition)
	fnConcurrency, err := api.opts.QueueManager.ScanConcurrencyKeys(ctx, "p")
	if err == nil {
		for key, count := range fnConcurrency {
			// Parse the key to extract function ID
			// Key format: f:uuid:hash or just uuid for function scope
			parts := strings.SplitN(key, ":", 3)
			var functionName, concurrencyKey string
			var fnID uuid.UUID

			if len(parts) >= 2 {
				// Try to parse as UUID
				if parsedID, err := uuid.Parse(parts[1]); err == nil {
					fnID = parsedID
					functionName = api.getFunctionSlug(ctx, fnID)
				} else {
					functionName = parts[1]
				}
				if len(parts) == 3 {
					concurrencyKey = parts[2] // Custom key hash
				}
			} else {
				// Try to parse the whole key as UUID
				if parsedID, err := uuid.Parse(key); err == nil {
					fnID = parsedID
					functionName = api.getFunctionSlug(ctx, fnID)
				} else {
					functionName = key
				}
			}

			api.concurrencyGauge.WithLabelValues("system", "fn", functionName, concurrencyKey).Set(float64(count))
		}
	}

	// Track account-level concurrency
	accountConcurrency, err := api.opts.QueueManager.ScanConcurrencyKeys(ctx, "account")
	if err == nil {
		for key, count := range accountConcurrency {
			// Key format: account:uuid or just uuid
			parts := strings.SplitN(key, ":", 2)
			accountID := key
			if len(parts) == 2 {
				accountID = parts[1]
			}

			api.concurrencyGauge.WithLabelValues("system", "account", accountID, "").Set(float64(count))
		}
	}

	// Track custom concurrency keys
	customConcurrency, err := api.opts.QueueManager.ScanConcurrencyKeys(ctx, "custom")
	if err == nil {
		for key, count := range customConcurrency {
			// Key format: f:uuid:hash, e:uuid:hash, or a:uuid:hash
			parts := strings.SplitN(key, ":", 3)
			if len(parts) >= 3 {
				scopePrefix := parts[0]
				entityID := parts[1]
				concurrencyKey := parts[2]

				// Map prefix to scope
				var scope string
				switch scopePrefix {
				case "f":
					scope = "fn"
					// For function scope, try to get function name
					if fnID, err := uuid.Parse(entityID); err == nil {
						entityID = api.getFunctionSlug(ctx, fnID)
					}
				case "e":
					scope = "env"
				case "a":
					scope = "account"
				default:
					scope = "unknown"
				}

				api.concurrencyGauge.WithLabelValues("custom", scope, entityID, concurrencyKey).Set(float64(count))
			}
		}
	}
}
