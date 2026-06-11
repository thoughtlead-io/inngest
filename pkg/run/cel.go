package run

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	sq "github.com/doug-martin/goqu/v9"
	"github.com/google/cel-go/common/operators"
	"github.com/inngest/expr"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/expressions"
	"golang.org/x/sync/errgroup"
)

var (
	eventRegex  = regexp.MustCompile(`^event\..+`)
	outputRegex = regexp.MustCompile(`^output`)

	exprErrorRegex = regexp.MustCompile(`^ERROR: <input>:\d+:\d+:\s+`)
)

type ExprHandlerOpt func(ctx context.Context, h *ExpressionHandler) error
type ExprSQLConverter func(ctx context.Context, n *expr.Node) ([]sq.Expression, error)

// normalizeCELExpression attempts to make CEL parsing more forgiving for users
// who type SQL-like equality (`=`) instead of CEL equality (`==`).
//
// This only rewrites single '=' tokens which are not part of '==', '!=', '<=', '>='
// and only when they appear outside of quoted string literals.
func normalizeCELExpression(s string) string {
	// Fast path.
	if !strings.Contains(s, "=") {
		return s
	}

	prevNonSpace := func(str string, i int) byte {
		for i >= 0 {
			switch str[i] {
			case ' ', '\t', '\n', '\r':
				i--
				continue
			default:
				return str[i]
			}
		}
		return 0
	}
	nextNonSpace := func(str string, i int) byte {
		for i < len(str) {
			switch str[i] {
			case ' ', '\t', '\n', '\r':
				i++
				continue
			default:
				return str[i]
			}
		}
		return 0
	}

	out := make([]byte, 0, len(s)+4)
	inSingle := false
	inDouble := false
	escaped := false

	for i := 0; i < len(s); i++ {
		c := s[i]

		if escaped {
			out = append(out, c)
			escaped = false
			continue
		}

		// Track escapes to avoid toggling quote state within escaped strings.
		if c == '\\' {
			out = append(out, c)
			escaped = true
			continue
		}

		// If we're inside a string literal, just copy until we exit.
		if inSingle {
			out = append(out, c)
			if c == '\'' {
				inSingle = false
			}
			continue
		}
		if inDouble {
			out = append(out, c)
			if c == '"' {
				inDouble = false
			}
			continue
		}

		switch c {
		case '\'':
			inSingle = true
			out = append(out, c)
		case '"':
			inDouble = true
			out = append(out, c)
		case '=':
			p := prevNonSpace(s, i-1)
			n := nextNonSpace(s, i+1)
			// Rewrite `a = b` into `a == b`.
			// Do not touch operators: ==, !=, <=, >=
			if p != '<' && p != '>' && p != '!' && p != '=' && n != '=' {
				out = append(out, '=', '=')
			} else {
				out = append(out, c)
			}
		default:
			out = append(out, c)
		}
	}

	return string(out)
}

func WithExpressionHandlerExpressions(cel []string) ExprHandlerOpt {
	return func(ctx context.Context, h *ExpressionHandler) error {
		if len(cel) == 0 {
			return nil
		}

		return h.add(ctx, cel)
	}
}

func WithExpressionHandlerBlob(exp string, delimiter string) ExprHandlerOpt {
	if delimiter == "" {
		delimiter = "\n"
	}
	cel := strings.Split(exp, delimiter)

	return func(ctx context.Context, h *ExpressionHandler) error {
		if exp == "" || len(cel) == 0 {
			return nil
		}

		return h.add(ctx, cel)
	}
}

func WithExpressionSQLConverter(c ExprSQLConverter) ExprHandlerOpt {
	return func(ctx context.Context, h *ExpressionHandler) error {
		h.SQLConverter = c
		return nil
	}
}

type ExpressionHandler struct {
	EventExprList  []string
	OutputExprList []string
	SQLConverter   ExprSQLConverter
}

func NewExpressionHandler(ctx context.Context, opts ...ExprHandlerOpt) (*ExpressionHandler, error) {
	h := &ExpressionHandler{
		EventExprList:  []string{},
		OutputExprList: []string{},
		SQLConverter:   SQLiteConverter,
	}

	for _, apply := range opts {
		if err := apply(ctx, h); err != nil {
			return nil, err
		}
	}

	return h, nil
}

// add adds the list of CEL strings passed in and store them in the list of expressions.
//
// currently not expecting to use nesting within a string, but if needed, this function
// should change to use recursion for importing the list of expressions instead.
func (h *ExpressionHandler) add(ctx context.Context, cel []string) error {
	parser := expressions.ParserSingleton()

	evtExprs := map[string]bool{}
	outputExprs := map[string]bool{}

	for _, e := range cel {
		original := e
		e = normalizeCELExpression(e)

		// empty string, skip
		if len(e) == 0 {
			continue
		}

		// parse and validate
		tree, err := parser.Parse(ctx, expr.StringExpression(e))
		if err != nil {
			// reformat the error message to be more comprehensive when propagated back to the user
			errs := strings.Split(err.Error(), "\n")
			if len(errs) == 1 {
				return err
			}

			// Only take the first one, the rest is not needed.
			// then remove the prefix `ERROR : <input>:1:\d:` and use the rest of the error body
			msg := exprErrorRegex.ReplaceAllString(errs[0], "")
			// Show the original expression to the user, even if we normalized it for parsing.
			return fmt.Errorf("%s\n | %s", msg, original)
		}
		if tree.HasMacros {
			return fmt.Errorf("macros are currently not supported")
		}
		// NOTE: if there are no predicates or AND or OR
		// it means an invalid syntax was used and it couldn't parse anything
		if !tree.Root.HasPredicate() && tree.Root.Ands == nil && tree.Root.Ors == nil {
			return fmt.Errorf("invalid syntax detected")
		}

		// Use the normalized expression for storage and future parsing.
		h.addToExprList(ctx, []*expr.Node{&tree.Root}, e, evtExprs, outputExprs)
	}

	for evt := range evtExprs {
		h.EventExprList = append(h.EventExprList, evt)
	}

	for output := range outputExprs {
		h.OutputExprList = append(h.OutputExprList, output)
	}

	return nil
}

func (h *ExpressionHandler) addToExprList(
	ctx context.Context,
	nodes []*expr.Node,
	cel string,
	evtDedup map[string]bool,
	outputDedup map[string]bool,
) {
	for _, n := range nodes {
		if n.HasPredicate() {
			switch {
			case eventRegex.MatchString(n.Predicate.Ident):
				if _, ok := evtDedup[cel]; !ok {
					evtDedup[cel] = true
				}
			case outputRegex.MatchString(n.Predicate.Ident):
				if _, ok := outputDedup[cel]; !ok {
					outputDedup[cel] = true
				}
			}
		}

		if n.Ands != nil {
			h.addToExprList(ctx, n.Ands, cel, evtDedup, outputDedup)
		}
		if n.Ors != nil {
			h.addToExprList(ctx, n.Ors, cel, evtDedup, outputDedup)
		}
	}
}

func (h *ExpressionHandler) HasFilters() bool {
	return h.HasEventFilters() || h.HasOutputFilters()
}

func (h *ExpressionHandler) HasEventFilters() bool {
	return len(h.EventExprList) > 0
}

func (h *ExpressionHandler) HasOutputFilters() bool {
	return len(h.OutputExprList) > 0
}

func (h *ExpressionHandler) ToSQLFilters(ctx context.Context) ([]sq.Expression, error) {
	filters := []sq.Expression{}
	parser := expressions.ParserSingleton()

	// used to dedup in case there's an expression that is included in both list
	dedup := map[string]bool{}
	exprs := []string{}
	for _, e := range h.EventExprList {
		if _, ok := dedup[e]; !ok {
			dedup[e] = true
			exprs = append(exprs, e)
		}
	}
	for _, e := range h.OutputExprList {
		if _, ok := dedup[e]; !ok {
			dedup[e] = true
			exprs = append(exprs, e)
		}
	}

	for _, exp := range exprs {
		tree, err := parser.Parse(ctx, expr.StringExpression(exp))
		if err != nil {
			return nil, fmt.Errorf("error evaluating event expression '%s': %w", exp, err)
		}

		expFilter, err := h.toSQLFilters(ctx, []*expr.Node{&tree.Root})
		if err != nil {
			return nil, err
		}
		filters = append(filters, expFilter...)
	}

	return filters, nil
}

func (h *ExpressionHandler) MatchEventExpressions(ctx context.Context, evt event.Event) (bool, error) {
	if !h.HasEventFilters() {
		return false, nil
	}

	eg, ctx := errgroup.WithContext(ctx)
	res := make([]bool, len(h.EventExprList))

	// Build CEL "event" object, but remap event.data to payload.event.data when present
	data := evt.Map()
	if payloadEvent, ok := evt.Data["event"].(map[string]any); ok {
		if inner, ok := payloadEvent["data"].(map[string]any); ok {
			data["data"] = inner
		}
	}
	// Optional fallback: if "event" isn't present but "events":[{data:...}] is
	if _, hasEvent := evt.Data["event"]; !hasEvent {
		if payloadEvents, ok := evt.Data["events"].([]any); ok && len(payloadEvents) > 0 {
			if first, ok := payloadEvents[0].(map[string]any); ok {
				if inner, ok := first["data"].(map[string]any); ok {
					data["data"] = inner
				}
			}
		}
	}

	for i, e := range h.EventExprList {
		idx := i
		exp := e

		eg.Go(func() error {
			eval, err := expressions.NewBooleanEvaluator(ctx, exp)
			if err != nil {
				return fmt.Errorf("error initializing expression evaluator for event: %w", err)
			}

			ok, err := eval.Evaluate(ctx, expressions.NewData(map[string]any{"event": data}))
			if err != nil {
				res[idx] = false
				return nil
			}

			res[idx] = ok
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return false, err
	}
	return allMatches(res), nil
}

func (h *ExpressionHandler) MatchOutputExpressions(ctx context.Context, output []byte) (bool, error) {
	if !h.HasOutputFilters() {
		return false, nil
	}
	// no output to match against, don't waste effort
	if string(output) == "" {
		return false, nil
	}

	eg, ctx := errgroup.WithContext(ctx)
	res := make([]bool, len(h.OutputExprList))

	var result any
	if err := json.Unmarshal(output, &result); err != nil {
		return false, fmt.Errorf("error deserializing output: %w", err)
	}

	var data map[string]any
	switch v := result.(type) {
	case map[string]any:
		data = map[string]any{"output": v}
	case []any:
		data = map[string]any{"output": v}
	case int64, float64, bool, string:
		data = map[string]any{"output": v}
	}

	for i, e := range h.OutputExprList {
		idx := i
		exp := e

		eg.Go(func() error {
			eval, err := expressions.NewBooleanEvaluator(ctx, exp)
			if err != nil {
				return fmt.Errorf("error initializing expression evaluator for output: %w", err)
			}

			ok, err := eval.Evaluate(ctx, expressions.NewData(data))
			if err != nil {
				// if there's an error, it likely means the data being matched is not of the same structure
				// map[string]any vs int64
				res[idx] = false
				return nil
			}

			res[idx] = ok
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return false, err
	}

	return allMatches(res), nil
}

func allMatches(res []bool) bool {
	for _, v := range res {
		if !v {
			return false
		}
	}
	return true
}

// toSQLEventFilter parses the passed in nodes and converts them into SQL filter expressions
func (h *ExpressionHandler) toSQLFilters(ctx context.Context, nodes []*expr.Node) ([]sq.Expression, error) {
	filters := []sq.Expression{}

	for _, n := range nodes {
		res, err := h.SQLConverter(ctx, n)
		if err != nil {
			return nil, err
		}
		filters = append(filters, res...)

		// check for further nesting
		if n.Ands != nil {
			nested, err := h.toSQLFilters(ctx, n.Ands)
			if err != nil {
				return nil, err
			}

			switch len(nested) {
			case 0: // no op
			case 1:
				filters = append(filters, nested[0])
			default:
				filters = append(filters, sq.And(nested...))
			}
		}

		if n.Ors != nil {
			nested, err := h.toSQLFilters(ctx, n.Ors)
			if err != nil {
				return nil, err
			}

			switch len(nested) {
			case 0: // no op
			case 1:
				filters = append(filters, nested[0])
			default:
				filters = append(filters, sq.Or(nested...))
			}
		}
	}

	return filters, nil
}

// create filters for database queries in sqlite
// - ULID
// - event id (idempotency key)
// - event name
// - version
// - timestamp
//
// This only applies to events
func SQLiteConverter(ctx context.Context, n *expr.Node) ([]sq.Expression, error) {
	filters := []sq.Expression{}
	if n.HasPredicate() {
		ident := n.Predicate.Ident
		literal := n.Predicate.Literal

		if strings.HasPrefix(ident, "event.data.") {
			// Generic event.data.<field>[.<nested>...] support
			fieldPath := strings.TrimPrefix(ident, "event.data.")
			pathParts := strings.Split(fieldPath, ".")

			// normalize duplicate event.data prefix
			if len(pathParts) >= 2 && pathParts[0] == "event" && pathParts[1] == "data" {
				pathParts = pathParts[2:]
			}

			// events.event_data commonly stores the full event payload (eg `{id,name,data,ts}`),
			// meaning the user-facing "event.data.*" maps to `event_data.data.*`.
			//
			// Some internal events wrap the original payload under `event.data`, and some store
			// the original event under `events[0].data`. Historically some deployments also
			// stored the data object directly at the root of event_data.
			//
			// To support all shapes, query via COALESCE(data, direct, wrapped, events[0].data).
			dataTextPath := fmt.Sprintf("{%s}", strings.Join(append([]string{"data"}, pathParts...), ","))
			directTextPath := fmt.Sprintf("{%s}", strings.Join(pathParts, ","))
			wrappedTextPath := fmt.Sprintf("{%s}", strings.Join(append([]string{"event", "data"}, pathParts...), ","))
			events0TextPath := fmt.Sprintf("{%s}", strings.Join(append([]string{"events", "0", "data"}, pathParts...), ","))

			val := fmt.Sprint(literal) // compare as text

			switch n.Predicate.Operator {
			case operators.Equals:
				filters = append(filters, sq.L(
					fmt.Sprintf(
						"COALESCE(event_data #>> '%s', event_data #>> '%s', event_data #>> '%s', event_data #>> '%s') = ?",
						dataTextPath,
						directTextPath,
						wrappedTextPath,
						events0TextPath,
					),
					val,
				))
			case operators.NotEquals:
				filters = append(filters, sq.L(
					fmt.Sprintf(
						"COALESCE(event_data #>> '%s', event_data #>> '%s', event_data #>> '%s', event_data #>> '%s') != ?",
						dataTextPath,
						directTextPath,
						wrappedTextPath,
						events0TextPath,
					),
					val,
				))
			default:
				return nil, fmt.Errorf("unsupported operator %s for %s", n.Predicate.Operator, ident)
			}

			return filters, nil
		}
		switch ident {
		case "event.id":
			id, ok := literal.(string)
			if !ok {
				return nil, fmt.Errorf("expects 'event.id' to be a string: %v", literal)
			}
			switch n.Predicate.Operator {
			case operators.Equals:
				filters = append(filters, sq.C("event_id").Eq(id))
			case operators.NotEquals:
				filters = append(filters, sq.C("event_id").Neq(id))
			}
		case "event.name":
			name, ok := literal.(string)
			if !ok {
				return nil, fmt.Errorf("expects 'event.name' to be a string: %v", literal)
			}
			switch n.Predicate.Operator {
			case operators.Equals:
				filters = append(filters, sq.C("event_name").Eq(name))
			case operators.NotEquals:
				filters = append(filters, sq.C("event_name").Neq(name))
			}
		case "event.ts":
			ts, ok := literal.(int64)
			if !ok {
				return nil, fmt.Errorf("expects 'event.ts' to be an integer: %v", literal)
			}
			var f sq.Expression
			field := "event_ts"

			switch n.Predicate.Operator {
			case operators.Greater:
				f = sq.C(field).Gt(ts)
			case operators.GreaterEquals:
				f = sq.C(field).Gte(ts)
			case operators.Equals:
				f = sq.C(field).Eq(ts)
			case operators.Less:
				f = sq.C(field).Lt(ts)
			case operators.LessEquals:
				f = sq.C(field).Lte(ts)
			case operators.NotEquals:
				f = sq.C(field).Neq(ts)
			}
			if f != nil {
				filters = append(filters, f)
			}
		case "event.v":
			v, ok := literal.(string)
			if !ok {
				return nil, fmt.Errorf("expects 'event.v' to be a string: %v", literal)
			}
			switch n.Predicate.Operator {
			case operators.Equals:
				filters = append(filters, sq.C("event_v").Eq(v))
			case operators.NotEquals:
				filters = append(filters, sq.C("event_v").Neq(v))
			}
		}
	}

	return filters, nil
}
