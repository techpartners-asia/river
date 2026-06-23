// Package waiteval provides a pure, side-effect-free CEL evaluation engine for
// workflow wait expressions. It has no database access, no time.Now() calls,
// and no dependency on the parent riverworkflow package (avoiding import cycles).
package waiteval

import (
	"fmt"
	"time"

	"github.com/google/cel-go/cel"
)

// TermData is the engine's decoupled view of a wait term. Task 3 maps from
// riverworkflow.WaitTermSpec to this type.
type TermData struct {
	Name     string
	Kind     string // "signal", "timer", "generic"
	Key      string // signal key (signal terms)
	CELExpr  string // sub-expression (signal/generic terms)
	HasTimer bool   // true for timer terms
}

// DepView is the engine's view of a dependency task's result.
type DepView struct {
	Output any
	State  string
}

// SignalView is the engine's full view of a received signal. It carries the
// payload alongside metadata supplied by the scheduler.
//
// created_at is exposed to CEL as a google.protobuf.Timestamp (cel.TimestampType)
// by passing time.Time directly; cel-go's default type adapter maps time.Time to
// that type automatically.
type SignalView struct {
	Payload   any
	Attempt   int
	CreatedAt time.Time
	ID        int64
	Source    string
}

// Inputs holds the runtime state that the engine evaluates against.
// The scheduler computes timer fire state before calling Evaluate; this engine
// reads results, it does not compute them.
type Inputs struct {
	Timers   map[string]bool       // keyed by term name
	Signals  map[string]SignalView // keyed by signal key
	Deps     map[string]DepView
	Workflow map[string]any
}

// compiledTerm holds a compiled sub-program for signal/generic terms.
type compiledTerm struct {
	data    TermData
	subProg cel.Program // nil for timer terms
}

// Program holds all compiled CEL programs for a WaitSpec.
type Program struct {
	terms   []compiledTerm
	topProg cel.Program // compiled top-level expr
}

// buildBaseEnv constructs the shared CEL environment with the four scope maps.
func buildBaseEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable("signals", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("timers", cel.MapType(cel.StringType, cel.BoolType)),
		cel.Variable("deps", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("workflow", cel.MapType(cel.StringType, cel.DynType)),
	)
}

// buildSignalEnv constructs the CEL environment for signal sub-expressions.
// Variables available to signal CEL expressions:
//   - payload    (dyn)       — the signal's raw payload value
//   - attempt    (int)       — delivery attempt number
//   - created_at (timestamp) — when the signal was created; exposed as
//     google.protobuf.Timestamp via cel-go's time.Time adapter
//   - id         (int)       — signal row ID
//   - source     (string)    — originating source label
func buildSignalEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable("payload", cel.DynType),
		cel.Variable("attempt", cel.IntType),
		cel.Variable("created_at", cel.TimestampType),
		cel.Variable("id", cel.IntType),
		cel.Variable("source", cel.StringType),
	)
}

// compileExpr compiles a CEL expression in the given environment and returns a
// ready-to-evaluate Program or an error.
func compileExpr(env *cel.Env, expr string) (cel.Program, error) {
	ast, iss := env.Compile(expr)
	if iss != nil && iss.Err() != nil {
		return nil, iss.Err()
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("waiteval: program construction: %w", err)
	}
	return prg, nil
}

// evalBool evaluates a compiled program with the given activation and returns
// the boolean result.
//
// CONTRACT: if prg.Eval returns a runtime error (e.g. "no such key" when a dep
// or signal is not yet in the inputs map, or field-access on a scalar payload),
// evalBool returns (false, nil). This is the "not yet satisfied" contract:
// compile-time type errors are caught earlier by Compile/WaitSpec.Validate, so
// a runtime error at scheduler time means the inputs are not ready yet, which
// maps to false and will be re-evaluated on the next scheduler tick.
//
// Truly internal non-recoverable failures (the evaluated expression does not
// return a bool value at all) are still returned as errors.
func evalBool(prg cel.Program, activation map[string]any) (bool, error) {
	out, _, err := prg.Eval(activation)
	if err != nil {
		// Runtime eval errors mean inputs are not ready yet — treat as false.
		return false, nil
	}
	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("waiteval: expression did not return bool, got %T", out.Value())
	}
	return b, nil
}

// Compile validates and compiles each term's sub-expression and the top-level
// boolean expr. Returns a *Program ready for repeated evaluation, or an error
// on any CEL syntax or type error.
func Compile(terms []TermData, expr string) (*Program, error) {
	baseEnv, err := buildBaseEnv()
	if err != nil {
		return nil, fmt.Errorf("waiteval: build base env: %w", err)
	}

	sigEnv, err := buildSignalEnv()
	if err != nil {
		return nil, fmt.Errorf("waiteval: build signal env: %w", err)
	}

	compiled := make([]compiledTerm, 0, len(terms))

	for _, td := range terms {
		ct := compiledTerm{data: td}

		switch td.Kind {
		case "timer":
			// Timer values are taken from Inputs.Timers[name] directly; no sub-program.

		case "signal":
			if td.CELExpr != "" {
				prg, err := compileExpr(sigEnv, td.CELExpr)
				if err != nil {
					return nil, fmt.Errorf("waiteval: compile signal term %q: %w", td.Name, err)
				}
				ct.subProg = prg
			}

		case "generic":
			if td.CELExpr != "" {
				prg, err := compileExpr(baseEnv, td.CELExpr)
				if err != nil {
					return nil, fmt.Errorf("waiteval: compile generic term %q: %w", td.Name, err)
				}
				ct.subProg = prg
			}

		default:
			return nil, fmt.Errorf("waiteval: unknown term kind %q for term %q", td.Kind, td.Name)
		}

		compiled = append(compiled, ct)
	}

	// Build the top-level env: declares each term name as a bool var, plus the
	// four scope maps so expressions can reference them directly.
	topOpts := []cel.EnvOption{
		cel.Variable("signals", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("timers", cel.MapType(cel.StringType, cel.BoolType)),
		cel.Variable("deps", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("workflow", cel.MapType(cel.StringType, cel.DynType)),
	}
	for _, td := range terms {
		topOpts = append(topOpts, cel.Variable(td.Name, cel.BoolType))
	}

	topEnv, err := cel.NewEnv(topOpts...)
	if err != nil {
		return nil, fmt.Errorf("waiteval: build top-level env: %w", err)
	}

	topProg, err := compileExpr(topEnv, expr)
	if err != nil {
		return nil, fmt.Errorf("waiteval: compile top-level expr: %w", err)
	}

	return &Program{
		terms:   compiled,
		topProg: topProg,
	}, nil
}

// Evaluate evaluates all terms against in and then evaluates the top-level
// expression with the term results. It is pure: no IO, no time.Now().
func (p *Program) Evaluate(in Inputs) (bool, error) {
	// Default nil maps to empty to avoid activation key-missing errors.
	sigViews := in.Signals
	if sigViews == nil {
		sigViews = map[string]SignalView{}
	}
	timers := in.Timers
	if timers == nil {
		timers = map[string]bool{}
	}
	workflow := in.Workflow
	if workflow == nil {
		workflow = map[string]any{}
	}

	// Build a payload-only signals map for use in the base/top-level CEL
	// activation where expressions access signals["key"].field.  The value is
	// the payload only (identical to CP2's map[string]any semantics) so that
	// existing generic terms like signals["k"].ok continue to work correctly.
	signalsPayload := make(map[string]any, len(sigViews))
	for k, sv := range sigViews {
		signalsPayload[k] = sv.Payload
	}

	// Convert Deps to map[string]any with lowercase field names for CEL access.
	deps := make(map[string]any, len(in.Deps))
	for k, dv := range in.Deps {
		deps[k] = map[string]any{
			"output": dv.Output,
			"state":  dv.State,
		}
	}

	// Build base activation for generic terms.
	baseActivation := map[string]any{
		"signals":  signalsPayload,
		"timers":   timers,
		"deps":     deps,
		"workflow": workflow,
	}

	// Evaluate each term.
	termValues := make(map[string]bool, len(p.terms))
	for _, ct := range p.terms {
		var val bool

		switch ct.data.Kind {
		case "timer":
			// Value comes directly from the pre-computed timer state.
			val = timers[ct.data.Name]

		case "signal":
			// Absence of the signal key means the signal has not yet been received.
			sv, present := sigViews[ct.data.Key]
			if !present {
				val = false
			} else if ct.subProg == nil {
				// A signal term with an empty CELExpr gates on signal presence alone,
				// independent of the payload.
				val = true
			} else {
				// Bind full signal metadata into the sub-environment.
				// created_at is passed as time.Time; cel-go's default adapter maps
				// it to google.protobuf.Timestamp (cel.TimestampType).
				sigActivation := map[string]any{
					"payload":    sv.Payload,
					"attempt":    int64(sv.Attempt),
					"created_at": sv.CreatedAt,
					"id":         sv.ID,
					"source":     sv.Source,
				}
				result, err := evalBool(ct.subProg, sigActivation)
				if err != nil {
					return false, fmt.Errorf("waiteval: signal term %q: %w", ct.data.Name, err)
				}
				val = result
			}

		case "generic":
			if ct.subProg == nil {
				val = false
			} else {
				result, err := evalBool(ct.subProg, baseActivation)
				if err != nil {
					return false, fmt.Errorf("waiteval: generic term %q: %w", ct.data.Name, err)
				}
				val = result
			}
		}

		termValues[ct.data.Name] = val
	}

	// Build top-level activation: term name booleans + scope maps.
	topActivation := map[string]any{
		"signals":  signalsPayload,
		"timers":   timers,
		"deps":     deps,
		"workflow": workflow,
	}
	for name, val := range termValues {
		topActivation[name] = val
	}

	return evalBool(p.topProg, topActivation)
}
