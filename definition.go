package jobs

import (
	"errors"
	"fmt"
	"reflect"
	"time"
)

const (
	defaultQueue       = "default"
	defaultPriority    = 100
	defaultMaxAttempts = 5
	defaultTimeout     = 5 * time.Minute
)

// Handler is the typed worker function registered for a definition.
type Handler[In any, Out any] func(Ctx, In) (Out, error)

// Definition is the stable typed job symbol applications keep in package scope.
type Definition[In any, Out any] struct {
	name        string
	queue       string
	maxAttempts int
	timeout     time.Duration
	backoff     BackoffStrategy
	retention   time.Duration
	priority    int
}

// New creates a typed durable job definition.
func New[In any, Out any](name string, opts ...DefinitionOption) *Definition[In, Out] {
	def := &Definition[In, Out]{
		name:        normalizeName(name),
		queue:       defaultQueue,
		maxAttempts: defaultMaxAttempts,
		timeout:     defaultTimeout,
		backoff:     ExponentialJitter(10*time.Second, 30*time.Minute),
		priority:    defaultPriority,
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyDefinition(def)
		}
	}
	return def
}

// Name returns the stable durable job name.
func (d *Definition[In, Out]) Name() string {
	if d == nil {
		return ""
	}
	return d.name
}

// Queue returns the configured queue name.
func (d *Definition[In, Out]) Queue() string {
	if d == nil {
		return ""
	}
	return d.queue
}

// MaxAttempts returns the configured max attempt count.
func (d *Definition[In, Out]) MaxAttempts() int {
	if d == nil {
		return 0
	}
	return d.maxAttempts
}

// Timeout returns the per-attempt timeout.
func (d *Definition[In, Out]) Timeout() time.Duration {
	if d == nil {
		return 0
	}
	return d.timeout
}

// Backoff returns the backoff strategy.
func (d *Definition[In, Out]) Backoff() BackoffStrategy {
	if d == nil {
		return nil
	}
	return d.backoff
}

// Retention returns the explicit terminal retention override.
func (d *Definition[In, Out]) Retention() time.Duration {
	if d == nil {
		return 0
	}
	return d.retention
}

// Priority returns the default enqueue priority for the definition.
func (d *Definition[In, Out]) Priority() int {
	if d == nil {
		return 0
	}
	return d.priority
}

func (d *Definition[In, Out]) base() baseDefinition {
	return baseDefinition{
		name:        d.name,
		queue:       d.queue,
		maxAttempts: d.maxAttempts,
		timeout:     d.timeout,
		backoff:     d.backoff,
		retention:   d.retention,
		priority:    d.priority,
	}
}

func (d *Definition[In, Out]) setBase(base baseDefinition) {
	d.name = base.name
	d.queue = base.queue
	d.maxAttempts = base.maxAttempts
	d.timeout = base.timeout
	d.backoff = base.backoff
	d.retention = base.retention
	d.priority = base.priority
}

func (d *Definition[In, Out]) inputType() reflect.Type {
	var in In
	return reflect.TypeOf(in)
}

func (d *Definition[In, Out]) outputType() reflect.Type {
	var out Out
	return reflect.TypeOf(out)
}

func (d *Definition[In, Out]) encodeInputAny(codec Codec, value any) ([]byte, error) {
	in, ok := value.(In)
	if !ok {
		return nil, fmt.Errorf("jobs: enqueue input type mismatch for %s: got %T", d.Name(), value)
	}
	return codec.Marshal(in)
}

func (d *Definition[In, Out]) decodeInputAny(codec Codec, payload []byte) (any, error) {
	var in In
	if err := codec.Unmarshal(payload, &in); err != nil {
		return nil, err
	}
	return in, nil
}

func (d *Definition[In, Out]) encodeOutputAny(codec Codec, value any) ([]byte, error) {
	out, ok := value.(Out)
	if !ok {
		return nil, fmt.Errorf("jobs: output type mismatch for %s: got %T", d.Name(), value)
	}
	return codec.Marshal(out)
}

func (d *Definition[In, Out]) decodeOutputAny(codec Codec, payload []byte) (any, error) {
	var out Out
	if len(payload) == 0 {
		return out, nil
	}
	if err := codec.Unmarshal(payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (d *Definition[In, Out]) typedSnapshot(codec Codec, raw *RawSnapshot) (any, error) {
	snapshot := &Snapshot[Out]{
		Ref:               raw.Ref,
		Queue:             raw.Queue,
		Status:            raw.Status,
		TenantID:          raw.TenantID,
		Actor:             raw.Actor,
		Attempts:          raw.Attempts,
		MaxAttempts:       raw.MaxAttempts,
		RunAt:             raw.RunAt,
		CreatedAt:         raw.CreatedAt,
		FirstStartedAt:    raw.FirstStartedAt,
		LastStartedAt:     raw.LastStartedAt,
		FinishedAt:        raw.FinishedAt,
		CancelRequestedAt: raw.CancelRequestedAt,
		Progress:          cloneProgress(raw.Progress),
		SafeError:         raw.SafeError,
		ErrorCode:         raw.ErrorCode,
		ParentJobID:       raw.ParentJobID,
		RetriedFromJobID:  raw.RetriedFromJobID,
	}
	if len(raw.Output) > 0 {
		var out Out
		if err := codec.Unmarshal(raw.Output, &out); err != nil {
			return nil, err
		}
		snapshot.Output = &out
	}
	return snapshot, nil
}

func (d *Definition[In, Out]) contract() definitionContract {
	return d
}

type baseDefinition struct {
	name        string
	queue       string
	maxAttempts int
	timeout     time.Duration
	backoff     BackoffStrategy
	retention   time.Duration
	priority    int
}

func (d baseDefinition) validate() error {
	if normalizeName(d.name) == "" {
		return fmtDefinitionError("name must not be empty")
	}
	if normalizeQueue(d.queue) == "" {
		return fmtDefinitionError("queue must not be empty")
	}
	if d.maxAttempts < 1 {
		return fmtDefinitionError("max attempts must be at least 1")
	}
	if d.timeout <= 0 {
		return fmtDefinitionError("timeout must be positive")
	}
	if d.backoff == nil {
		return fmtDefinitionError("backoff must not be nil")
	}
	return nil
}

func fmtDefinitionError(msg string) error {
	return errors.Join(ErrInvalidDefinition, errorString(msg))
}

type errorString string

func (e errorString) Error() string { return string(e) }

type definitionContract interface {
	base() baseDefinition
	setBase(baseDefinition)
	inputType() reflect.Type
	outputType() reflect.Type
	encodeInputAny(Codec, any) ([]byte, error)
	decodeInputAny(Codec, []byte) (any, error)
	encodeOutputAny(Codec, any) ([]byte, error)
	decodeOutputAny(Codec, []byte) (any, error)
	typedSnapshot(Codec, *RawSnapshot) (any, error)
}

func definitionFromAny(def any) (definitionContract, error) {
	contract, ok := def.(definitionContract)
	if !ok {
		return nil, fmt.Errorf("%w: unsupported definition type %T", ErrInvalidDefinition, def)
	}
	if err := contract.base().validate(); err != nil {
		return nil, err
	}
	return contract, nil
}
