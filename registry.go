package jobs

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
)

// Registry owns startup-time handler and schedule registration.
type Registry struct {
	mu        sync.RWMutex
	handlers  map[string]registeredHandler
	schedules map[string]registeredSchedule
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers:  make(map[string]registeredHandler),
		schedules: make(map[string]registeredSchedule),
	}
}

var (
	errorType = reflect.TypeOf((*error)(nil)).Elem()
	ctxType   = reflect.TypeOf((*Ctx)(nil)).Elem()
)

// Handle registers a handler for a definition.
func (r *Registry) Handle(def any, fn any) error {
	if r == nil {
		return errors.New("jobs: registry is nil")
	}
	contract, err := definitionFromAny(def)
	if err != nil {
		return err
	}
	if fn == nil {
		return errors.Join(ErrInvalidDefinition, errorString("handler must not be nil"))
	}
	fv := reflect.ValueOf(fn)
	ft := fv.Type()
	if ft.Kind() != reflect.Func || ft.NumIn() != 2 || ft.NumOut() != 2 {
		return fmt.Errorf("%w: handler for %s must be func(jobs.Ctx, In) (Out, error)", ErrInvalidDefinition, contract.base().name)
	}
	if !ft.In(0).Implements(ctxType) {
		return fmt.Errorf("%w: first handler arg for %s must implement jobs.Ctx", ErrInvalidDefinition, contract.base().name)
	}
	if ft.In(1) != contract.inputType() {
		return fmt.Errorf("%w: handler input for %s must be %s, got %s", ErrInvalidDefinition, contract.base().name, contract.inputType(), ft.In(1))
	}
	if ft.Out(0) != contract.outputType() {
		return fmt.Errorf("%w: handler output for %s must be %s, got %s", ErrInvalidDefinition, contract.base().name, contract.outputType(), ft.Out(0))
	}
	if !ft.Out(1).Implements(errorType) {
		return fmt.Errorf("%w: second handler return for %s must be error", ErrInvalidDefinition, contract.base().name)
	}
	base := contract.base()
	entry := registeredHandlerEntry{
		definition: contract,
		runFn: func(ctx Ctx, codec Codec, payload []byte) ([]byte, error) {
			in, err := contract.decodeInputAny(codec, payload)
			if err != nil {
				return nil, err
			}
			results := fv.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(in)})
			if errValue := results[1].Interface(); errValue != nil {
				return nil, errValue.(error)
			}
			return contract.encodeOutputAny(codec, results[0].Interface())
		},
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[base.name]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateHandler, base.name)
	}
	r.handlers[base.name] = entry
	return nil
}

// MustHandle registers a handler and panics on error.
func (r *Registry) MustHandle(def any, fn any) {
	if err := r.Handle(def, fn); err != nil {
		panic(err)
	}
}

func (r *Registry) resolve(name string) (registeredHandler, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[name]
	return h, ok
}

func (r *Registry) names() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

type registeredHandler interface {
	name() string
	baseDefinition() baseDefinition
	encodeInput(Codec, any) ([]byte, error)
	run(Ctx, Codec, []byte) ([]byte, error)
	typedSnapshot(Codec, *RawSnapshot) (any, error)
}

type registeredHandlerEntry struct {
	definition definitionContract
	runFn      func(Ctx, Codec, []byte) ([]byte, error)
}

func (h registeredHandlerEntry) name() string {
	return h.definition.base().name
}

func (h registeredHandlerEntry) baseDefinition() baseDefinition {
	return h.definition.base()
}

func (h registeredHandlerEntry) encodeInput(codec Codec, value any) ([]byte, error) {
	return h.definition.encodeInputAny(codec, value)
}

func (h registeredHandlerEntry) run(ctx Ctx, codec Codec, payload []byte) ([]byte, error) {
	return h.runFn(ctx, codec, payload)
}

func (h registeredHandlerEntry) typedSnapshot(codec Codec, raw *RawSnapshot) (any, error) {
	return h.definition.typedSnapshot(codec, raw)
}

func cloneProgress(p *Progress) *Progress {
	if p == nil {
		return nil
	}
	out := *p
	out.Metadata = copyMetadata(p.Metadata)
	return &out
}
