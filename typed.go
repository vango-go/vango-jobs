package jobs

import "context"

// EnqueueTyped is the generic convenience wrapper over the low-level Enqueuer API.
func EnqueueTyped[In any, Out any](ctx context.Context, q Enqueuer, def *Definition[In, Out], input In, opts ...EnqueueOption) (Ref, error) {
	return q.Enqueue(ctx, def, input, opts...)
}

// LookupTyped is the generic convenience wrapper over the low-level TypedReader API.
func LookupTyped[In any, Out any](ctx context.Context, r TypedReader, def *Definition[In, Out], id string, opts ...InspectOption) (*Snapshot[Out], error) {
	value, err := r.Lookup(ctx, def, id, opts...)
	if err != nil {
		return nil, err
	}
	snapshot, _ := value.(*Snapshot[Out])
	return snapshot, nil
}
