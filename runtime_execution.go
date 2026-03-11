package jobs

import "time"

func (r *Runtime) backoffFor(name, jobID string, attempt int) time.Duration {
	handler, ok := r.registry.resolve(name)
	if !ok {
		return r.cfg.HeartbeatInterval
	}
	return handler.baseDefinition().backoff.Next(jobID, attempt, nil)
}

func (r *Runtime) retentionFor(def baseDefinition, status Status) time.Duration {
	if def.retention > 0 {
		return def.retention
	}
	return r.retentionForName(def.name, status)
}

func (r *Runtime) retentionForName(_ string, status Status) time.Duration {
	switch status {
	case StatusSucceeded:
		return r.cfg.SuccessRetention
	case StatusCanceled:
		return r.cfg.CanceledRetention
	case StatusFailed:
		return r.cfg.FailedRetention
	default:
		return r.cfg.SuccessRetention
	}
}
