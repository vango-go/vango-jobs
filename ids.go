package jobs

import (
	"crypto/rand"
	"io"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

type idGenerator struct {
	mu      sync.Mutex
	entropy io.Reader
}

func newIDGenerator() *idGenerator {
	return &idGenerator{
		entropy: ulid.Monotonic(rand.Reader, 0),
	}
}

func (g *idGenerator) newJobID(now time.Time) string {
	return "job_" + g.newULID(now)
}

func (g *idGenerator) newAttemptID(now time.Time) string {
	return "jat_" + g.newULID(now)
}

func (g *idGenerator) newLeaseToken(now time.Time) string {
	return g.newULID(now)
}

func (g *idGenerator) newULID(now time.Time) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return ulid.MustNew(ulid.Timestamp(now.UTC()), g.entropy).String()
}
