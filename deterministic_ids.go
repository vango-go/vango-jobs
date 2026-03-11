package jobs

import (
	mrand "math/rand"
	"sync"

	"github.com/oklog/ulid/v2"
)

type deterministicEntropy struct {
	mu  sync.Mutex
	rnd *mrand.Rand
}

func (e *deterministicEntropy) Read(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range p {
		p[i] = byte(e.rnd.Intn(256))
	}
	return len(p), nil
}

func newDeterministicIDGenerator(seed int64) *idGenerator {
	return &idGenerator{
		entropy: ulid.Monotonic(&deterministicEntropy{rnd: mrand.New(mrand.NewSource(seed))}, 0),
	}
}
