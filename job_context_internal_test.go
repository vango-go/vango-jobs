package jobs

import (
	"testing"
	"time"
)

func TestJobContextProgressForFlushUsesProvidedClock(t *testing.T) {
	base := time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)
	ctx := &jobContext{
		progress:      &Progress{Stage: "running"},
		progressDirty: true,
	}

	if got := ctx.progressForFlush(time.Second, base, base.Add(500*time.Millisecond)); got != nil {
		t.Fatalf("progress flush should be throttled before interval")
	}
	if !ctx.progressDirty {
		t.Fatal("progressDirty cleared before flush interval elapsed")
	}

	got := ctx.progressForFlush(time.Second, base, base.Add(2*time.Second))
	if got == nil || got.Stage != "running" {
		t.Fatalf("progress flush=%v want running progress", got)
	}
	if ctx.progressDirty {
		t.Fatal("progressDirty not cleared after flush")
	}
}
