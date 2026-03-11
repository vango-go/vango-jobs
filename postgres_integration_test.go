package jobs_test

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	jobs "github.com/vango-go/vango-jobs"
)

func TestPostgresRuntime(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres integration test in short mode")
	}

	ctx, pool := setupPostgres(t)

	t.Run("round trip", func(t *testing.T) {
		reg := jobs.NewRegistry()
		def := jobs.New[string, string]("postgres.echo.v1")
		reg.MustHandle(def, func(ctx jobs.Ctx, in string) (string, error) {
			return in + "!", nil
		})

		rt, err := jobs.Open(pool, reg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rt.Close() })

		ref, err := rt.Enqueue(ctx, def, "hello", jobs.WithUnique("u1", jobs.UniqueReturnExisting))
		if err != nil {
			t.Fatal(err)
		}
		ref2, err := rt.Enqueue(ctx, def, "ignored", jobs.WithUnique("u1", jobs.UniqueReturnExisting))
		if err != nil {
			t.Fatal(err)
		}
		if ref.ID != ref2.ID {
			t.Fatalf("unique refs mismatch: %s vs %s", ref.ID, ref2.ID)
		}

		worker := rt.NewWorker(jobs.WithWorkerConcurrency(1))
		if ran, err := worker.RunOnce(ctx); err != nil || ran != 1 {
			t.Fatalf("RunOnce ran=%d err=%v", ran, err)
		}

		snap, err := jobs.LookupTyped(ctx, rt, def, ref.ID, jobs.InspectAdmin())
		if err != nil {
			t.Fatal(err)
		}
		if snap.Status != jobs.StatusSucceeded {
			t.Fatalf("status=%s want succeeded", snap.Status)
		}
		if snap.Output == nil || *snap.Output != "hello!" {
			t.Fatalf("output=%v want hello!", snap.Output)
		}
	})

	t.Run("transactional enqueue commit and rollback", func(t *testing.T) {
		reg := jobs.NewRegistry()
		def := jobs.New[string, string]("postgres.tx.v1")
		reg.MustHandle(def, func(ctx jobs.Ctx, in string) (string, error) {
			return in + "!", nil
		})

		rt, err := jobs.Open(pool, reg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rt.Close() })

		tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		rolledBackRef, err := rt.EnqueueTx(ctx, tx, def, "rolled-back")
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := rt.Inspect(ctx, rolledBackRef.ID, jobs.InspectAdmin()); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("rollback inspect err=%v want ErrNotFound", err)
		}

		tx, err = pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		committedRef, err := rt.EnqueueTx(ctx, tx, def, "committed")
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}

		worker := rt.NewWorker(jobs.WithWorkerConcurrency(1))
		if ran, err := worker.RunOnce(ctx); err != nil || ran != 1 {
			t.Fatalf("RunOnce ran=%d err=%v", ran, err)
		}

		snap, err := jobs.LookupTyped(ctx, rt, def, committedRef.ID, jobs.InspectAdmin())
		if err != nil {
			t.Fatal(err)
		}
		if snap.Status != jobs.StatusSucceeded {
			t.Fatalf("status=%s want succeeded", snap.Status)
		}
		if snap.Output == nil || *snap.Output != "committed!" {
			t.Fatalf("output=%v want committed!", snap.Output)
		}
	})

	t.Run("pause and resume queue", func(t *testing.T) {
		reg := jobs.NewRegistry()
		def := jobs.New[string, string](
			"postgres.pause.v1",
			jobs.Queue("postgres_pause_queue"),
		)
		reg.MustHandle(def, func(ctx jobs.Ctx, in string) (string, error) {
			return in + "!", nil
		})

		rt, err := jobs.Open(pool, reg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rt.Close() })

		ref, err := rt.Enqueue(ctx, def, "pause-me")
		if err != nil {
			t.Fatal(err)
		}
		if err := rt.PauseQueue(ctx, "postgres_pause_queue", "maintenance", jobs.ControlAdmin()); err != nil {
			t.Fatal(err)
		}

		worker := rt.NewWorker(jobs.WithWorkerQueues("postgres_pause_queue"), jobs.WithWorkerConcurrency(1))
		if ran, err := worker.RunOnce(ctx); err != nil || ran != 0 {
			t.Fatalf("paused RunOnce ran=%d err=%v", ran, err)
		}

		snap, err := rt.Inspect(ctx, ref.ID, jobs.InspectAdmin())
		if err != nil {
			t.Fatal(err)
		}
		if snap.Status != jobs.StatusQueued {
			t.Fatalf("paused status=%s want queued", snap.Status)
		}

		if err := rt.ResumeQueue(ctx, "postgres_pause_queue", jobs.ControlAdmin()); err != nil {
			t.Fatal(err)
		}
		if ran, err := worker.RunOnce(ctx); err != nil || ran != 1 {
			t.Fatalf("resumed RunOnce ran=%d err=%v", ran, err)
		}

		typed, err := jobs.LookupTyped(ctx, rt, def, ref.ID, jobs.InspectAdmin())
		if err != nil {
			t.Fatal(err)
		}
		if typed.Status != jobs.StatusSucceeded {
			t.Fatalf("resumed status=%s want succeeded", typed.Status)
		}
	})

	t.Run("list supports terminal time ordering", func(t *testing.T) {
		reg := jobs.NewRegistry()
		def := jobs.New[string, string]("postgres.ordering.v1")
		reg.MustHandle(def, func(ctx jobs.Ctx, in string) (string, error) {
			return "", jobs.Permanent(jobs.Safe(errors.New(in), in))
		})

		rt, err := jobs.Open(pool, reg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rt.Close() })

		olderRef, err := rt.Enqueue(ctx, def, "older-failed-later", jobs.WithDelay(200*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
		newerRef, err := rt.Enqueue(ctx, def, "newer-failed-earlier")
		if err != nil {
			t.Fatal(err)
		}

		worker := rt.NewWorker(jobs.WithWorkerConcurrency(1))
		if ran, err := worker.RunOnce(ctx); err != nil || ran != 1 {
			t.Fatalf("first RunOnce ran=%d err=%v", ran, err)
		}

		time.Sleep(250 * time.Millisecond)
		if ran, err := worker.RunOnce(ctx); err != nil || ran != 1 {
			t.Fatalf("second RunOnce ran=%d err=%v", ran, err)
		}

		defaultOrder, err := rt.List(ctx, jobs.ListFilter{
			Statuses: []jobs.Status{jobs.StatusFailed},
			Limit:    10,
		}, jobs.InspectAdmin())
		if err != nil {
			t.Fatal(err)
		}
		if len(defaultOrder) != 2 {
			t.Fatalf("len(defaultOrder)=%d want 2", len(defaultOrder))
		}
		if defaultOrder[0].Ref.ID != newerRef.ID || defaultOrder[1].Ref.ID != olderRef.ID {
			t.Fatalf("default order got %s then %s, want %s then %s", defaultOrder[0].Ref.ID, defaultOrder[1].Ref.ID, newerRef.ID, olderRef.ID)
		}

		terminalOrder, err := rt.List(ctx, jobs.ListFilter{
			Statuses: []jobs.Status{jobs.StatusFailed},
			Limit:    10,
			Order:    jobs.ListOrderTerminalTimeDesc,
		}, jobs.InspectAdmin())
		if err != nil {
			t.Fatal(err)
		}
		if len(terminalOrder) != 2 {
			t.Fatalf("len(terminalOrder)=%d want 2", len(terminalOrder))
		}
		if terminalOrder[0].Ref.ID != olderRef.ID || terminalOrder[1].Ref.ID != newerRef.ID {
			t.Fatalf("terminal order got %s then %s, want %s then %s", terminalOrder[0].Ref.ID, terminalOrder[1].Ref.ID, olderRef.ID, newerRef.ID)
		}
	})
}

func setupPostgres(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()

	port := freePort(t)
	portNum, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	db := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Port(uint32(portNum)).
		Username("postgres").
		Password("postgres").
		Database("jobs_test"))
	if err := db.Start(); err != nil {
		t.Skipf("embedded postgres unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Stop()
	})

	pool, err := pgxpool.New(context.Background(), "postgres://postgres:postgres@127.0.0.1:"+port+"/jobs_test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	if err := jobs.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return ctx, pool
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return port
}
