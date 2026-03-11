module github.com/vango-go/vango-jobs

go 1.24.0

toolchain go1.26.0

require (
	github.com/fergusstrange/embedded-postgres v1.33.0
	github.com/jackc/pgx/v5 v5.7.6
	github.com/oklog/ulid/v2 v2.1.1
	github.com/robfig/cron/v3 v3.0.1
	github.com/vango-go/vango v0.0.0-00010101000000-000000000000
)

require golang.org/x/sys v0.41.0 // indirect

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/fxamacker/cbor/v2 v2.7.0 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/redis/go-redis/v9 v9.6.1 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/xi2/xz v0.0.0-20171230120015-48954b6210f8 // indirect
	go.opentelemetry.io/otel v1.24.0
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.opentelemetry.io/otel/sdk v1.24.0
	go.opentelemetry.io/otel/trace v1.24.0
	golang.org/x/crypto v0.48.0 // indirect
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/text v0.34.0 // indirect
)

replace github.com/vango-go/vango => ../vango
