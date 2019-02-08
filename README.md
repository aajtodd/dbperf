DB Performance Test Utility
----------------------------

`dbperf` is a test utility for benchmarking TimescaleDB performance.


# Building

This project requires Go 1.11+ (for modules support).

Build `dbperf` executable:


From the root project directory: 

`go build ./cmd/dbperf` 


# Running Benchmarks

It is assumed that a test database has already been configured. Basic Docker instructions are provided below to get started.


Basic usage `./dbperf [-n workers] FILENAME.csv` where filename is path to CSV file containing the queries to execute. See `cmd/dbperf/main.go` for additional environment variables.


## Docker

Start the TimescaleDB instance

```
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb
```

Create the test database

```
psql --host localhost --port 5437 -U postgres < ./scripts/cpu_usage.sql
```

Ingest our test data

```
psql --host localhost --port 5437 -U postgres -d homework -c "\COPY cpu_usage FROM ./scripts/cpu_usage.csv CSV HEADER"
```


# Development

## Running Tests

Run all tests `go test ./...`

