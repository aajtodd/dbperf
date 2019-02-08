// dbperf is a command line utility for testing SELECT performance of a TimescaleDB / postgres database.
//
// Environment Variables
//
// DB_HOST: The database host to connect to (default: localhost)
// DB_PORT: The database port (default: 5432)
// DB_USER: The database username to use (default: postgres)
// DB_PASSWORD: The database username to use (default: password)
// DB_NAME: The database name to use (default: homework)
//
// The DBPERFDEBUG variable controls debugging variables within the runtime. It is a comma-separated list of name=val pairs setting these named variables:
//
// pprof: Setting pprof=X causes an HTTP server listening on port X to serve the profiling data expected by the pprof tool. See https://golang.org/pkg/net/http/pprof
//
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"timescale/dbperf"

	"net/http"
	_ "net/http/pprof"

	_ "github.com/lib/pq"
)

var (
	host     = getenv("DB_HOST", "localhost")
	port     = getenv("DB_PORT", "5432")
	user     = getenv("DB_USER", "postgres")
	password = getenv("DB_PW", "password")
	dbName   = getenv("DB_NAME", "homework")
)

// getenv is a utility function to get a value from the environment or return the default if not found
func getenv(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}

	return val
}

func usage(fs *flag.FlagSet) func() {
	return func() {
		fmt.Fprintf(os.Stdout, "usage: dbperf [FLAGS] FILENAME\n\n")
		fmt.Fprintf(os.Stdout, "Filename may be specified as either an argument or via the -f flag\n\n")
		fs.PrintDefaults()
	}
}

func main() {
	var cli CliArgs
	fs := flag.NewFlagSet("dbperf", flag.ExitOnError)
	fs.Usage = usage(fs)
	cli.Register(fs)

	if err := fs.Parse(os.Args[1:]); err != nil {
		fs.Usage()
		os.Exit(1)
	}

	args := fs.Args()
	if cli.filename == "" && len(args) != 1 {
		fs.Usage()
		os.Exit(1)
	}

	filename := cli.filename
	if filename == "" {
		filename = args[0]
	}

	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("open %s: %s\n", filename, err)
	}
	defer f.Close()

	log.SetFlags(log.Ldate | log.Lmicroseconds)
	log.Println("starting dbperf...")

	// run pprof monitor if asked
	if debug.pprof > 0 {
		go func() {
			laddr := fmt.Sprintf(":%d", debug.pprof)
			log.Printf("DEBUG: starting pprof server: http://%s/debug/pprof\n", laddr)
			log.Fatal(http.ListenAndServe(laddr, nil))
		}()
	}

	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=disable", user, password, dbName, host, port)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("failed to connect to database: %s\n", err)
	}

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("failed to ping database: %s\n", err)
	}
	log.Println("database connection good...starting test")

	controller := dbperf.NewController(cli.nworkers)
	generator := dbperf.NewCPUTestGenerator(f)

	stats, err := controller.RunTest(ctx, db, generator)
	if err != nil {
		log.Fatalf("test run failed: %s\n", err)
	}

	fmt.Printf("%d queries processed after %s\n", stats.Processed, stats.TotalElapsed)
	fmt.Printf("min: %s; max: %s; avg: %s; median: %s\n", stats.Min, stats.Max, stats.Avg, stats.Median)

}
