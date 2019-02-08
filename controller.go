package dbperf

import (
	"context"
	"database/sql"
	"io"
	"math"
	"sort"
	"sync"
	"time"
)

// jobQueueSize is the size of each individual worker queue
const jobQueueSize = 20

// Queryable is the interace that wraps the basic database query operations. It is expected the implementation
// is safe for concurrent use by multiple goroutines.
//
// NOTE: The standard library sql.DB satisfies this interface
type Queryable interface {
	// QueryContext executes a a query that returns rows, typically a SELECT. The args are for any placeholder parameters in the query.
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	// PrepareContext creates a prepared statement for later queries or executations. Multiple queries or executions
	// may be run concurrently from the returned statement. The caller must call the statement's Close method when the
	// statement is no longer needed.
	//
	// The provided context is used for the preparation of the statement, not for the execution of the statement.
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)

	// QueryRowContext executes a query that is expected to return at most one row. QueryRowContext always returns a non-nil value.
	// Errors are deferred until Row's Scan method is called. If the query selects no rows, the *Row's Scan will return ErrNoRows.
	// Otherwise, the *Row's Scan scans the first selected row and discards the rest.
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row

	// ExecContext executes a query without returning any rows. The args are for any placeholder parameters in the query.
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// QueryStats is a container for query statistics for a single test run
type QueryStats struct {
	Processed    int64         // total # queries processed
	TotalElapsed time.Duration // total processing time across all queries
	Min          time.Duration // min query time
	Max          time.Duration // max query time
	Avg          time.Duration // average query time
	Median       time.Duration // median query time
}

// result of a single query that was executed
type result struct {
	elapsed time.Duration
	err     error
}

type worker struct {
	id        int
	db        Queryable       // the database interface
	jobs      chan *Query     // individual worker queue
	results   chan<- result   // result channel
	done      chan struct{}   // stop channel worker exits on
	wg        *sync.WaitGroup // signalled when the worker has exited
	processed int             // the number of queries processed by this worker
}

func (w *worker) run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer w.wg.Done()

	for {
		select {
		case q, ok := <-w.jobs:
			if !ok {
				// no more jobs will be sent, exit normally
				return
			}

			// execute a single query
			start := time.Now()
			_, err := w.db.QueryContext(ctx, q.Query, q.Args...)
			elapsed := time.Since(start)

			// post the results
			w.results <- result{elapsed, err}

			w.processed++
		case <-w.done:
			// hard exit
			return
		}
	}
}

// Controller is a handle for executing a single test run
type Controller struct {
	poolSize         int                // worker pool size
	workers          []*worker          // worker pool
	byKey            map[string]*worker // route same key to the same worker every time
	nextWorker       int                // next random worker when key has not been seen before
	completedQueries chan result

	quit chan struct{}
	wg   sync.WaitGroup
}

// NewController initializes a test controller with the given worker pool size
func NewController(poolSize int) *Controller {
	if poolSize <= 0 {
		poolSize = 1
	}

	return &Controller{
		poolSize:         poolSize,
		quit:             make(chan struct{}),
		workers:          make([]*worker, 0, poolSize),
		byKey:            make(map[string]*worker),
		completedQueries: make(chan result, jobQueueSize),
	}
}

// get the next available worker for the given query
func (c *Controller) getWorker(q *Query) *worker {
	// TODO - implement an option to turn this pinning behavior off
	worker, ok := c.byKey[q.key]
	if !ok {
		// round robin when the key hasn't been seen
		worker = c.workers[c.nextWorker]
		c.nextWorker = (c.nextWorker + 1) % len(c.workers)
		c.byKey[q.key] = worker
	}

	return worker
}

func (c *Controller) initPool(db Queryable) {
	// start the workers
	for i := 0; i < c.poolSize; i++ {
		w := &worker{
			id:      i,
			db:      db,
			jobs:    make(chan *Query, jobQueueSize),
			results: c.completedQueries,
			done:    c.quit,
			wg:      &c.wg,
		}

		c.workers = append(c.workers, w)
		go w.run()
	}

	c.wg.Add(len(c.workers))
}

func (c *Controller) seedWorkers(g QueryGenerator) error {
	seeded := -1

	// ensure every worker starts off with 1 job or until generator is exhausted
	for seeded < len(c.workers) {
		query, err := g.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// get the correct worker for the job
		worker := c.getWorker(query)
		worker.jobs <- query

		if worker.id > seeded {
			seeded = worker.id
		}
	}

	return nil
}

// closeQueues closes all of the individual worker job queus signalling them to finish
// what they are doing and exit normally
func (c *Controller) closeQueues() {
	for _, w := range c.workers {
		close(w.jobs)
	}
}

func (c *Controller) RunTest(ctx context.Context, db Queryable, g QueryGenerator) (*QueryStats, error) {
	results := make([]time.Duration, 0)

	// start the worker pool
	c.initPool(db)

	// seed the workers
	if err := c.seedWorkers(g); err != nil {
		return nil, err
	}

outer:
	for {
		select {
		case result := <-c.completedQueries:
			// process completed query
			if result.err != nil {
				close(c.quit)
				return nil, result.err
			}

			results = append(results, result.elapsed)

			// queue up more work if available
			q, err := g.Next()
			if err != nil {
				if err == io.EOF {
					// done, gather results
					break outer
				}
				close(c.quit)
				return nil, err
			}

			worker := c.getWorker(q)

			// FIXME - there is potential here that if the input query's are skewed to a single key we may starve the other workers when this worker's job queue is full
			//         this is dependent on the input queries generated and how clustered the queries are by a particular key are

			worker.jobs <- q

		case <-ctx.Done():
			close(c.quit)
			return nil, ctx.Err()
		}
	}

	// signal each worker to finish processing their queues
	c.closeQueues()

	// wait for workers to exit
	c.wg.Wait()
	close(c.completedQueries)

	// drain any remaining results
	for result := range c.completedQueries {
		if result.err != nil {
			return nil, result.err
		}

		results = append(results, result.elapsed)
	}

	return calculateStats(results), nil
}

func calculateStats(results []time.Duration) *QueryStats {
	sort.Slice(results, func(i, j int) bool {
		return results[i] < results[j]
	})

	n := len(results)
	stats := QueryStats{
		Processed: int64(n),
		Min:       time.Duration(math.MaxInt64),
		Max:       time.Duration(math.MinInt64),
	}

	for _, v := range results {
		if v < stats.Min {
			stats.Min = v
		}

		if v > stats.Max {
			stats.Max = v
		}

		stats.TotalElapsed += v
	}

	stats.Avg = time.Duration(int64(stats.TotalElapsed) / int64(n))

	if n%2 == 0 {
		n--
		// average the middle
		stats.Median = (results[n/2] + results[n/2+1]) / 2
	} else {
		stats.Median = results[n/2]
	}

	return &stats
}
