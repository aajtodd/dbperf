package dbperf

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"time"
)

// Query is a container that contains the necessary query and any arguments to form
// a valid sql.Statement that abstracts where the query came from.
type Query struct {
	Query string        // The query to run
	Args  []interface{} // Any arguments to pass on and fill placeholders in the query
	key   string        // Internal key used for pinning workers - this is dependent on the test being run
}

// QueryGenerator is an interface for generating queries
type QueryGenerator interface {

	// Next returns the next query or io.EOF as an error when the generator is exhausted and
	// there are no more queries to execute
	Next() (*Query, error)
}

// NewCPUTestGenerator creats a query generator that understands the cpu usage select test case from the given source
func NewCPUTestGenerator(r io.Reader) QueryGenerator {
	return &cpuTestGenerator{
		reader: csv.NewReader(r),
	}
}

type cpuTestGenerator struct {
	reader     *csv.Reader
	headerRead bool
}

// dateTimeLayout specifies the expected format of datetime strings in the CSV file for time.Parse
const dateTimeLayout = "2006-01-02 15:04:05"

const cpuTestQuery = `SELECT date_trunc('minute', ts) AS minute, MIN(usage), MAX(usage) from cpu_usage 
	WHERE host = $1
    AND ts BETWEEN $2 AND $3
    GROUP BY date_trunc('minute', ts);`

func isValidDateTime(s string) bool {
	_, err := time.Parse(dateTimeLayout, s)
	return err == nil
}

func (g *cpuTestGenerator) Next() (*Query, error) {
	if !g.headerRead {
		if _, err := g.reader.Read(); err != nil {
			return nil, err
		}
		g.headerRead = true
	}

	records, err := g.reader.Read()
	if err != nil {
		return nil, err
	}

	if len(records) != 3 || !isValidDateTime(records[1]) || !isValidDateTime(records[2]) {
		return nil, fmt.Errorf("invalid query specification: %s", strings.Join(records, ","))
	}

	args := make([]interface{}, 0, 3)
	for _, r := range records {
		args = append(args, r)
	}

	q := &Query{
		key:   records[0],
		Query: cpuTestQuery,
		Args:  args,
	}

	return q, nil

}
