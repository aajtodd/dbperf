package dbperf

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// SELECT date_trunc('minute', ts) AS minute, MIN(usage), MAX(usage) from cpu_usage
//     WHERE host = 'host_000001'
//     AND ts BETWEEN '2017-01-01 00:00:00' AND '2017-01-01 00:01:59'
//     GROUP BY date_trunc('minute', ts);

func TestCPUGenerator(t *testing.T) {

	t.Run("success", func(t *testing.T) {
		input := `hostname,start_time,end_time
host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000001,2017-01-02 13:02:02,2017-01-02 14:02:02`

		buf := strings.NewReader(input)

		g := NewCPUTestGenerator(buf)

		tests := []struct {
			expected *Query
			err      error
		}{
			{
				&Query{
					key:   "host_000008",
					Query: cpuTestQuery,
					Args: []interface{}{
						"host_000008",
						"2017-01-01 08:59:22",
						"2017-01-01 09:59:22",
					},
				}, nil,
			},
			{
				&Query{
					key:   "host_000001",
					Query: cpuTestQuery,
					Args: []interface{}{
						"host_000001",
						"2017-01-02 13:02:02",
						"2017-01-02 14:02:02",
					},
				}, nil,
			},
			{
				nil,
				io.EOF,
			},
		}

		for _, tt := range tests {
			actual, err := g.Next()

			if tt.err != nil {
				assert.Equal(t, tt.err, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, actual)
			}
		}
	})

	t.Run("invalid record", func(t *testing.T) {
		input := `hostname,start_time,end_time
host_000008,2017-01-0108:59:22,2017-01-01 09:59:22`

		buf := strings.NewReader(input)

		g := NewCPUTestGenerator(buf)

		_, err := g.Next()
		assert.Contains(t, err.Error(), "invalid query specification")
	})
}
