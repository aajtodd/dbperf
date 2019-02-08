package dbperf

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
	"timescale/dbperf/test/mocks/mock_dbperf"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestWorker(t *testing.T) {
	t.Run("done", func(t *testing.T) {
		var wg sync.WaitGroup
		w := &worker{
			done: make(chan struct{}),
			wg:   &wg,
		}

		wg.Add(1)
		close(w.done)
		w.run()
		wg.Wait()
	})

	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mdb := mock_dbperf.NewMockQueryable(ctrl)
		query := &Query{
			Query: "query",
			Args:  []interface{}{"arg1", "arg2"},
		}

		mdb.EXPECT().ExecContext(gomock.Any(), query.Query, query.Args...).Return(nil, nil)

		results := make(chan result, 1)
		jobs := make(chan *Query, 1)
		jobs <- query

		var wg sync.WaitGroup
		w := &worker{
			db:      mdb,
			done:    make(chan struct{}),
			jobs:    jobs,
			results: results,
			wg:      &wg,
		}

		wg.Add(1)

		go w.run()

		time.Sleep(time.Millisecond * 10)

		close(w.done)
		wg.Wait()

		r := <-results
		assert.NoError(t, r.err)

	})

}

func TestCalculateResults(t *testing.T) {
	t.Run("even", func(t *testing.T) {
		results := []time.Duration{
			time.Millisecond * 3000,
			time.Millisecond * 1200,
			time.Millisecond * 900,
			time.Millisecond * 1350,
		}

		expected := &QueryStats{
			Processed:    4,
			TotalElapsed: time.Millisecond * 6450,
			Min:          time.Millisecond * 900,
			Max:          time.Millisecond * 3000,
			Avg:          (time.Millisecond * 6450) / 4,
			Median:       time.Millisecond * 1275,
		}

		actual := calculateStats(results)
		assert.Equal(t, expected, actual)

	})
	t.Run("odd", func(t *testing.T) {
		results := []time.Duration{
			time.Millisecond * 3000,
			time.Millisecond * 1200,
			time.Millisecond * 1275,
			time.Millisecond * 900,
			time.Millisecond * 1350,
		}

		expected := &QueryStats{
			Processed:    5,
			TotalElapsed: time.Millisecond * 7725,
			Min:          time.Millisecond * 900,
			Max:          time.Millisecond * 3000,
			Avg:          time.Millisecond * 1545,
			Median:       time.Millisecond * 1275,
		}

		actual := calculateStats(results)
		assert.Equal(t, expected, actual)

	})
}

const testQueries = `hostname,start_time,end_time
host_000008,2017-01-01 08:59:22,2017-01-01 09:59:22
host_000001,2017-01-02 13:02:02,2017-01-02 14:02:02
host_000008,2017-01-02 18:50:28,2017-01-02 19:50:28
host_000002,2017-01-02 15:16:29,2017-01-02 16:16:29
host_000003,2017-01-01 08:52:14,2017-01-01 09:52:14
host_000002,2017-01-02 00:25:56,2017-01-02 01:25:56
host_000008,2017-01-01 07:36:28,2017-01-01 08:36:28
host_000000,2017-01-02 12:54:10,2017-01-02 13:54:10
host_000005,2017-01-02 11:29:42,2017-01-02 12:29:42
host_000006,2017-01-02 01:18:53,2017-01-02 02:18:53`

func TestRunTest(t *testing.T) {
	// basic smoke test
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewController(4)

	generator := NewCPUTestGenerator(strings.NewReader(testQueries))

	mdb := mock_dbperf.NewMockQueryable(ctrl)
	mdb.EXPECT().ExecContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(10)

	c.RunTest(context.Background(), mdb, generator)

	assert.Equal(t, 4, c.workers[0].processed) // 08, 08, 08, 00
	assert.Equal(t, 2, c.workers[1].processed) // 01, 05
	assert.Equal(t, 3, c.workers[2].processed) // 02, 02, 06
	assert.Equal(t, 1, c.workers[3].processed) // 03
}
