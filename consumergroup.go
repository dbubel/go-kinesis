package go_kinesis

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alitto/pond"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/sirupsen/logrus"
)

var (
	// ErrShardNotFound is returned when polling for an available shard times out
	ErrShardNotFound = fmt.Errorf("polling for shard timed out")
	// ErrStoreNotInitialized is returned when the store is not properly initialized
	ErrStoreNotInitialized = fmt.Errorf("store is not initialized")
)

// ConsumerGroup manages multiple consumers reading from different shards
// of the same Kinesis stream, providing coordination to ensure each shard
// is processed by only one consumer at a time
type ConsumerGroup struct {
	*Consumer
	activeShards  int
	mu            *sync.Mutex
	wg            *sync.WaitGroup
	monitorCancel context.CancelFunc
	monitorDone   chan struct{}
	shardRefresh  time.Duration
}

// NewConsumerGroup creates a new consumer group for processing multiple shards
func NewConsumerGroup(client *kinesis.Client, streamName string, opts ...Option) (*ConsumerGroup, error) {
	consumer, err := NewConsumer(client, streamName, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &ConsumerGroup{
		Consumer:     consumer,
		activeShards: 0,
		mu:           &sync.Mutex{},
		wg:           &sync.WaitGroup{},
		monitorDone:  make(chan struct{}),
		shardRefresh: 30 * time.Second, // Default shard refresh interval
	}, nil
}

// WithShardRefreshInterval sets the interval at which the consumer group checks for new shards
func WithShardRefreshInterval(d time.Duration) Option {
	return func(c *Consumer) error {
		if d <= 0 {
			return fmt.Errorf("shard refresh interval must be positive, got %v", d)
		}

		if cg, ok := interface{}(c).(*ConsumerGroup); ok {
			cg.shardRefresh = d
		}
		return nil
	}
}

// Add increments the active shard count
func (c *ConsumerGroup) Add(n int) {
	c.mu.Lock()
	c.activeShards += n
	c.mu.Unlock()
}

// Sub decrements the active shard count
func (c *ConsumerGroup) Sub(n int) {
	c.mu.Lock()
	c.activeShards -= n
	c.mu.Unlock()
}

// GetActiveShardCount returns the current number of active shards being processed
func (c *ConsumerGroup) GetActiveShardCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.activeShards
}

// ScanAll starts scanning all available shards in the stream
// It will continuously monitor for new shards and start processing them
func (cg *ConsumerGroup) ScanAll(ctx context.Context, fn ScanFunc) error {
	if cg.store == nil {
		return ErrStoreNotInitialized
	}

	// Create a monitoring context with cancel function
	monitorCtx, cancel := context.WithCancel(ctx)
	cg.monitorCancel = cancel

	// Start the initial scan
	if err := cg.scanAvailableShards(ctx, fn); err != nil {
		cancel()
		return err
	}

	// Start monitoring for new shards in the background
	go cg.monitorShards(monitorCtx, fn)

	// Wait for all consumers to complete
	cg.wg.Wait()
	close(cg.monitorDone)
	return nil
}

// scanAvailableShards scans for available shards and starts consuming from them
func (cg *ConsumerGroup) scanAvailableShards(ctx context.Context, fn ScanFunc) error {
	shards, err := cg.listShards()
	if err != nil {
		return fmt.Errorf("failed to list shards: %w", err)
	}

	cg.logger.WithFields(logrus.Fields{"shards": shards}).Info("shards on stream")

	// Sync the shards with the store to ensure it's aware of all shards
	if err := cg.store.SyncShards(shards); err != nil {
		return fmt.Errorf("failed to sync shards: %w", err)
	}

	// Start processing each shard up to the shard limit
	for i := 0; i < len(shards) && cg.GetActiveShardCount() < cg.shardLimit; i++ {
		// Poll for an available shard that's not being processed by another consumer
		shardID, err := cg.store.PollForAvailableShard(ctx, time.Second*30, shards)

		if errors.Is(err, ErrShardNotFound) {
			cg.logger.Info("no available shards for processing")
			continue
		}

		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("error polling for available shard: %w", err)
		}

		if shardID == "" {
			continue
		}

		// Start processing the shard
		cg.wg.Add(1)
		go cg.consume(ctx, shardID, fn)
	}

	return nil
}

// monitorShards continuously monitors for new shards and starts consuming from them
func (cg *ConsumerGroup) monitorShards(ctx context.Context, fn ScanFunc) {
	ticker := time.NewTicker(cg.shardRefresh)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			cg.logger.Info("shard monitor stopping due to context cancellation")
			return
		case <-ticker.C:
			// Check if we're under the shard limit
			if cg.GetActiveShardCount() >= cg.shardLimit {
				continue
			}

			// Scan for new shards
			if err := cg.scanAvailableShards(ctx, fn); err != nil {
				cg.logger.WithError(err).Error("error scanning for new shards")
			}
		}
	}
}

// consume processes a single shard
func (cg *ConsumerGroup) consume(ctx context.Context, shardID string, fn ScanFunc) {
	defer cg.wg.Done()

	// Track this shard as active
	cg.Add(1)
	cg.logger.WithField("shardId", shardID).Info("starting to consume shard")

	// Create a child context that can be canceled separately
	shardCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Process the shard - this blocks until the shard is closed or an error occurs
	cg.ScanShard(shardCtx, shardID, fn)

	// Release the shard so other consumers can pick it up if needed
	if err := cg.store.ReleaseStream(shardID); err != nil {
		cg.logger.WithError(err).Error("error releasing shard")
	} else {
		cg.logger.WithFields(logrus.Fields{"shard": shardID}).Info("shard released")
	}

	// Update active shard count
	cg.Sub(1)
}

// Stop gracefully stops the consumer group and waits for all processing to complete
func (cg *ConsumerGroup) Stop() {
	if cg.monitorCancel != nil {
		cg.monitorCancel()
		<-cg.monitorDone // Wait for monitor to finish
	}
}

// ScanAllAsync starts processing all shards with concurrent record processing
func (cg *ConsumerGroup) ScanAllAsync(ctx context.Context, concurrency, poolsize int, fn ScanFunc) error {
	return cg.ScanAll(ctx, func(record *Record) error {
		// Create a wrapper function that processes records with the worker pool
		asyncFn := func(r *Record) error {
			// Process records concurrently using the worker pool
			// This doesn't block the shard consumer, allowing it to fetch more records
			err := fn(r)
			if err != nil && !errors.Is(err, ErrSkipCheckpoint) {
				cg.logger.WithError(err).Error("error processing record in worker")
			}
			return nil
		}

		// Use an async pool to process this record
		pool := cg.getWorkerPool(concurrency, poolsize)
		pool.Submit(func() {
			_ = asyncFn(record)
		})

		return nil
	})
}

// workerPools maintains a mapping of shard IDs to worker pools
var workerPools = struct {
	sync.Mutex
	pools map[string]*pond.WorkerPool
}{
	pools: make(map[string]*pond.WorkerPool),
}

// getWorkerPool gets or creates a worker pool for the given concurrency and size
func (cg *ConsumerGroup) getWorkerPool(concurrency, poolsize int) *pond.WorkerPool {
	key := fmt.Sprintf("%d-%d", concurrency, poolsize)

	workerPools.Lock()
	defer workerPools.Unlock()

	if pool, ok := workerPools.pools[key]; ok {
		return pool
	}

	// Create a new pool
	pool := pond.New(concurrency, poolsize)
	workerPools.pools[key] = pool

	return pool
}
