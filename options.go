package go_kinesis

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// Option is used to override defaults when creating a new Consumer.
// It follows the functional options pattern for flexible and extensible configuration.
type Option func(*Consumer) error

// Apply applies all provided options to the Consumer, returning any error encountered.
func applyOptions(c *Consumer, opts ...Option) error {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(c); err != nil {
			return fmt.Errorf("failed to apply option: %w", err)
		}
	}
	return nil
}

// WithLogger overrides the default logger.
// If nil is provided, the function will return an error.
func WithLogger(logger *logrus.Logger) Option {
	return func(c *Consumer) error {
		if logger == nil {
			return fmt.Errorf("logger cannot be nil")
		}
		c.logger = logger
		return nil
	}
}

// WithShardLimit sets the maximum number of shards to process concurrently.
// The value must be positive, otherwise an error is returned.
func WithShardLimit(shardLimit int) Option {
	return func(c *Consumer) error {
		if shardLimit <= 0 {
			return fmt.Errorf("shard limit must be positive, got %d", shardLimit)
		}
		c.shardLimit = shardLimit
		return nil
	}
}

// WithStore sets the store implementation for the consumer.
// If nil is provided, the function will return an error.
func WithStore(store Store) Option {
	return func(c *Consumer) error {
		if store == nil {
			return fmt.Errorf("store cannot be nil")
		}
		c.store = store
		return nil
	}
}

// WithClient overrides the default client.
// This option is currently commented out but kept for reference.
//
// func WithClient(client *kinesis.Client) Option {
// 	return func(c *Consumer) error {
// 		if client == nil {
// 			return fmt.Errorf("client cannot be nil")
// 		}
// 		c.client = client
// 		return nil
// 	}
// }

// WithShardIteratorType overrides the starting point for the consumer.
// If empty string is provided, the function will return an error.
func WithShardIteratorType(t IteratorTypeStrat) Option {
	return func(c *Consumer) error {
		if t == "" {
			return fmt.Errorf("iterator type strategy cannot be empty")
		}
		c.initialShardIteratorType = t
		return nil
	}
}

// WithTimestamp overrides the starting point for the consumer.
// Time must not be the zero value, otherwise an error is returned.
func WithTimestamp(t time.Time) Option {
	return func(c *Consumer) error {
		if t.IsZero() {
			return fmt.Errorf("timestamp cannot be zero value")
		}
		c.initialTimestamp = &t
		return nil
	}
}

// WithScanInterval overrides the scan interval for the consumer.
// The duration must be positive, otherwise an error is returned.
func WithScanInterval(d time.Duration) Option {
	return func(c *Consumer) error {
		if d <= 0 {
			return fmt.Errorf("scan interval must be positive, got %v", d)
		}
		c.scanInterval = d
		return nil
	}
}

// WithMaxRecords overrides the maximum number of records to be
// returned in a single GetRecords call for the consumer.
// The value must be between 1 and 10,000 (AWS Kinesis limit),
// otherwise an error is returned.
func WithMaxRecords(n int64) Option {
	return func(c *Consumer) error {
		if n < 1 || n > 10000 {
			return fmt.Errorf("max records must be between 1 and 10000, got %d", n)
		}
		c.maxRecords = n
		return nil
	}
}

// WithAggregation specifies whether the consumer should expect
// and handle aggregated records.
// This option is currently commented out but kept for reference.
//
// func WithAggregation(a bool) Option {
// 	return func(c *Consumer) error {
// 		c.isAggregated = a
// 		return nil
// 	}
// }

// ShardClosedHandler is a handler that will be called when the consumer
// has reached the end of a closed shard.
// No more records for that shard will be provided by the consumer.
// An error can be returned to stop the consumer.
//
// type ShardClosedHandler = func(streamName, shardID string) error
//
// func WithShardClosedHandler(h ShardClosedHandler) Option {
// 	return func(c *Consumer) error {
// 		if h == nil {
// 			return fmt.Errorf("shard closed handler cannot be nil")
// 		}
// 		c.shardClosedHandler = h
// 		return nil
// 	}
// }
