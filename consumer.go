package go_kinesis

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/alitto/pond"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/sirupsen/logrus"
)

// ErrSkipCheckpoint is a special error value that can be returned by ScanFunc
// to indicate that the current record should not update the checkpoint.
var ErrSkipCheckpoint = errors.New("skip checkpoint")

type IteratorTypeStrat string

const (
	// Iterator types supported by Kinesis
	Latest        IteratorTypeStrat = "LATEST"
	TrimHorizon   IteratorTypeStrat = "TRIM_HORIZON"
	AtSequence    IteratorTypeStrat = "AT_SEQUENCE_NUMBER"
	AfterSequence IteratorTypeStrat = "AFTER_SEQUENCE_NUMBER"
	AtTimestamp   IteratorTypeStrat = "AT_TIMESTAMP"
)

type Consumer struct {
	streamName               string
	initialShardIteratorType IteratorTypeStrat
	initialTimestamp         *time.Time
	client                   *kinesis.Client
	logger                   *logrus.Logger
	scanInterval             time.Duration
	store                    Store
	maxRecords               int64
	shardLimit               int
}

// NewConsumer creates a new consumer for processing a shard
func NewConsumer(client *kinesis.Client, streamName string, opts ...Option) (*Consumer, error) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	c := &Consumer{
		client:                   client,
		streamName:               streamName,
		initialShardIteratorType: Latest,
		scanInterval:             250 * time.Millisecond,
		maxRecords:               10000,
		shardLimit:               1000,
		logger:                   log,
		store:                    noopStore{},
	}

	// Apply options and handle errors
	if err := applyOptions(c, opts...); err != nil {
		return nil, err
	}

	return c, nil
}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan if a store is provided
// when creating a consumer or consumer group.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) {
	var lastSeqNum string
	var shardIterator *string
	var err error
	var backoff = 50 * time.Millisecond
	var maxBackoff = 5 * time.Second

	c.logger.WithFields(logrus.Fields{"shardId": shardID, "lastSeqNum": lastSeqNum}).Info("start scan")
	defer func() {
		c.logger.WithFields(logrus.Fields{"shardId": shardID, "lastSeqNum": lastSeqNum}).Info("stop scan")
	}()

	defer func() {
		if err := recover(); err != nil {
			c.logger.Errorf("ScanShard panic:\n%s", string(debug.Stack()))
		}
	}()

	iterator, err := c.getShardIterator(ctx, c.streamName, shardID)
	if err != nil {
		c.logger.WithError(err).Error("get shard iterator error")
		return
	}
	shardIterator = iterator.ShardIterator

	scanTicker := time.NewTicker(c.scanInterval)
	defer scanTicker.Stop()

	for {
		resp, err := c.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			Limit:         aws.Int32(int32(c.maxRecords)),
			ShardIterator: shardIterator,
		})

		// attempt to recover from GetRecords error
		if err != nil {
			if !isRecoverable(err) {
				c.logger.WithError(err).Error("get records error")
				return
			}

			// Implement exponential backoff for recoverable errors
			c.logger.WithFields(logrus.Fields{
				"error":   err.Error(),
				"backoff": backoff,
			}).Warn("recoverable error, backing off before retry")

			select {
			case <-time.After(backoff):
				// Double the backoff time for next iteration, up to maxBackoff
				backoff = time.Duration(min(float64(backoff)*2, float64(maxBackoff)))
			case <-ctx.Done():
				return
			}

			iterator, err = c.getShardIterator(ctx, c.streamName, shardID)
			if err != nil {
				c.logger.WithError(err).Error("get shard iterator error")
				return
			}
			shardIterator = iterator.ShardIterator

		} else {
			// Reset backoff on successful request
			backoff = 50 * time.Millisecond

			for _, r := range resp.Records {
				err := fn(&Record{r, shardID, resp.MillisBehindLatest})
				if err != nil {
					if errors.Is(err, ErrSkipCheckpoint) {
						// Continue processing but don't checkpoint this record
						continue
					}
					c.logger.WithError(err).Error("error in scan func")
					return
				}

				// Only update sequence number if record was processed successfully
				lastSeqNum = *r.SequenceNumber
				err = c.store.SetLastSeq(shardID, lastSeqNum)
				if err != nil {
					c.logger.WithError(err).Error("error setting last seq")
				}
			}

			if shardClosed(resp.NextShardIterator, shardIterator) {
				c.logger.WithFields(logrus.Fields{"shardId": shardID, "lastSeqNum": lastSeqNum}).Info("shard closed")
				return
			}
			shardIterator = resp.NextShardIterator
		}

		// Wait for next scan
		select {
		case <-ctx.Done():
			return
		case <-scanTicker.C:
			continue
		}
	}
}

// ScanShardAsync experimental function for scanning a single shard and processing the items
// concurrently
func (c *Consumer) ScanShardAsync(ctx context.Context, shardID string, concurrency, poolsize int, fn ScanFunc) {
	pool := pond.New(concurrency, poolsize)
	defer pool.Stop() // Ensure pool is properly stopped when function returns

	c.ScanShard(ctx, shardID, func(record *Record) error {
		pool.Submit(func() {
			err := fn(record)
			if err != nil && !errors.Is(err, ErrSkipCheckpoint) {
				c.logger.WithError(err).Error("error processing record in worker")
			}
		})
		return nil
	})
}

// ScanShards function used to scan a slice of shardIDs and process the items with fn
func (c *Consumer) ScanShards(ctx context.Context, shardIDs []string, fn ScanFunc) {
	// Create a wait group to wait for all goroutines to complete
	wg := &sync.WaitGroup{}

	for _, shardID := range shardIDs {
		wg.Add(1)
		go func(shardID string) {
			defer wg.Done()
			c.ScanShard(ctx, shardID, fn)
		}(shardID)
	}

	// Wait for all goroutines to complete
	wg.Wait()
}

// ScanShardsAsync experimental feature for scanning a list of shards async with concurrency
// workers and poolsize for size of the items in the pool
func (c *Consumer) ScanShardsAsync(ctx context.Context, shardIDs []string, concurrency, poolsize int, fn ScanFunc) {
	// Create a wait group to wait for all goroutines to complete
	wg := &sync.WaitGroup{}

	for _, shard := range shardIDs {
		shardID := shard
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.ScanShardAsync(ctx, shardID, concurrency, poolsize, fn)
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()
}

func (c *Consumer) getShardIterator(ctx context.Context, streamName, shardID string) (*kinesis.GetShardIteratorOutput, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(streamName),
	}

	switch c.initialShardIteratorType {
	case Latest:
		seqNum, err := c.store.GetLastSeq(shardID)
		if err != nil {
			return nil, fmt.Errorf("error getting last sequence number: %w", err)
		}

		if seqNum == "" {
			params.ShardIteratorType = types.ShardIteratorTypeLatest
		} else {
			params.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
			params.StartingSequenceNumber = aws.String(seqNum)
		}
	case TrimHorizon:
		params.ShardIteratorType = types.ShardIteratorTypeTrimHorizon
	case AtSequence, AfterSequence:
		seqNum, err := c.store.GetLastSeq(shardID)
		if err != nil {
			return nil, fmt.Errorf("error getting last sequence number: %w", err)
		}

		if seqNum == "" {
			return nil, fmt.Errorf("no sequence number available for %s iterator type", c.initialShardIteratorType)
		}

		if c.initialShardIteratorType == AtSequence {
			params.ShardIteratorType = types.ShardIteratorTypeAtSequenceNumber
		} else {
			params.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		}
		params.StartingSequenceNumber = aws.String(seqNum)
	case AtTimestamp:
		if c.initialTimestamp == nil {
			return nil, fmt.Errorf("timestamp required for AT_TIMESTAMP iterator type")
		}
		params.ShardIteratorType = types.ShardIteratorTypeAtTimestamp
		params.Timestamp = c.initialTimestamp
	default:
		return nil, fmt.Errorf("unsupported iterator type: %s", c.initialShardIteratorType)
	}

	return c.client.GetShardIterator(ctx, params)
}

// ListShards lists shards that the stream has and returns ones that are in an active state and
// ready to be consumed from. This method is exported so it can be used by client code.
func (c *Consumer) ListShards() ([]string, error) {
	return c.listShards()
}

// listShards list shards that the stream has and returns ones that are in an active state and
// ready to be consumed from.
func (c *Consumer) listShards() ([]string, error) {
	var shardList []string
	var nextToken *string

	for {
		input := &kinesis.ListShardsInput{
			StreamName: aws.String(c.streamName),
		}

		if nextToken != nil {
			input.NextToken = nextToken
		}

		resp, err := c.client.ListShards(context.Background(), input)
		if err != nil {
			// If stream not found, try using DescribeStream instead
			var resourceNotFound *types.ResourceNotFoundException
			if errors.As(err, &resourceNotFound) {
				return c.listShardsViaDescribe()
			}
			return nil, err
		}

		// Add active shards to the list
		for _, shard := range resp.Shards {
			if shard.ShardId != nil && !isShardClosed(&shard) {
				shardList = append(shardList, *shard.ShardId)
			}
		}

		// Continue until we've processed all pages
		if resp.NextToken == nil {
			break
		}
		nextToken = resp.NextToken
	}

	return shardList, nil
}

// listShardsViaDescribe is a fallback method that uses DescribeStream
// in case ListShards is not supported
func (c *Consumer) listShardsViaDescribe() ([]string, error) {
	var shardList []string
	var nextToken *string

	for {
		input := &kinesis.DescribeStreamInput{
			StreamName: aws.String(c.streamName),
		}

		if nextToken != nil {
			input.ExclusiveStartShardId = nextToken
		}

		streamDesc, err := c.client.DescribeStream(context.Background(), input)
		if err != nil {
			return nil, err
		}

		if streamDesc.StreamDescription.StreamStatus != types.StreamStatusActive {
			return nil, fmt.Errorf("stream status is not active")
		}

		// figure out which shards we can read from
		for _, shard := range streamDesc.StreamDescription.Shards {
			if shard.ShardId != nil && shard.SequenceNumberRange != nil {
				if shard.SequenceNumberRange.EndingSequenceNumber == nil {
					shardList = append(shardList, *shard.ShardId)
				}
			}
		}

		// Check if we need to get more shards
		if streamDesc.StreamDescription.HasMoreShards == nil || !*streamDesc.StreamDescription.HasMoreShards {
			break
		}

		// Get the last shard ID to use as the exclusive start key for the next call
		lastShard := streamDesc.StreamDescription.Shards[len(streamDesc.StreamDescription.Shards)-1]
		nextToken = lastShard.ShardId
	}

	return shardList, nil
}

// isShardClosed checks if a shard is closed based on the shard information
func isShardClosed(shard *types.Shard) bool {
	if shard.SequenceNumberRange == nil {
		return false
	}
	return shard.SequenceNumberRange.EndingSequenceNumber != nil
}

func isRecoverable(err error) bool {
	switch err.(type) {
	case *types.ExpiredIteratorException:
		return true
	case *types.ProvisionedThroughputExceededException:
		return true
	case *types.KMSThrottlingException:
		return true
	}
	return false
}

func shardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil ||
		(currentShardIterator != nil && nextShardIterator != nil && *currentShardIterator == *nextShardIterator)
}

// min returns the smaller of x or y.
func min(x, y float64) float64 {
	if x < y {
		return x
	}
	return y
}

// withRetry executes the given function with exponential backoff retries
// when encountering throttling or other temporary errors
func (c *Consumer) withRetry(ctx context.Context, operation string, fn func() error) error {
	var err error
	backoff := 50 * time.Millisecond
	maxBackoff := 5 * time.Second
	retries := 0
	maxRetries := 5

	for retries < maxRetries {
		err = fn()
		if err == nil {
			return nil
		}

		// Check if error is recoverable
		if !isRecoverable(err) {
			return err
		}

		// Log retry attempt
		c.logger.WithFields(logrus.Fields{
			"operation": operation,
			"error":     err.Error(),
			"retry":     retries + 1,
			"backoff":   backoff,
		}).Warn("temporary error, retrying operation")

		// Wait before retrying
		select {
		case <-time.After(backoff):
			backoff = time.Duration(min(float64(backoff)*2, float64(maxBackoff)))
			retries++
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("operation %s failed after %d retries: %w", operation, maxRetries, err)
}
