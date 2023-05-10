package go_kinesis

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/alitto/pond"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/sirupsen/logrus"
)

type IteratorTypeStrat string

// TODO: create more interator types
const (
	Latest IteratorTypeStrat = "LATEST"
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
func NewConsumer(client *kinesis.Client, streamName string, opts ...Option) *Consumer {
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

	// override defaults
	for _, opt := range opts {
		opt(c)
	}

	return c
}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan if a store is provided
// when creating a consumer or consumer group.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) {
	var lastSeqNum string
	var shardIterator *string
	//var err error

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

			iterator, err = c.getShardIterator(ctx, c.streamName, shardID)
			if err != nil {
				c.logger.WithError(err).Error("get shard iterator error")
				return
			}
			shardIterator = iterator.ShardIterator

		} else {
			for _, r := range resp.Records {
				err := fn(&Record{r, shardID, resp.MillisBehindLatest})
				if err != nil {
					c.logger.WithError(err).Error("error in scan func")
					return
				}

				err = c.store.SetLastSeq(shardID, *r.SequenceNumber)
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
		t := time.Now()
		select {
		case <-ctx.Done():
			return
		case <-scanTicker.C:
			c.logger.WithFields(logrus.Fields{"waitTime": time.Now().Sub(t).Milliseconds()}).Debug("time wait batch")
			continue
		}
	}
}

// ScanShardAsync experimental function for scanning a single shard and processing the items
// concurrently
func (c *Consumer) ScanShardAsync(ctx context.Context, shardID string, concurrency, poolsize int, fn ScanFunc) {
	pool := pond.New(concurrency, poolsize)
	c.ScanShard(ctx, shardID, func(record *Record) error {
		pool.Submit(func() {
			err := fn(record)
			if err != nil {
				fmt.Println(err.Error())
			}
		})
		return nil
	})
}

// ScanShards function used to scan a sclice of shardIDs and process the items with fn
func (c *Consumer) ScanShards(ctx context.Context, shardIDs []string, fn ScanFunc) {
	for i := 0; i < len(shardIDs); i++ {
		shardID := shardIDs[i]
		c.ScanShard(ctx, shardID, fn)
	}
}

// ScanShardsAsync experimental feature for scanning a list of shards async with concurrency
// workers and poolsize for size of the items in the pool
func (c *Consumer) ScanShardsAsync(ctx context.Context, shardIDs []string, concurrency, poolsize int, fn ScanFunc) {
	for _, shard := range shardIDs {
		shardID := shard
		c.ScanShardAsync(ctx, shardID, concurrency, poolsize, fn)
	}
}

func (c *Consumer) getShardIterator(ctx context.Context, streamName, shardID string) (*kinesis.GetShardIteratorOutput, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(streamName),
	}

	// TODO: implement more strategies
	if c.initialShardIteratorType == Latest {
		seqNum, err := c.store.GetLastSeq(shardID)
		if err != nil {
			return &kinesis.GetShardIteratorOutput{}, err
		}

		if seqNum == "" {
			params.ShardIteratorType = types.ShardIteratorTypeLatest
		} else {
			params.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
			params.StartingSequenceNumber = aws.String(seqNum)
		}
	}

	return c.client.GetShardIterator(ctx, params)
}

// listShards list shards that the stream has and returns ones that are in an active state and
// ready to be consumed from.
func (c *Consumer) listShards() ([]string, error) {
	var shardList []string

	streamDesc, err := c.client.DescribeStream(context.TODO(), &kinesis.DescribeStreamInput{
		StreamName: aws.String(c.streamName),
	})

	if err != nil {
		return nil, err
	}

	if streamDesc.StreamDescription.StreamStatus != types.StreamStatusActive {
		return nil, fmt.Errorf("stream status is not active")
	}

	if err != nil {
		return shardList, err
	}

	// figure out which shards we can read from
	for i := 0; i < len(streamDesc.StreamDescription.Shards); i++ {
		status := *streamDesc.StreamDescription.Shards[i].SequenceNumberRange
		if status.EndingSequenceNumber != nil {
			// TODO: identify new shards here
			//if err := c.store.MarkBadShard(*streamDesc.StreamDescription.Shards[i].ShardId); err != nil {
			//	return shardList, err
			//}
		} else {
			shardList = append(shardList, *streamDesc.StreamDescription.Shards[i].ShardId)
		}
	}
	return shardList, nil
}

func isRecoverable(err error) bool {
	switch err.(type) {
	case *types.ExpiredIteratorException:
		return true
	case *types.ProvisionedThroughputExceededException:
		return true
	}
	return false
}

func shardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}
