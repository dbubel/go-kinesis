package consumer

import (
	"context"
	"fmt"
	"github.com/alitto/pond"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/sirupsen/logrus"
	"time"
)

type TimeFormatter struct {
	logrus.Formatter
}

func (u TimeFormatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Time = e.Time.In(time.Local)
	return u.Formatter.Format(e)
}

type Consumer struct {
	streamName               string
	initialShardIteratorType types.ShardIteratorType
	initialTimestamp         *time.Time
	client                   *kinesis.Client
	logger                   *logrus.Logger
	scanInterval             time.Duration
	maxRecords               int64
}

func NewConsumer(client *kinesis.Client, streamName string, opts ...Option) *Consumer {
	c := &Consumer{
		streamName:               streamName,
		initialShardIteratorType: types.ShardIteratorTypeLatest,
		client:                   client,
		logger:                   logrus.New(),
		scanInterval:             250 * time.Millisecond,
		maxRecords:               10000,
	}

	// override defaults
	for _, opt := range opts {
		opt(c)
	}
	shards, _ := listShards(context.TODO(), client, streamName)
	for _, s := range shards {
		fmt.Println(*s.ShardId)
	}

	fmt.Println(listShards(context.TODO(), client, streamName))
	return c
}

// listShards pulls a list of Shard IDs from the kinesis api
func listShards(ctx context.Context, kc *kinesis.Client, streamName string) ([]types.Shard, error) {
	var ss []types.Shard
	var listShardsInput = &kinesis.ListShardsInput{
		StreamName: aws.String(streamName),
	}

	for {
		resp, err := kc.ListShards(ctx, listShardsInput)
		if err != nil {
			return nil, fmt.Errorf("ListShards error: %w", err)
		}
		ss = append(ss, resp.Shards...)

		if resp.NextToken == nil {
			return ss, nil
		}

		listShardsInput = &kinesis.ListShardsInput{
			NextToken: resp.NextToken,
		}
	}
}

func (c *Consumer) ScanShardAsync(ctx context.Context, shardID string, concurrency, poolsize int, fn ScanFunc) error {
	pool := pond.New(concurrency, poolsize)
	return c.ScanShard(ctx, shardID, func(record *Record) error {
		pool.Submit(func() {
			err := fn(record)
			if err != nil {
				fmt.Println(err.Error())
			}
		})
		return nil
	})
}

func (c *Consumer) ScanShards(ctx context.Context, shardIDs []string, fn ScanFunc) {
	for i := 0; i < len(shardIDs); i++ {
		shardID := shardIDs[i]
		err := c.ScanShard(ctx, shardID, fn)
		if err != nil {
			fmt.Println(err.Error())
		}
	}
}

func (c *Consumer) ScanShardsAsync(ctx context.Context, shardIDs []string, concurrency, poolsize int, fn ScanFunc) {
	for _, shard := range shardIDs {
		shardID := shard
		c.ScanShardAsync(ctx, shardID, concurrency, poolsize, fn)
	}
}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) error {
	// get last seq number from checkpoint
	//lastSeqNum, err := c.group.GetCheckpoint(c.streamName, shardID)
	//if err != nil {
	//	return fmt.Errorf("get checkpoint error: %w", err)
	//}
	fmt.Println("scan shard shard id", shardID)
	lastSeqNum := ""

	// get shard iterator
	out, err := c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
	if err != nil {
		return fmt.Errorf("get shard iterator error: %w", err)
	}
	shardIterator := out.ShardIterator

	c.logger.Debug("[CONSUMER] start scan:", shardID, lastSeqNum)
	defer func() {
		c.logger.Debug("[CONSUMER] stop scan:", shardID)
	}()

	scanTicker := time.NewTicker(c.scanInterval)
	defer scanTicker.Stop()

	for {
		resp, err := c.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			Limit:         aws.Int32(int32(c.maxRecords)),
			ShardIterator: shardIterator,
		})

		// attempt to recover from GetRecords error
		if err != nil {
			c.logger.Debug("[CONSUMER] get records error:", err.Error())

			if !isRecoverable(err) {
				return fmt.Errorf("get records error: %v", err.Error())
			}

			out, err = c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
			if err != nil {
				return fmt.Errorf("get shard iterator error: %w", err)
			}
			shardIterator = out.ShardIterator
		} else {
			// loop over records, call callback func
			var records []types.Record

			records = resp.Records

			for _, r := range records {
				select {
				case <-ctx.Done():
					return nil
				default:
					err := fn(&Record{r, shardID, resp.MillisBehindLatest})
					if err != nil && err != ErrSkipCheckpoint {
						return err
					}
					lastSeqNum = *r.SequenceNumber
				}
			}

			if shardClosed(resp.NextShardIterator, shardIterator) {
				c.logger.Debug("[CONSUMER] shard closed:", shardID)
				return nil
			}
			shardIterator = resp.NextShardIterator
		}

		// Wait for next scan
		select {
		case <-ctx.Done():
			return nil
		case <-scanTicker.C:
			continue
		}
	}
}

var ErrSkipCheckpoint = fmt.Errorf("skip checkpoint")

func (c *Consumer) getShardIterator(ctx context.Context, streamName, shardID, seqNum string) (*kinesis.GetShardIteratorOutput, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(streamName),
	}

	if seqNum != "" {
		params.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		params.StartingSequenceNumber = aws.String(seqNum)
	} else if c.initialTimestamp != nil {
		params.ShardIteratorType = types.ShardIteratorTypeAtTimestamp
		params.Timestamp = c.initialTimestamp
	} else {
		params.ShardIteratorType = c.initialShardIteratorType
	}

	return c.client.GetShardIterator(ctx, params)

	//return res, err
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
