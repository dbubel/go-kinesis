package go_kinesis

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/sirupsen/logrus"
)

//type TimeFormatter struct {
//	logrus.Formatter
//}
//
//func (u TimeFormatter) Format(e *logrus.Entry) ([]byte, error) {
//	e.Time = e.Time.In(time.Local)
//	return u.Formatter.Format(e)
//}

type Consumer struct {
	streamName               string
	initialShardIteratorType types.ShardIteratorType
	initialTimestamp         *time.Time
	client                   *kinesis.Client
	logger                   *logrus.Logger
	scanInterval             time.Duration
	maxRecords               int64
	store                    Store
	shardLimit               int
}

func NewConsumer(client *kinesis.Client, streamName string, opts ...Option) *Consumer {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	c := &Consumer{
		streamName:               streamName,
		initialShardIteratorType: types.ShardIteratorTypeLatest,
		client:                   client,
		logger:                   log,
		scanInterval:             250 * time.Millisecond,
		maxRecords:               10000,
		shardLimit:               10000,
	}

	// override defaults
	for _, opt := range opts {
		opt(c)
	}

	return c
}

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

	for i := 0; i < len(streamDesc.StreamDescription.Shards); i++ {
		shardList = append(shardList, *streamDesc.StreamDescription.Shards[i].ShardId)
	}
	return shardList, nil
}

//func (c *Consumer) ScanShardAsync(ctx context.Context, shardID string, concurrency, poolsize int, fn ScanFunc) error {
//	pool := pond.New(concurrency, poolsize)
//	return c.ScanShard(ctx, shardID, func(record *Record) error {
//		pool.Submit(func() {
//			err := fn(record)
//			if err != nil {
//				fmt.Println(err.Error())
//			}
//		})
//		return nil
//	})
//}

//func (c *Consumer) ScanShards(ctx context.Context, shardIDs []string, fn ScanFunc) {
//	for i := 0; i < len(shardIDs); i++ {
//		shardID := shardIDs[i]
//		err := c.ScanShard(ctx, shardID, fn)
//		if err != nil {
//			c.logger.WithError(err).Error("error in ScanShards")
//		}
//	}
//}

//func (c *Consumer) ScanShardsAsync(ctx context.Context, shardIDs []string, concurrency, poolsize int, fn ScanFunc) {
//	for _, shard := range shardIDs {
//		shardID := shard
//		c.ScanShardAsync(ctx, shardID, concurrency, poolsize, fn)
//	}
//}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) {

	defer func() {

		if err := recover(); err != nil {
			c.logger.Error(string(debug.Stack()))
		}
	}()

	lastSeqNum := ""

	// get shard iterator
	out, err := c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
	if err != nil {
		c.logger.WithError(err).Error("get shard iterator error")
		return
	}
	shardIterator := out.ShardIterator

	c.logger.WithFields(logrus.Fields{
		"shardId":    shardID,
		"lastSeqNum": lastSeqNum,
	}).Info("start scan")
	defer func() {
		c.logger.WithFields(logrus.Fields{
			"shardId":    shardID,
			"lastSeqNum": lastSeqNum,
		}).Info("stop scan")
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
			if !isRecoverable(err) {
				c.logger.WithError(err).Error("get records error")
				return
			}

			out, err = c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
			if err != nil {
				c.logger.WithError(err).Error("get shard iterator error")
				return
			}
			shardIterator = out.ShardIterator
		} else {
			for _, r := range resp.Records {
				select {
				case <-ctx.Done():
					c.logger.WithError(err).Error("ScanShard context cancelled during record scan")
					return
				default:
					err := fn(&Record{r, shardID, resp.MillisBehindLatest})
					if err != nil {
						c.logger.WithError(err).Error("error in scan func")
						return
					}
					lastSeqNum = *r.SequenceNumber
				}
			}

			if shardClosed(resp.NextShardIterator, shardIterator) {
				c.logger.WithFields(logrus.Fields{
					"shardId":    shardID,
					"lastSeqNum": lastSeqNum,
				}).Info("shard closed")
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

//var ErrSkipCheckpoint = fmt.Errorf("skip checkpoint")

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
