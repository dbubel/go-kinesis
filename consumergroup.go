package go_kinesis

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type ConsumerGroup struct {
	*Consumer
	activeShards int
	mu           *sync.Mutex
	wg           *sync.WaitGroup
}

func NewConsumerGroup(client *kinesis.Client, streamName string, opts ...Option) *ConsumerGroup {
	return &ConsumerGroup{
		Consumer:     NewConsumer(client, streamName, opts...),
		activeShards: 0,
		mu:           &sync.Mutex{},
		wg:           &sync.WaitGroup{},
	}
}

func (c *ConsumerGroup) Add(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activeShards += n
}

func (c *ConsumerGroup) Sub(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activeShards -= n
}

func (cg *ConsumerGroup) Forever(ctx context.Context, fn ScanFunc) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			time.Sleep(time.Second)
			cg.ScanAll(ctx, fn)
		}
	}
}

var shardNotFound = fmt.Errorf("polling for shard timed out")

func (cg *ConsumerGroup) ScanAll(ctx context.Context, fn ScanFunc) error {
	if cg.store == nil {
		return fmt.Errorf("store is not initialized")
	}

	shards, err := cg.listShards()
	if err != nil {
		return err
	}

	cg.logger.WithFields(logrus.Fields{"shards": shards}).Info("shards on stream")

	for i := 0; i < len(shards); i++ {
		if i >= cg.shardLimit {
			break
		}

		shardID, err := cg.store.PollForAvailableShard(ctx, time.Second*60, shards)

		if errors.Is(err, shardNotFound) {

			continue
		}

		if err != nil && err != sql.ErrNoRows {
			return err
		}
		cg.wg.Add(1)
		go cg.consume(ctx, shardID, fn)
	}

	cg.wg.Wait()
	return nil
}

func (cg *ConsumerGroup) consume(ctx context.Context, shardID string, fn ScanFunc) {
	cg.Add(1)
	cg.ScanShard(ctx, shardID, fn) // this blocks while scanning

	if err := cg.store.ReleaseStream(shardID); err != nil {
		cg.logger.WithError(err).Error("error releasing shard")
	} else {
		cg.logger.WithFields(logrus.Fields{"shard": shardID}).Info("shard released")
	}

	cg.Sub(1)
	cg.wg.Done()
}
