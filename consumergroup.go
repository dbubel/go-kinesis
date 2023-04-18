package go_kinesis

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/sirupsen/logrus"
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
			cg.ScanAll(ctx, fn)
			time.Sleep(time.Second)
			return nil
		}
	}
	return nil
}

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

		shardID, err := cg.store.PollForAvailableShard(ctx, shards)

		if err != nil && err != sql.ErrNoRows {
			return err
		}

		go cg.consume(ctx, shardID, fn)
	}

	cg.wg.Wait()
	return nil
}

func (cg *ConsumerGroup) consume(ctx context.Context, shardID string, fn ScanFunc) {
	defer func() {
		if err := cg.store.ReleaseStream(shardID); err != nil {
			cg.logger.WithError(err).Error("error releasing shard")
		} else {
			cg.logger.WithFields(logrus.Fields{"shard": shardID}).Info("shard released")
		}
		cg.Sub(1)
		cg.wg.Done()
	}()

	cg.Add(1)
	cg.wg.Add(1)
	cg.ScanShard(ctx, shardID, fn)
}

//
//func (cg *ConsumerGroup) shardDiscovery(ctx context.Context, fn ScanFunc) {
//	scanTicker := time.NewTicker(time.Second)
//	defer scanTicker.Stop()
//
//	for {
//		// Wait for next scan
//		select {
//		case <-ctx.Done():
//			return
//		case <-scanTicker.C:
//			cg.ScanAll(ctx, fn)
//		}
//	}
//}

//
//func (cg *ConsumerGroup) ScanShards(ctx context.Context, shardIDs []string, fn ScanFunc) error {
//	g, ctx := errgroup.WithContext(ctx)
//	for i := 0; i < len(shardIDs); i++ {
//		shardID := shardIDs[i]
//		g.Go(func() error {
//			return cg.ScanShard(ctx, shardID, fn)
//		})
//	}
//
//	if err := g.Wait(); err != nil {
//		return err
//	}
//
//	return nil
//}
