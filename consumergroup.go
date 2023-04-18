package go_kinesis

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"sync"
	"time"
)

type ConsumerGroup struct {
	*Consumer
	activeShards int
	mu           *sync.Mutex
}

func NewConsumerGroup(client *kinesis.Client, streamName string, opts ...Option) *ConsumerGroup {
	return &ConsumerGroup{
		Consumer:     NewConsumer(client, streamName, opts...),
		activeShards: 0,
		mu:           &sync.Mutex{},
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

func (cg *ConsumerGroup) listShards(ctx context.Context) ([]string, error) {
	var shardList []string

	shards, err := listShards(ctx, cg.client, cg.streamName)
	if err != nil {
		return shardList, err
	}

	for i := 0; i < len(shards); i++ {
		shardList = append(shardList, *shards[i].ShardId)
	}
	return shardList, nil
}

func (cg *ConsumerGroup) dostuff(ctx context.Context, g *errgroup.Group, fn ScanFunc) error {
	shards, err := cg.listShards(ctx)
	if err != nil {
		return err
	}

	for i := 0; i < len(shards); i++ {
		if i >= cg.shardLimit {
			break
		}

		shardID, err := cg.store.PollForAvailableShard(ctx, shards)
		if err == sql.ErrNoRows {
			continue
		}

		if err != nil {
			return err
		}
		cg.consume(ctx, g, shardID, fn)
	}
	return nil
}

func (cg *ConsumerGroup) ScanAll(ctx context.Context, fn ScanFunc) error {
	if cg.store == nil {
		return fmt.Errorf("store is not initialized")
	}
	g, ctx := errgroup.WithContext(ctx)
	cg.dostuff(ctx, g, fn)

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (cg *ConsumerGroup) shardDiscovery(ctx context.Context, fn ScanFunc) {
	scanTicker := time.NewTicker(time.Second)
	defer scanTicker.Stop()

	for {
		// Wait for next scan
		select {
		case <-ctx.Done():
			return
		case <-scanTicker.C:
			cg.ScanAll(ctx, fn)
		}
	}
}

func (cg *ConsumerGroup) consume(ctx context.Context, g *errgroup.Group, shardID string, fn ScanFunc) {
	g.Go(func() error {
		defer func() {
			cg.Sub(1)
			if err := cg.store.ReleaseStream(shardID); err != nil {
				cg.logger.WithError(err).Error()
			} else {
				cg.logger.WithFields(logrus.Fields{"shard": shardID}).Info("shard released")
			}
		}()

		cg.Add(1)
		return cg.ScanShard(ctx, shardID, fn)
	})
}

func (cg *ConsumerGroup) ScanShards(ctx context.Context, shardIDs []string, fn ScanFunc) error {
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < len(shardIDs); i++ {
		shardID := shardIDs[i]
		g.Go(func() error {
			return cg.ScanShard(ctx, shardID, fn)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}
