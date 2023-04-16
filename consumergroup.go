package go_kinesis

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
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

func (cg *ConsumerGroup) Scan(ctx context.Context, fn ScanFunc) error {
	scanTicker := time.NewTicker(time.Second)
	defer scanTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-scanTicker.C:
			cg.Scan(ctx, fn)
		}
	}
}

func (cg *ConsumerGroup) ScanWrapper(ctx context.Context, fn ScanFunc) error {
	if cg.store == nil {
		return fmt.Errorf("store is not initialized")
	}

	shards, err := listShards(ctx, cg.client, cg.streamName)
	if err != nil {
		return err
	}

	var shardList []string
	for i := 0; i < len(shards); i++ {
		shardList = append(shardList, *shards[i].ShardId)
	}

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < len(shards); i++ {
		if i >= cg.shardLimit {
			break
		}

		shardID, err := cg.store.PollForAvailableShard(shardList)
		if err == sql.ErrNoRows {
			continue
		}

		if err != nil {
			return err
		}

		g.Go(func() error {
			defer func() {
				cg.Sub(1)
				if err := cg.store.ReleaseStream(shardID); err != nil {
					cg.logger.WithError(err).Error()
				}
			}()

			cg.Add(1)
			return cg.ScanShard(ctx, shardID, fn)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
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
