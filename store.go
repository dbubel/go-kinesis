package go_kinesis

import (
	"context"
)

type Store interface {
	HealthCheck(ctx context.Context) error
	ReleaseStream(shardID string) error
	PollForAvailableShard(shards []string) (string, error)
	FindFreeShard(shards []string) (string, error)
	SyncShards(shards []string) error
}
