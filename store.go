package go_kinesis

import (
	"context"
	_ "github.com/jackc/pgx/stdlib"
)

type Store interface {
	HealthCheck(ctx context.Context) error
	ReleaseStream(shardID string) error
	PollForAvailableShard(ctx context.Context, shards []string) (string, error)
	FindFreeShard(shards []string) (string, error)
	SyncShards(shards []string) error
}
