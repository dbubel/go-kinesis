package go_kinesis

import (
	"context"
	_ "github.com/jackc/pgx/stdlib"
)

type Store interface {
	HealthCheck(ctx context.Context) error
	ReleaseStream(shardID string) error
	findAvailableShard(shards []string) (string, error)
	PollForShard(shards []string) (string, error)
}
