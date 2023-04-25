package go_kinesis

import (
	"context"
	"time"

	_ "github.com/jackc/pgx/stdlib"
)

type Store interface {
	HealthCheck(ctx context.Context, fn context.CancelFunc) error
	ReleaseStream(shardID string) error
	PollForAvailableShard(ctx context.Context, t time.Duration, shards []string) (string, error)
	FindFreeShard(shards []string) (string, error)
	SyncShards(shards []string) error
	SetLastSeq(shardID, lastSeq string) error
	GetLastSeq(shardID string) (string, error)
}

// TODO: create an in memory store that syncs with db
