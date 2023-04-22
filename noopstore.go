package go_kinesis

import (
	"context"
	"time"
)

// noopStore is the default implementation of store if none is provided
// on creation. Just provides a pass through if you dont wish to have
// persistent storage of last sequence position.
type noopStore struct{}

func (n noopStore) ReleaseStream(shardID string) error {
	return nil
}

func (n noopStore) PollForAvailableShard(ctx context.Context, t time.Duration, shards []string) (string, error) {
	return "", nil
}

func (n noopStore) FindFreeShard(shards []string) (string, error) {
	return "", nil
}

func (n noopStore) SyncShards(shards []string) error {
	return nil
}

func (n noopStore) SetLastSeq(shardID, lastSeq string) error {
	return nil
}

func (n noopStore) GetLastSeq(shardID string) (string, error) {
	return "", nil
}

func (n noopStore) HealthCheck(ctx context.Context, fn context.CancelFunc) error {
	return nil
}
