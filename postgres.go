package go_kinesis

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

// Common errors
var (
	// ErrShardNotFound is already defined in the package
	ErrDBConnection = errors.New("database connection error")
	ErrQueryFailed  = errors.New("database query failed")
)

// Postgres represents a PostgreSQL database connection
type Postgres struct {
	DbReader *sqlx.DB
	log      *logrus.Logger
	timeout  time.Duration
}

// NewPostgresStore creates a new Postgres store with the given connection string and logger
func NewPostgresStore(dsn string, log *logrus.Logger) (*Postgres, error) {
	if dsn == "" {
		return nil, errors.New("database connection string cannot be empty")
	}

	if log == nil {
		log = logrus.New()
	}

	var s Postgres
	var dbReader *sqlx.DB
	var errReader error

	dbReader, errReader = sqlx.Connect("pgx", dsn)
	if errReader != nil {
		return nil, fmt.Errorf("%w: %v", ErrDBConnection, errReader)
	}

	dbReader.SetMaxOpenConns(32)
	dbReader.SetMaxIdleConns(8)
	dbReader.SetConnMaxLifetime(30 * time.Minute)

	s.DbReader = dbReader
	s.log = log
	s.timeout = 5 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	if err := s.HealthCheck(ctx, cancel); err != nil {
		return nil, fmt.Errorf("health check failed: %w", err)
	}

	return &s, nil
}

// HealthCheck verifies the database connection is working
func (s *Postgres) HealthCheck(ctx context.Context, fn context.CancelFunc) error {
	if fn != nil {
		defer fn()
	}
	return s.DbReader.PingContext(ctx)
}

// ReleaseStream marks a shard as no longer in use
func (s *Postgres) ReleaseStream(shardID string) error {
	if shardID == "" {
		return errors.New("shardID cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	_, err := s.DbReader.ExecContext(ctx,
		"UPDATE kinesis_shards SET in_use = false, last_updated = now() WHERE shard_id = $1",
		shardID)

	if err != nil {
		return fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}
	return nil
}

// SyncShards ensures all provided shards exist in the database
func (s *Postgres) SyncShards(shards []string) error {
	if len(shards) == 0 {
		return errors.New("shards list cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	tx, err := s.DbReader.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	for _, shardID := range shards {
		if shardID == "" {
			continue
		}

		_, err := tx.ExecContext(ctx,
			"INSERT INTO kinesis_shards (shard_id) VALUES ($1) ON CONFLICT (shard_id) DO NOTHING",
			shardID)

		if err != nil {
			pgErr, ok := err.(pgx.PgError)
			if !ok || pgErr.Code != "23505" { // Ignore duplicate key errors
				return fmt.Errorf("failed to insert shard %s: %w", shardID, err)
			}
		}
	}

	return tx.Commit()
}

// FindFreeShard locates an available shard from the provided list
func (s *Postgres) FindFreeShard(shards []string) (string, error) {
	if len(shards) == 0 {
		return "", errors.New("shards list cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	var shardID string
	tx, err := s.DbReader.BeginTxx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	query, args, err := sqlx.In("SELECT shard_id FROM kinesis_shards WHERE in_use = false AND shard_id IN (?) LIMIT 1 FOR UPDATE", shards)
	if err != nil {
		return "", fmt.Errorf("failed to prepare query: %w", err)
	}

	query = tx.Rebind(query)
	err = tx.GetContext(ctx, &shardID, query, args...)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrShardNotFound
		}
		return "", fmt.Errorf("failed to get free shard: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		"UPDATE kinesis_shards SET in_use = true, last_updated = now() WHERE shard_id = $1",
		shardID)

	if err != nil {
		return "", fmt.Errorf("failed to update shard status: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	return shardID, nil
}

// PollForAvailableShard attempts to find an available shard for a specified duration
func (s *Postgres) PollForAvailableShard(ctx context.Context, t time.Duration, shards []string) (string, error) {
	if len(shards) == 0 {
		return "", errors.New("shards list cannot be empty")
	}

	if t <= 0 {
		t = 10 * time.Second // Default timeout
	}

	if err := s.SyncShards(shards); err != nil {
		return "", fmt.Errorf("failed to sync shards: %w", err)
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	stopTimer := time.NewTimer(t)
	defer stopTimer.Stop()

	for {
		select {
		case <-ticker.C:
			shard, err := s.FindFreeShard(shards)
			if err == nil {
				return shard, nil
			}
			if !errors.Is(err, ErrShardNotFound) {
				return "", err
			}
			s.log.WithField("attempt", "poll").Info("polling for available shards")
		case <-ctx.Done():
			return "", ctx.Err()
		case <-stopTimer.C:
			return "", ErrShardNotFound
		}
	}
}

// SetLastSeq updates the last sequence number for a given shard
func (s *Postgres) SetLastSeq(shardID, lastSeq string) error {
	if shardID == "" {
		return errors.New("shardID cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	result, err := s.DbReader.ExecContext(ctx,
		"UPDATE kinesis_shards SET last_seq = $1, last_updated = now() WHERE shard_id = $2",
		lastSeq, shardID)

	if err != nil {
		return fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("shard %s not found", shardID)
	}

	return nil
}

// GetLastSeq retrieves the last sequence number for a given shard
func (s *Postgres) GetLastSeq(shardID string) (string, error) {
	if shardID == "" {
		return "", errors.New("shardID cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	var lastSeq string
	err := s.DbReader.GetContext(ctx, &lastSeq,
		"SELECT last_seq FROM kinesis_shards WHERE shard_id = $1",
		shardID)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("shard %s not found", shardID)
		}
		return "", fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}

	return lastSeq, nil
}

// Close closes the database connections
func (s *Postgres) Close() error {
	if s.DbReader != nil {
		return s.DbReader.Close()
	}
	return nil
}
