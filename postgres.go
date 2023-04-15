package go_kinesis

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Postgres struct {
	DbReader *sqlx.DB
}

func NewPostgresStore(dsn string) (*Postgres, error) {
	var s Postgres
	var dbReader *sqlx.DB
	var errReader error

	dbReader, errReader = sqlx.Connect("pgx", dsn)
	if errReader != nil {
		return &s, errReader
	}

	dbReader.SetMaxOpenConns(32)

	s.DbReader = dbReader
	//s.log = log
	return &s, nil
}

func (s *Postgres) HealthCheck(ctx context.Context) error {
	return s.DbReader.PingContext(ctx)
}

func (s *Postgres) ReleaseStream(shardID string) error {
	//s.log.WithFields(logrus.Fields{"shard": shardID}).Info("releasing stream")
	_, err := s.DbReader.Exec("update kinesis_shards set in_use = false,last_updated=now() where shard_id=$1", shardID)
	return err
}

func (s *Postgres) findAvailableShard(shards []string) (string, error) {
	var shardID string
	tx, err := s.DbReader.Beginx()
	query, args, err := sqlx.In("select shard_id from kinesis_shards where in_use is false and shard_id in (?) limit 1 for update;", shards)
	if err != nil {
		tx.Rollback()
		return shardID, err
	}

	query = tx.Rebind(query)
	err = tx.Get(&shardID, query, args...)
	if err != nil {
		tx.Rollback()
		return shardID, err
	}
	_, err = tx.Exec("update kinesis_shards set in_use = true,last_updated=now() where shard_id=$1", shardID)

	if err != nil {
		tx.Rollback()
		return shardID, err
	}

	var temp bool

	err = tx.Get(&temp, "select in_use from kinesis_shards where shard_id = $1", shardID)

	//s.log.WithFields(logrus.Fields{"shard": shardID, "inUse": temp}).Info("claiming shard")
	tx.Commit()
	return shardID, nil
}

func (s *Postgres) PollForShard(shards []string) (string, error) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	// Create a ticker to trigger every second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	stopTimer := time.NewTimer(30 * time.Second)

	// Loop until the stopTimer triggers or a condition is met
	for {
		select {
		case <-ticker.C:
			shard, err := s.findAvailableShard(shards)
			if err == nil {
				return shard, nil
			}
			//s.log.Info("polling for available shard")
		case <-stopTimer.C:
			return "", fmt.Errorf("could not get a free shard after 5 minutes")
		case <-sigs:
			return "", fmt.Errorf("polling interupted")
		}
	}
}
