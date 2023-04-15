package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/dbubel/go-kinesis/consumer"
	"github.com/dbubel/go-kinesis/store"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	endpointResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		endpoint := aws.Endpoint{
			PartitionID:   "aws",
			SigningRegion: "us-east-1",
		}
		endpoint.URL = "http://localhost:4566"
		return endpoint, nil
	})

	cfg, _ := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(endpointResolver),
	)

	pg, err := store.NewPostgresStore("host=localhost port=5432 user=cohesion_content password=1234 dbname=cohesion_content sslmode=disable")
	_ = pg
	if err != nil {
		fmt.Println(err.Error())
	}

	var client = kinesis.NewFromConfig(cfg)
	c := consumer.NewConsumer(
		client,
		"test_stream",
		consumer.WithTimestamp(time.Now().Add(-time.Second*5)),
		consumer.WithShardIteratorType("AT_TIMESTAMP"),
	)

	c.ScanShards(cancelScan(), []string{"shardId-000000000000", "shardId-000000000001"}, func(record *consumer.Record) error {
		fmt.Println(string(record.ShardID), string(record.Data))
		time.Sleep(time.Second)
		return nil
	})

	if err != nil {
		fmt.Println(err.Error())
	}
}

func cancelScan() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		<-sigs
		cancel()
	}()

	return ctx
}
