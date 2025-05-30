package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	go_kinesis "github.com/dbubel/go-kinesis"
	"github.com/sirupsen/logrus"
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

	l := logrus.New()
	l.SetLevel(logrus.DebugLevel)
	pg, err := go_kinesis.NewPostgresStore("host=localhost port=5432 user=gokinesis password=1234 dbname=gokinesis sslmode=disable", l)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	var client = kinesis.NewFromConfig(cfg)
	c, err := go_kinesis.NewConsumerGroup(
		client,
		"test_stream",
		go_kinesis.WithShardIteratorType(go_kinesis.Latest),
		go_kinesis.WithStore(pg),
		go_kinesis.WithShardLimit(10),
	)

	if err != nil {
		fmt.Println("Error creating consumer group:", err.Error())
		return
	}

	err = c.ScanAll(cancelScan(), func(record *go_kinesis.Record) error {
		l.WithFields(logrus.Fields{"shard": record.ShardID}).Debug(string(record.Data))
		return nil
	})

	if err != nil {
		fmt.Println(err.Error())
	}
}

func cancelScan() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)

	go func() {
		<-sigs
		cancel()
	}()

	return ctx
}
