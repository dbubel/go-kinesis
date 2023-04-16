package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/dbubel/go-kinesis"
	"github.com/sirupsen/logrus"
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

	pg, _ := go_kinesis.NewPostgresStore("host=localhost port=5432 user=cohesion_content password=1234 dbname=cohesion_content sslmode=disable")
	_ = pg
	//if err != nil {
	//	fmt.Println(err.Error())
	//}

	var client = kinesis.NewFromConfig(cfg)
	c := go_kinesis.NewConsumerGroup(
		client,
		"test_stream",
		go_kinesis.WithTimestamp(time.Now().Add(-time.Second*5)),
		go_kinesis.WithShardIteratorType("AT_TIMESTAMP"),
		go_kinesis.WithStore(pg),
		go_kinesis.WithShardLimit(1),
	)

	l := logrus.New()
	l.SetLevel(logrus.DebugLevel)

	err := c.Scan(cancelScan(), func(record *go_kinesis.Record) error {
		time.Sleep(time.Second)
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
