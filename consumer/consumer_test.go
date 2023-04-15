package consumer

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/stretchr/testify/assert"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

func TestConsumer_ScanShard(t *testing.T) {
	endpointResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		endpoint := aws.Endpoint{
			PartitionID:   "aws",
			SigningRegion: "us-east-1",
		}
		endpoint.URL = "http://localhost:4566"
		return endpoint, nil
	})

	cfg2, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(endpointResolver),
	)
	var client = kinesis.NewFromConfig(cfg2)

	if assert.NoError(t, err) {

		consumer := NewConsumer(
			"test_stream",
			client,
			WithTimestamp(time.Now().Add(-time.Second)),
		)

		submidtRecord(client)

		err = consumer.ScanShard(cancelScan(), "shardId-000000000000", "", func(record *Record) error {
			fmt.Println(string(record.Data))
			return nil
		})

		if err != nil {

		}
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

func submidtRecord(k *kinesis.Client) {

	hash := md5.Sum([]byte("hello"))
	hashString := hex.EncodeToString(hash[:])

	k.PutRecord(context.TODO(), &kinesis.PutRecordInput{
		Data:         []byte("hello"),
		PartitionKey: aws.String(hashString),
		StreamName:   aws.String("test_stream"),
	})
}

//type ContentUpdatedProcessor struct {
//	Kinesis *kinesis.Kinesis
//	Log     *logrus.Logger
//	Store   *store.Store
//	Cfg     config.Config
//}

//func NewMockKinesisProducer(log *logrus.Logger, kc *kinesis.Kinesis, store *store.Store, cfg config.Config) *ContentUpdatedProcessor {
//	return &ContentUpdatedProcessor{
//		Kinesis: kc,
//		Log:     log,
//		Store:   store,
//		Cfg:     cfg,
//	}
//}
