package go_kinesis

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/stretchr/testify/assert"
	"os"
	"os/signal"
	"syscall"
	"testing"
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

	cfg2, _ := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(endpointResolver),
	)
	var client = kinesis.NewFromConfig(cfg2)

	t.Run("test with store", func(t *testing.T) {
		pg, err := NewPostgresStore("host=localhost port=5432 user=gokinesis password=1234 dbname=gokinesis sslmode=disable")
		if !assert.NoError(t, err) {
			return
		}
		NewConsumer(client, "", WithStore(pg))
	})

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
