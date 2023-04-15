package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
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

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithEndpointResolverWithOptions(endpointResolver),
	)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	var client = kinesis.NewFromConfig(cfg)

	hash := md5.Sum([]byte("hello"))
	hashString := hex.EncodeToString(hash[:])

	for i := 0; i < 10000; i++ {
		s := fmt.Sprintf("hello %d", i)
		client.PutRecord(context.TODO(), &kinesis.PutRecordInput{
			Data:         []byte(s),
			PartitionKey: aws.String(hashString),
			StreamName:   aws.String("test_stream"),
		})
		fmt.Println("tick")
		if err != nil {
			fmt.Println(err.Error())
		}

		time.Sleep(time.Millisecond * 500)
	}
}
