package go_kinesis

import "github.com/aws/aws-sdk-go-v2/service/kinesis/types"

// Record wraps the record returned from the Kinesis library and
// extends to include the shard id.
type Record struct {
	types.Record
	ShardID            string
	MillisBehindLatest *int64
}
