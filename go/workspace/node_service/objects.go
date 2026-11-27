package node_service

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// deleteBatchSize is the most keys one S3 DeleteObjects takes.
const deleteBatchSize = 1000

// object is a stored object's key and size.
type object struct {
	Key  string
	Size int64
}

// objects is the object storage holding the snapshots.
type objects interface {
	// List returns every object under prefix.
	List(ctx context.Context, prefix string) ([]object, error)
	// Put uploads a file's content to key.
	Put(ctx context.Context, key string, file *os.File) error
	// Get opens key's content.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// Delete deletes keys; a missing key is not an error.
	Delete(ctx context.Context, keys []string) error
}

// s3Objects keeps the snapshots in one S3 bucket.
type s3Objects struct {
	client *s3.Client
	bucket string
}

// List implements objects.
func (o *s3Objects) List(ctx context.Context, prefix string) ([]object, error) {
	listObjectsV2Input := &s3.ListObjectsV2Input{Bucket: aws.String(o.bucket), Prefix: aws.String(prefix)}
	paginator := s3.NewListObjectsV2Paginator(o.client, listObjectsV2Input)
	var listed []object
	for paginator.HasMorePages() {
		listObjectsV2Output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing %s: %w", prefix, err)
		}
		for _, s3Object := range listObjectsV2Output.Contents {
			listed = append(listed, object{Key: aws.ToString(s3Object.Key), Size: aws.ToInt64(s3Object.Size)})
		}
	}
	return listed, nil
}

// Put implements objects.
func (o *s3Objects) Put(ctx context.Context, key string, file *os.File) error {
	putObjectInput := &s3.PutObjectInput{Bucket: aws.String(o.bucket), Key: aws.String(key), Body: file}
	if _, err := o.client.PutObject(ctx, putObjectInput); err != nil {
		return fmt.Errorf("putting %s: %w", key, err)
	}
	return nil
}

// Get implements objects.
func (o *s3Objects) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	getObjectInput := &s3.GetObjectInput{Bucket: aws.String(o.bucket), Key: aws.String(key)}
	getObjectOutput, err := o.client.GetObject(ctx, getObjectInput)
	if err != nil {
		return nil, fmt.Errorf("getting %s: %w", key, err)
	}
	return getObjectOutput.Body, nil
}

// Delete implements objects.
func (o *s3Objects) Delete(ctx context.Context, keys []string) error {
	for start := 0; start < len(keys); start += deleteBatchSize {
		end := min(start+deleteBatchSize, len(keys))
		objectIdentifiers := make([]s3types.ObjectIdentifier, 0, end-start)
		for _, key := range keys[start:end] {
			objectIdentifiers = append(objectIdentifiers, s3types.ObjectIdentifier{Key: aws.String(key)})
		}
		deleteObjectsInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(o.bucket),
			Delete: &s3types.Delete{Objects: objectIdentifiers, Quiet: aws.Bool(true)},
		}
		deleteObjectsOutput, err := o.client.DeleteObjects(ctx, deleteObjectsInput)
		if err != nil {
			return fmt.Errorf("deleting objects: %w", err)
		}
		if len(deleteObjectsOutput.Errors) > 0 {
			deleteError := deleteObjectsOutput.Errors[0]
			return fmt.Errorf("deleting %s: %s", aws.ToString(deleteError.Key), aws.ToString(deleteError.Message))
		}
	}
	return nil
}
