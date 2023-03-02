package ibmcloud

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/IBM/ibm-cos-sdk-go/aws/request"
	"github.com/IBM/ibm-cos-sdk-go/service/s3"
	"github.com/IBM/ibm-cos-sdk-go/service/s3/s3iface"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/loki/pkg/storage/chunk/client/hedging"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type RoundTripperFunc func(*http.Request) (*http.Response, error)

func (f RoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func Test_COSConfig(t *testing.T) {
	tests := []struct {
		name          string
		cosConfig     COSConfig
		expectedError error
	}{
		{
			"empty accessKeyID and secretAccessKey",
			COSConfig{
				BucketNames: "test",
				Endpoint:    "test",
				Region:      "dummy",
				AccessKeyID: "dummy",
			},
			errors.Wrap(errInvalidCOSHMACCredentials, "failed to build cos config"),
		},
	}
	for _, tt := range tests {
		_, err := NewCOSObjectClient(tt.cosConfig, hedging.Config{})
		require.Equal(t, tt.expectedError.Error(), err.Error())
	}
}

func Test_Hedging(t *testing.T) {
	for _, tc := range []struct {
		name          string
		expectedCalls int32
		hedgeAt       time.Duration
		upTo          int
		do            func(c *COSObjectClient)
	}{
		{
			"delete/put/list are not hedged",
			1,
			20 * time.Nanosecond,
			10,
			func(c *COSObjectClient) {
				// _ = c.DeleteObject(context.Background(), "foo")
				// _, _, _ = c.List(context.Background(), "foo", "/")
				_ = c.PutObject(context.Background(), "foo", bytes.NewReader([]byte("bar")))
			},
		},
		{
			"gets are hedged",
			3,
			20 * time.Nanosecond,
			3,
			func(c *COSObjectClient) {
				_, _, _ = c.GetObject(context.Background(), "foo")
			},
		},
		{
			"gets are not hedged when not configured",
			1,
			0,
			0,
			func(c *COSObjectClient) {
				_, _, _ = c.GetObject(context.Background(), "foo")
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			count := atomic.NewInt32(0)

			c, err := NewCOSObjectClient(COSConfig{
				AccessKeyID:     "foo",
				SecretAccessKey: flagext.SecretWithValue("bar"),
				BackoffConfig:   backoff.Config{MaxRetries: 1},
				BucketNames:     "foo",
				Inject: func(next http.RoundTripper) http.RoundTripper {
					return RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
						count.Inc()
						time.Sleep(200 * time.Millisecond)
						return nil, errors.New("foo")
					})
				},
			}, hedging.Config{
				At:           tc.hedgeAt,
				UpTo:         tc.upTo,
				MaxPerSecond: 1000,
			})
			require.NoError(t, err)
			tc.do(c)
			require.Equal(t, tc.expectedCalls, count.Load())
		})
	}
}

type mockS3Client struct {
	s3iface.S3API
	data   map[string][]byte
	bucket string
}

var (
	bucket   = "test"
	testData = map[string][]byte{
		"key-1": []byte("test data 1"),
		"key-2": []byte("test data 2"),
		"key-3": []byte("test data 3"),
	}
	errMissingBucket = errors.New("bucket not found")
	errMissingKey    = errors.New("key not found")
	errMissingObject = errors.New("Object data not found")
)

func newMockS3Client() *mockS3Client {
	return &mockS3Client{
		data:   testData,
		bucket: bucket,
	}
}

func (s3Client *mockS3Client) GetObjectWithContext(ctx context.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if *input.Bucket != s3Client.bucket {
		return &s3.GetObjectOutput{}, errMissingBucket
	}
	data, ok := s3Client.data[*input.Key]
	if !ok {
		return &s3.GetObjectOutput{}, errMissingKey
	}
	contentLength := int64(len(data))
	body := ioutil.NopCloser(bytes.NewReader(data))
	output := s3.GetObjectOutput{
		Body:          body,
		ContentLength: &contentLength,
	}

	return &output, nil
}

func Test_GetObject(t *testing.T) {
	tests := []struct {
		key       string
		wantBytes []byte
		wantErr   error
	}{
		{
			"key-1",
			[]byte("test data 1"),
			nil,
		},
		{
			"key-0",
			nil,
			errors.Wrap(errMissingKey, "failed to get cos object"),
		},
	}

	for _, tt := range tests {
		cosConfig := COSConfig{
			BucketNames:     bucket,
			Endpoint:        "test",
			Region:          "dummy",
			AccessKeyID:     "dummy",
			SecretAccessKey: flagext.SecretWithValue("dummy"),
			BackoffConfig: backoff.Config{
				MaxRetries: 1,
			},
		}
		cosClient, err := NewCOSObjectClient(cosConfig, hedging.Config{})
		require.NoError(t, err)
		cosClient.hedgedS3 = newMockS3Client()
		reader, _, err := cosClient.GetObject(context.Background(), tt.key)
		if tt.wantErr != nil {
			require.Equal(t, tt.wantErr.Error(), err.Error())
			continue
		}
		data, err := ioutil.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, tt.wantBytes, data)
	}
}

func Test_PutObject(t *testing.T) {
	tests := []struct {
		key       string
		Body      []byte
		wantBytes []byte
		wantErr   error
	}{
		{
			"key-5",
			[]byte("test data 5"),
			[]byte("test data 5"),
			nil,
		},
	}

	for _, tt := range tests {
		cosConfig := COSConfig{
			BucketNames:     bucket,
			Endpoint:        "test",
			Region:          "dummy",
			AccessKeyID:     "dummy",
			SecretAccessKey: flagext.SecretWithValue("dummy"),
			BackoffConfig: backoff.Config{
				MaxRetries: 1,
			},
		}
		cosClient, err := NewCOSObjectClient(cosConfig, hedging.Config{})
		require.NoError(t, err)
		cosClient.cos = newMockS3Client()
		body := bytes.NewReader(tt.Body)
		err = cosClient.PutObject(context.Background(), tt.key, body)
		if tt.wantErr != nil {
			require.Equal(t, tt.wantErr.Error(), err.Error())
			continue
		}
		require.NoError(t, err)
		cosClient.hedgedS3 = newMockS3Client()
		reader, _, err := cosClient.GetObject(context.Background(), tt.key)
		if tt.wantErr != nil {
			require.Equal(t, tt.wantErr.Error(), err.Error())
			continue
		}
		data, err := ioutil.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, tt.Body, data)
	}
}

func (s3Client *mockS3Client) PutObjectWithContext(ctx context.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	if *input.Bucket != s3Client.bucket {
		return &s3.PutObjectOutput{}, errMissingBucket
	}
	dataBytes, err := ioutil.ReadAll(input.Body)
	if err != nil {
		return &s3.PutObjectOutput{}, errMissingObject
	}
	if string(dataBytes) == "" {
		return &s3.PutObjectOutput{}, errMissingObject
	}
	_, ok := s3Client.data[*input.Key]
	if !ok {
		s3Client.data[*input.Key] = dataBytes
	}
	return nil, nil
}
