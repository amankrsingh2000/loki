package ibmcloud

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

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
			errors.Wrap(errInvalidCOSHMACCredentials, errCOSConfig),
		},
		{
			"region is empty",
			COSConfig{
				BucketNames:     "test",
				Endpoint:        "test",
				Region:          "",
				AccessKeyID:     "dummy",
				SecretAccessKey: flagext.SecretWithValue("dummy"),
			},
			errors.Wrap(errEmptyRegion, errCOSConfig),
		},
		{
			"endpoint is empty",
			COSConfig{
				BucketNames:     "test",
				Endpoint:        "",
				Region:          "dummy",
				AccessKeyID:     "dummy",
				SecretAccessKey: flagext.SecretWithValue("dummy"),
			},
			errors.Wrap(errEmptyEndpoint, errCOSConfig),
		},
		{
			"bucket is empty",
			COSConfig{
				BucketNames:     "",
				Endpoint:        "",
				Region:          "dummy",
				AccessKeyID:     "dummy",
				SecretAccessKey: flagext.SecretWithValue("dummy"),
			},
			errEmptyBucket,
		},
		{
			"valid config",
			COSConfig{
				BucketNames:     "test",
				Endpoint:        "test",
				Region:          "dummy",
				AccessKeyID:     "dummy",
				SecretAccessKey: flagext.SecretWithValue("dummy"),
			},
			nil,
		},
	}
	for _, tt := range tests {
		cosClient, err := NewCOSObjectClient(tt.cosConfig, hedging.Config{})
		if tt.expectedError != nil {
			require.Equal(t, tt.expectedError.Error(), err.Error())
			continue
		}
		require.NotNil(t, cosClient.cos)
		require.NotNil(t, cosClient.hedgedS3)
		require.Equal(t, []string{tt.cosConfig.BucketNames}, cosClient.bucketNames)
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
