package ibmcloud

import (
	"context"
	//"crypto/tls"
	//"crypto/x509"
	"flag"
	//"fmt"

	"io"
	"net"
	"net/http"

	//"os"
	"strings"
	"time"

	ibm "github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/aws/awserr"
	"github.com/IBM/ibm-cos-sdk-go/aws/credentials"
	"github.com/IBM/ibm-cos-sdk-go/aws/session"
	cos "github.com/IBM/ibm-cos-sdk-go/service/s3"
	cosiface "github.com/IBM/ibm-cos-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"

	"github.com/grafana/loki/pkg/storage/chunk/client"
	"github.com/grafana/loki/pkg/storage/chunk/client/hedging"
)

var (
	errUnsupportedSignatureVersion = errors.New("unsupported signature version")
	errInvalidCOSHMACCredentials   = errors.New("must supply both an Access Key ID and Secret Access Key or neither")
	errEmptyRegion                 = errors.New("region should not be empty")
	errEmptyEndpoint               = errors.New("endpoint should not be empty")
	errEmptyBucket                 = errors.New("at least one bucket name must be specified")
	errCOSConfig                   = "failed to build cos config"
)

var cosRequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "loki",
	Name:      "cos_request_duration_seconds",
	Help:      "Time spent doing cos requests.",
	Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
}, []string{"operation", "status_code"}))

// InjectRequestMiddleware gives users of this client the ability to make arbitrary
// changes to outgoing requests.
type InjectRequestMiddleware func(next http.RoundTripper) http.RoundTripper

func init() {
	cosRequestDuration.Register()
}

// COSConfig specifies config for storing chunks on IBM cos.
type COSConfig struct {
	ForcePathStyle  bool           `yaml:"forcepathstyle"`
	BucketNames     string         `yaml:"bucketnames"`
	Endpoint        string         `yaml:"endpoint"`
	Region          string         `yaml:"region"`
	AccessKeyID     string         `yaml:"access_key_id"`
	SecretAccessKey flagext.Secret `yaml:"secret_access_key"`
	HTTPConfig      HTTPConfig     `yaml:"http_config"`
	BackoffConfig   backoff.Config `yaml:"backoff_config" doc:"description=Configures back off when cos get Object."`
}

// HTTPConfig stores the http.Transport configuration
type HTTPConfig struct {
	IdleConnTimeout       time.Duration `yaml:"idle_conn_timeout"`
	ResponseHeaderTimeout time.Duration `yaml:"response_header_timeout"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *COSConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet with a specified prefix
func (cfg *COSConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.ForcePathStyle, prefix+"cos.force-path-style", false, "Set this to `true` to force the request to use path-style addressing.")
	f.StringVar(&cfg.BucketNames, prefix+"cos.buckets", "", "Comma separated list of bucket names to evenly distribute chunks over.")

	f.StringVar(&cfg.Endpoint, prefix+"cos.endpoint", "", "COS Endpoint to connect to.")
	f.StringVar(&cfg.Region, prefix+"cos.region", "", "COS region to use.")
	f.StringVar(&cfg.AccessKeyID, prefix+"cos.access-key-id", "", "COS HMAC Access Key ID")
	f.Var(&cfg.SecretAccessKey, prefix+"cos.secret-access-key", "COS HMAC Secret Access Key")

	f.DurationVar(&cfg.HTTPConfig.IdleConnTimeout, prefix+"cos.http.idle-conn-timeout", 90*time.Second, "The maximum amount of time an idle connection will be held open.")
	f.DurationVar(&cfg.HTTPConfig.ResponseHeaderTimeout, prefix+"cos.http.response-header-timeout", 0, "If non-zero, specifies the amount of time to wait for a server's response headers after fully writing the request.")

	f.DurationVar(&cfg.BackoffConfig.MinBackoff, prefix+"cos.min-backoff", 100*time.Millisecond, "Minimum backoff time when cos get Object")
	f.DurationVar(&cfg.BackoffConfig.MaxBackoff, prefix+"cos.max-backoff", 3*time.Second, "Maximum backoff time when cos get Object")
	f.IntVar(&cfg.BackoffConfig.MaxRetries, prefix+"cos.max-retries", 5, "Maximum number of times to retry when cos get Object")
}

type COSObjectClient struct {
	cfg COSConfig

	bucketNames []string
	cos         cosiface.S3API
	hedgedS3    cosiface.S3API
}

// NewCOSObjectClient makes a new COS backed ObjectClient.
func NewCOSObjectClient(cfg COSConfig, hedgingCfg hedging.Config) (*COSObjectClient, error) {
	bucketNames, err := buckets(cfg)
	if err != nil {
		return nil, err
	}
	cosClient, err := buildCOSClient(cfg, hedgingCfg, false)
	if err != nil {
		return nil, errors.Wrap(err, errCOSConfig)
	}
	cosClientHedging, err := buildCOSClient(cfg, hedgingCfg, true)
	if err != nil {
		return nil, errors.Wrap(err, errCOSConfig)
	}
	client := COSObjectClient{
		cfg:         cfg,
		cos:         cosClient,
		hedgedS3:    cosClientHedging,
		bucketNames: bucketNames,
	}
	return &client, nil
}

func validate(cfg COSConfig) error {
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey.String() == "" ||
		cfg.AccessKeyID == "" && cfg.SecretAccessKey.String() != "" {
		return errInvalidCOSHMACCredentials
	}

	if cfg.Region == "" {
		return errEmptyRegion
	}

	if cfg.Endpoint == "" {
		return errEmptyEndpoint
	}
	return nil
}

func buildCOSClient(cfg COSConfig, hedgingCfg hedging.Config, hedging bool) (*cos.S3, error) {
	var err error
	if err = validate(cfg); err != nil {
		return nil, err
	}
	cosConfig := &ibm.Config{}

	cosConfig = cosConfig.WithMaxRetries(0)                        // We do our own retries, so we can monitor them
	cosConfig = cosConfig.WithS3ForcePathStyle(cfg.ForcePathStyle) // support for Path Style cos url if has the flag

	cosConfig = cosConfig.WithEndpoint(cfg.Endpoint)

	cosConfig = cosConfig.WithRegion(cfg.Region)

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey.String() != "" {
		creds := credentials.NewStaticCredentials(cfg.AccessKeyID, cfg.SecretAccessKey.String(), "")
		cosConfig = cosConfig.WithCredentials(creds)
	}

	transport := http.RoundTripper(&http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          200,
		IdleConnTimeout:       cfg.HTTPConfig.IdleConnTimeout,
		MaxIdleConnsPerHost:   200,
		TLSHandshakeTimeout:   3 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: cfg.HTTPConfig.ResponseHeaderTimeout,
	})

	httpClient := &http.Client{
		Transport: transport,
	}

	if hedging {
		httpClient, err = hedgingCfg.ClientWithRegisterer(httpClient, prometheus.WrapRegistererWithPrefix("loki_", prometheus.DefaultRegisterer))
		if err != nil {
			return nil, err
		}
	}

	cosConfig = cosConfig.WithHTTPClient(httpClient)

	sess, err := session.NewSession(cosConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new cos session")
	}

	cosClient := cos.New(sess)

	return cosClient, nil
}

func buckets(cfg COSConfig) ([]string, error) {
	// bucketnames
	var bucketNames []string

	if cfg.BucketNames != "" {
		bucketNames = strings.Split(cfg.BucketNames, ",") // comma separated list of bucket names
	}

	if len(bucketNames) == 0 {
		return nil, errEmptyBucket
	}
	return bucketNames, nil
}

// Stop fulfills the chunk.ObjectClient interface
func (c *COSObjectClient) Stop() {}

// DeleteObject deletes the specified objectKey from the appropriate S3 bucket
func (c *COSObjectClient) DeleteObject(ctx context.Context, objectKey string) error {
	return nil
}

// GetObject returns a reader and the size for the specified object key from the configured S3 bucket.
func (c *COSObjectClient) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, int64, error) {
	return nil, 0, nil
}

// PutObject into the store
func (c *COSObjectClient) PutObject(ctx context.Context, objectKey string, object io.ReadSeeker) error {
	return nil
}

// List implements chunk.ObjectClient.
func (c *COSObjectClient) List(ctx context.Context, prefix, delimiter string) ([]client.StorageObject, []client.StorageCommonPrefix, error) {
	return nil, nil, nil
}

// IsObjectNotFoundErr returns true if error means that object is not found. Relevant to GetObject and DeleteObject operations.
func (c *COSObjectClient) IsObjectNotFoundErr(err error) bool {
	if aerr, ok := errors.Cause(err).(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
		return true
	}

	return false
}