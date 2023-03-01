package ibmcloud

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"


	cos "github.com/IBM/ibm-cos-sdk-go/aws"
	"github.com/IBM/ibm-cos-sdk-go/aws/awserr"
	"github.com/IBM/ibm-cos-sdk-go/aws/credentials"
	"github.com/IBM/ibm-cos-sdk-go/aws/request"
	"github.com/IBM/ibm-cos-sdk-go/aws/session"
	v4 "github.com/IBM/ibm-cos-sdk-go/aws/signer/v4"

	"github.com/IBM/ibm-cos-sdk-go/service/s3/s3iface" //no required module provides package "github.com/IBM/ibm-cos-sdk-go/service/s3/s3iface"

	"github.com/IBM/ibm-cos-sdk-go/service/s3"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/flagext"
	"github.com/minio/minio-go/v7/pkg/signer"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	awscommon "github.com/weaveworks/common/aws"
	"github.com/weaveworks/common/instrument"

	bucket_s3 "github.com/grafana/loki/pkg/storage/bucket/s3"
	"github.com/grafana/loki/pkg/storage/chunk/client"
	"github.com/grafana/loki/pkg/storage/chunk/client/hedging"
	storageawscommon "github.com/grafana/loki/pkg/storage/common/aws"
	"github.com/grafana/loki/pkg/util"
)

const (
	SignatureVersionV4 = "v4"
	SignatureVersionV2 = "v2"
)
// Constants for IBM cos values



var (
	supportedSignatureVersions     = []string{SignatureVersionV4, SignatureVersionV2}
	errUnsupportedSignatureVersion = errors.New("unsupported signature version")
	errUnsupportedStorageClass     = errors.New("unsupported cos storage class")
)

var s3RequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "loki",
	Name:      "s3_request_duration_seconds",
	Help:      "Time spent doing cos requests.",
	Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
}, []string{"operation", "status_code"}))

// InjectRequestMiddleware gives users of this client the ability to make arbitrary
// changes to outgoing requests.
type InjectRequestMiddleware func(next http.RoundTripper) http.RoundTripper

func init() {
	s3RequestDuration.Register()
}

// CosConfig specifies config for storing chunks on IBM cos.
type CosConfig struct {
	cos              flagext.URLValue
	S3ForcePathStyle bool

	BucketNames      string
	Endpoint         string              `yaml:"endpoint"`
	Region           string              `yaml:"region"`
	AccessKeyID      string              `yaml:"access_key_id"`
	SecretAccessKey  flagext.Secret      `yaml:"secret_access_key"`
	Insecure         bool                `yaml:"insecure"`
	SSEEncryption    bool                `yaml:"sse_encryption"`
	HTTPConfig       HTTPConfig          `yaml:"http_config"`
	SignatureVersion string              `yaml:"signature_version"`
	StorageClass     string              `yaml:"storage_class"`
	SSEConfig        bucket_s3.SSEConfig `yaml:"sse"`
	BackoffConfig    backoff.Config      `yaml:"backoff_config" doc:"description=Configures back off when cos get Object."`
	Inject InjectRequestMiddleware `yaml:"-"`
}

// HTTPConfig stores the http.Transport configuration
type HTTPConfig struct {
	IdleConnTimeout       time.Duration `yaml:"idle_conn_timeout"`
	ResponseHeaderTimeout time.Duration `yaml:"response_header_timeout"`
	InsecureSkipVerify    bool          `yaml:"insecure_skip_verify"`
	CAFile                string        `yaml:"ca_file"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *CosConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet with a specified prefix
func (cfg *CosConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Var(&cfg.cos, prefix+"cos.url", "cos endpoint URL with escaped Key and Secret encoded. "+
		"If only region is specified as a host, proper endpoint will be deduced. Use inmemory:///<bucket-name> to use a mock in-memory implementation.")
	f.BoolVar(&cfg.S3ForcePathStyle, prefix+"cos.force-path-style", false, "Set this to `true` to force the request to use path-style addressing.")
	f.StringVar(&cfg.BucketNames, prefix+"cos.buckets", "", "Comma separated list of bucket names to evenly distribute chunks over. Overrides any buckets specified in cos.url flag")

	f.StringVar(&cfg.Endpoint, prefix+"cos.endpoint", "", "cos Endpoint to connect to.")
	f.StringVar(&cfg.Region, prefix+"cos.region", "", "AWS region to use.")
	f.StringVar(&cfg.AccessKeyID, prefix+"cos.access-key-id", "", "AWS Access Key ID")
	f.Var(&cfg.SecretAccessKey, prefix+"cos.secret-access-key", "AWS Secret Access Key")
	f.BoolVar(&cfg.Insecure, prefix+"cos.insecure", false, "Disable https on cos connection.")

	// TODO Remove in Cortex 1.10.0
	f.BoolVar(&cfg.SSEEncryption, prefix+"cos.sse-encryption", false, "Enable AWS Server Side Encryption [Deprecated: Use .sse instead. if cos.sse-encryption is enabled, it assumes .sse.type SSE-cos]")

	cfg.SSEConfig.RegisterFlagsWithPrefix(prefix+"cos.sse.", f)

	f.DurationVar(&cfg.HTTPConfig.IdleConnTimeout, prefix+"cos.http.idle-conn-timeout", 90*time.Second, "The maximum amount of time an idle connection will be held open.")
	f.DurationVar(&cfg.HTTPConfig.ResponseHeaderTimeout, prefix+"cos.http.response-header-timeout", 0, "If non-zero, specifies the amount of time to wait for a server's response headers after fully writing the request.")
	f.BoolVar(&cfg.HTTPConfig.InsecureSkipVerify, prefix+"cos.http.insecure-skip-verify", false, "Set to true to skip verifying the certificate chain and hostname.")
	f.StringVar(&cfg.HTTPConfig.CAFile, prefix+"cos.http.ca-file", "", "Path to the trusted CA file that signed the SSL certificate of the cos endpoint.")
	f.StringVar(&cfg.SignatureVersion, prefix+"cos.signature-version", SignatureVersionV4, fmt.Sprintf("The signature version to use for authenticating against cos. Supported values are: %s.", strings.Join(supportedSignatureVersions, ", ")))
	f.StringVar(&cfg.StorageClass, prefix+"cos.storage-class", storageawscommon.StorageClassStandard, fmt.Sprintf("The cos storage class which objects will use. Supported values are: %s.", strings.Join(storageawscommon.SupportedStorageClasses, ", ")))

	f.DurationVar(&cfg.BackoffConfig.MinBackoff, prefix+"cos.min-backoff", 100*time.Millisecond, "Minimum backoff time when cos get Object")
	f.DurationVar(&cfg.BackoffConfig.MaxBackoff, prefix+"cos.max-backoff", 3*time.Second, "Maximum backoff time when cos get Object")
	f.IntVar(&cfg.BackoffConfig.MaxRetries, prefix+"cos.max-retries", 5, "Maximum number of times to retry when cos get Object")
}

// Validate config and returns error on failure
func (cfg *CosConfig) Validate() error {
	if !util.StringsContain(supportedSignatureVersions, cfg.SignatureVersion) {
		return errUnsupportedSignatureVersion
	}

	return storageawscommon.ValidateStorageClass(cfg.StorageClass)
}

type CosObjectClient struct {
	cfg CosConfig

	bucketNames []string
	cos          s3iface.S3API
	hedgedS3    s3iface.S3API
	sseConfig   *SSEParsedConfig //Handle it later
}

// NewCOSObjectClient makes a new cos-backed ObjectClient.
func NewCOSObjectClient(cfg CosConfig, hedgingCfg hedging.Config) (*CosObjectClient, error) {
	bucketNames, err := buckets(cfg)
	if err != nil {
		return nil, err
	}
	cosClient, err := buildCOSClient(cfg, hedgingCfg, false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build cos config")
	}
	s3ClientHedging, err := buildCOSClient(cfg, hedgingCfg, true)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build cos config")
	}

	sseCfg, err := buildSSEParsedConfig(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build SSE config")
	}

	client := CosObjectClient{
		cfg:         cfg,
		cos:         cosClient,
		hedgedS3:    s3ClientHedging,
		bucketNames: bucketNames,
		sseConfig:   sseCfg,
	}
	return &client, nil
}
//ERRROR IN SSE
func buildSSEParsedConfig(cfg CosConfig) (*SSEParsedConfig, error) {
	if cfg.SSEConfig.Type != "" {
		return NewSSEParsedConfig(cfg.SSEConfig)
	}

	// deprecated, but if used it assumes SSE-cos type
	if cfg.SSEEncryption {
		return NewSSEParsedConfig(bucket_s3.SSEConfig{
			Type: bucket_s3.SSES3,
		})
	}

	return nil, nil
}

func v2SignRequestHandler(cfg CosConfig) request.NamedHandler {
	return request.NamedHandler{
		Name: "v2.SignRequestHandler",
		Fn: func(req *request.Request) {
			credentials, err := req.Config.Credentials.GetWithContext(req.Context())
			if err != nil {
				if err != nil {
					req.Error = err
					return
				}
			}

			req.HTTPRequest = signer.SignV2(
				*req.HTTPRequest,
				credentials.AccessKeyID,
				credentials.SecretAccessKey,
				!cfg.S3ForcePathStyle,
			)
		},
	}
}

func buildCOSClient(cfg CosConfig, hedgingCfg hedging.Config, hedging bool) (*s3.COS, error) {
	var CosConfig *cos.Config
	var err error

	// if an cos url is passed use it to initialize the CosConfig and then override with any additional params
	if cfg.cos.URL != nil {
		CosConfig, err = awscommon.ConfigFromURL(cfg.cos.URL) //Any Storage Config for COS?
		if err != nil {
			return nil, err
		}
	} else {
		CosConfig = &cos.Config{}
		CosConfig = CosConfig.WithRegion("dummy")
	}

	CosConfig = CosConfig.WithMaxRetries(0)                          // We do our own retries, so we can monitor them
	CosConfig = CosConfig.WithS3ForcePathStyle(cfg.S3ForcePathStyle) // support for Path Style cos url if has the flag

	if cfg.Endpoint != "" {
		CosConfig = CosConfig.WithEndpoint(cfg.Endpoint)
	}

	if cfg.Insecure {
		CosConfig = CosConfig.WithDisableSSL(true)
	}

	if cfg.Region != "" {
		CosConfig = CosConfig.WithRegion(cfg.Region)
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey.String() == "" ||
		cfg.AccessKeyID == "" && cfg.SecretAccessKey.String() != "" {
		return nil, errors.New("must supply both an Access Key ID and Secret Access Key or neither")
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey.String() != "" {
		creds := credentials.NewStaticCredentials(cfg.AccessKeyID, cfg.SecretAccessKey.String(), "")
		CosConfig = CosConfig.WithCredentials(creds)
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.HTTPConfig.InsecureSkipVerify,
	}

	if cfg.HTTPConfig.CAFile != "" {
		tlsConfig.RootCAs = x509.NewCertPool()
		data, err := os.ReadFile(cfg.HTTPConfig.CAFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs.AppendCertsFromPEM(data)
	}

	// While extending cos configuration this http config was copied in order to
	// to maintain backwards compatibility with previous versions of Cortex while providing
	// more flexible configuration of the http client
	// https://github.com/weaveworks/common/blob/4b1847531bc94f54ce5cf210a771b2a86cd34118/aws/config.go#L23
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
		TLSClientConfig:       tlsConfig,
	})

	if cfg.Inject != nil {
		transport = cfg.Inject(transport)
	}
	httpClient := &http.Client{
		Transport: transport,
	}

	if hedging {
		httpClient, err = hedgingCfg.ClientWithRegisterer(httpClient, prometheus.WrapRegistererWithPrefix("loki_", prometheus.DefaultRegisterer))
		if err != nil {
			return nil, err
		}
	}

	CosConfig = CosConfig.WithHTTPClient(httpClient)

	sess, err := session.NewSession(CosConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new cos session")
	}

	cosClient := s3.New(sess)

	if cfg.SignatureVersion == SignatureVersionV2 {
		cosClient.Handlers.Sign.Swap(v4.SignRequestHandler.Name, v2SignRequestHandler(cfg))
	}

	return cosClient, nil
}

func buckets(cfg CosConfig) ([]string, error) {
	// bucketnames
	var bucketNames []string
	if cfg.cos.URL != nil {
		bucketNames = []string{strings.TrimPrefix(cfg.cos.URL.Path, "/")}
	}

	if cfg.BucketNames != "" {
		bucketNames = strings.Split(cfg.BucketNames, ",") // comma separated list of bucket names
	}

	if len(bucketNames) == 0 {
		return nil, errors.New("at least one bucket name must be specified")
	}
	return bucketNames, nil
}

// Stop fulfills the chunk.ObjectClient interface
func (a *CosObjectClient) Stop() {}

