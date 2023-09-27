package s3_utils

import (
	"crypto/x509"
	"io/ioutil"
	"net/http"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Client struct {
	Client *minio.Client
}

func NewS3Client(cfg Config) (*S3Client, error) {
	tr, err := customTransport(cfg.S3CaCertLocation)
	if err != nil {
		return nil, err
	}
	// Initialize minio client object.
	minioClient, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure:    cfg.UseSSL,
		Transport: tr,
	})
	if err != nil {
		return nil, err
	}

	return &S3Client{
		Client: minioClient,
	}, err
}

func customTransport(certPath string) (*http.Transport, error) {
	tr, err := minio.DefaultTransport(true)
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadFile(certPath)
	if err != nil {
		return nil, err
	}

	if tr.TLSClientConfig.RootCAs == nil {
		tr.TLSClientConfig.RootCAs = x509.NewCertPool()
	}

	if ok := tr.TLSClientConfig.RootCAs.AppendCertsFromPEM(b); !ok {
		return nil, err
	}

	return tr, nil
}
