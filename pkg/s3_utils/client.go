package s3_utils

import (
	"crypto/x509"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var client *minio.Client
var lock = &sync.Mutex{}

func NewS3Client(log log.Logger, cfg Config) (*minio.Client, error) {
	lock.Lock()
	defer lock.Unlock()
	if client != nil {
		return client, nil
	}
	tr, err := customTransport(cfg.S3CaCertLocation)
	if err != nil {
		return nil, err
	}

	// Initialize minio client object.
	minioClient, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure:    cfg.UseSSL,
		Transport: tr,
		// Trace:     getHttpTrace(log),
	})
	if err != nil {
		return nil, err
	}
	client = minioClient
	// minioClient.TraceOn(nil)
	return client, err
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
	// tr.TLSHandshakeTimeout = 5 * time.Second

	return tr, nil
}

// func getHttpTrace(log log.Logger) *httptrace.ClientTrace {
// 	trace := &httptrace.ClientTrace{
// 		GetConn: func(hostPort string) {
// 		},
// 		GotConn: func(connInfo httptrace.GotConnInfo) {
// 			log.Infof("Got Conn: %+v\n", connInfo)
// 		},
// 		PutIdleConn: func(err error) {
// 		},
// 		GotFirstResponseByte: func() {
// 			log.Infof("first response byte received")
// 		},
// 		Got100Continue: func() {
// 		},
// 		Got1xxResponse: func(code int, header textproto.MIMEHeader) error {
// 			return nil
// 		},
// 		DNSStart: func(httptrace.DNSStartInfo) {
// 		},
// 		DNSDone: func(dnsInfo httptrace.DNSDoneInfo) {
// 			log.Infof("DNS Info: %+v\n", dnsInfo)
// 		},
// 		ConnectStart: func(network string, addr string) {
// 		},
// 		ConnectDone: func(network string, addr string, err error) {
// 		},
// 		TLSHandshakeStart: func() {
// 			log.Infof("tls handshake start")
// 		},
// 		TLSHandshakeDone: func(cs tls.ConnectionState, err error) {
// 			if err != nil {
// 				log.Errorf("error in TLS handshake: %w", err)
// 			}
// 			log.Infof("TLS handshake info: %+v\n", cs)
// 		},
// 		WroteHeaderField: func(key string, value []string) {
// 			// log.Infof("header key is: %s", key)
// 			// for _, v := range value {
// 			// 	log.Infof("value is: %s", v)
// 			// }
// 		},
// 		WroteHeaders: func() {
// 		},
// 		Wait100Continue: func() {
// 		},
// 		WroteRequest: func(reqInfo httptrace.WroteRequestInfo) {
// 			if reqInfo.Err != nil {
// 				log.Infof("wrote request info is %+s\n", reqInfo.Err.Error())
// 			}
// 		},
// 	}
// 	return trace
// }
