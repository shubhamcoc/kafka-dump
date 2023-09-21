package s3_utils

type Config struct {
	Endpoint         string
	AccessKeyID      string
	BucketName       string
	SecretAccessKey  string
	S3CaCertLocation string
	UseSSL           bool
	StatusChan       chan int
}
