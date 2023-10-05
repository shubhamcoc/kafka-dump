package cmd

import (
	"context"

	"github.com/huantt/kafka-dump/impl"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/huantt/kafka-dump/pkg/s3_utils"
	"github.com/minio/minio-go/v7"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func CreateCountParquetRowCommand() (*cobra.Command, error) {
	var filePathMessage string
	var filePathOffset string
	var s3endpoint string
	var s3AccessKeyID string
	var s3SecretAccessKey string
	var bucketName string
	var s3CaCertLocation string
	var s3SSL bool

	command := cobra.Command{
		Use: "count-parquet-rows",
		Run: func(cmd *cobra.Command, args []string) {
			var s3Client *minio.Client
			var err error
			logger := log.FromContext(context.Background())
			if s3endpoint != "" {
				s3conf := s3_utils.Config{
					Endpoint:         s3endpoint,
					AccessKeyID:      s3AccessKeyID,
					SecretAccessKey:  s3SecretAccessKey,
					UseSSL:           s3SSL,
					BucketName:       bucketName,
					S3CaCertLocation: s3CaCertLocation,
				}

				s3Client, err = s3_utils.NewS3Client(logger, s3conf)
				if err != nil {
					panic(errors.Wrap(err, "Unable to init s3 client"))
				}
			}
			parquetReader, err := impl.NewParquetReader(filePathMessage, filePathOffset, bucketName, s3Client, false)
			if err != nil {
				panic(errors.Wrap(err, "Unable to init parquet file reader"))
			}

			log.Infof("Number of rows in message file: %d", parquetReader.GetNumberOfRowsInMessageFile())
			log.Infof("Number of rows in offset file: %d", parquetReader.GetNumberOfRowsInOffsetFile())
		},
	}
	command.Flags().StringVarP(&filePathMessage, "file", "f", "", "File path of stored kafka message (required)")
	command.Flags().StringVarP(&filePathOffset, "offset-file", "o", "", "File path of stored kafka offset (required)")
	command.Flags().StringVar(&s3endpoint, "endpoint", "", "Endpoint to connect to S3")
	command.Flags().StringVar(&s3AccessKeyID, "accesskeyid", "", "Access Key of S3 instance")
	command.Flags().StringVar(&s3SecretAccessKey, "secretaccesskey", "", "Secret Key of S3 instance")
	command.Flags().StringVar(&bucketName, "bucket", "", "Bucket name to connect to s3 bucket")
	command.Flags().StringVar(&s3CaCertLocation, "ca-cert", "", "ca cert location to connect to s3 bucket")
	command.Flags().BoolVar(&s3SSL, "ssl", true, "Enable SSL for s3 connection")
	err := command.MarkFlagRequired("file")
	if err != nil {
		return nil, err
	}
	err = command.MarkFlagRequired("offset-file")
	if err != nil {
		return nil, err
	}
	return &command, nil
}
