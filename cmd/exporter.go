package cmd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/acomagu/bufpipe"
	"github.com/huantt/kafka-dump/impl"
	"github.com/huantt/kafka-dump/pkg/gcs_utils"
	"github.com/huantt/kafka-dump/pkg/kafka_utils"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/huantt/kafka-dump/pkg/s3_utils"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/xitongsys/parquet-go-source/gcs"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
)

func CreateExportCommand() (*cobra.Command, error) {
	var messageFilePath string
	var offsetFilePath string
	var kafkaServers string
	var kafkaUsername string
	var kafkaPassword string
	var kafkaSecurityProtocol string
	var kafkaSASKMechanism string
	var queuedMaxMessagesKbytes int64
	var fetchMessageMaxBytes int64
	var kafkaGroupID string
	var topics *[]string
	var exportLimitPerFile uint64
	var maxWaitingSecondsForNewMessage int
	var concurrentConsumers = 1
	var googleCredentialsFile string
	var storageType string
	var gcsBucketName string
	var gcsProjectID string
	var sslCaLocation string
	var sslKeyPassword string
	var sslCertLocation string
	var sslKeyLocation string
	var enableAutoOffsetStore bool
	var s3endpoint string
	var s3AccessKeyID string
	var s3SecretAccessKey string
	var bucketName string
	var s3CaCertLocation string
	var s3SSL bool

	command := cobra.Command{
		Use: "export",
		Run: func(cmd *cobra.Command, args []string) {
			log.Infof("Limit: %d - Concurrent consumers: %d", exportLimitPerFile, concurrentConsumers)
			kafkaConsumerConfig := kafka_utils.Config{
				BootstrapServers:      kafkaServers,
				SecurityProtocol:      kafkaSecurityProtocol,
				SASLMechanism:         kafkaSASKMechanism,
				SASLUsername:          kafkaUsername,
				SASLPassword:          kafkaPassword,
				GroupId:               kafkaGroupID,
				SSLCALocation:         sslCaLocation,
				SSLKeyPassword:        sslKeyPassword,
				SSLKeyLocation:        sslKeyLocation,
				SSLCertLocation:       sslCertLocation,
				EnableAutoOffsetStore: enableAutoOffsetStore,
			}
			adminClient, err := kafka_utils.NewAdminClient(kafkaConsumerConfig)
			if err != nil {
				panic(errors.Wrap(err, "Unable to init admin client"))
			}

			consumer, err := kafka_utils.NewConsumer(kafkaConsumerConfig)
			if err != nil {
				panic(errors.Wrap(err, "Unable to init consumer"))
			}

			maxWaitingTimeForNewMessage := time.Duration(maxWaitingSecondsForNewMessage) * time.Second
			options := &impl.Options{
				Limit:                       exportLimitPerFile,
				MaxWaitingTimeForNewMessage: &maxWaitingTimeForNewMessage,
			}

			var wg sync.WaitGroup
			wg.Add(concurrentConsumers)
			for i := 0; i < concurrentConsumers; i++ {
				go func(workerID int) {
					defer wg.Done()
					for {
						outputFilePath := messageFilePath
						if exportLimitPerFile > 0 {
							outputFilePath = fmt.Sprintf("%s.%d", messageFilePath, time.Now().UnixMilli())
						}
						log.Infof("[Worker-%d] Exporting to: %s", workerID, outputFilePath)
						msc := make(chan int)
						osc := make(chan int)
						s3conf := s3_utils.Config{
							Endpoint:         s3endpoint,
							AccessKeyID:      s3AccessKeyID,
							SecretAccessKey:  s3SecretAccessKey,
							UseSSL:           s3SSL,
							BucketName:       bucketName,
							S3CaCertLocation: s3CaCertLocation,
						}
						messagepr, messagepw := bufpipe.New(nil)
						messageFileWriter, err := createParquetFileWriter(
							Storage(storageType),
							outputFilePath,
							gcs_utils.Config{
								ProjectId:       gcsProjectID,
								BucketName:      gcsBucketName,
								CredentialsFile: googleCredentialsFile,
							},
							s3_utils.Config{
								Endpoint:         s3endpoint,
								AccessKeyID:      s3AccessKeyID,
								SecretAccessKey:  s3SecretAccessKey,
								UseSSL:           s3SSL,
								BucketName:       bucketName,
								S3CaCertLocation: s3CaCertLocation,
								StatusChan:       msc,
							},
							messagepr,
							messagepw,
						)
						if err != nil {
							panic(errors.Wrap(err, "[NewLocalFileWriter]"))
						}
						offsetpr, offsetpw := bufpipe.New(nil)
						offsetFileWriter, err := createParquetFileWriter(
							Storage(storageType),
							offsetFilePath,
							gcs_utils.Config{
								ProjectId:       gcsProjectID,
								BucketName:      gcsBucketName,
								CredentialsFile: googleCredentialsFile,
							},
							s3_utils.Config{
								Endpoint:         s3endpoint,
								AccessKeyID:      s3AccessKeyID,
								SecretAccessKey:  s3SecretAccessKey,
								UseSSL:           s3SSL,
								BucketName:       bucketName,
								S3CaCertLocation: s3CaCertLocation,
								StatusChan:       osc,
							},
							offsetpr,
							offsetpw,
						)
						if err != nil {
							panic(errors.Wrap(err, "[NewLocalFileWriter]"))
						}
						logger := log.WithContext(context.Background())
						s3Client, err := s3_utils.NewS3Client(logger, s3conf)
						if err != nil {
							panic(errors.Wrap(err, "Unable to init s3 client"))
						}
						wg.Add(2)
						go s3_utils.PutObejct(s3Client, bucketName, outputFilePath, messagepr, msc, &wg)
						go s3_utils.PutObejct(s3Client, bucketName, offsetFilePath, offsetpr, osc, &wg)
						log.Infof("creating parquet writer")
						parquetWriter, err := impl.NewParquetWriter(*messageFileWriter, *offsetFileWriter)
						if err != nil {
							panic(errors.Wrap(err, "Unable to init parquet file writer"))
						}
						log.Infof("creating parquet exporter")
						exporter, err := impl.NewExporter(adminClient, consumer, *topics, parquetWriter, options, msc, osc)
						if err != nil {
							panic(errors.Wrap(err, "Failed to init exporter"))
						}

						exportedCount, err := exporter.Run()
						if err != nil {
							panic(errors.Wrap(err, "Error while running exporter"))
						}
						log.Infof("[Worker-%d] Exported %d messages", workerID, exportedCount)
						if exportLimitPerFile == 0 || exportedCount < exportLimitPerFile {
							log.Infof("[Worker-%d] Finished!", workerID)
							return
						}
					}
				}(i)
			}
			wg.Wait()
		},
	}
	command.Flags().StringVar(&storageType, "storage", "file", "Storage type: local file (file) or Google cloud storage (gcs)")
	command.Flags().StringVarP(&messageFilePath, "file", "f", "", "Output file path for storing message (required)")
	command.Flags().StringVarP(&offsetFilePath, "offset-file", "o", "", "Output file path for storing offset (required)")
	command.Flags().StringVar(&googleCredentialsFile, "google-credentials", "", "Path to Google Credentials file")
	command.Flags().StringVar(&gcsBucketName, "gcs-bucket", "", "Google Cloud Storage bucket name")
	command.Flags().StringVar(&gcsProjectID, "gcs-project-id", "", "Google Cloud Storage Project ID")
	command.Flags().StringVar(&kafkaServers, "kafka-servers", "", "Kafka servers string")
	command.Flags().StringVar(&kafkaUsername, "kafka-username", "", "Kafka username")
	command.Flags().StringVar(&kafkaPassword, "kafka-password", "", "Kafka password")
	command.Flags().StringVar(&kafkaSASKMechanism, "kafka-sasl-mechanism", "", "Kafka password")
	command.Flags().StringVar(&sslCaLocation, "ssl-ca-location", "", "Location of client ca cert file in pem")
	command.Flags().StringVar(&sslKeyPassword, "ssl-key-password", "", "Password for ssl private key passphrase")
	command.Flags().StringVar(&sslCertLocation, "ssl-certificate-location", "", "Client's certificate location")
	command.Flags().StringVar(&sslKeyLocation, "ssl-key-location", "", "Path to ssl private key")
	command.Flags().BoolVar(&enableAutoOffsetStore, "enable-auto-offset-store", true, "To store offset in kafka broker")
	command.Flags().StringVar(&kafkaSecurityProtocol, "kafka-security-protocol", "", "Kafka security protocol")
	command.Flags().StringVar(&kafkaGroupID, "kafka-group-id", "", "Kafka consumer group ID")
	command.Flags().StringVar(&s3endpoint, "endpoint", "", "Endpoint to connect to S3")
	command.Flags().StringVar(&s3AccessKeyID, "accesskeyid", "", "Access Key of S3 instance")
	command.Flags().StringVar(&s3SecretAccessKey, "secretaccesskey", "", "Secret Key of S3 instance")
	command.Flags().StringVar(&bucketName, "bucket", "", "Bucket name to connect to s3 bucket")
	command.Flags().StringVar(&s3CaCertLocation, "ca-cert", "", "ca cert location to connect to s3 bucket")
	command.Flags().BoolVar(&s3SSL, "ssl", true, "Enable SSL for s3 connection")
	command.Flags().Uint64Var(&exportLimitPerFile, "limit", 0, "Supports file splitting. Files are split by the number of messages specified")
	command.Flags().IntVar(&maxWaitingSecondsForNewMessage, "max-waiting-seconds-for-new-message", 30, "Max waiting seconds for new message, then this process will be marked as finish. Set -1 to wait forever.")
	command.Flags().IntVar(&concurrentConsumers, "concurrent-consumers", 1, "Number of concurrent consumers")
	command.Flags().Int64Var(&queuedMaxMessagesKbytes, "queued-max-messages-kbytes", 128000, "Maximum number of kilobytes per topic+partition in the local consumer queue. This value may be overshot by fetch.message.max.bytes")
	command.Flags().Int64Var(&fetchMessageMaxBytes, "fetch-message-max-bytes", 1048576, "Maximum number of bytes per topic+partition to request when fetching messages from the broker.")
	topics = command.Flags().StringArray("kafka-topics", nil, "Kafka topics")
	command.MarkFlagsRequiredTogether("kafka-username", "kafka-password", "kafka-sasl-mechanism", "kafka-security-protocol")
	command.MarkFlagsRequiredTogether("google-credentials", "gcs-bucket", "gcs-project-id")
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

type Storage string

const (
	StorageLocalFile          Storage = "file"
	StorageGoogleCloudStorage Storage = "gcs"
	StorageS3                 Storage = "s3"
)

func createParquetFileWriter(storage Storage, filePath string, gcsConfig gcs_utils.Config, s3Config s3_utils.Config, pr *bufpipe.PipeReader, pw *bufpipe.PipeWriter) (*source.ParquetFile, error) {
	switch storage {
	case StorageLocalFile:
		fw, err := local.NewLocalFileWriter(filePath)
		if err != nil {
			return nil, errors.Wrap(err, "[NewLocalFileWriter]")
		}
		return &fw, nil
	case StorageGoogleCloudStorage:
		ctx := context.Background()
		client, err := gcs_utils.Singleton(gcsConfig.CredentialsFile)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create Singleton GCS client")
		}
		gcsFile, err := gcs.NewGcsFileWriterWithClient(ctx, client, gcsConfig.ProjectId, gcsConfig.BucketName, filePath)
		if err != nil {
			return nil, errors.Wrap(err, "[NewGcsFileWriterWithClient]")
		}
		fw, err := gcsFile.Open(filePath)
		if err != nil {
			return nil, errors.Wrap(err, "[GcsFile.Open]")
		}
		return &fw, nil
	case StorageS3:
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		fileWriter, err := s3_utils.NewS3FileWriterWithClient(ctx, s3Config.BucketName, filePath, pr, pw)
		if err != nil {
			return nil, errors.Wrap(err, "[NewS3FileWriterWithClient]")
		}
		return &fileWriter, nil
	default:
		return nil, errors.New(fmt.Sprintf("Storage type must be either file or gcs. Got %s", storage))
	}
}
