package s3_utils

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/acomagu/bufpipe"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/minio/minio-go/v7"
	"github.com/xitongsys/parquet-go/source"
)

// MinioFile is ParquetFile for MinIO S3 API
type MinioFile struct {
	ctx    context.Context
	client *minio.Client
	offset int64
	whence int

	// write-related fields
	pipeReader *bufpipe.PipeReader
	pipeWriter *bufpipe.PipeWriter

	// read-related fields
	fileSize   int64
	downloader *minio.Object

	err        error
	BucketName string
	Key        string
}

var (
	errWhence        = errors.New("Seek: invalid whence")
	errInvalidOffset = errors.New("Seek: invalid offset")
	// errFailedUpload  = errors.New("Write: failed upload")
)

// NewS3FileWriterWithClient is the same as NewMinioFileWriter but allows passing
// your own S3 client.
func NewS3FileWriterWithClient(
	ctx context.Context,
	// s3Client *minio.Client,
	bucket string,
	key string,
	// statusChan chan int,
	pr *bufpipe.PipeReader,
	pw *bufpipe.PipeWriter,
) (source.ParquetFile, error) {
	file := &MinioFile{
		ctx: ctx,
		// client:     s3Client,
		BucketName: bucket,
		Key:        key,
		// statusChan: statusChan,
		pipeReader: pr,
		pipeWriter: pw,
	}
	// log.Info("minio file create func called")
	return file.Create(key)
}

// NewS3FileReaderWithClient is the same as NewMinioFileReader but allows passing
// your own S3 client
func NewS3FileReaderWithClient(ctx context.Context, s3Client *minio.Client, bucket string, key string) (source.ParquetFile, error) {
	file := &MinioFile{
		ctx:        ctx,
		client:     s3Client,
		BucketName: bucket,
		Key:        key,
	}

	return file.Open(key)
}

// Seek tracks the offset for the next Read. Has no effect on Write.
func (s *MinioFile) Seek(offset int64, whence int) (int64, error) {
	if whence < io.SeekStart || whence > io.SeekEnd {
		return 0, errWhence
	}

	// log.Infof("seek function called offset and whence is before seek: %d, %d", offset, whence)
	if s.fileSize > 0 {
		switch whence {
		case io.SeekStart:
			if offset < 0 || offset > s.fileSize {
				return 0, errInvalidOffset
			}
		case io.SeekCurrent:
			offset += s.offset
			if offset < 0 || offset > s.fileSize {
				return 0, errInvalidOffset
			}
		case io.SeekEnd:
			if offset > -1 || -offset > s.fileSize {
				return 0, errInvalidOffset
			}
		}
	}
	o, err := s.downloader.Seek(offset, whence)
	if err != nil {
		return 0, err
	}
	s.offset = o
	s.whence = whence
	// log.Infof("offset and whence is after seek: %d, %d", s.offset, s.whence)
	return offset, nil
}

// Read up to len(p) bytes into p and return the number of bytes read
func (s *MinioFile) Read(p []byte) (n int, err error) {
	// log.Infof("read: file size and offset is: %d, %d", s.fileSize, s.offset)
	if s.fileSize > 0 && s.offset >= s.fileSize {
		return 0, io.EOF
	}

	// log.Info("calling readAt")
	bytesDownloaded, err := s.downloader.ReadAt(p, s.offset)
	if err != nil && err != io.EOF {
		log.Errorf("unable to read miniofile: %w", err)
		return 0, err
	}
	// log.Infof("read: bytesDownloaded after readat is: %d", bytesDownloaded)

	// if bytesDownloaded == 0 {
	// log.Info("calling read")
	// bytesDownloaded, err := s.downloader.Read(p)
	// if err != nil && err != io.EOF {
	// 	log.Errorf("unable to read miniofile: %w", err)
	// 	return 0, err
	// }
	// log.Infof("read: bytesDownloaded after read is: %d", bytesDownloaded)
	// }

	s.offset += int64(bytesDownloaded)
	// log.Infof("read: s.offset after read is: %d", s.offset)
	return bytesDownloaded, err
}

// Write len(p) bytes from p to the Minio data stream
func (s *MinioFile) Write(p []byte) (n int, err error) {
	// prevent further writes upon error
	bytesWritten, writeError := s.pipeWriter.Write(p)
	if writeError != nil {
		s.err = writeError
		s.pipeWriter.CloseWithError(err)
		return 0, writeError
	}

	return bytesWritten, nil
}

// Close signals write completion and cleans up any
// open streams. Will block until pending uploads are complete.
func (s *MinioFile) Close() error {
	var err error
	if s.pipeWriter != nil {
		if err = s.pipeWriter.Close(); err != nil {
			return err
		}
	}

	return err
}

// Open creates a new Minio File instance to perform concurrent reads
func (s *MinioFile) Open(name string) (source.ParquetFile, error) {
	// new instance
	pf := &MinioFile{
		ctx:        s.ctx,
		client:     s.client,
		BucketName: s.BucketName,
		Key:        name,
		offset:     0,
	}
	// init object info
	downloader, err := s.client.GetObject(s.ctx, s.BucketName, s.Key, minio.GetObjectOptions{})
	if err != nil {
		return pf, err
	}
	info, err := downloader.Stat()
	if err != nil {
		return pf, err
	}
	pf.downloader = downloader
	pf.fileSize = info.Size
	return pf, nil
}

// Create creates a new Minio File instance to perform writes
func (s *MinioFile) Create(key string) (source.ParquetFile, error) {
	pf := &MinioFile{
		ctx:        s.ctx,
		client:     s.client,
		BucketName: s.BucketName,
		Key:        key,
		pipeReader: s.pipeReader,
		pipeWriter: s.pipeWriter,
	}
	return pf, nil
}

func PutObejct(client *minio.Client, bucketName, key string, reader *bufpipe.PipeReader, statusChan chan int, wg *sync.WaitGroup) error {
	defer wg.Done()
	<-statusChan
	log.Infof("received data")
	ctx := context.Background()
	minioUploadInfo, err := client.PutObject(ctx, bucketName, key, reader, -1, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		log.Errorf("not able to put object in minio: %w", err.Error())
		reader.CloseWithError(err)
		return err
	}
	log.Infof("minio upload info: %v", minioUploadInfo)
	return nil
}
