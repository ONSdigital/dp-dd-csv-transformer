package ons_aws

import (
	"bytes"
	"io"
	"io/ioutil"

	"compress/gzip"
	"fmt"
	"github.com/ONSdigital/dp-dd-csv-transformer/config"
	"github.com/ONSdigital/go-ns/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"time"
)

const CONTENT_ENCODING_GZIP = "gzip"

// AWSClient interface defining the AWS client.
type AWSService interface {
	GetCSV(requestID string, s3url S3URL) (io.Reader, error)
	SaveFile(requestID string, reader io.Reader, s3url S3URL) error
}

// Client AWS client implementation.
type Service struct{}

// NewClient create new AWSClient.
func NewService() AWSService {
	return &Service{}
}

func (cli *Service) SaveFile(requestID string, reader io.Reader, s3url S3URL) error {

	startTime := time.Now()
	defer func() {
		endTime := time.Now()
		log.DebugC(requestID, fmt.Sprintf("SaveFile, duration_ns: %d", endTime.Sub(startTime).Nanoseconds()), log.Data{})
	}()

	uploader := s3manager.NewUploader(session.New(&aws.Config{Region: aws.String(config.AWSRegion)}))

	var contentEncoding *string = nil
	var uploadInput io.Reader = reader
	if config.UseGzipCompression {
		pipeReader, pipeWriter := io.Pipe()
		go func() {
			log.DebugC(requestID, "Compressing output on-the-fly", nil)
			bytesWritten, err := io.Copy(gzip.NewWriter(pipeWriter), reader)
			if err != nil {
				log.ErrorC(requestID, err, nil)
				pipeWriter.CloseWithError(err)
			} else {
				log.DebugC(requestID, fmt.Sprintf("Copied %d bytes via gzip", bytesWritten), nil)
				pipeWriter.Close()
			}
		}()
		uploadInput = pipeReader
		// The Go AWS SDK takes a *string for headers, and Go won't let you take a pointer to a string literal/constant
		contentEncoding = new(string)
		*contentEncoding = CONTENT_ENCODING_GZIP
	}

	result, err := uploader.Upload(&s3manager.UploadInput{
		Body:            uploadInput,
		Bucket:          aws.String(s3url.GetBucketName()),
		Key:             aws.String(s3url.GetFilePath()),
		ContentEncoding: contentEncoding,
	})

	if err != nil {
		log.Error(err, log.Data{"message": "Failed to upload"})
		return err
	}

	log.Debug("Upload successful", log.Data{
		"uploadLocation": result.Location,
	})

	return nil
}

// GetFile get the requested file from AWS.
func (cli *Service) GetCSV(requestID string, s3url S3URL) (io.Reader, error) {
	startTime := time.Now()
	defer func() {
		endTime := time.Now()
		log.DebugC(requestID, fmt.Sprintf("GetCSV, duration_ns: %d", endTime.Sub(startTime).Nanoseconds()), log.Data{})
	}()

	session, err := session.NewSession(&aws.Config{
		Region: aws.String(config.AWSRegion),
	})

	if err != nil {
		log.ErrorC(requestID, err, nil)
		return nil, err
	}

	s3Service := s3.New(session)
	request := &s3.GetObjectInput{}
	request.SetBucket(s3url.GetBucketName())
	request.SetKey(s3url.GetFilePath())

	log.Debug("Requesting .csv file from AWS S3 bucket", log.Data{
		"S3BucketName": request.Bucket,
		"key":          request.Key,
	})
	result, err := s3Service.GetObject(request)

	if err != nil {
		log.ErrorC(requestID, err, log.Data{"request": request})
		return nil, err
	}

	b, err := ioutil.ReadAll(result.Body)
	defer result.Body.Close()

	if err != nil {
		log.ErrorC(requestID, err, log.Data{"request": request})
		return nil, err
	}

	return bytes.NewReader(b), nil
}
