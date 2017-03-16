package handlers

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"fmt"

	"github.com/ONSdigital/dp-dd-csv-transformer/ons_aws"
	"github.com/ONSdigital/dp-dd-csv-transformer/hierarchy"
	"github.com/ONSdigital/dp-dd-csv-transformer/message/event"
	"github.com/ONSdigital/dp-dd-csv-transformer/transformer"
	"github.com/ONSdigital/go-ns/log"
)

const csvFileExt = ".csv"

type requestBodyReader func(r io.Reader) ([]byte, error)

// TransformResponse struct defines the response for the /transformer API.
type TransformResponse struct {
	Message string `json:"message,omitempty"`
}

// TransformFunc defines a function (implemented by HandleRequest) that performs the transformering requested in a TransformRequest
type TransformFunc func(event.TransformRequest) TransformResponse

var unsupportedFileTypeErr = errors.New("Unspported file type.")
var awsClientErr = errors.New("Error while attempting get to get from from AWS s3 bucket.")
var awsService = ons_aws.NewService()
var csvTransformer transformer.CSVTransformer = transformer.NewTransformer()

// Responses
var transformRespUnsupportedFileType = TransformResponse{"Unspported file type. Please specify a filePath for a .csv file."}
var transformResponseSuccess = TransformResponse{"Your request is being processed."}

// Performs the transforming as specified in the TransformRequest, returning a TransformResponse
func HandleRequest(transformRequest event.TransformRequest) (resp TransformResponse) {

	startTime := time.Now()
	defer func() {
		endTime := time.Now()
		log.DebugC(transformRequest.RequestID, fmt.Sprintf("Processed TransformRequest, duration_ns: %d", endTime.Sub(startTime).Nanoseconds()), log.Data{"start": startTime, "end": endTime})
	}()

	if fileType := filepath.Ext(transformRequest.InputURL.GetFilePath()); fileType != csvFileExt {
		log.ErrorC(transformRequest.RequestID, unsupportedFileTypeErr, log.Data{"expected": csvFileExt, "actual": fileType})
		return transformRespUnsupportedFileType
	}

	awsReadCloser, err := awsService.GetCSV(transformRequest.RequestID, transformRequest.InputURL)
	defer awsReadCloser.Close()
	if err != nil {
		log.ErrorC(transformRequest.RequestID, awsClientErr, log.Data{"details": err.Error()})
		return TransformResponse{err.Error()}
	}

	outputFileLocation := "/var/tmp/csv_transformer_" + transformRequest.RequestID + "_" + strconv.Itoa(time.Now().Nanosecond()) + ".csv"
	outputFile, err := os.Create(outputFileLocation)
	if err != nil {
		log.ErrorC(transformRequest.RequestID, err, log.Data{"message": "Error creating temp output file  " + outputFileLocation})
		return TransformResponse{err.Error()}
	}

	defer func() {
		if r := recover(); r != nil {
			log.ErrorC(transformRequest.RequestID, errors.New(fmt.Sprintf("%v", r)), log.Data{"inputUrl": transformRequest.InputURL, "outputUrl": transformRequest.OutputURL})
			resp = TransformResponse{fmt.Sprintf("%s", r)}
		}
		os.Remove(outputFileLocation)
	}()

	err = csvTransformer.Transform(awsReadCloser, bufio.NewWriter(outputFile), hierarchy.NewHierarchyClient(), transformRequest.RequestID)
	if err != nil {
		log.ErrorC(transformRequest.RequestID, err, log.Data{"message": "Failed to transform"})
		return TransformResponse{err.Error()}
	}

	tmpFile, err := os.Open(outputFileLocation)
	if err != nil {
		log.ErrorC(transformRequest.RequestID, err, log.Data{"message": "Failed to get tmp output file for s3 uploading!", "outputFileLocation": outputFileLocation})
		return TransformResponse{err.Error()}
	}

	err = awsService.SaveFile(transformRequest.RequestID, bufio.NewReader(tmpFile), transformRequest.OutputURL)
	if err != nil {
		log.ErrorC(transformRequest.RequestID, err, log.Data{"message": "Failed to save output file to ons_aws", "OutputURL": transformRequest.OutputURL})
		return TransformResponse{err.Error()}
	}

	os.Remove(outputFileLocation)

	return transformResponseSuccess
}

func setCSVTransformer(t transformer.CSVTransformer) {
	csvTransformer = t
}

func setAWSClient(c ons_aws.AWSService) {
	awsService = c
}
