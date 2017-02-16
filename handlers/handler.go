package handlers

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"fmt"

	"github.com/ONSdigital/dp-dd-csv-transformer/aws"
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
var awsService = aws.NewService()
var csvTransformer transformer.CSVTransformer = transformer.NewTransformer()
var readFilterRequestBody requestBodyReader = ioutil.ReadAll

// Responses
var transformRespReadReqBodyErr = TransformResponse{"Error when attempting to read request body."}
var transformRespUnmarshalBody = TransformResponse{"Error when attempting to unmarshal request body."}
var transformRespUnsupportedFileType = TransformResponse{"Unspported file type. Please specify a filePath for a .csv file."}
var transformResponseSuccess = TransformResponse{"Your request is being processed."}

// Handle CSV transformer handler. Get the requested file from AWS S3, transform it to a temporary file, then upload the temporary file.
func Handle(w http.ResponseWriter, req *http.Request) {
	bytes, err := readFilterRequestBody(req.Body)
	defer req.Body.Close()

	if err != nil {
		log.ErrorR(req, err, nil)
		WriteResponse(w, transformRespReadReqBodyErr, http.StatusBadRequest)
		return
	}

	var transformRequest event.TransformRequest
	if err := json.Unmarshal(bytes, &transformRequest); err != nil {
		log.ErrorR(req, err, nil)
		WriteResponse(w, transformRespUnmarshalBody, http.StatusBadRequest)
		return
	}
	if len(transformRequest.RequestID) == 0 {
		transformRequest.RequestID = log.Context(req)
	}

	response := HandleRequest(transformRequest)
	status := http.StatusBadRequest
	if response == transformResponseSuccess {
		status = http.StatusOK
	}
	WriteResponse(w, response, status)
}

// Performs the transforming as specified in the TransformRequest, returning a TransformResponse
func HandleRequest(transformRequest event.TransformRequest) (resp TransformResponse) {

	if fileType := filepath.Ext(transformRequest.InputURL.GetFilePath()); fileType != csvFileExt {
		log.ErrorC(transformRequest.RequestID, unsupportedFileTypeErr, log.Data{"expected": csvFileExt, "actual": fileType})
		return transformRespUnsupportedFileType
	}

	awsReader, err := awsService.GetCSV(transformRequest.InputURL)
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

	err = csvTransformer.Transform(awsReader, bufio.NewWriter(outputFile), hierarchy.NewHierarchyClient(), transformRequest.RequestID)
	if err != nil {
		log.ErrorC(transformRequest.RequestID, err, log.Data{"message": "Failed to transform"})
		return TransformResponse{err.Error()}
	}

	tmpFile, err := os.Open(outputFileLocation)
	if err != nil {
		log.ErrorC(transformRequest.RequestID, err, log.Data{"message": "Failed to get tmp output file for s3 uploading!", "outputFileLocation": outputFileLocation})
		return TransformResponse{err.Error()}
	}

	err = awsService.SaveFile(bufio.NewReader(tmpFile), transformRequest.OutputURL)
	if err != nil {
		log.ErrorC(transformRequest.RequestID, err, log.Data{"message": "Failed to save output file to aws", "OutputURL": transformRequest.OutputURL})
		return TransformResponse{err.Error()}
	}

	os.Remove(outputFileLocation)

	return transformResponseSuccess
}

func setReader(reader requestBodyReader) {
	readFilterRequestBody = reader
}

func setCSVTransformer(t transformer.CSVTransformer) {
	csvTransformer = t
}

func setAWSClient(c aws.AWSService) {
	awsService = c
}
