package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-dd-csv-transformer/aws"
	"github.com/ONSdigital/dp-dd-csv-transformer/hierarchy"
	"github.com/ONSdigital/dp-dd-csv-transformer/message/event"
	. "github.com/smartystreets/goconvey/convey"
)

var mutex = &sync.Mutex{}

const PANIC_MESSAGE = "Panic!!!"

// MockAWSCli mock implementation of aws.Client
type MockAWSCli struct {
	requestedFiles map[string]int
	savedFiles     map[string]int
	fileBytes      []byte
	err            error
}

func newMockAwsClient() *MockAWSCli {
	mock := &MockAWSCli{requestedFiles: make(map[string]int), savedFiles: make(map[string]int)}
	setAWSClient(mock)
	return mock
}

func (mock *MockAWSCli) GetCSV(fileURI aws.S3URL) (io.Reader, error) {
	mutex.Lock()
	defer mutex.Unlock()

	mock.requestedFiles[fileURI.String()]++
	return bytes.NewReader(mock.fileBytes), mock.err
}

func (mock *MockAWSCli) SaveFile(reader io.Reader, filePath aws.S3URL) error {
	mutex.Lock()
	defer mutex.Unlock()

	mock.savedFiles[filePath.String()]++
	return nil
}

func (mock *MockAWSCli) getTotalInvocations() int {
	var count = 0
	for _, val := range mock.requestedFiles {
		count += val
	}
	return count
}

func (mock *MockAWSCli) getInvocationsByURI(uri string) int {
	return mock.requestedFiles[uri]
}

func (mock *MockAWSCli) countOfSaveInvocations(uri string) int {
	return mock.savedFiles[uri]
}

// MockCSVTransformer
type MockCSVTransformer struct {
	invocations int
	shouldPanic bool
	err         error
}

func newMockCSVTransformer() *MockCSVTransformer {
	mock := &MockCSVTransformer{invocations: 0}
	setCSVTransformer(mock)
	return mock
}

// Transform mock implementation of the Transform function.
func (t *MockCSVTransformer) Transform(r io.Reader, w io.Writer, hc hierarchy.HierarchyClient, requestId string) error {
	mutex.Lock()
	defer mutex.Unlock()
	t.invocations++
	if t.shouldPanic {
		panic(PANIC_MESSAGE)
	}
	return t.err
}

func TestHandler(t *testing.T) {

	Convey("Should invoke AWSClient once with the request file path.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		inputFile := "s3://bucket/test.csv"
		outputFile := "s3://bucket/test.out"
		transformerRequest := createFilterRequest(inputFile, outputFile)

		Handle(recorder, createRequest(transformerRequest))

		splitterResponse, status := extractResponseBody(recorder)

		So(splitterResponse, ShouldResemble, transformResponseSuccess)
		So(status, ShouldResemble, http.StatusOK)
		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(inputFile))
		So(1, ShouldEqual, mockAWSCli.countOfSaveInvocations(outputFile))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
	})

	Convey("Should return appropriate error if cannot unmarshall the request body into a TransformRequest.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest("This is not a TransformRequest"))

		splitterResponse, status := extractResponseBody(recorder)

		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, transformRespUnmarshalBody)
		So(status, ShouldResemble, http.StatusBadRequest)
	})

	Convey("Should return appropriate error if the awsClient returns an error.", t, func() {
		recorder := httptest.NewRecorder()
		uri := "s3://bucket/target.csv"
		awsErrMsg := "THIS IS AN AWS ERROR"

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)
		mockAWSCli.err = errors.New(awsErrMsg)

		Handle(recorder, createRequest(createFilterRequest(uri, uri)))
		splitterResponse, status := extractResponseBody(recorder)

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(uri))
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, TransformResponse{awsErrMsg})
		So(status, ShouldResemble, http.StatusBadRequest)
	})

	Convey("Should return success response for happy path scenario", t, func() {
		recorder := httptest.NewRecorder()
		uri := "s3://bucket/target.csv"

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest(createFilterRequest(uri, uri)))
		splitterResponse, statusCode := extractResponseBody(recorder)

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(uri))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, transformResponseSuccess)
		So(statusCode, ShouldResemble, http.StatusOK)
	})

	Convey("Should return appropriate error for unsupported file types", t, func() {
		recorder := httptest.NewRecorder()
		uri := "s3://bucket/unsupported.txt"

		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		Handle(recorder, createRequest(createFilterRequest(uri, uri)))

		splitterResponse, status := extractResponseBody(recorder)
		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVProcessor.invocations)
		So(splitterResponse, ShouldResemble, transformRespUnsupportedFileType)
		So(status, ShouldResemble, http.StatusBadRequest)
	})

	Convey("Should handle a panic.", t, func() {
		recorder := httptest.NewRecorder()
		mockAWSCli, mockCSVProcessor := setMocks(ioutil.ReadAll)

		inputFile := "s3://bucket/test.csv"
		outputFile := "s3://bucket/test.out"
		transformerRequest := createFilterRequest(inputFile, outputFile)

		mockCSVProcessor.shouldPanic = true

		Handle(recorder, createRequest(transformerRequest))

		splitterResponse, status := extractResponseBody(recorder)

		So(splitterResponse, ShouldResemble, TransformResponse{PANIC_MESSAGE})
		So(status, ShouldResemble, http.StatusBadRequest)
		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(inputFile))
		So(0, ShouldEqual, mockAWSCli.countOfSaveInvocations(outputFile))
		So(1, ShouldEqual, mockCSVProcessor.invocations)
	})

}

func extractResponseBody(rec *httptest.ResponseRecorder) (TransformResponse, int) {
	var actual = &TransformResponse{}
	json.Unmarshal([]byte(rec.Body.String()), actual)
	return *actual, rec.Code
}

func createRequest(body interface{}) *http.Request {
	b, _ := json.Marshal(body)
	request, _ := http.NewRequest("POST", "/transformer", bytes.NewBuffer(b))
	return request
}

func createFilterRequest(input string, output string) event.TransformRequest {
	req, err := event.NewFilterRequest(input, output, "foo")
	if err != nil {
		panic(err)
	}
	return req
}

func setMocks(reader requestBodyReader) (*MockAWSCli, *MockCSVTransformer) {
	mockAWSCli := newMockAwsClient()
	mockCSVProcessor := newMockCSVTransformer()
	setReader(reader)
	return mockAWSCli, mockCSVProcessor
}
