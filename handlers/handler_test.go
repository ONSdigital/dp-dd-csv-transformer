package handlers

import (
	"bytes"
	"errors"
	"io"
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
	getCsvErr      error
	saveFileErr    error
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
	return bytes.NewReader(mock.fileBytes), mock.getCsvErr
}

func (mock *MockAWSCli) SaveFile(reader io.Reader, filePath aws.S3URL) error {
	mutex.Lock()
	defer mutex.Unlock()

	mock.savedFiles[filePath.String()]++
	return mock.saveFileErr
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
		mockAWSCli, mockCSVTransformer := setMocks()

		inputFile := "s3://bucket/test.csv"
		outputFile := "s3://bucket/test.out"
		transformRequest := createTransformRequest(inputFile, outputFile)

		response := HandleRequest(transformRequest)

		So(response, ShouldResemble, transformResponseSuccess)
		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(inputFile))
		So(1, ShouldEqual, mockAWSCli.countOfSaveInvocations(outputFile))
		So(1, ShouldEqual, mockCSVTransformer.invocations)
	})

	Convey("Should return appropriate error if the awsClient returns an error on read.", t, func() {
		uri := "s3://bucket/target.csv"
		awsErrMsg := "THIS IS AN AWS ERROR"

		mockAWSCli, mockCSVTransformer := setMocks()
		mockAWSCli.getCsvErr = errors.New(awsErrMsg)

		response := HandleRequest(createTransformRequest(uri, uri))

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(uri))
		So(0, ShouldEqual, mockCSVTransformer.invocations)
		So(response, ShouldResemble, TransformResponse{awsErrMsg})
	})

	Convey("Should return appropriate error if the awsClient returns an error on save.", t, func() {
		uri := "s3://bucket/target.csv"
		awsErrMsg := "THIS IS AN AWS ERROR"

		mockAWSCli, mockCSVTransformer := setMocks()
		mockAWSCli.saveFileErr = errors.New(awsErrMsg)

		response := HandleRequest(createTransformRequest(uri, uri))

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(uri))
		So(1, ShouldEqual, mockCSVTransformer.invocations)
		So(response, ShouldResemble, TransformResponse{awsErrMsg})
	})

	Convey("Should return success response for happy path scenario", t, func() {
		uri := "s3://bucket/target.csv"

		mockAWSCli, mockCSVTransformer := setMocks()

		response := HandleRequest(createTransformRequest(uri, uri))

		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(uri))
		So(1, ShouldEqual, mockCSVTransformer.invocations)
		So(response, ShouldResemble, transformResponseSuccess)
	})

	Convey("Should return appropriate error for unsupported file types", t, func() {
		uri := "s3://bucket/unsupported.txt"

		mockAWSCli, mockCSVTransformer := setMocks()

		response := HandleRequest(createTransformRequest(uri, uri))

		So(0, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(0, ShouldEqual, mockCSVTransformer.invocations)
		So(response, ShouldResemble, transformRespUnsupportedFileType)
	})

	Convey("Should handle a panic.", t, func() {
		mockAWSCli, mockCSVTransformer := setMocks()

		inputFile := "s3://bucket/test.csv"
		outputFile := "s3://bucket/test.out"

		mockCSVTransformer.shouldPanic = true

		response := HandleRequest(createTransformRequest(inputFile, outputFile))

		So(response, ShouldResemble, TransformResponse{PANIC_MESSAGE})
		So(1, ShouldEqual, mockAWSCli.getTotalInvocations())
		So(1, ShouldEqual, mockAWSCli.getInvocationsByURI(inputFile))
		So(0, ShouldEqual, mockAWSCli.countOfSaveInvocations(outputFile))
		So(1, ShouldEqual, mockCSVTransformer.invocations)
	})

}

func createTransformRequest(input string, output string) event.TransformRequest {
	req, err := event.NewTransformRequest(input, output, "foo")
	if err != nil {
		panic(err)
	}
	return req
}

func setMocks() (*MockAWSCli, *MockCSVTransformer) {
	mockAWSCli := newMockAwsClient()
	mockCSVTransformer := newMockCSVTransformer()
	return mockAWSCli, mockCSVTransformer
}
