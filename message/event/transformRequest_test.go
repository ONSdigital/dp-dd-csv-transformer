package event

import (
	"encoding/json"
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const inputBucket = "input-bucket-name"
const inputFile = "input_folder/transformer.csv"
const outputBucket = "output-bucket-name"
const outputFile = "output_folder/transformer.csv"

var inputUrl = fmt.Sprintf("s3://%s/%s", inputBucket, inputFile)
var outputUrl = fmt.Sprintf("s3://%s/%s", outputBucket, outputFile)

func TestNewFilterRequest(t *testing.T) {

	Convey("Given a call to NewFilterRequest", t, func() {

		var transformerRequest, _ = NewFilterRequest(inputUrl, outputUrl, "foo")

		Convey("Then the inputUrl should have correct bucket and filename", func() {
			So(transformerRequest.InputURL.GetBucketName(), ShouldEqual, inputBucket)
			So(transformerRequest.InputURL.GetFilePath(), ShouldEqual, inputFile)
		})
		Convey("And the outputUrl should have correct bucket and filename", func() {
			So(transformerRequest.OutputURL.GetBucketName(), ShouldEqual, outputBucket)
			So(transformerRequest.OutputURL.GetFilePath(), ShouldEqual, outputFile)
		})
		Convey("And the request id should be coorect", func() {
			So(transformerRequest.RequestID, ShouldEqual, "foo")
		})

	})
}

func TestNewValidatesInputURL(t *testing.T) {
	Convey("Given a call to NewFilterRequest with an invalid input", t, func() {
		var transformerRequest, err = NewFilterRequest("invalid url", outputUrl, "foo")
		Convey("Then returned request is nil and err is not", func() {
			So(err, ShouldNotEqual, nil)
			So(transformerRequest, ShouldResemble, NilRequest)
		})
	})
}

func TestNewValidatesOutputURL(t *testing.T) {
	Convey("Given a call to NewFilterRequest with an invalid input", t, func() {
		var transformerRequest, err = NewFilterRequest(inputUrl, "invalid url", "foo")
		Convey("Then returned request is nil and err is not", func() {
			So(err, ShouldNotEqual, nil)
			So(transformerRequest, ShouldResemble, NilRequest)
		})
	})
}

func TestFilterRequestCanBeMarshaledAndUnmarshaled(t *testing.T) {
	var transformerRequest, _ = NewFilterRequest(inputUrl, outputUrl, "foo")

	Convey("Given a transformerRequest marshaled to json", t, func() {
		var marshaled, _ = json.Marshal(transformerRequest)
		Convey("Then the unmarshaled object should resemble the original", func() {
			var unmarshaled TransformRequest
			err := json.Unmarshal(marshaled, &unmarshaled)
			So(err, ShouldEqual, nil)
			So(unmarshaled, ShouldResemble, transformerRequest)
		})
	})
}
func TestString(t *testing.T) {
	var transformerRequest, _ = NewFilterRequest(inputUrl, outputUrl, "foo")

	Convey("Given a transformerRequest", t, func() {
		Convey("Then the String() should resemble the original", func() {
			So(transformerRequest.String(), ShouldEqual, `TransformRequest{RequestID: "foo", InputURL:"s3://input-bucket-name/input_folder/transformer.csv", OutputURL: "s3://output-bucket-name/output_folder/transformer.csv"}`)
		})
	})
}
