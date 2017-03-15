package event

import (
	"fmt"

	"github.com/ONSdigital/dp-dd-csv-transformer/ons_aws"
	"github.com/ONSdigital/go-ns/log"
)

type TransformRequest struct {
	InputURL  ons_aws.S3URL `json:"inputUrl"`
	OutputURL ons_aws.S3URL `json:"outputUrl"`
	RequestID string    `json:"requestId"`
}

var NilRequest = TransformRequest{}

func NewTransformRequest(inputUrl string, outputUrl string, requestId string) (TransformRequest, error) {
	var input, output ons_aws.S3URL
	var err error
	if input, err = ons_aws.NewS3URL(inputUrl); err != nil {
		log.Error(err, log.Data{"Details": "Invalid inputUrl"})
		return NilRequest, err
	}
	if output, err = ons_aws.NewS3URL(outputUrl); err != nil {
		log.Error(err, log.Data{"Details": "Invalid outputUrl"})
		return NilRequest, err
	}
	return TransformRequest{InputURL: input, OutputURL: output, RequestID: requestId}, nil
}

func (f *TransformRequest) String() string {
	return fmt.Sprintf(`TransformRequest{RequestID: "%v", InputURL:"%s", OutputURL: "%s"}`, f.RequestID, f.InputURL.String(), f.OutputURL.String())
}
