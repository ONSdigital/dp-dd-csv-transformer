package event

import (
	"fmt"

	"github.com/ONSdigital/dp-dd-csv-transformer/aws"
	"github.com/ONSdigital/go-ns/log"
)

type TransformRequest struct {
	InputURL  aws.S3URL `json:"inputUrl"`
	OutputURL aws.S3URL `json:"outputUrl"`
	RequestID string    `json:"requestId"`
}

var NilRequest = TransformRequest{}

func NewTransformRequest(inputUrl string, outputUrl string, requestId string) (TransformRequest, error) {
	var input, output aws.S3URL
	var err error
	if input, err = aws.NewS3URL(inputUrl); err != nil {
		log.Error(err, log.Data{"Details": "Invalid inputUrl"})
		return NilRequest, err
	}
	if output, err = aws.NewS3URL(outputUrl); err != nil {
		log.Error(err, log.Data{"Details": "Invalid outputUrl"})
		return NilRequest, err
	}
	return TransformRequest{InputURL: input, OutputURL: output, RequestID: requestId}, nil
}

func (f *TransformRequest) String() string {
	return fmt.Sprintf(`TransformRequest{RequestID: "%v", InputURL:"%s", OutputURL: "%s"}`, f.RequestID, f.InputURL.String(), f.OutputURL.String())
}
